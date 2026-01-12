package rdb

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"smartdatastream/server/cluster"
	"sync"
	"time"
)

// ExecuteRequest represents the request body for /v1/rdb/execute
type ExecuteRequest struct {
	SQL       string       `json:"sql"`
	Params    []ParamValue `json:"params,omitempty"`
	TxID      *string      `json:"txId,omitempty"`
	TimeoutMs *int         `json:"timeoutMs,omitempty"`
	LimitRows *int         `json:"limitRows,omitempty"`
}

// ParamValue represents a parameter value
type ParamValue struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value,omitempty"`
}

// ExecuteResponse represents the response for /v1/rdb/execute
type QueryResponse struct {
	Meta ResultMeta               `json:"meta"`
	Rows []map[string]interface{} `json:"rows"`
}

type ExecuteResponse struct {
	EffectedRows    int64 `json:"effectedRows"`
	ExecutionTimeMs int64 `json:"executionTimeMs"`
}

// ResultMeta contains metadata about the query result
type ResultMeta struct {
	TotalCount      int          `json:"totalCount"`
	Columns         []ColumnMeta `json:"columns,omitempty"`
	ExecutionTimeMs int64        `json:"executionTimeMs"`
}

// ColumnMeta contains metadata about a column
type ColumnMeta struct {
	Name     string `json:"name"`
	DBType   string `json:"dbType"`
	Nullable bool   `json:"nullable"`
}

// ExecuteHandler handles /v1/rdb/execute requests
type ExecHandler struct {
	selfNode  *cluster.NodeInfo
	txManager *TxManager
	mu        sync.RWMutex
}

// NewExecuteHandler creates a new ExecuteHandler
func NewExecHandler(nodeInfo *cluster.NodeInfo, txManager *TxManager) *ExecHandler {
	return &ExecHandler{
		selfNode:  nodeInfo,
		txManager: txManager,
		mu:        sync.RWMutex{},
	}
}

func (exec *ExecHandler) Query(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	dsIDX, req, args, ctx, cancel, err := exec.parseRequest(r)
	defer cancel()
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", fmt.Sprintf("Failed to parse request: %v", err))
		return
	}

	// Update health info: increment RunningSql
	exec.statisticsRequest(dsIDX, 1)
	defer exec.statisticsRequest(dsIDX, -1)

	var rows *sql.Rows
	var queryErr error

	if req.TxID != nil {
		// Get transaction entry
		entry, err := exec.txManager.Get(*req.TxID)
		if err != nil {
			if err == ErrTxNotFound || err == ErrTxExpired {
				writeError(w, http.StatusConflict, "TX_NOT_FOUND", fmt.Sprintf("Transaction not found or expired: %v", err))
				return
			}
			writeError(w, http.StatusServiceUnavailable, "TX_ERROR", fmt.Sprintf("Failed to get transaction: %v", err))
			return
		}

		// Execute query in transaction
		rows, queryErr = entry.Tx.QueryContext(ctx, req.SQL, args...)
		if queryErr != nil {
			if ctx.Err() == context.DeadlineExceeded {
				exec.statisticsResult(dsIDX, time.Since(startTime).Milliseconds(), true, true)
				writeError(w, http.StatusRequestTimeout, "TIMEOUT", "Request timeout")
				return
			}
			exec.statisticsResult(dsIDX, time.Since(startTime).Milliseconds(), true, false)
			writeError(w, http.StatusServiceUnavailable, "QUERY_ERROR", fmt.Sprintf("Query failed: %v", queryErr))
			return
		}
		defer rows.Close()
	} else {
		// Execute query without transaction
		rows, queryErr = exec.txManager.QueryContext(ctx, dsIDX, req.SQL, args...)
		if queryErr != nil {
			if ctx.Err() == context.DeadlineExceeded {
				exec.statisticsResult(dsIDX, time.Since(startTime).Milliseconds(), true, true)
				writeError(w, http.StatusRequestTimeout, "TIMEOUT", "Request timeout")
				return
			}
			exec.statisticsResult(dsIDX, time.Since(startTime).Milliseconds(), true, false)
			writeError(w, http.StatusServiceUnavailable, "QUERY_ERROR", fmt.Sprintf("Query failed: %v", queryErr))
			return
		}
		defer rows.Close()
	}

	// Get column information
	columns, err := rows.Columns()
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "QUERY_ERROR", fmt.Sprintf("Failed to get columns: %v", err))
		return
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "QUERY_ERROR", fmt.Sprintf("Failed to get column types: %v", err))
		return
	}

	// Build column metadata
	columnMeta := make([]ColumnMeta, len(columns))
	for i, colType := range columnTypes {
		nullable, _ := colType.Nullable()
		columnMeta[i] = ColumnMeta{
			Name:     columns[i],
			DBType:   colType.DatabaseTypeName(),
			Nullable: nullable,
		}
	}

	// Read rows
	var resultRows []map[string]interface{}
	limit := 1000
	if req.LimitRows != nil && *req.LimitRows > 0 {
		limit = *req.LimitRows
	}

	rowCount := 0
	for rows.Next() {
		if rowCount >= limit {
			break
		}

		// Create slice for scanning
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			writeError(w, http.StatusServiceUnavailable, "QUERY_ERROR", fmt.Sprintf("Failed to scan row: %v", err))
			return
		}

		// Convert row to map
		rowMap := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			rowMap[col] = convertValue(val)
		}

		resultRows = append(resultRows, rowMap)
		rowCount++
	}

	if err := rows.Err(); err != nil {
		writeError(w, http.StatusServiceUnavailable, "QUERY_ERROR", fmt.Sprintf("Row iteration error: %v", err))
		return
	}

	// Count remaining rows if needed
	if rowCount < limit {
		// We've read all rows, rowCount is the total
	} else {
		// We hit the limit, count remaining (this is approximate)
		for rows.Next() {
			rowCount++
		}
	}

	// Calculate execution time
	executionTimeMs := time.Since(startTime).Milliseconds()

	// Update health info: record successful query
	exec.statisticsResult(dsIDX, executionTimeMs, false, false)

	// Write response
	response := QueryResponse{
		Meta: ResultMeta{
			TotalCount:      rowCount,
			Columns:         columnMeta,
			ExecutionTimeMs: executionTimeMs,
		},
		Rows: resultRows,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (exec *ExecHandler) Execute(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	dsIDX, req, args, ctx, cancel, err := exec.parseRequest(r)
	defer cancel()
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", fmt.Sprintf("Failed to parse request: %v", err))
		return
	}

	// Update health info: increment RunningSql
	exec.statisticsRequest(dsIDX, 1)
	defer exec.statisticsRequest(dsIDX, -1)

	var result sql.Result
	var execErr error

	if req.TxID != nil {
		// Get transaction entry
		entry, err := exec.txManager.Get(*req.TxID)
		if err != nil {
			if err == ErrTxNotFound || err == ErrTxExpired {
				writeError(w, http.StatusConflict, "TX_NOT_FOUND", fmt.Sprintf("Transaction not found or expired: %v", err))
				return
			}
			writeError(w, http.StatusServiceUnavailable, "TX_ERROR", fmt.Sprintf("Failed to get transaction: %v", err))
			return
		}

		// Execute in transaction
		result, execErr = entry.Tx.ExecContext(ctx, req.SQL, args...)
		if execErr != nil {
			if ctx.Err() == context.DeadlineExceeded {
				exec.statisticsResult(dsIDX, time.Since(startTime).Milliseconds(), true, true)
				writeError(w, http.StatusRequestTimeout, "TIMEOUT", "Request timeout")
				return
			}
			exec.statisticsResult(dsIDX, time.Since(startTime).Milliseconds(), true, false)
			writeError(w, http.StatusServiceUnavailable, "EXEC_ERROR", fmt.Sprintf("Exec failed: %v", execErr))
			return
		}
	} else {
		// Get database
		result, execErr = exec.txManager.ExecContext(ctx, dsIDX, req.SQL, args...)
		if execErr != nil {
			if ctx.Err() == context.DeadlineExceeded {
				exec.statisticsResult(dsIDX, time.Since(startTime).Milliseconds(), true, true)
				writeError(w, http.StatusRequestTimeout, "TIMEOUT", "Request timeout")
				return
			}
			exec.statisticsResult(dsIDX, time.Since(startTime).Milliseconds(), true, false)
			writeError(w, http.StatusServiceUnavailable, "EXEC_ERROR", fmt.Sprintf("Exec failed: %v", execErr))
			return
		}
	}

	// Get affected rows
	affectedRows, err := result.RowsAffected()
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "EXEC_ERROR", fmt.Sprintf("Failed to get affected rows: %v", err))
		return
	}

	// Calculate execution time
	executionTimeMs := time.Since(startTime).Milliseconds()

	// Update health info: record successful execution
	exec.statisticsResult(dsIDX, executionTimeMs, false, false)

	// Write response
	response := ExecuteResponse{
		EffectedRows:    affectedRows,
		ExecutionTimeMs: executionTimeMs,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// updateRunningSql updates the RunningSql count for a datasource
func (exec *ExecHandler) statisticsRequest(datasourceIdx int, delta int) {
	// Find or create datasource info
	dsInfo := &exec.selfNode.HealthInfo.Datasources[datasourceIdx]
	dsInfo.Mu.Lock()
	defer dsInfo.Mu.Unlock()
	dsInfo.RunningSql += delta
	if dsInfo.RunningSql < 0 {
		dsInfo.RunningSql = 0
	}
}

func (exec *ExecHandler) statisticsResult(datasourceIdx int, latencyMs int64, isError bool, isTimeout bool) {
	// Get data that requires TxManager locks BEFORE acquiring SqlHandler lock
	// This prevents potential deadlock if TxManager ever needs to call SqlHandler methods

	// Now acquire SqlHandler lock to update shared state
	exec.mu.Lock()
	defer exec.mu.Unlock()

	// Find or create datasource info
	dsInfo := &exec.selfNode.HealthInfo.Datasources[datasourceIdx]

	// Update latency
	dsInfo.LatencyMs = int(latencyMs)

	// Update error rate and timeout count (simplified: track last minute)
	// Note: For production, you'd want a proper time-windowed tracking mechanism
	if isError {
		if isTimeout {
			dsInfo.Timeouts1m++
		}
		// Simple error rate calculation (would need proper time-windowed tracking)
		// For now, we'll just increment a counter that gets reset periodically
	}
}

/*
POST /query?dbName=&txId=
Body:

	{
		"requestId": "query-1234567890",
		"sql": "SELECT * FROM users",
		"params": [
			{ "type": "text", "value": "John Doe" },
			{ "type": "int64", "value": 1234567890 },
		],
		"timeoutMs": 30000,
		"limitRows": 1000
	}
*/
func (exec *ExecHandler) parseRequest(r *http.Request) (int, ExecuteRequest, []interface{}, context.Context, context.CancelFunc, error) {
	var req ExecuteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return -1, ExecuteRequest{}, nil, nil, nil, fmt.Errorf("failed to parse request: %w", err)
	}

	// get datasourceId from context
	dsIDX, ok := r.Context().Value("dsIDX").(int)
	if !ok {
		return -1, ExecuteRequest{}, nil, nil, nil, fmt.Errorf("datasource INDEX is required")
	}
	if dsIDX < 0 || dsIDX >= len(exec.selfNode.HealthInfo.Datasources) {
		return -1, ExecuteRequest{}, nil, nil, nil, fmt.Errorf("invalid datasource INDEX: %d", dsIDX)
	}

	// Convert params
	args, err := convertParams(req.Params)
	if err != nil {
		return -1, ExecuteRequest{}, nil, nil, nil, fmt.Errorf("failed to convert params: %w", err)
	}

	// Set timeout
	timeout := 30 * time.Second
	if req.TimeoutMs != nil && *req.TimeoutMs > 0 {
		timeout = time.Duration(*req.TimeoutMs) * time.Millisecond
	}

	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	return dsIDX, req, args, ctx, cancel, nil
}

/************************************************************
 * Private methods
 ************************************************************/

func convertParams(params []ParamValue) ([]interface{}, error) {
	args := make([]interface{}, len(params))
	for i, p := range params {
		arg, err := convertParam(p)
		if err != nil {
			return nil, fmt.Errorf("param[%d]: %w", i, err)
		}
		args[i] = arg
	}
	return args, nil
}

func convertParam(p ParamValue) (interface{}, error) {
	switch p.Type {
	case "null":
		return nil, nil
	case "int32":
		if val, ok := p.Value.(float64); ok {
			return int32(val), nil
		}
		return nil, fmt.Errorf("invalid int32 value: %v", p.Value)
	case "int64":
		if val, ok := p.Value.(float64); ok {
			return int64(val), nil
		}
		return nil, fmt.Errorf("invalid int64 value: %v", p.Value)
	case "float64":
		if val, ok := p.Value.(float64); ok {
			return val, nil
		}
		return nil, fmt.Errorf("invalid float64 value: %v", p.Value)
	case "bool":
		if val, ok := p.Value.(bool); ok {
			return val, nil
		}
		return nil, fmt.Errorf("invalid bool value: %v", p.Value)
	case "text":
		if val, ok := p.Value.(string); ok {
			return val, nil
		}
		return nil, fmt.Errorf("invalid text value: %v", p.Value)
	case "bytes_base64":
		if val, ok := p.Value.(string); ok {
			decoded, err := base64.StdEncoding.DecodeString(val)
			if err != nil {
				return nil, fmt.Errorf("invalid base64: %w", err)
			}
			return decoded, nil
		}
		return nil, fmt.Errorf("invalid bytes_base64 value: %v", p.Value)
	case "timestamp_rfc3339":
		if val, ok := p.Value.(string); ok {
			t, err := time.Parse(time.RFC3339, val)
			if err != nil {
				return nil, fmt.Errorf("invalid RFC3339 timestamp: %w", err)
			}
			return t, nil
		}
		return nil, fmt.Errorf("invalid timestamp_rfc3339 value: %v", p.Value)
	default:
		return nil, fmt.Errorf("unknown param type: %s", p.Type)
	}
}

func convertValue(val interface{}) interface{} {
	if val == nil {
		return nil
	}

	switch v := val.(type) {
	case []byte:
		return base64.StdEncoding.EncodeToString(v)
	case time.Time:
		return v.Format(time.RFC3339)
	case int64, int32, int, float64, bool, string:
		return v
	default:
		// Try to convert to string
		return fmt.Sprintf("%v", v)
	}
}

func writeError(w http.ResponseWriter, statusCode int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	errorResp := map[string]interface{}{
		"error": map[string]interface{}{
			"code":    code,
			"message": message,
		},
	}
	json.NewEncoder(w).Encode(errorResp)
}
