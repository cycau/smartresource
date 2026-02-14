package rdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"sync"
	"time"

	. "smartdatastream/server/global"

	"github.com/paulbellamy/ratecounter"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/shopspring/decimal"
)

// ExecuteRequest represents the request body for /v1/rdb/execute
type ExecuteRequest struct {
	SQL        string       `json:"sql"`
	Params     []ParamValue `json:"params,omitempty"`
	TimeoutSec *int         `json:"timeoutSec,omitempty"`
	LimitRows  *int         `json:"limitRows,omitempty"`
}

// INT, LONG, DOUBLE, DECIMAL, BOOL
// DATE, DATETIME
// STRING, BINARY
type ValueType string

const (
	NULL     ValueType = "NULL"
	INT      ValueType = "INT"
	LONG     ValueType = "LONG"
	DOUBLE   ValueType = "DOUBLE"
	DECIMAL  ValueType = "DECIMAL"
	BOOL     ValueType = "BOOL"
	DATE     ValueType = "DATE"
	DATETIME ValueType = "DATETIME"
	STRING   ValueType = "STRING"
	BINARY   ValueType = "BINARY"
)

// ParamValue represents a parameter value
type ParamValue struct {
	Type  ValueType `json:"type"`
	Value any       `json:"value,omitempty"`
}

// ExecuteResponse represents the response for /v1/rdb/execute
type QueryResponse struct {
	Meta          []ColumnMeta `json:"meta,omitempty"`
	Rows          []any        `json:"rows"`
	TotalCount    int          `json:"totalCount"`
	ElapsedTimeMs int64        `json:"elapsedTimeMs"`
}

type ExecuteResponse struct {
	EffectedRows  int64 `json:"effectedRows"`
	ElapsedTimeMs int64 `json:"elapsedTimeMs"`
}

// ColumnMeta contains metadata about a column
type ColumnMeta struct {
	Name     string `json:"name"`
	DBType   string `json:"dbType"`
	Nullable bool   `json:"nullable"`
}

const STAT_WINDOW_INTERVAL = 5 * time.Minute

type StatsInfo struct {
	mu sync.Mutex

	statLatency  *prometheus.SummaryVec
	statTotal    *ratecounter.RateCounter
	statErrors   *ratecounter.RateCounter
	statTimeouts *ratecounter.RateCounter
}

// ExecuteHandler handles /v1/rdb/execute requests
type DmlHandler struct {
	dsManager  *DsManager
	statsInfos []*StatsInfo
}

/************************************************************
 * NewExecuteHandler creates a new ExecuteHandler
 ************************************************************/
func NewDmlHandler(dsManager *DsManager) *DmlHandler {
	statsInfos := make([]*StatsInfo, len(dsManager.dss))
	for i := range statsInfos {
		var statLatency = prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Objectives: map[float64]float64{0.95: 0.01},
			MaxAge:     STAT_WINDOW_INTERVAL,
		}, []string{"latency"})
		statsInfos[i] = &StatsInfo{
			statLatency:  statLatency,
			statTotal:    ratecounter.NewRateCounter(STAT_WINDOW_INTERVAL),
			statErrors:   ratecounter.NewRateCounter(STAT_WINDOW_INTERVAL),
			statTimeouts: ratecounter.NewRateCounter(STAT_WINDOW_INTERVAL),
		}
	}
	return &DmlHandler{
		dsManager:  dsManager,
		statsInfos: statsInfos,
	}
}

func (dh *DmlHandler) Query(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()

	dsIDX, req, parameters, txID, err := dh.parseRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", fmt.Sprintf("Failed to parse request: %v", err))
		return
	}
	log.Printf("### Executing DsIDX: %d, Query: %s, Params: %+v, TxID: %s", dsIDX, req.SQL, parameters, txID)

	var rows *sql.Rows
	var releaseResource ReleaseResourceFunc
	var queryErr error

	if txID == "" {
		// Execute query without transaction
		rows, releaseResource, queryErr = dh.dsManager.Query(r.Context(), req.TimeoutSec, dsIDX, req.SQL, parameters...)
		defer releaseResource()
		if queryErr != nil {
			if r.Context().Err() == context.DeadlineExceeded {
				dh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), false, true)
				writeError(w, http.StatusRequestTimeout, "TIMEOUT", "Request timeout")
				return
			}
			dh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), true, false)
			code := statusCodeForDbError(queryErr)
			if code == http.StatusBadGateway {
				log.Printf("[502] DB connection error (dsIDX=%d): %v", dsIDX, queryErr)
			}
			writeError(w, code, "QUERY_ERROR", fmt.Sprintf("Query failed: %v", queryErr))
			return
		}
		defer rows.Close()
	} else {
		// Execute query in transaction
		rows, releaseResource, queryErr = dh.dsManager.QueryTx(r.Context(), req.TimeoutSec, txID, req.SQL, parameters...)
		defer releaseResource()
		if queryErr != nil {
			if r.Context().Err() == context.DeadlineExceeded {
				dh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), false, true)
				writeError(w, http.StatusRequestTimeout, "TIMEOUT", "Request timeout")
				return
			}
			dh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), true, false)
			code := statusCodeForDbError(queryErr)
			if code == http.StatusBadGateway {
				log.Printf("[502] DB connection error (dsIDX=%d): %v", dsIDX, queryErr)
			}
			writeError(w, code, "QUERY_ERROR", fmt.Sprintf("Query failed: %v", queryErr))
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
	var resultRows []any
	limit := 1000
	if req.LimitRows != nil && *req.LimitRows > 0 {
		limit = *req.LimitRows
	}

	rowCount := 0
	for rows.Next() {
		if rowCount >= limit {
			rowCount++
			continue
		}

		// Create slice for scanning
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			writeError(w, http.StatusServiceUnavailable, "QUERY_ERROR", fmt.Sprintf("Failed to scan row: %v", err))
			return
		}

		for i, val := range values {
			if val == nil {
				continue
			}

			switch v := val.(type) {
			case []byte:
				values[i] = base64.StdEncoding.EncodeToString(v)
			case time.Time:
				values[i] = v.Format(time.RFC3339)
			case decimal.Decimal:
				values[i] = v.String()
			default:
				values[i] = v
			}
		}

		resultRows = append(resultRows, values)
		rowCount++
	}

	if err := rows.Err(); err != nil {
		writeError(w, http.StatusServiceUnavailable, "QUERY_ERROR", fmt.Sprintf("Row iteration error: %v", err))
		return
	}

	// Calculate execution time
	elapsedTimeMs := time.Since(startTime).Milliseconds()

	// Update health info: record successful query
	dh.statsSetResult(dsIDX, elapsedTimeMs, false, false)

	// Write response
	response := QueryResponse{
		Meta:          columnMeta,
		Rows:          resultRows,
		TotalCount:    rowCount,
		ElapsedTimeMs: elapsedTimeMs,
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (dh *DmlHandler) Execute(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()

	dsIDX, req, parameters, txID, err := dh.parseRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", fmt.Sprintf("Failed to parse request: %v", err))
		return
	}
	log.Printf("### Executing DsId: %d, Query: %s, Params: %+v, TxID: %s", dsIDX, req.SQL, parameters, txID)

	var result sql.Result
	var releaseResource ReleaseResourceFunc
	var execErr error

	if txID == "" {
		// Get database
		result, releaseResource, execErr = dh.dsManager.Execute(r.Context(), req.TimeoutSec, dsIDX, req.SQL, parameters...)
		defer releaseResource()
		if execErr != nil {
			if r.Context().Err() == context.DeadlineExceeded {
				dh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), false, true)
				writeError(w, http.StatusRequestTimeout, "TIMEOUT", "Request timeout")
				return
			}
			dh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), true, false)
			code := statusCodeForDbError(execErr)
			if code == http.StatusBadGateway {
				log.Printf("[502] DB connection error (dsIDX=%d): %v", dsIDX, execErr)
			}
			writeError(w, code, "EXEC_ERROR", fmt.Sprintf("Exec failed: %v", execErr))
			return
		}
	} else {
		// Execute in transaction
		result, releaseResource, execErr = dh.dsManager.ExecuteTx(r.Context(), req.TimeoutSec, txID, req.SQL, parameters...)
		defer releaseResource()
		if execErr != nil {
			if r.Context().Err() == context.DeadlineExceeded {
				dh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), false, true)
				writeError(w, http.StatusRequestTimeout, "TIMEOUT", "Request timeout")
				return
			}
			dh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), true, false)
			code := statusCodeForDbError(execErr)
			if code == http.StatusBadGateway {
				log.Printf("[502] DB connection error (dsIDX=%d): %v", dsIDX, execErr)
			}
			writeError(w, code, "EXEC_ERROR", fmt.Sprintf("Exec failed: %v", execErr))
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
	elapsedTimeMs := time.Since(startTime).Milliseconds()

	// Update health info: record successful execution
	dh.statsSetResult(dsIDX, elapsedTimeMs, false, false)

	// Write response
	response := ExecuteResponse{
		EffectedRows:  affectedRows,
		ElapsedTimeMs: elapsedTimeMs,
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (dh *DmlHandler) statsSetResult(datasourceIdx int, latencyMs int64, isError bool, isTimeout bool) {
	stats := dh.statsInfos[datasourceIdx]
	stats.mu.Lock()
	defer stats.mu.Unlock()

	stats.statTotal.Incr(1)

	if isError {
		stats.statErrors.Incr(1)
		return
	}
	if isTimeout {
		stats.statTimeouts.Incr(1)
		return
	}
	stats.statLatency.WithLabelValues("p95").Observe(float64(latencyMs))
}

func (dh *DmlHandler) StatsGet(datasourceIdx int) (latencyP95Ms int, errorRate1m float64, timeoutRate1m float64) {
	stats := dh.statsInfos[datasourceIdx]
	stats.mu.Lock()
	defer stats.mu.Unlock()

	latency := &dto.Metric{}
	stats.statLatency.WithLabelValues("p95").(prometheus.Metric).Write(latency)
	p95 := latency.GetSummary().GetQuantile()[0].GetValue()
	if math.IsNaN(p95) {
		p95 = 16.0
	}

	total := float64(stats.statTotal.Rate())
	errorRate1m = 0.0
	timeoutRate1m = 0.0
	if total > 0 {
		errorRate1m = float64(stats.statErrors.Rate()) / total
		timeoutRate1m = float64(stats.statTimeouts.Rate()) / total
	}

	return int(p95), errorRate1m, timeoutRate1m
}

/************************************************************
 * Private methods
 ************************************************************/
func (dh *DmlHandler) parseRequest(r *http.Request) (dsIDX int, request ExecuteRequest, params []any, txID string, err error) {
	var req ExecuteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return -1, ExecuteRequest{}, nil, "", fmt.Errorf("failed to parse request: %w", err)
	}

	if req.SQL == "" {
		return -1, ExecuteRequest{}, nil, "", fmt.Errorf("SQL is required")
	}

	txID = r.URL.Query().Get(QUERYP_TX_ID)
	// get datasource index from context
	dsIDX, ok := GetCtxDsIdx(r)
	if !ok && txID == "" {
		return -1, ExecuteRequest{}, nil, "", fmt.Errorf("Datasource INDEX hasn't decided")
	}

	// Convert params
	parameters, err := convertParams(req.Params)
	if err != nil {
		return -1, ExecuteRequest{}, nil, "", fmt.Errorf("failed to convert params: %w", err)
	}

	return dsIDX, req, parameters, txID, nil
}

func convertParams(params []ParamValue) ([]any, error) {
	args := make([]any, len(params))
	for i, p := range params {
		log.Printf("Converting Param[%d]: %+v", i, p)
		arg, err := convertParam(p)
		if err != nil {
			return nil, fmt.Errorf("param[%d]: %w", i, err)
		}
		args[i] = arg
	}
	return args, nil
}

func convertParam(p ParamValue) (any, error) {
	if p.Value == nil {
		return nil, nil
	}

	switch p.Type {
	case NULL:
		return nil, nil
	case INT:
		if val, ok := p.Value.(int32); ok {
			return val, nil
		}
		if val, ok := p.Value.(float64); ok {
			return int32(val), nil
		}
		if val, ok := p.Value.(int); ok {
			return int32(val), nil
		}
		if val, ok := p.Value.(int64); ok {
			return int32(val), nil
		}
		if val, ok := p.Value.(string); ok {
			var intVal int
			_, err := fmt.Sscanf(val, "%d", &intVal)
			if err != nil {
				return nil, fmt.Errorf("invalid int32 string: %v", err)
			}
			return intVal, nil
		}
		return nil, fmt.Errorf("invalid int32 value: %v", p.Value)
	case LONG:
		if val, ok := p.Value.(int64); ok {
			return val, nil
		}
		if val, ok := p.Value.(float64); ok {
			return int64(val), nil
		}
		if val, ok := p.Value.(int); ok {
			return int64(val), nil
		}
		if val, ok := p.Value.(int32); ok {
			return int64(val), nil
		}
		if val, ok := p.Value.(string); ok {
			var intVal int64
			_, err := fmt.Sscanf(val, "%d", &intVal)
			if err != nil {
				return nil, fmt.Errorf("invalid int64 string: %v", err)
			}
			return intVal, nil
		}
		return nil, fmt.Errorf("invalid int64 value: %v", p.Value)
	case DOUBLE:
		if val, ok := p.Value.(float32); ok {
			return val, nil
		}
		if val, ok := p.Value.(float64); ok {
			return val, nil
		}
		if val, ok := p.Value.(string); ok {
			var floatVal float64
			_, err := fmt.Sscanf(val, "%f", &floatVal)
			if err != nil {
				return nil, fmt.Errorf("invalid float64 string: %v", err)
			}
			return floatVal, nil
		}
		return nil, fmt.Errorf("invalid float64 value: %v", p.Value)
	case DECIMAL:
		if val, ok := p.Value.(decimal.Decimal); ok {
			return val, nil
		}
		if val, ok := p.Value.(string); ok {
			dec, err := decimal.NewFromString(val)
			if err != nil {
				return nil, fmt.Errorf("invalid decimal string: %w", err)
			}
			return dec, nil
		}
		return nil, fmt.Errorf("invalid decimal value: %v", p.Value)
	case BOOL:
		if val, ok := p.Value.(bool); ok {
			return val, nil
		}
		if val, ok := p.Value.(string); ok {
			switch val {
			case "true":
				return true, nil
			case "false":
				return false, nil
			}
		}
		return nil, fmt.Errorf("invalid bool value: %v", p.Value)
	case STRING:
		if val, ok := p.Value.(string); ok {
			return val, nil
		}
		return fmt.Sprintf("%v", p.Value), nil
	case BINARY:
		if val, ok := p.Value.([]byte); ok {
			return val, nil
		}
		if val, ok := p.Value.(string); ok {
			decoded, err := base64.StdEncoding.DecodeString(val)
			if err != nil {
				return nil, fmt.Errorf("invalid base64: %w", err)
			}
			return decoded, nil
		}
		return nil, fmt.Errorf("invalid bytes_base64 value: %v", p.Value)
	case DATE, DATETIME:
		if val, ok := p.Value.(time.Time); ok {
			return val, nil
		}
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

// statusCodeForDbError returns 502 for DB connection errors (driver.ErrBadConn), 503 otherwise.
// PostgreSQL ConnectError is normalized to ErrBadConn at the datasource connector layer.
func statusCodeForDbError(err error) int {
	if errors.Is(err, driver.ErrBadConn) {
		return http.StatusBadGateway
	}
	return http.StatusServiceUnavailable
}

func writeError(w http.ResponseWriter, statusCode int, code, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)

	errorResp := map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": message,
		},
	}
	json.NewEncoder(w).Encode(errorResp)
}
