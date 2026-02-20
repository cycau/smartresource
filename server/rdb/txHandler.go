package rdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	. "smartdatastream/server/global"
	"time"
)

// BeginTxRequest represents the request body for /v1/rdb/tx/begin
type TxRequestParams struct {
	IsolationLevel  *string `json:"isolationLevel,omitempty"`
	MaxTxTimeoutSec *int    `json:"maxTxTimeoutSec,omitempty"`
}

// BeginTxResponse represents the response for /v1/rdb/tx/begin
type BeginTxResponse struct {
	TxID      string    `json:"txId"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// OkResponse represents a simple OK response
type OkResponse struct {
	OK bool `json:"ok"`
}

// TxHandler handles transaction API requests
type TxHandler struct {
	dsManager *DsManager
}

// NewTxHandler creates a new TxHandler
func NewTxHandler(dsManager *DsManager) *TxHandler {
	return &TxHandler{
		dsManager: dsManager,
	}
}

// BeginTx handles /v1/rdb/tx/begin
func (th *TxHandler) BeginTx(w http.ResponseWriter, r *http.Request) {
	dsIDX, req, isolationLevel, err := th.parseBeginRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", fmt.Sprintf("Failed to parse request: %v", err))
		return
	}

	// Begin transaction (default isolation level: ReadCommitted)
	txEntry, err := th.dsManager.BeginTx(dsIDX, isolationLevel, req.MaxTxTimeoutSec)
	if err != nil {
		if err == context.DeadlineExceeded {
			writeError(w, http.StatusRequestTimeout, "TIMEOUT", "Request timeout")
			return
		}
		writeError(w, statusCodeForDbError(err), "BEGIN_ERROR", fmt.Sprintf("Failed to begin transaction: %v", err))
		return
	}

	// Write response
	response := BeginTxResponse{
		TxID:      txEntry.TxID,
		ExpiresAt: txEntry.ExpiresAt,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// CommitTx handles /v1/rdb/tx/commit
func (th *TxHandler) CommitTx(w http.ResponseWriter, r *http.Request) {
	txID := getTxID(r)

	if txID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "txId is required")
		return
	}

	// Commit transaction
	err := th.dsManager.CommitTx(txID)
	if err != nil {
		if err == ErrTxNotFound {
			writeError(w, http.StatusConflict, "TX_NOT_FOUND", "Transaction not found")
			return
		}
		writeError(w, statusCodeForDbError(err), "COMMIT_ERROR", fmt.Sprintf("Failed to commit transaction: %v", err))
		return
	}

	// Write response
	response := OkResponse{
		OK: true,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// RollbackTx handles /v1/rdb/tx/rollback
func (th *TxHandler) RollbackTx(w http.ResponseWriter, r *http.Request) {
	txID := getTxID(r)

	if txID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "txId is required")
		return
	}

	// Rollback transaction
	err := th.dsManager.RollbackTx(txID)
	if err != nil {
		if err == ErrTxNotFound {
			writeError(w, http.StatusConflict, "TX_NOT_FOUND", "Transaction not found")
			return
		}
		writeError(w, statusCodeForDbError(err), "ROLLBACK_ERROR", fmt.Sprintf("Failed to rollback transaction: %v", err))
		return
	}

	// Write response
	response := OkResponse{
		OK: true,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// CloseTx handles /v1/rdb/tx/done/:requestId
func (th *TxHandler) CloseTx(w http.ResponseWriter, r *http.Request) {
	txID := getTxID(r)

	if txID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "txId is required")
		return
	}

	// Close transaction
	err := th.dsManager.CloseTx(txID)
	if err != nil {
		if err == ErrTxNotFound {
			writeError(w, http.StatusConflict, "TX_NOT_FOUND", "Transaction not found")
			return
		}
		writeError(w, statusCodeForDbError(err), "CLOSE_ERROR", fmt.Sprintf("Failed to close transaction: %v", err))
		return
	}

	// Write response
	response := OkResponse{
		OK: true,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (th *TxHandler) parseBeginRequest(r *http.Request) (int, *TxRequestParams, *sql.IsolationLevel, error) {
	if r.Body != nil {
		defer func() {
			io.Copy(io.Discard, r.Body)
			r.Body.Close()
		}()
	}

	var req TxRequestParams
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return -1, nil, nil, fmt.Errorf("failed to parse request: %w", err)
	}

	// get datasourceId from context
	dsIDX, ok := GetCtxDsIdx(r)
	if !ok {
		return -1, nil, nil, fmt.Errorf("datasource INDEX is required")
	}

	if req.IsolationLevel == nil {
		return dsIDX, &req, nil, nil
	}

	isolationLevel := sql.LevelDefault
	switch *req.IsolationLevel {
	case "READ_UNCOMMITTED":
		isolationLevel = sql.LevelReadUncommitted
	case "READ_COMMITTED":
		isolationLevel = sql.LevelReadCommitted
	case "REPEATABLE_READ":
		isolationLevel = sql.LevelRepeatableRead
	case "SERIALIZABLE":
		isolationLevel = sql.LevelSerializable
	default:
		return -1, nil, nil, fmt.Errorf("invalid isolation level: %s", *req.IsolationLevel)
	}

	return dsIDX, &req, &isolationLevel, nil
}

func getTxID(r *http.Request) string {
	return r.Header.Get(HEADER_TX_ID)
}
