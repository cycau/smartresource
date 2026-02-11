package rdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"smartdatastream/server/cluster"
	. "smartdatastream/server/global"
	"time"
)

// BeginTxRequest represents the request body for /v1/rdb/tx/begin
type BeginTxRequest struct {
	IsolationLevel string `json:"isolationLevel,omitempty"`
	TimeoutSec     *int   `json:"timeoutSec,omitempty"`
}

// BeginTxResponse represents the response for /v1/rdb/tx/begin
type BeginTxResponse struct {
	TxID      string    `json:"txId"`
	NodeID    string    `json:"nodeId"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// OkResponse represents a simple OK response
type OkResponse struct {
	OK bool `json:"ok"`
}

// TxHandler handles transaction API requests
type TxHandler struct {
	selfNode  *cluster.NodeInfo
	txManager *TxManager
}

// NewTxHandler creates a new TxHandler
func NewTxHandler(nodeInfo *cluster.NodeInfo, txManager *TxManager) *TxHandler {
	return &TxHandler{
		selfNode:  nodeInfo,
		txManager: txManager,
	}
}

// BeginTx handles /v1/rdb/tx/begin
func (tx *TxHandler) BeginTx(w http.ResponseWriter, r *http.Request) {
	dsIDX, req, isolationLevel, err := tx.parseBeginRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", fmt.Sprintf("Failed to parse request: %v", err))
		return
	}

	// Begin transaction (default isolation level: ReadCommitted)
	txEntry, err := tx.txManager.Begin(dsIDX, isolationLevel, req.TimeoutSec)
	if err != nil {
		if err == context.DeadlineExceeded {
			writeError(w, http.StatusRequestTimeout, "TIMEOUT", "Request timeout")
			return
		}
		writeError(w, http.StatusServiceUnavailable, "BEGIN_ERROR", fmt.Sprintf("Failed to begin transaction: %v", err))
		return
	}

	// Write response
	response := BeginTxResponse{
		TxID:      txEntry.TxID,
		NodeID:    tx.selfNode.NodeID,
		ExpiresAt: txEntry.ExpiresAt,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// CommitTx handles /v1/rdb/tx/commit
func (tx *TxHandler) CommitTx(w http.ResponseWriter, r *http.Request) {
	txID := getTxID(r)

	if txID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "txId is required")
		return
	}

	// Commit transaction
	err := tx.txManager.Commit(txID)
	if err != nil {
		if err == ErrTxNotFound {
			writeError(w, http.StatusConflict, "TX_NOT_FOUND", "Transaction not found")
			return
		}
		writeError(w, http.StatusServiceUnavailable, "COMMIT_ERROR", fmt.Sprintf("Failed to commit transaction: %v", err))
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
func (tx *TxHandler) RollbackTx(w http.ResponseWriter, r *http.Request) {
	txID := getTxID(r)

	if txID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "txId is required")
		return
	}

	// Rollback transaction
	err := tx.txManager.Rollback(txID)
	if err != nil {
		if err == ErrTxNotFound {
			writeError(w, http.StatusConflict, "TX_NOT_FOUND", "Transaction not found")
			return
		}
		writeError(w, http.StatusServiceUnavailable, "ROLLBACK_ERROR", fmt.Sprintf("Failed to rollback transaction: %v", err))
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

// DoneTx handles /v1/rdb/tx/done/:requestId
func (tx *TxHandler) DoneTx(w http.ResponseWriter, r *http.Request) {
	txID := getTxID(r)

	if txID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "txId is required")
		return
	}

	// Close transaction
	err := tx.txManager.Close(txID)
	if err != nil {
		if err == ErrTxNotFound {
			writeError(w, http.StatusConflict, "TX_NOT_FOUND", "Transaction not found")
			return
		}
		writeError(w, http.StatusServiceUnavailable, "CLOSE_ERROR", fmt.Sprintf("Failed to close transaction: %v", err))
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

func (tx *TxHandler) parseBeginRequest(r *http.Request) (int, BeginTxRequest, sql.IsolationLevel, error) {
	var req BeginTxRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return -1, BeginTxRequest{}, sql.LevelReadCommitted, fmt.Errorf("failed to parse request: %w", err)
	}

	// get datasourceId from context
	dsIDX, ok := GetCtxDsIdx(r)
	if !ok {
		return -1, BeginTxRequest{}, sql.LevelReadCommitted, fmt.Errorf("datasource INDEX is required")
	}
	if dsIDX < 0 || dsIDX >= len(tx.selfNode.Datasources) {
		return -1, BeginTxRequest{}, sql.LevelReadCommitted, fmt.Errorf("invalid datasource INDEX: %d", dsIDX)
	}

	isolationLevel := sql.LevelReadCommitted
	switch req.IsolationLevel {
	case "READ_UNCOMMITTED":
		isolationLevel = sql.LevelReadUncommitted
	case "READ_COMMITTED":
		isolationLevel = sql.LevelReadCommitted
	case "WRITE_COMMITTED":
		isolationLevel = sql.LevelWriteCommitted
	case "REPEATABLE_READ":
		isolationLevel = sql.LevelRepeatableRead
	case "SNAPSHOT":
		isolationLevel = sql.LevelSnapshot
	case "SERIALIZABLE":
		isolationLevel = sql.LevelSerializable
	case "LINEARIZABLE":
		isolationLevel = sql.LevelLinearizable
	}

	return dsIDX, req, isolationLevel, nil
}

func getTxID(r *http.Request) string {
	return r.URL.Query().Get(QUERYP_TX_ID)
}
