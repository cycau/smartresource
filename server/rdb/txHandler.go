package rdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"smartdatastream/server/cluster"
	"time"
)

// BeginTxRequest represents the request body for /v1/rdb/tx/begin
type BeginTxRequest struct {
	ClientNodeIndex int  `json:"clientNodeIndex"`
	TimeoutSec      *int `json:"timeoutSec,omitempty"`
}

// BeginTxResponse represents the response for /v1/rdb/tx/begin
type BeginTxResponse struct {
	TxID      string    `json:"txId"`
	NodeID    string    `json:"nodeId"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// TxIdRequest represents the request body for /v1/rdb/tx/commit and /v1/rdb/tx/rollback
type TxIdRequest struct {
	TxID string `json:"txId"`
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
	dsIDX, req, err := tx.parseRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", fmt.Sprintf("Failed to parse request: %v", err))
		return
	}

	// Begin transaction (default isolation level: ReadCommitted)
	txEntry, err := tx.txManager.Begin(req.ClientNodeIndex, dsIDX, sql.LevelReadCommitted, req.TimeoutSec)
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
	// Parse request body
	var req TxIdRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", fmt.Sprintf("Failed to parse request: %v", err))
		return
	}

	if req.TxID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "txId is required")
		return
	}

	// Commit transaction
	err := tx.txManager.Commit(req.TxID)
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
	// Parse request body
	var req TxIdRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", fmt.Sprintf("Failed to parse request: %v", err))
		return
	}

	if req.TxID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "txId is required")
		return
	}

	// Rollback transaction
	err := tx.txManager.Rollback(req.TxID)
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
}

func (tx *TxHandler) parseRequest(r *http.Request) (int, BeginTxRequest, error) {
	var req BeginTxRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return -1, BeginTxRequest{}, fmt.Errorf("failed to parse request: %w", err)
	}

	// get datasourceId from context
	dsIDX, ok := r.Context().Value("$S_IDX").(int)
	if !ok {
		return -1, BeginTxRequest{}, fmt.Errorf("datasource INDEX is required")
	}
	if dsIDX < 0 || dsIDX >= len(tx.selfNode.HealthInfo.Datasources) {
		return -1, BeginTxRequest{}, fmt.Errorf("invalid datasource INDEX: %d", dsIDX)
	}

	return dsIDX, req, nil
}
