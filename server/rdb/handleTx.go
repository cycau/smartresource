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
	DatasourceID string `json:"datasourceId,omitempty"`
	TimeoutMs    *int   `json:"timeoutMs,omitempty"`
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
func (h *TxHandler) BeginTx(w http.ResponseWriter, r *http.Request) {
	// Parse request body (optional)
	var req BeginTxRequest
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST", fmt.Sprintf("Failed to parse request: %v", err))
			return
		}
	}

	// Default datasource
	if req.DatasourceID == "" {
		req.DatasourceID = "main" // or get from config
	}

	// Begin transaction (default isolation level: ReadCommitted)
	txEntry, err := h.txManager.Begin(r.Context(), req.DatasourceID, sql.LevelReadCommitted, *req.TimeoutMs)
	if err != nil {
		if err == context.DeadlineExceeded {
			h.writeError(w, http.StatusRequestTimeout, "TIMEOUT", "Request timeout")
			return
		}
		h.writeError(w, http.StatusServiceUnavailable, "BEGIN_ERROR", fmt.Sprintf("Failed to begin transaction: %v", err))
		return
	}

	// Write response
	response := BeginTxResponse{
		TxID:      txEntry.TxID,
		NodeID:    h.selfNode.NodeID,
		ExpiresAt: txEntry.ExpiresAt,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// CommitTx handles /v1/rdb/tx/commit
func (h *TxHandler) CommitTx(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var req TxIdRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST", fmt.Sprintf("Failed to parse request: %v", err))
		return
	}

	if req.TxID == "" {
		h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "txId is required")
		return
	}

	// Commit transaction
	err := h.txManager.Commit(req.TxID)
	if err != nil {
		if err == ErrTxNotFound {
			h.writeError(w, http.StatusConflict, "TX_NOT_FOUND", "Transaction not found")
			return
		}
		h.writeError(w, http.StatusServiceUnavailable, "COMMIT_ERROR", fmt.Sprintf("Failed to commit transaction: %v", err))
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
func (h *TxHandler) RollbackTx(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var req TxIdRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST", fmt.Sprintf("Failed to parse request: %v", err))
		return
	}

	if req.TxID == "" {
		h.writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "txId is required")
		return
	}

	// Rollback transaction
	err := h.txManager.Rollback(req.TxID)
	if err != nil {
		if err == ErrTxNotFound {
			h.writeError(w, http.StatusConflict, "TX_NOT_FOUND", "Transaction not found")
			return
		}
		h.writeError(w, http.StatusServiceUnavailable, "ROLLBACK_ERROR", fmt.Sprintf("Failed to rollback transaction: %v", err))
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

func (h *TxHandler) writeError(w http.ResponseWriter, statusCode int, code, message string) {
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
