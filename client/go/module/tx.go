package module

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// TxHandle handles transaction operations
type TxHandle struct {
	client *SmartClient
	txID   string
	nodeID string
	dbName string
}

// Query executes a SELECT query within the transaction
func (tx *TxHandle) Query(sql string, params []ParamValue, opts *QueryOptions) (*QueryResponse, error) {
	// Extract node index from txID
	nodeIndex, err := ExtractNodeIndexFromTxID(tx.txID)
	if err != nil {
		// Fallback to nodeID if txID parsing fails
		node := tx.findNodeByID(tx.nodeID)
		if node == nil {
			return nil, fmt.Errorf("node not found: %s", tx.nodeID)
		}
		return tx.queryRequest(node, sql, params, opts)
	}

	// Get node by index
	if nodeIndex < 0 || nodeIndex >= len(tx.client.nodes) {
		return nil, fmt.Errorf("invalid node index: %d", nodeIndex)
	}
	node := tx.client.nodes[nodeIndex]

	return tx.queryRequest(node, sql, params, opts)
}

// Execute executes an INSERT/UPDATE/DELETE statement within the transaction
func (tx *TxHandle) Execute(sql string, params []ParamValue, opts *ExecuteOptions) (*ExecuteResponse, error) {
	// Extract node index from txID
	nodeIndex, err := ExtractNodeIndexFromTxID(tx.txID)
	if err != nil {
		// Fallback to nodeID if txID parsing fails
		node := tx.findNodeByID(tx.nodeID)
		if node == nil {
			return nil, fmt.Errorf("node not found: %s", tx.nodeID)
		}
		return tx.executeRequest(node, sql, params, opts)
	}

	// Get node by index
	if nodeIndex < 0 || nodeIndex >= len(tx.client.nodes) {
		return nil, fmt.Errorf("invalid node index: %d", nodeIndex)
	}
	node := tx.client.nodes[nodeIndex]

	return tx.executeRequest(node, sql, params, opts)
}

// Commit commits the transaction
func (tx *TxHandle) Commit() error {
	// Extract node index from txID
	nodeIndex, err := ExtractNodeIndexFromTxID(tx.txID)
	if err != nil {
		// Fallback to nodeID if txID parsing fails
		node := tx.findNodeByID(tx.nodeID)
		if node == nil {
			return fmt.Errorf("node not found: %s", tx.nodeID)
		}
		return tx.commitRequest(node)
	}

	// Get node by index
	if nodeIndex < 0 || nodeIndex >= len(tx.client.nodes) {
		return fmt.Errorf("invalid node index: %d", nodeIndex)
	}
	node := tx.client.nodes[nodeIndex]

	return tx.commitRequest(node)
}

// Rollback rolls back the transaction
func (tx *TxHandle) Rollback() error {
	// Extract node index from txID
	nodeIndex, err := ExtractNodeIndexFromTxID(tx.txID)
	if err != nil {
		// Fallback to nodeID if txID parsing fails
		node := tx.findNodeByID(tx.nodeID)
		if node == nil {
			return fmt.Errorf("node not found: %s", tx.nodeID)
		}
		return tx.rollbackRequest(node)
	}

	// Get node by index
	if nodeIndex < 0 || nodeIndex >= len(tx.client.nodes) {
		return fmt.Errorf("invalid node index: %d", nodeIndex)
	}
	node := tx.client.nodes[nodeIndex]

	return tx.rollbackRequest(node)
}

// findNodeByID finds a node by its ID
func (tx *TxHandle) findNodeByID(nodeID string) *NodeInfo {
	for _, node := range tx.client.nodes {
		if node.NodeID == nodeID {
			return node
		}
	}
	return nil
}

// HTTP request helpers for transaction operations
func (tx *TxHandle) queryRequest(node *NodeInfo, sql string, params []ParamValue, opts *QueryOptions) (*QueryResponse, error) {
	reqBody := map[string]interface{}{
		"sql":    sql,
		"params": params,
		"txId":   tx.txID,
	}
	if opts != nil {
		if opts.TimeoutMs != nil {
			reqBody["timeoutMs"] = *opts.TimeoutMs
		}
		if opts.LimitRows != nil {
			reqBody["limitRows"] = *opts.LimitRows
		}
	}

	result, err := tx.makeRequest(node, "/v1/rdb/execute", reqBody, func(body []byte) (interface{}, error) {
		var resp QueryResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, err
		}
		return &resp, nil
	})
	if err != nil {
		return nil, err
	}
	return result.(*QueryResponse), nil
}

func (tx *TxHandle) executeRequest(node *NodeInfo, sql string, params []ParamValue, opts *ExecuteOptions) (*ExecuteResponse, error) {
	reqBody := map[string]interface{}{
		"sql":    sql,
		"params": params,
		"txId":   tx.txID,
	}
	if opts != nil && opts.TimeoutMs != nil {
		reqBody["timeoutMs"] = *opts.TimeoutMs
	}

	result, err := tx.makeRequest(node, "/v1/rdb/execute", reqBody, func(body []byte) (interface{}, error) {
		var resp ExecuteResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, err
		}
		return &resp, nil
	})
	if err != nil {
		return nil, err
	}
	return result.(*ExecuteResponse), nil
}

func (tx *TxHandle) commitRequest(node *NodeInfo) error {
	reqBody := map[string]interface{}{
		"txId": tx.txID,
	}

	_, err := tx.makeRequest(node, "/v1/rdb/tx/commit", reqBody, func(body []byte) (interface{}, error) {
		var resp OkResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, err
		}
		if !resp.OK {
			return nil, fmt.Errorf("commit failed")
		}
		return &resp, nil
	})
	return err
}

func (tx *TxHandle) rollbackRequest(node *NodeInfo) error {
	reqBody := map[string]interface{}{
		"txId": tx.txID,
	}

	_, err := tx.makeRequest(node, "/v1/rdb/tx/rollback", reqBody, func(body []byte) (interface{}, error) {
		var resp OkResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, err
		}
		if !resp.OK {
			return nil, fmt.Errorf("rollback failed")
		}
		return &resp, nil
	})
	return err
}

func (tx *TxHandle) makeRequest(node *NodeInfo, path string, reqBody map[string]interface{}, parseResponse func([]byte) (interface{}, error)) (interface{}, error) {
	// Build URL
	reqURL := node.BaseURL + path

	// Marshal request body
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Create request
	req, err := http.NewRequest("POST", reqURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Secret-Key", node.SecretKey)

	// Make request (no retry for transaction operations)
	resp, err := tx.client.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("network error: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Handle errors
	if resp.StatusCode >= 400 {
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err == nil {
			return nil, &APIError{
				StatusCode: resp.StatusCode,
				Code:       errResp.Error.Code,
				Message:    errResp.Error.Message,
			}
		}
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	return parseResponse(body)
}
