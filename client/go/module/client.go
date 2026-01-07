package module

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// SmartClient is the main client for executing SQL queries
type SmartClient struct {
	nodes  []*NodeInfo
	dbMap  map[string][]*NodeInfo // dbName -> []NodeInfo
	mu     sync.RWMutex
	client *http.Client
}

var (
	singletonInstance = &SmartClient{}
)

// newSmartClient is the internal function that creates a SmartClient
func Initialize(configPath string) (*SmartClient, error) {
	config, err := LoadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}
	// Convert config nodes to pointers
	nodes := make([]*NodeInfo, len(config.Nodes))
	for i := range config.Nodes {
		nodes[i] = &config.Nodes[i]
	}

	// Build dbMap: dbName -> []NodeInfo
	dbMap := make(map[string][]*NodeInfo)
	for _, node := range nodes {
		for _, ds := range node.Datasources {
			if dbMap[ds.DBName] == nil {
				dbMap[ds.DBName] = make([]*NodeInfo, 0)
			}
			dbMap[ds.DBName] = append(dbMap[ds.DBName], node)
		}
	}

	singletonInstance := &SmartClient{
		nodes:  nodes,
		dbMap:  dbMap,
		client: &http.Client{Timeout: 30 * time.Second},
	}

	return singletonInstance, nil
}

func Get() (*SmartClient, error) {
	// Check if instance already exists
	if singletonInstance == nil {
		return nil, fmt.Errorf("singleton instance is not initialized")
	}

	return singletonInstance, nil
}

// Query executes a SELECT query
func (c *SmartClient) Query(dbName string, sql string, params []ParamValue, opts *QueryOptions) (*QueryResponse, error) {
	return c.executeQuery(dbName, sql, params, opts, false)
}

// Execute executes an INSERT/UPDATE/DELETE statement
func (c *SmartClient) Execute(dbName string, sql string, params []ParamValue, opts *ExecuteOptions) (*ExecuteResponse, error) {
	return c.executeStatement(dbName, sql, params, opts, false)
}

// BeginTx begins a new transaction
func (c *SmartClient) BeginTx(dbName string, opts *BeginTxOptions) (*TxHandle, error) {
	c.mu.RLock()
	availableNodes := c.getAvailableNodesForDB(dbName, false)
	c.mu.RUnlock()

	if len(availableNodes) == 0 {
		return nil, fmt.Errorf("no available nodes for database: %s", dbName)
	}

	// Select a random node
	node := SelectRandomNode(availableNodes)
	if node == nil {
		return nil, fmt.Errorf("failed to select node for database: %s", dbName)
	}

	// Find the datasource in the node
	var datasource *Datasource
	for _, ds := range node.Datasources {
		if ds.DBName == dbName {
			datasource = ds
			break
		}
	}
	if datasource == nil {
		return nil, fmt.Errorf("datasource %s not found in node %s", dbName, node.NodeID)
	}

	// Build request
	reqBody := map[string]interface{}{
		"datasourceId": dbName,
	}
	if opts != nil && opts.TimeoutMs != nil {
		reqBody["timeoutMs"] = *opts.TimeoutMs
	}

	// Find node index in nodes array
	nodeIndex := -1
	for i, n := range c.nodes {
		if n.NodeID == node.NodeID {
			nodeIndex = i
			break
		}
	}
	if nodeIndex == -1 {
		return nil, fmt.Errorf("node index not found for node: %s", node.NodeID)
	}

	// Add nodeIndex to request (according to spec.md)
	reqBody["nodeIndex"] = nodeIndex

	var resp *BeginTxResponse
	var err error
	maxRetries := 3
	for retry := 0; retry < maxRetries; retry++ {
		resp, err = c.beginTxRequest(node, reqBody)
		if err == nil {
			break
		}

		// Check if it's a network error
		if isNetworkError(err) {
			MarkNodeUnavailable(node)
			// Try next node
			availableNodes = c.getAvailableNodesForDB(dbName, false)
			availableNodes = filterNode(availableNodes, node.NodeID)
			if len(availableNodes) == 0 {
				return nil, fmt.Errorf("no available nodes after retry: %w", err)
			}
			node = SelectRandomNode(availableNodes)
			continue
		}

		// Check if it's a redirect
		if isRedirectError(err) {
			redirectErr, ok := err.(*RedirectError)
			if ok {
				MarkDatasourceUnavailable(datasource)
				// Find redirect node by base URL
				redirectNode := c.extractNodeFromRedirect(redirectErr)
				if redirectNode != nil {
					node = redirectNode
					continue
				}
			}
		}

		// Other errors, return
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	// Create TxHandle
	// Use APINodeID if available, otherwise use NodeID
	nodeID := resp.APINodeID
	if nodeID == "" {
		nodeID = resp.NodeID
	}
	txHandle := &TxHandle{
		client: c,
		txID:   resp.TxID,
		nodeID: nodeID,
		dbName: dbName,
	}

	return txHandle, nil
}

// Close closes the client (currently no-op, but kept for API compatibility)
func (c *SmartClient) Close() error {
	return nil
}

// executeQuery executes a query with retry logic
func (c *SmartClient) executeQuery(dbName string, sql string, params []ParamValue, opts *QueryOptions, readonly bool) (*QueryResponse, error) {
	c.mu.RLock()
	availableNodes := c.getAvailableNodesForDB(dbName, readonly)
	c.mu.RUnlock()

	if len(availableNodes) == 0 {
		return nil, fmt.Errorf("no available nodes for database: %s", dbName)
	}

	var resp *QueryResponse
	var err error
	maxRetries := 3

	for retry := 0; retry < maxRetries; retry++ {
		// Select a random node
		node := SelectRandomNode(availableNodes)
		if node == nil {
			return nil, fmt.Errorf("failed to select node for database: %s", dbName)
		}

		// Find the datasource
		var datasource *Datasource
		for _, ds := range node.Datasources {
			if ds.DBName == dbName {
				datasource = ds
				break
			}
		}
		if datasource == nil {
			return nil, fmt.Errorf("datasource %s not found in node %s", dbName, node.NodeID)
		}

		// Check if datasource is available
		if !IsDatasourceAvailable(datasource) {
			availableNodes = filterNode(availableNodes, node.NodeID)
			if len(availableNodes) == 0 {
				return nil, fmt.Errorf("no available datasources for database: %s", dbName)
			}
			continue
		}

		// Build request
		reqBody := map[string]interface{}{
			"sql":    sql,
			"params": params,
		}
		if opts != nil {
			if opts.TimeoutMs != nil {
				reqBody["timeoutMs"] = *opts.TimeoutMs
			}
			if opts.LimitRows != nil {
				reqBody["limitRows"] = *opts.LimitRows
			}
		}
		reqBody["datasourceId"] = dbName

		resp, err = c.queryRequest(node, reqBody)
		if err == nil {
			return resp, nil
		}

		// Check if it's a network error
		if isNetworkError(err) {
			MarkNodeUnavailable(node)
			// Try next node
			availableNodes = c.getAvailableNodesForDB(dbName, readonly)
			availableNodes = filterNode(availableNodes, node.NodeID)
			if len(availableNodes) == 0 {
				return nil, fmt.Errorf("no available nodes after retry: %w", err)
			}
			continue
		}

		// Check if it's a redirect
		if isRedirectError(err) {
			redirectErr, ok := err.(*RedirectError)
			if ok {
				MarkDatasourceUnavailable(datasource)
				// Find redirect node by base URL
				redirectNode := c.extractNodeFromRedirect(redirectErr)
				if redirectNode != nil {
					node = redirectNode
					continue
				}
			}
		}

		// Other errors, return
		return nil, err
	}

	return nil, fmt.Errorf("max retries exceeded, last error: %w", err)
}

// executeStatement executes a statement with retry logic
func (c *SmartClient) executeStatement(dbName string, sql string, params []ParamValue, opts *ExecuteOptions, readonly bool) (*ExecuteResponse, error) {
	c.mu.RLock()
	availableNodes := c.getAvailableNodesForDB(dbName, false) // execute is never readonly
	c.mu.RUnlock()

	if len(availableNodes) == 0 {
		return nil, fmt.Errorf("no available nodes for database: %s", dbName)
	}

	var resp *ExecuteResponse
	var err error
	maxRetries := 3

	for retry := 0; retry < maxRetries; retry++ {
		// Select a random node
		node := SelectRandomNode(availableNodes)
		if node == nil {
			return nil, fmt.Errorf("failed to select node for database: %s", dbName)
		}

		// Find the datasource
		var datasource *Datasource
		for _, ds := range node.Datasources {
			if ds.DBName == dbName {
				datasource = ds
				break
			}
		}
		if datasource == nil {
			return nil, fmt.Errorf("datasource %s not found in node %s", dbName, node.NodeID)
		}

		// Check if datasource is readonly (execute cannot be done on readonly)
		if datasource.Readonly {
			availableNodes = filterNode(availableNodes, node.NodeID)
			if len(availableNodes) == 0 {
				return nil, fmt.Errorf("no writable datasources for database: %s", dbName)
			}
			continue
		}

		// Check if datasource is available
		if !IsDatasourceAvailable(datasource) {
			availableNodes = filterNode(availableNodes, node.NodeID)
			if len(availableNodes) == 0 {
				return nil, fmt.Errorf("no available datasources for database: %s", dbName)
			}
			continue
		}

		// Build request
		reqBody := map[string]interface{}{
			"sql":    sql,
			"params": params,
		}
		if opts != nil && opts.TimeoutMs != nil {
			reqBody["timeoutMs"] = *opts.TimeoutMs
		}
		reqBody["datasourceId"] = dbName

		resp, err = c.executeRequest(node, reqBody)
		if err == nil {
			return resp, nil
		}

		// Check if it's a network error
		if isNetworkError(err) {
			MarkNodeUnavailable(node)
			// Try next node
			availableNodes = c.getAvailableNodesForDB(dbName, false)
			availableNodes = filterNode(availableNodes, node.NodeID)
			if len(availableNodes) == 0 {
				return nil, fmt.Errorf("no available nodes after retry: %w", err)
			}
			continue
		}

		// Check if it's a redirect
		if isRedirectError(err) {
			redirectErr, ok := err.(*RedirectError)
			if ok {
				MarkDatasourceUnavailable(datasource)
				// Find redirect node by base URL
				redirectNode := c.extractNodeFromRedirect(redirectErr)
				if redirectNode != nil {
					node = redirectNode
					continue
				}
			}
		}

		// Other errors, return
		return nil, err
	}

	return nil, fmt.Errorf("max retries exceeded, last error: %w", err)
}

// getAvailableNodesForDB returns available nodes for a database
func (c *SmartClient) getAvailableNodesForDB(dbName string, readonly bool) []*NodeInfo {
	nodes := c.dbMap[dbName]
	if nodes == nil {
		return []*NodeInfo{}
	}

	available := make([]*NodeInfo, 0)
	for _, node := range nodes {
		if !IsNodeAvailable(node) {
			continue
		}

		// Check if datasource exists and matches readonly requirement
		for _, ds := range node.Datasources {
			if ds.DBName == dbName {
				if readonly || !ds.Readonly {
					if IsDatasourceAvailable(ds) {
						available = append(available, node)
					}
				}
				break
			}
		}
	}

	return available
}

// filterNode filters out a node from the list
func filterNode(nodes []*NodeInfo, nodeID string) []*NodeInfo {
	filtered := make([]*NodeInfo, 0)
	for _, node := range nodes {
		if node.NodeID != nodeID {
			filtered = append(filtered, node)
		}
	}
	return filtered
}

// HTTP request helpers
func (c *SmartClient) queryRequest(node *NodeInfo, reqBody map[string]interface{}) (*QueryResponse, error) {
	result, err := c.makeRequest(node, "/v1/rdb/execute", reqBody, func(body []byte) (interface{}, error) {
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

func (c *SmartClient) executeRequest(node *NodeInfo, reqBody map[string]interface{}) (*ExecuteResponse, error) {
	result, err := c.makeRequest(node, "/v1/rdb/execute", reqBody, func(body []byte) (interface{}, error) {
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

func (c *SmartClient) beginTxRequest(node *NodeInfo, reqBody map[string]interface{}) (*BeginTxResponse, error) {
	result, err := c.makeRequest(node, "/v1/rdb/tx/begin", reqBody, func(body []byte) (interface{}, error) {
		var resp BeginTxResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, err
		}
		return &resp, nil
	})
	if err != nil {
		return nil, err
	}
	return result.(*BeginTxResponse), nil
}

func (c *SmartClient) makeRequest(node *NodeInfo, path string, reqBody map[string]interface{}, parseResponse func([]byte) (interface{}, error)) (interface{}, error) {
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

	// Make request
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("network error: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Handle redirect (307)
	if resp.StatusCode == http.StatusTemporaryRedirect {
		location := resp.Header.Get("Location")
		if location != "" {
			return nil, &RedirectError{Location: location, StatusCode: resp.StatusCode}
		}
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

// Error types
type APIError struct {
	StatusCode int
	Code       string
	Message    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("API error [%d] %s: %s", e.StatusCode, e.Code, e.Message)
}

type RedirectError struct {
	Location   string
	StatusCode int
}

func (e *RedirectError) Error() string {
	return fmt.Sprintf("redirect to %s", e.Location)
}

// Helper functions
func isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*url.Error)
	return ok || strings.Contains(err.Error(), "network error")
}

func isRedirectError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*RedirectError)
	return ok
}

func (c *SmartClient) extractNodeFromRedirect(redirectErr *RedirectError) *NodeInfo {
	// Parse redirect URL
	u, err := url.Parse(redirectErr.Location)
	if err != nil {
		return nil
	}

	// Extract base URL (scheme + host)
	baseURL := u.Scheme + "://" + u.Host

	// Find node with matching base URL
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, node := range c.nodes {
		if node.BaseURL == baseURL {
			return node
		}
	}
	return nil
}
