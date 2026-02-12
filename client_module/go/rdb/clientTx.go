package smartclient

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

/**************************************************
* TxClient
**************************************************/
type TxClient struct {
	dbName   string
	executor *Switcher
	TxId     string
}

func NewTx(databaseName string, isolationLevel IsolationLevel) (*TxClient, error) {
	txId, nodeIdx, err := beginTx(databaseName, isolationLevel)
	if err != nil {
		return nil, err
	}

	return &TxClient{
		dbName:   databaseName,
		executor: switcher,
		TxId:     txId + "." + strconv.Itoa(nodeIdx),
	}, nil
}

func GetTx(txId string) (*TxClient, error) {
	return &TxClient{
		executor: switcher,
		TxId:     txId,
	}, nil
}

func beginTx(databaseName string, isolationLevel IsolationLevel) (txId string, nodeIdx int, err error) {

	body := map[string]any{"isolationLevel": isolationLevel}
	query := map[string]string{"_DbName": databaseName}

	resp, nodeIdx, err := switcher.Request(databaseName, EP_BEGIN_TX, http.MethodPost, query, body, 3, 3)
	if err != nil {
		return "", -1, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", -1, fmt.Errorf("begin tx status %d: %s", resp.StatusCode, string(bodyBytes))
	}
	var result BeginTxResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", -1, err
	}

	return result.TxId, nodeIdx, nil
}

func (c *TxClient) Query(sql string, params Params, opts QueryOptions) (*QueryResult, error) {
	body := map[string]any{
		"sql":    sql,
		"params": params,
	}
	if opts.LimitRows > 0 {
		body["limitRows"] = opts.LimitRows
	}
	if opts.TimeoutSec > 0 {
		body["timeoutSec"] = opts.TimeoutSec
	}
	nodeIdx, txId, err := extractTxId(c.TxId)
	if err != nil {
		return nil, err
	}
	query := map[string]string{"_TxID": txId}

	resp, err := c.executor.RequestTargetNode(nodeIdx, EP_QUERY, http.MethodPost, query, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var result QueryResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (c *TxClient) Execute(sql string, params Params) (*ExecuteResult, error) {
	body := map[string]any{
		"sql":    sql,
		"params": params,
	}
	nodeIdx, txId, err := extractTxId(c.TxId)
	if err != nil {
		return nil, err
	}
	query := map[string]string{"_TxID": txId}

	resp, err := c.executor.RequestTargetNode(nodeIdx, EP_EXECUTE, http.MethodPost, query, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var result ExecuteResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *TxClient) Commit() error {
	nodeIdx, txId, err := extractTxId(c.TxId)
	if err != nil {
		return err
	}
	query := map[string]string{"_TxID": txId}

	resp, err := c.executor.RequestTargetNode(nodeIdx, EP_COMMIT_TX, http.MethodPut, query, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("commit tx status %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

func (c *TxClient) Rollback() error {
	nodeIdx, txId, err := extractTxId(c.TxId)
	if err != nil {
		return err
	}
	query := map[string]string{"_TxID": txId}

	resp, err := c.executor.RequestTargetNode(nodeIdx, EP_ROLLBACK_TX, http.MethodPut, query, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("rollback tx status %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

func (c *TxClient) Close() error {
	nodeIdx, txId, err := extractTxId(c.TxId)
	if err != nil {
		return err
	}
	query := map[string]string{"_TxID": txId}

	resp, err := c.executor.RequestTargetNode(nodeIdx, EP_DONE_TX, http.MethodPut, query, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("close tx status %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

func extractTxId(txId string) (int, string, error) {
	// Check size
	if len(txId) < 20 {
		return -1, "", fmt.Errorf("invalid txId size")
	}

	idx := strings.LastIndex(txId, ".")
	if idx == -1 {
		return -1, "", fmt.Errorf("invalid txId format")
	}
	nodeIdx, err := strconv.ParseInt(txId[idx+1:], 10, 8)
	if err != nil {
		return -1, "", err
	}

	return int(nodeIdx), txId[:idx], nil
}
