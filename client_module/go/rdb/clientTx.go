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
	executor *switcher
	nodeIdx  int
	orgTxId  string
}

func NewTx(databaseName string, isolationLevel *IsolationLevel, maxTxTimeoutSec *int) (*TxClient, error) {
	if databaseName == "" {
		databaseName = dEFAULT_DATABASE
	}
	txId, nodeIdx, err := beginTx(databaseName, isolationLevel, maxTxTimeoutSec)
	if err != nil {
		return nil, err
	}

	return &TxClient{
		dbName:   databaseName,
		executor: executor,
		nodeIdx:  nodeIdx,
		orgTxId:  txId,
	}, nil
}

func GetTx(txId string) (*TxClient, error) {
	// Check size
	if len(txId) < 20 {
		return nil, fmt.Errorf("invalid txId size")
	}

	idx := strings.LastIndex(txId, ".")
	if idx == -1 {
		return nil, fmt.Errorf("invalid txId format")
	}
	nodeIdx, err := strconv.ParseInt(txId[idx+1:], 10, 8)
	if err != nil {
		return nil, err
	}

	return &TxClient{
		executor: executor,
		nodeIdx:  int(nodeIdx),
		orgTxId:  txId[:idx],
	}, nil
}

func (c *TxClient) GetTxId() string {
	return c.orgTxId + "." + strconv.Itoa(c.nodeIdx)
}

func beginTx(databaseName string, isolationLevel *IsolationLevel, maxTxTimeoutSec *int) (txId string, nodeIdx int, err error) {
	headers := map[string]string{
		hEADER_DB_NAME: databaseName,
	}
	body := map[string]any{}
	if isolationLevel != nil {
		body["isolationLevel"] = *isolationLevel
	}
	if maxTxTimeoutSec != nil {
		body["maxTxTimeoutSec"] = *maxTxTimeoutSec
	}
	resp, err := executor.Request(databaseName, ep_TX_BEGIN, http.MethodPost, headers, body, 3, 3, 0)
	if err != nil {
		return "", -1, err
	}
	defer resp.Release()
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", -1, fmt.Errorf("begin tx status %d: %s", resp.StatusCode, string(bodyBytes))
	}
	var result TxInfo
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", -1, err
	}

	return result.TxId, resp.NodeIdx, nil
}

func (c *TxClient) Query(sql string, params *Params, opts *QueryOptions) (*Records, error) {
	headers := map[string]string{
		hEADER_TX_ID: c.orgTxId,
	}
	paramValues := []paramValue{}
	if params != nil {
		paramValues = params.data
	}
	body := map[string]any{
		"sql":    sql,
		"params": paramValues,
	}
	timeoutSec := 0
	if opts != nil {
		if opts.LimitRows > 0 {
			body["limitRows"] = opts.LimitRows
		}
		timeoutSec = opts.TimeoutSec
	}
	resp, err := c.executor.RequestTargetNode(c.nodeIdx, ep_TX_QUERY, http.MethodPost, headers, body, timeoutSec)
	if err != nil {
		return nil, err
	}
	defer resp.Release()
	var result queryResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return convertResult(result), nil
}

func (c *TxClient) Execute(sql string, params *Params) (*ExecuteResult, error) {
	headers := map[string]string{
		hEADER_TX_ID: c.orgTxId,
	}
	paramValues := []paramValue{}
	if params != nil {
		paramValues = params.data
	}
	body := map[string]any{
		"sql":    sql,
		"params": paramValues,
	}

	resp, err := c.executor.RequestTargetNode(c.nodeIdx, ep_TX_EXECUTE, http.MethodPost, headers, body, 0)
	if err != nil {
		return nil, err
	}
	defer resp.Release()
	var result ExecuteResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *TxClient) Commit() error {
	headers := map[string]string{
		hEADER_TX_ID: c.orgTxId,
	}
	resp, err := c.executor.RequestTargetNode(c.nodeIdx, ep_TX_COMMIT, http.MethodPut, headers, nil, 0)
	if err != nil {
		return err
	}
	defer resp.Release()
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("commit tx status %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

func (c *TxClient) Rollback() error {
	headers := map[string]string{
		hEADER_TX_ID: c.orgTxId,
	}
	resp, err := c.executor.RequestTargetNode(c.nodeIdx, ep_TX_ROLLBACK, http.MethodPut, headers, nil, 0)
	if err != nil {
		return err
	}
	defer resp.Release()
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("rollback tx status %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

func (c *TxClient) Close() error {
	headers := map[string]string{
		hEADER_TX_ID: c.orgTxId,
	}
	resp, err := c.executor.RequestTargetNode(c.nodeIdx, ep_TX_CLOSE, http.MethodPut, headers, nil, 0)
	if err != nil {
		return err
	}
	defer resp.Release()
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("close tx status %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}
