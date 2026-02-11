package smartclient

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

/**************************************************
* TxClient
**************************************************/
type TxClient struct {
	datasourceName string
	txId           string
	nodeIdx        int
	executor       *Switcher
}

func GetTx(datasourceName string, isolationLevel IsolationLevel) (*TxClient, error) {
	txId, nodeIdx, err := beginTx(datasourceName, isolationLevel)
	if err != nil {
		return nil, err
	}

	return &TxClient{
		datasourceName: datasourceName,
		txId:           txId + "." + strconv.Itoa(nodeIdx),
		executor:       switcher,
	}, nil
}

func beginTx(datasourceName string, isolationLevel IsolationLevel) (txId string, nodeIdx int, err error) {

	resp, nodeIdx, err := switcher.Request(datasourceName, EP_BEGIN_TX, http.MethodPost, map[string]string{"_DsID": datasourceName}, map[string]any{"isolationLevel": isolationLevel}, 3, 3)
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

	resp, err := c.executor.RequestTx(c.txId, EP_QUERY, http.MethodPost, body)
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

	resp, err := c.executor.RequestTx(c.txId, EP_EXECUTE, http.MethodPost, body)
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
	resp, err := c.executor.RequestTx(c.txId, EP_COMMIT_TX, http.MethodPut, nil)
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
	resp, err := c.executor.RequestTx(c.txId, EP_ROLLBACK_TX, http.MethodPut, nil)
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
	resp, err := c.executor.RequestTx(c.txId, EP_DONE_TX, http.MethodPut, nil)
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
