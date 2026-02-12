package smartclient

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"gopkg.in/yaml.v3"
)

type EndpointType int

const (
	EP_QUERY EndpointType = iota
	EP_EXECUTE
	EP_BEGIN_TX
	EP_COMMIT_TX
	EP_ROLLBACK_TX
	EP_DONE_TX
	EP_OTHER
)

func GetEndpointPath(ep EndpointType) string {
	switch ep {
	case EP_QUERY:
		return "/query"
	case EP_EXECUTE:
		return "/execute"
	case EP_BEGIN_TX:
		return "/tx/begin"
	case EP_COMMIT_TX:
		return "/tx/commit"
	case EP_ROLLBACK_TX:
		return "/tx/rollback"
	case EP_DONE_TX:
		return "/tx/done"
	default:
		return "/other"
	}
}

func Init(nodes []NodeEntry) error {
	return switcher.Init(nodes)
}

func InitWithConfig(configPath string) error {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	var config clientConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}
	// defaultSecretKey をノードに適用
	for i := range config.ClusterNodes {
		if config.ClusterNodes[i].SecretKey == "" {
			config.ClusterNodes[i].SecretKey = config.DefaultSecretKey
		}
	}

	return switcher.Init(config.ClusterNodes)
}

/**************************************************
* DsClient
**************************************************/
type Client struct {
	dbName   string
	executor *Switcher
}

// Get は databaseName 用のクライアントを返す。空の場合は defaultDatabase を使用する
func Get(databaseName string) *Client {
	return &Client{
		dbName:   databaseName,
		executor: switcher,
	}
}

func ParamVal(val any, valType ValueType) ParamValue {
	if val == nil {
		return ParamValue{
			Value: nil,
			Type:  ValueType_NULL,
		}
	}

	return ParamValue{
		Value: val,
		Type:  valType,
	}
}

type Params []ParamValue

func (c *Client) Query(sql string, params Params, opts QueryOptions) (*QueryResult, error) {

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
	query := map[string]string{"_DbName": c.dbName}

	resp, _, err := c.executor.Request(c.dbName, EP_QUERY, http.MethodPost, query, body, 3, 3)
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

func (c *Client) Execute(sql string, params Params) (*ExecuteResult, error) {

	body := map[string]any{
		"sql":    sql,
		"params": params,
	}
	query := map[string]string{"_DbName": c.dbName}

	resp, _, err := c.executor.Request(c.dbName, EP_EXECUTE, http.MethodPost, query, body, 3, 3)
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
