package smartclient

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"gopkg.in/yaml.v3"
)

type endpointType int

const (
	epQuery endpointType = iota
	epExecute
	epBeginTx
	epOther
)

func Init(nodes []NodeEntry) error {
	return switcher.Init(nodes)
}

func Configure(configPath string) error {
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
	datasourceName string
	executor       *Switcher
}

// Get は databaseName 用のクライアントを返す。空の場合は defaultDatabase を使用する
func Get(datasourceName string) (*Client, error) {
	return &Client{
		datasourceName: datasourceName,
		executor:       switcher,
	}, nil
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

func (c *Client) Query(sql string, params Params, opts QueryOptions) (*QueryResult, error) {
	nodeIdx, err := c.executor.selectNode(c.datasourceName, epQuery)
	if err != nil {
		return nil, err
	}

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
	q := map[string]string{"_DbName": c.datasourceName}

	resp, err := c.executor.request(nodeIdx, "/query", http.MethodPost, q, body, 3)
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
	nodeIdx, err := c.executor.selectNode(c.datasourceName, epExecute)
	if err != nil {
		return nil, err
	}
	body := map[string]any{
		"sql":    sql,
		"params": params,
	}
	q := map[string]string{"_DbName": c.datasourceName}
	resp, err := c.executor.request(nodeIdx, "/execute", http.MethodPost, q, body, 2)
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
