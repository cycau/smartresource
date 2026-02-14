package smartclient

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

type endpointType int

const (
	ep_QUERY endpointType = iota
	ep_EXECUTE
	ep_BEGIN_TX
	ep_COMMIT_TX
	ep_ROLLBACK_TX
	ep_DONE_TX
	ep_OTHER
)

func getEndpointPath(ep endpointType) string {
	switch ep {
	case ep_QUERY:
		return "/query"
	case ep_EXECUTE:
		return "/execute"
	case ep_BEGIN_TX:
		return "/tx/begin"
	case ep_COMMIT_TX:
		return "/tx/commit"
	case ep_ROLLBACK_TX:
		return "/tx/rollback"
	case ep_DONE_TX:
		return "/tx/done"
	default:
		return "/other"
	}
}

// clientConfig は config.yaml の構造
type clientConfig struct {
	DefaultSecretKey string      `yaml:"defaultSecretKey"`
	DefaultDatabase  string      `yaml:"defaultDatabase"`
	ClusterNodes     []NodeEntry `yaml:"clusterNodes"`
	MaxConcurrency   int         `yaml:"maxConcurrency"`
}

// nodeHealth は /healz レスポンス（サーバーと互換）
type nodeInfo struct {
	BaseURL      string           `yaml:"baseUrl"`
	SecretKey    string           `yaml:"secretKey"`
	NodeID       string           `json:"nodeId"`
	Status       string           `json:"status"`
	MaxHttpQueue int              `json:"maxHttpQueue"`
	CheckTime    time.Time        `json:"checkTime"`
	Datasources  []datasourceInfo `json:"datasources"`
	Mu           sync.RWMutex     `json:"-"`
}

type datasourceInfo struct {
	DatasourceID  string `json:"datasourceId"`
	DatabaseName  string `json:"databaseName"`
	Active        bool   `json:"active"`
	MaxOpenConns  int    `json:"maxOpenConns"`
	MaxWriteConns int    `json:"maxWriteConns"`
	MinWriteConns int    `json:"minWriteConns"`
}

var DEFAULT_DATABASE = ""

func Init(nodes []NodeEntry, maxConcurrency int) error {
	defaultDatabase, err := switcher.Init(nodes, maxConcurrency)
	DEFAULT_DATABASE = defaultDatabase
	return err
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

	defaultDatabase, err := switcher.Init(config.ClusterNodes, config.MaxConcurrency)
	if config.DefaultDatabase != "" {
		DEFAULT_DATABASE = config.DefaultDatabase
	} else {
		DEFAULT_DATABASE = defaultDatabase
	}
	return err
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
	if databaseName == "" {
		databaseName = DEFAULT_DATABASE
	}
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

	resp, _, err := c.executor.Request(c.dbName, ep_QUERY, http.MethodPost, query, body, 3, 3)
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

	resp, _, err := c.executor.Request(c.dbName, ep_EXECUTE, http.MethodPost, query, body, 3, 3)
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
