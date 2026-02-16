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
	ep_TX_BEGIN
	ep_TX_QUERY
	ep_TX_EXECUTE
	ep_TX_COMMIT
	ep_TX_ROLLBACK
	ep_TX_CLOSE
	ep_OTHER
)

func getEndpointPath(ep endpointType) string {
	switch ep {
	case ep_QUERY:
		return "/query"
	case ep_EXECUTE:
		return "/execute"
	case ep_TX_BEGIN:
		return "/tx/begin"
	case ep_TX_QUERY:
		return "/tx/query"
	case ep_TX_EXECUTE:
		return "/tx/execute"
	case ep_TX_COMMIT:
		return "/tx/commit"
	case ep_TX_ROLLBACK:
		return "/tx/rollback"
	case ep_TX_CLOSE:
		return "/tx/close"
	default:
		return "/other"
	}
}

const HEADER_SECRET_KEY = "X-Secret-Key"
const HEADER_DB_NAME = "_Cy_DbName"
const HEADER_TX_ID = "_Cy_TxID"
const HEADER_REDIRECT_COUNT = "_Cy_RdCount"
const UNLIMITED_REQUEST_TIMEOUT_SEC = 900

// clientConfig は config.yaml の構造
type clientConfig struct {
	MaxConcurrency           int         `yaml:"maxConcurrency"`
	DefaultSecretKey         string      `yaml:"defaultSecretKey"`
	DefaultDatabase          string      `yaml:"defaultDatabase"`
	DefaultRequestTimeoutSec int         `yaml:"defaultRequestTimeoutSec"`
	ClusterNodes             []NodeEntry `yaml:"clusterNodes"`
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

func Init(nodes []NodeEntry, maxConcurrency int, defaultRequestTimeoutSec int) error {
	defaultDatabase, err := switcher.Init(nodes, maxConcurrency, defaultRequestTimeoutSec)
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

	defaultDatabase, err := switcher.Init(config.ClusterNodes, config.MaxConcurrency, config.DefaultRequestTimeoutSec)
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

	headers := map[string]string{
		HEADER_DB_NAME: c.dbName,
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

	resp, _, err := c.executor.Request(c.dbName, ep_QUERY, http.MethodPost, headers, body, 3, 3, opts.TimeoutSec)
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

	headers := map[string]string{
		HEADER_DB_NAME: c.dbName,
	}
	body := map[string]any{
		"sql":    sql,
		"params": params,
	}

	resp, _, err := c.executor.Request(c.dbName, ep_EXECUTE, http.MethodPost, headers, body, 3, 3, UNLIMITED_REQUEST_TIMEOUT_SEC)
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
