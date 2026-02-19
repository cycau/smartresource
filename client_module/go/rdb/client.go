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

const hEADER_SECRET_KEY = "X-Secret-Key"
const hEADER_DB_NAME = "_Cy_DbName"
const hEADER_TX_ID = "_Cy_TxID"
const hEADER_REDIRECT_COUNT = "_Cy_RdCount"
const uNLIMITED_REQUEST_TIMEOUT_SEC = 900

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

var dEFAULT_DATABASE = ""

func Init(nodes []NodeEntry, maxConcurrency int, defaultRequestTimeoutSec int) error {
	defaultDatabase, err := switcher.Init(nodes, maxConcurrency, defaultRequestTimeoutSec)
	dEFAULT_DATABASE = defaultDatabase
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
		dEFAULT_DATABASE = config.DefaultDatabase
	} else {
		dEFAULT_DATABASE = defaultDatabase
	}
	return err
}

/**************************************************
* DsClient
**************************************************/

type Client struct {
	dbName   string
	executor *smartSwitcher
}

// Get は databaseName 用のクライアントを返す。空の場合は defaultDatabase を使用する
func Get(databaseName string) *Client {
	if databaseName == "" {
		databaseName = dEFAULT_DATABASE
	}
	return &Client{
		dbName:   databaseName,
		executor: switcher,
	}
}

type Params struct {
	data []paramValue
}

func (p *Params) Add(val any, valType ValueType) *Params {
	if val == nil {
		p.data = append(p.data, paramValue{
			Value: nil,
			Type:  ValueType_NULL,
		})
		return p
	}

	p.data = append(p.data, paramValue{
		Value: val,
		Type:  valType,
	})
	return p
}

func NewParams() *Params {
	return &Params{
		data: []paramValue{},
	}
}

func (c *Client) Query(sql string, params *Params, opts QueryOptions) (*Records, error) {

	headers := map[string]string{
		hEADER_DB_NAME: c.dbName,
	}
	paramValues := []paramValue{}
	if params != nil {
		paramValues = params.data
	}
	body := map[string]any{
		"sql":    sql,
		"params": paramValues,
	}
	if opts.LimitRows > 0 {
		body["limitRows"] = opts.LimitRows
	}
	if opts.TimeoutSec > 0 {
		body["timeoutSec"] = opts.TimeoutSec
	}

	resp, err := c.executor.Request(c.dbName, ep_QUERY, http.MethodPost, headers, body, 3, 3, opts.TimeoutSec)
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

func (c *Client) Execute(sql string, params *Params) (*ExecuteResult, error) {

	headers := map[string]string{
		hEADER_DB_NAME: c.dbName,
	}
	paramValues := []paramValue{}
	if params != nil {
		paramValues = params.data
	}
	body := map[string]any{
		"sql":    sql,
		"params": paramValues,
	}

	resp, err := c.executor.Request(c.dbName, ep_EXECUTE, http.MethodPost, headers, body, 3, 3, uNLIMITED_REQUEST_TIMEOUT_SEC)
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

func convertResult(result queryResult) *Records {
	records := Records{
		colMap:        make(map[string]int),
		Rows:          make([]Record, len(result.Rows)),
		TotalCount:    result.TotalCount,
		ElapsedTimeMs: result.ElapsedTimeMs,
	}
	for i, col := range result.Meta {
		records.colMap[col.Name] = i
	}
	for i := range records.Rows {
		records.Rows[i].meta = &records.colMap
		records.Rows[i].data = &result.Rows[i]
	}
	return &records
}
