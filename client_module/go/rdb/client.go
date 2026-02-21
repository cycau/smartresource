package smartclient

import (
	"encoding/json"
	"fmt"
	"log"
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

const hEADER_SECRET_KEY = "_cy_SecretKey"
const hEADER_DB_NAME = "_cy_DbName"
const hEADER_TX_ID = "_cy_TxID"
const hEADER_REDIRECT_COUNT = "_cy_RdCount"
const hEADER_TIMEOUT_SEC = "_cy_TimeoutSec"

// clientConfig は config.yaml の構造
type clientConfig struct {
	MaxTotalConcurrency      int         `yaml:"maxTotalConcurrency"`
	MaxTxConcurrency         int         `yaml:"maxTxConcurrency"`
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
	PoolConns     int    `json:"poolConns"`
	MaxWriteConns int    `json:"maxWriteConns"`
	MinWriteConns int    `json:"minWriteConns"`
}

var dEFAULT_DATABASE = ""

func Init(nodes []NodeEntry, maxTotalConcurrency int, maxTxConcurrency int, defaultQueryTimeoutSec int) error {
	if len(nodes) < 1 {
		return fmt.Errorf("No cluster nodes configured.")
	}

	maxTotalConcurrency = max(2, maxTotalConcurrency)
	maxTxConcurrency = max(1, maxTxConcurrency)
	if defaultQueryTimeoutSec < 1 {
		defaultQueryTimeoutSec = 30
	}

	log.Println("### [Init] maxTotalConcurrency:", maxTotalConcurrency)
	log.Println("### [Init] maxTxConcurrency:", maxTxConcurrency)
	log.Println("### [Init] entries:", nodes)

	defaultDatabase, err := executor.Init(nodes, maxTotalConcurrency, maxTxConcurrency, defaultQueryTimeoutSec)
	if dEFAULT_DATABASE == "" {
		dEFAULT_DATABASE = defaultDatabase
	}

	log.Println("### [Init] defaultDatabase:", dEFAULT_DATABASE)
	log.Println("### [Init] defaultQueryTimeoutSec:", defaultQueryTimeoutSec)
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

	dEFAULT_DATABASE = config.DefaultDatabase
	return Init(config.ClusterNodes, config.MaxTotalConcurrency, config.MaxTxConcurrency, config.DefaultRequestTimeoutSec)
}

/**************************************************
* DsClient
**************************************************/

type Client struct {
	dbName   string
	executor *switcher
}

// Get は databaseName 用のクライアントを返す。空の場合は defaultDatabase を使用する
func Get(databaseName string) *Client {
	if databaseName == "" {
		databaseName = dEFAULT_DATABASE
	}
	return &Client{
		dbName:   databaseName,
		executor: executor,
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

func (c *Client) Query(sql string, params *Params, opts *QueryOptions) (*Records, error) {

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
	timeoutSec := 0
	if opts != nil {
		if opts.LimitRows > 0 {
			body["limitRows"] = opts.LimitRows
		}
		timeoutSec = opts.TimeoutSec
	}

	resp, err := c.executor.Request(false, c.dbName, ep_QUERY, http.MethodPost, headers, body, timeoutSec, 2)
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

	resp, err := c.executor.Request(false, c.dbName, ep_EXECUTE, http.MethodPost, headers, body, 0, 2)
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
		ElapsedTimeUs: result.ElapsedTimeUs,
	}
	for i, col := range result.Meta {
		records.colMap[col.Name] = i
	}
	for i := range records.Rows {
		records.Rows[i].colMap = &records.colMap
		records.Rows[i].data = &result.Rows[i]
	}
	return &records
}
