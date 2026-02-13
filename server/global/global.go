package global

import (
	"context"
	"net/http"
	"strings"
)

type Config struct {
	NodeName      string             `yaml:"nodeName"`
	NodePort      int                `yaml:"nodePort"`
	SecretKey     string             `yaml:"secretKey"`
	MaxHttpQueue  int                `yaml:"maxHttpQueue"`
	MyDatasources []DatasourceConfig `yaml:"myDatasources"`
	ClusterNodes  []string           `yaml:"clusterNodes"`
}

type DatasourceConfig struct {
	DatasourceID       string `yaml:"datasourceId"`
	DatabaseName       string `yaml:"databaseName"`
	Driver             string `yaml:"driver"`
	DSN                string `yaml:"dsn"`
	MaxOpenConns       int    `yaml:"maxOpenConns"`
	MaxIdleConns       int    `yaml:"maxIdleConns"`
	MaxConnLifetimeSec int    `yaml:"maxConnLifetimeSec"`

	MaxWriteConns          int `yaml:"maxWriteConns"`
	MinWriteConns          int `yaml:"minWriteConns"`
	MaxTxIdleTimeoutSec    int `yaml:"maxTxIdleTimeoutSec"`
	DefaultQueryTimeoutSec int `yaml:"defaultQueryTimeoutSec"`
}

const QUERYP_DB_NAME = "_DbName"
const QUERYP_TX_ID = "_TxID"
const QUERYP_DS_ID = "_DsID"
const QUERYP_REDIRECT_COUNT = "_RCount"

const EP_PATH_QUERY = "/query"
const EP_PATH_EXECUTE = "/execute"
const EP_PATH_BEGIN_TX = "/tx/begin"
const EP_PATH_COMMIT_TX = "/tx/commit"
const EP_PATH_ROLLBACK_TX = "/tx/rollback"
const EP_PATH_CLOSE_TX = "/tx/close"

// 定数定義
type ENDPOINT_TYPE int

const (
	EP_Query ENDPOINT_TYPE = iota
	EP_Execute
	EP_BeginTx
	EP_Other
)

// エンドポイントタイプ取得
func GetEndpointType(path string) ENDPOINT_TYPE {

	if strings.HasSuffix(path, EP_PATH_QUERY) {
		return EP_Query
	}
	if strings.HasSuffix(path, EP_PATH_EXECUTE) {
		return EP_Execute
	}
	if strings.HasSuffix(path, EP_PATH_BEGIN_TX) {
		return EP_BeginTx
	}

	return EP_Other
}

const CTX_DS_IDX = "$S_IDX"

func GetCtxDsIdx(r *http.Request) (int, bool) {
	value, ok := r.Context().Value(CTX_DS_IDX).(int)
	return value, ok
}
func PutCtxDsIdx(r *http.Request, value any) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), CTX_DS_IDX, value))
}
