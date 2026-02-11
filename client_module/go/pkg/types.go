package smartclient

import (
	"sync"
	"time"
)

// ValType はパラメータの型（JSON の "type" に使用）
type ValueType string

const (
	ValueType_NULL     ValueType = "NULL"
	ValueType_INT      ValueType = "INT"
	ValueType_LONG     ValueType = "LONG"
	ValueType_DOUBLE   ValueType = "DOUBLE"
	ValueType_DECIMAL  ValueType = "DECIMAL"
	ValueType_BOOL     ValueType = "BOOL"
	ValueType_DATE     ValueType = "DATE"
	ValueType_DATETIME ValueType = "DATETIME"
	ValueType_STRING   ValueType = "STRING"
	ValueType_BINARY   ValueType = "BINARY"
)

// ParamValue はクエリ/実行のパラメータ
type ParamValue struct {
	Type  ValueType `json:"type"`
	Value any       `json:"value,omitempty"`
}

type Params []ParamValue

// QueryOptions は Query のオプション
type QueryOptions struct {
	LimitRows  int // 最大行数
	TimeoutSec int // タイムアウト秒
}

// ColumnMeta はカラムメタデータ
type ColumnMeta struct {
	Name     string `json:"name"`
	DBType   string `json:"dbType"`
	Nullable bool   `json:"nullable"`
}

// QueryResult は Query の結果
type QueryResult struct {
	Meta          []ColumnMeta `json:"meta,omitempty"`
	Rows          []any        `json:"rows"`
	TotalCount    int          `json:"totalCount"`
	ElapsedTimeMs int64        `json:"elapsedTimeMs"`
}

// ExecuteResult は Execute の結果
type ExecuteResult struct {
	EffectedRows  int64 `json:"effectedRows"`
	ElapsedTimeMs int64 `json:"elapsedTimeMs"`
}

type BeginTxResponse struct {
	TxId      string    `json:"txId"`
	NodeID    string    `json:"nodeId"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// IsolationLevel はトランザクション分離レベル
type IsolationLevel string

const (
	Isolation_ReadUncommitted IsolationLevel = "READ_UNCOMMITTED"
	Isolation_ReadCommitted   IsolationLevel = "READ_COMMITTED"
	Isolation_RepeatableRead  IsolationLevel = "REPEATABLE_READ"
	Isolation_Serializable    IsolationLevel = "SERIALIZABLE"
)

// clientConfig は config.yaml の構造
type clientConfig struct {
	DefaultSecretKey string      `yaml:"defaultSecretKey"`
	ClusterNodes     []NodeEntry `yaml:"clusterNodes"`
}

type NodeEntry struct {
	BaseURL   string `yaml:"baseUrl"`
	SecretKey string `yaml:"secretKey"`
}

// nodeHealth は /healz レスポンス（サーバーと互換）
type NodeInfo struct {
	BaseURL      string           `yaml:"baseUrl"`
	SecretKey    string           `yaml:"secretKey"`
	NodeID       string           `json:"nodeId"`
	Status       string           `json:"status"`
	MaxHttpQueue int              `json:"maxHttpQueue"`
	CheckTime    time.Time        `json:"checkTime"`
	Datasources  []DatasourceInfo `json:"datasources"`
	Mu           sync.RWMutex     `json:"-"`
}

type DatasourceInfo struct {
	DatasourceID string `json:"datasourceId"`
	DatabaseName string `json:"databaseName"`
	Active       bool   `json:"active"`
	Readonly     bool   `json:"readonly"`
	MaxOpenConns int    `json:"maxOpenConns"`
	MaxIdleConns int    `json:"maxIdleConns"`
	MaxTxConns   int    `json:"maxTxConns"`
}
