package module

import (
	"time"
)

// Datasource represents a datasource configuration
type Datasource struct {
	DBName           string
	Readonly         bool
	ErrorCount       int
	UnavailableUntil time.Time
}

// NodeInfo represents a node configuration
type NodeInfo struct {
	NodeID           string
	BaseURL          string
	SecretKey        string
	Datasources      []*Datasource
	ErrorCount       int
	UnavailableUntil time.Time
}

// Config represents the client configuration
type Config struct {
	DefaultSecretKey string     `yaml:"defaultSecretKey"`
	Nodes            []NodeInfo `yaml:"nodes"`
}

// QueryOptions represents options for query operations
type QueryOptions struct {
	TimeoutMs *int
	LimitRows *int
}

// ExecuteOptions represents options for execute operations
type ExecuteOptions struct {
	TimeoutMs *int
}

// BeginTxOptions represents options for begin transaction operations
type BeginTxOptions struct {
	TimeoutMs *int
}

// ExecuteResponse represents the response from execute operation
type ExecuteResponse struct {
	EffectedRows    int64 `json:"effectedRows"`
	ExecutionTimeMs int64 `json:"executionTimeMs"`
}

// QueryResponse represents the response from query operation
type QueryResponse struct {
	Meta ResultMeta               `json:"meta"`
	Rows []map[string]interface{} `json:"rows"`
}

// ResultMeta contains metadata about the query result
type ResultMeta struct {
	TotalCount      int          `json:"totalCount"`
	Columns         []ColumnMeta `json:"columns,omitempty"`
	ExecutionTimeMs int64        `json:"executionTimeMs"`
}

// ColumnMeta contains metadata about a column
type ColumnMeta struct {
	Name     string `json:"name"`
	DBType   string `json:"dbType"`
	Nullable bool   `json:"nullable"`
}

// BeginTxResponse represents the response from begin transaction operation
type BeginTxResponse struct {
	TxID         string    `json:"txId"`
	APINodeID    string    `json:"apiNodeId"` // OpenAPI仕様
	NodeID       string    `json:"nodeId"`    // サーバー実装（互換性のため）
	DatasourceID string    `json:"datasourceId"`
	TimeoutMs    int       `json:"timeoutMs"`
	ExpiresAt    time.Time `json:"expiresAt"` // サーバー実装（互換性のため）
}

// OkResponse represents a simple OK response
type OkResponse struct {
	OK bool `json:"ok"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail contains error details
type ErrorDetail struct {
	Code    string                 `json:"code"`
	Message string                 `json:"message"`
	Details map[string]interface{} `json:"details,omitempty"`
}

// ParamValue represents a parameter value
type ParamValue struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value,omitempty"`
}
