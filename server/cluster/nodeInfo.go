package cluster

import "sync"

type DatasourceInfo struct {
	DatasourceID string `json:"datasourceId"`
	Active       bool   `json:"active"`
	Readonly     bool   `json:"readonly"`
	MaxOpenConns int    `json:"maxOpenConns"`
	MaxTxConns   int    `json:"maxTxConns"`

	OpenConns    int     `json:"openConns"`
	IdleConns    int     `json:"idleConns"`
	WaitConns    int     `json:"waitConns"`
	RunningSql   int     `json:"runningSql"`
	RunningTx    int     `json:"runningTx"`
	P95LatencyMs int     `json:"p95LatencyMs"`
	ErrorRate1m  float64 `json:"errorRate1m"`
	Timeouts1m   int     `json:"timeouts1m"`
	LatencyMs    int     `json:"latencyMs"`
}

type HealthInfo struct {
	MaxHttpSessions int              `json:"maxHttpSessions"`
	RunningHttp     int              `json:"runningHttp"`
	UptimeSec       int              `json:"uptimeSec"`
	DatasourceInfo  []DatasourceInfo `json:"datasourceInfo"`
	CheckTime       string           `json:"checkTime"`
}

type NodeStatus string

const (
	STARTING NodeStatus = "STARTING"
	SERVING  NodeStatus = "SERVING"
	DRAINING NodeStatus = "DRAINING"
	STOPPING NodeStatus = "STOPPING"
)

type NodeInfo struct {
	NodeID     string       `json:"nodeId"`
	Status     NodeStatus   `json:"status"`
	BaseURL    string       `json:"baseUrl"`
	Weight     float64      `json:"weight,omitempty"` // ノードの重み
	HealthInfo HealthInfo   `json:"healthInfo"`
	mu         sync.RWMutex `json:"-"`
}
