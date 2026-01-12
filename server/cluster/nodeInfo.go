package cluster

import (
	"sync"
	"time"
)

type DatasourceInfo struct {
	DatasourceID string `json:"datasourceId"`
	DatabaseName string `json:"databaseName"`
	Active       bool   `json:"active"`
	Readonly     bool   `json:"readonly"`
	MaxOpenConns int    `json:"maxOpenConns"`
	MaxTxConns   int    `json:"maxTxConns"`

	OpenConns    int          `json:"openConns"`
	IdleConns    int          `json:"idleConns"`
	WaitConns    int          `json:"waitConns"`
	RunningSql   int          `json:"runningSql"`
	RunningTx    int          `json:"runningTx"`
	ErrorRate1m  float64      `json:"errorRate1m"`
	Timeouts1m   int          `json:"timeouts1m"`
	LatencyMs    int          `json:"latencyMs"`
	LatencyP95Ms int          `json:"latencyP95Ms"`
	Mu           sync.RWMutex `json:"-"`
}

type HealthInfo struct {
	MaxHttpSessions int              `json:"maxHttpSessions"`
	RunningHttp     int              `json:"runningHttp"`
	Datasources     []DatasourceInfo `json:"datasources"`
	UpTime          time.Time        `json:"upTime"`
	CheckTime       time.Time        `json:"checkTime"`
}

type NodeStatus string

const (
	STARTING NodeStatus = "STARTING"
	SERVING  NodeStatus = "SERVING"
	DRAINING NodeStatus = "DRAINING"
	STOPPING NodeStatus = "STOPPING"
	HEALZERR NodeStatus = "HEALZERR"
)

type NodeInfo struct {
	NodeID     string       `json:"nodeId"`
	Status     NodeStatus   `json:"status"`
	BaseURL    string       `json:"baseUrl"`
	HealthInfo HealthInfo   `json:"healthInfo"`
	Mu         sync.RWMutex `json:"-"`
}

// 必要の場合はatomic.Pointerでラップして使うこと

func (node *NodeInfo) Clone() NodeInfo {
	node.Mu.RLock()
	defer node.Mu.RUnlock()

	datasources := make([]DatasourceInfo, len(node.HealthInfo.Datasources))
	copy(datasources, node.HealthInfo.Datasources)

	return NodeInfo{
		NodeID:  node.NodeID,
		Status:  node.Status,
		BaseURL: node.BaseURL,
		HealthInfo: HealthInfo{
			MaxHttpSessions: node.HealthInfo.MaxHttpSessions,
			RunningHttp:     node.HealthInfo.RunningHttp,
			UpTime:          node.HealthInfo.UpTime,
			Datasources:     datasources,
			CheckTime:       node.HealthInfo.CheckTime,
		},
	}
}
