package cluster

import (
	"sync"
	"time"

	"github.com/paulbellamy/ratecounter"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type DatasourceInfo struct {
	DatasourceID string `json:"datasourceId"` // TODO: for pinpoint identification to execute
	DatabaseName string `json:"databaseName"`
	Active       bool   `json:"active"`
	Readonly     bool   `json:"readonly"`
	MaxOpenConns int    `json:"maxOpenConns"`
	MinIdleConns int    `json:"minIdleConns"`
	MaxTxConns   int    `json:"maxTxConns"`

	OpenConns     int     `json:"openConns"`
	IdleConns     int     `json:"idleConns"`
	RunningSql    int     `json:"runningSql"`
	RunningTx     int     `json:"runningTx"`
	LatencyP95Ms  int     `json:"latencyP95Ms"`
	ErrorRate1m   float64 `json:"errorRate1m"`
	TimeoutRate1m float64 `json:"timeoutRate1m"`

	StatLatency  *prometheus.SummaryVec   `json:"-"`
	StatTotal    *ratecounter.RateCounter `json:"-"`
	StatErrors   *ratecounter.RateCounter `json:"-"`
	StatTimeouts *ratecounter.RateCounter `json:"-"`
}

type HealthInfo struct {
	MaxHttpQueue int              `json:"maxHttpQueue"`
	RunningHttp  int              `json:"runningHttp"`
	UpTime       time.Time        `json:"upTime"`
	CheckTime    time.Time        `json:"checkTime"`
	Datasources  []DatasourceInfo `json:"datasources"`
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
	BaseURL    string       `json:"-"`
	SecretKey  string       `json:"-"`
	HealthInfo HealthInfo   `json:"healthInfo"`
	Mu         sync.RWMutex `json:"-"`
}

func (d *DatasourceInfo) StatisticsResult(latencyMs int64, isError bool, isTimeout bool) {
	d.StatTotal.Incr(1)

	if isError {
		d.StatErrors.Incr(1)
		return
	}
	if isTimeout {
		d.StatTimeouts.Incr(1)
		return
	}
	d.StatLatency.WithLabelValues("p95").Observe(float64(latencyMs))
}

// 必要の場合はatomic.Pointerでラップして使うこと
func (node *NodeInfo) Clone() NodeInfo {
	node.Mu.RLock()
	defer node.Mu.RUnlock()

	datasources := make([]DatasourceInfo, len(node.HealthInfo.Datasources))
	for idx, ds := range node.HealthInfo.Datasources {
		latency95 := &dto.Metric{}
		ds.StatLatency.WithLabelValues("p95").(prometheus.Metric).Write(latency95)
		ds.LatencyP95Ms = int(latency95.GetSummary().GetQuantile()[0].GetValue())

		total := float64(ds.StatTotal.Rate())
		errCnt := float64(ds.StatErrors.Rate())
		toutCnt := float64(ds.StatTimeouts.Rate())

		if total == 0 {
			ds.ErrorRate1m = 0
			ds.TimeoutRate1m = 0
		} else {
			ds.ErrorRate1m = errCnt / total
			ds.TimeoutRate1m = toutCnt / total
		}

		datasources[idx] = ds
	}

	return NodeInfo{
		NodeID:    node.NodeID,
		Status:    node.Status,
		BaseURL:   "-",
		SecretKey: "-",
		HealthInfo: HealthInfo{
			MaxHttpQueue: node.HealthInfo.MaxHttpQueue,
			RunningHttp:  node.HealthInfo.RunningHttp,
			UpTime:       node.HealthInfo.UpTime,
			CheckTime:    node.HealthInfo.CheckTime,
			Datasources:  datasources,
		},
	}
}
