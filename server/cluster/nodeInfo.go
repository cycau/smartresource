package cluster

import (
	"math"
	"strings"
	"sync"
	"time"

	"smartdatastream/server/global"
	. "smartdatastream/server/global"
)

type NodeStatus string

const (
	STARTING NodeStatus = "STARTING"
	SERVING  NodeStatus = "SERVING"
	DRAINING NodeStatus = "DRAINING"
	STOPPING NodeStatus = "STOPPING"
	HEALZERR NodeStatus = "HEALZERR"
)

type NodeInfo struct {
	NodeID    string     `json:"nodeId"`
	Status    NodeStatus `json:"status"`
	BaseURL   string     `json:"-"`
	SecretKey string     `json:"-"`

	MaxHttpQueue int              `json:"maxHttpQueue"`
	RunningHttp  int              `json:"runningHttp"`
	UpTime       time.Time        `json:"upTime"`
	CheckTime    time.Time        `json:"checkTime"`
	Datasources  []DatasourceInfo `json:"datasources"`
	Mu           sync.Mutex       `json:"-"`
}

type DatasourceInfo struct {
	DatasourceID  string `json:"datasourceId"`
	DatabaseName  string `json:"databaseName"`
	Active        bool   `json:"active"`
	Readonly      bool   `json:"readonly"`
	MaxOpenConns  int    `json:"maxOpenConns"`
	MaxWriteConns int    `json:"maxWriteConns"`
	MinWriteConns int    `json:"minWriteConns"`

	RunningRead   int     `json:"runningRead"`
	RunningWrite  int     `json:"runningWrite"`
	RunningTx     int     `json:"runningTx"`
	LatencyP95Ms  int     `json:"latencyP95Ms"`
	ErrorRate1m   float64 `json:"errorRate1m"`
	TimeoutRate1m float64 `json:"timeoutRate1m"`
}

/*****************************
 * initialize DatasourceInfo
 *****************************/
const STAT_WINDOW_INTERVAL = 5 * time.Minute

func NewDatasourceInfo(config global.DatasourceConfig) *DatasourceInfo {

	return &DatasourceInfo{
		DatasourceID:  config.DatasourceID,
		DatabaseName:  config.DatabaseName,
		Active:        true,
		Readonly:      config.Readonly,
		MaxOpenConns:  config.MaxOpenConns,
		MaxWriteConns: config.MaxWriteConns,
		MinWriteConns: config.MinWriteConns,

		RunningRead:   0,
		RunningWrite:  0,
		RunningTx:     0,
		LatencyP95Ms:  0,
		ErrorRate1m:   0,
		TimeoutRate1m: 0,
	}
}

// 必要の場合はatomic.Pointerでラップして使うこと
func (node *NodeInfo) Clone() NodeInfo {
	node.Mu.Lock()
	defer node.Mu.Unlock()

	datasources := make([]DatasourceInfo, len(node.Datasources))
	for idx := range node.Datasources {
		dsInfo := &node.Datasources[idx]
		datasources[idx] = *dsInfo
	}

	return NodeInfo{
		NodeID:       node.NodeID,
		Status:       node.Status,
		BaseURL:      "-",
		SecretKey:    "-",
		MaxHttpQueue: node.MaxHttpQueue,
		RunningHttp:  node.RunningHttp,
		UpTime:       node.UpTime,
		CheckTime:    node.CheckTime,
		Datasources:  datasources,
	}
}

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

/*****************************
 * スコア計算
 *****************************/
const (
	LAT_BAD_MS       = 3000.0 // 3秒を"かなり悪い"基準
	ERR_RATE_BAD     = 0.05   // 1分5%をかなり悪い
	TIMEOUT_RATE_BAD = 0.05   // 1分5%をかなり悪い

	UPTIME_OK = 300.0 // サーバー起動から5分以上でOK
	TOP_K     = 3     // スコア上位TopK
)

func (node *NodeInfo) GetScore(dsIdx int, tarDbName string, endpoint ENDPOINT_TYPE) *ScoreWithWeight {

	if node.Status != SERVING {
		return nil
	}
	if node.RunningHttp >= node.MaxHttpQueue {
		return nil
	}

	dsInfo := &node.Datasources[dsIdx]

	if !dsInfo.Active {
		return nil
	}
	if dsInfo.Readonly && (endpoint == EP_Execute || endpoint == EP_BeginTx) {
		return nil
	}
	if dsInfo.DatabaseName != tarDbName {
		return nil
	}

	m := calculateMetrics(node, dsInfo)
	score := calculateScore(endpoint, m)

	weight := 0.0
	switch endpoint {
	case EP_Query:
		weight = float64(dsInfo.MaxOpenConns - dsInfo.MinWriteConns)
	case EP_Execute:
		weight = float64(dsInfo.MaxWriteConns)
	case EP_BeginTx:
		weight = float64(dsInfo.MaxWriteConns+dsInfo.MinWriteConns) / 2.0
	default:
		weight = float64(dsInfo.MaxOpenConns)
	}

	return &ScoreWithWeight{score: score, weight: weight, exIndex: dsIdx}
}

// 正規化された指標を計算
type normalizedMetrics struct {
	httpFree float64

	readFree  float64
	writeFree float64
	txFree    float64

	latScore  float64
	errScore  float64
	toutScore float64

	uptimeScore float64
}

// Node score information
type ScoreWithWeight struct {
	score   float64
	weight  float64
	exIndex int
}

// clamp01 0〜1にクランプ
func clamp01(x float64) float64 {
	return math.Min(1.0, math.Max(0.0, x))
}

const STAT_COLLECT_INTERVAL = 500 * time.Millisecond

func calculateMetrics(node *NodeInfo, dsInfo *DatasourceInfo) normalizedMetrics {
	httpFree := 1 - clamp01(float64(node.RunningHttp)/float64(node.MaxHttpQueue))

	capacityMultiple := float64(node.MaxHttpQueue) / float64(dsInfo.MaxOpenConns)

	readCap := float64(dsInfo.MaxOpenConns-dsInfo.MinWriteConns) * capacityMultiple
	ratio := clamp01(float64(dsInfo.RunningRead) / readCap)
	readFree := 1 - ratio*ratio

	writeCap := float64(dsInfo.MaxWriteConns) * capacityMultiple
	ratio = clamp01(float64(dsInfo.RunningWrite) / writeCap)
	writeFree := 1 - ratio*ratio

	txCap := float64(dsInfo.MaxWriteConns+dsInfo.MinWriteConns) / 2.0 * capacityMultiple
	ratio = clamp01(float64(dsInfo.RunningTx) / txCap)
	txFree := 1 - ratio*ratio

	latScore := 1 - clamp01(math.Log1p(float64(dsInfo.LatencyP95Ms))/math.Log1p(LAT_BAD_MS))
	errScore := 1 - clamp01(dsInfo.ErrorRate1m/ERR_RATE_BAD)
	toutScore := 1 - clamp01(dsInfo.TimeoutRate1m/TIMEOUT_RATE_BAD)

	uptimeScore := clamp01(float64(time.Since(node.UpTime).Seconds()) / UPTIME_OK)

	return normalizedMetrics{
		httpFree: httpFree,

		readFree:  readFree,
		writeFree: writeFree,
		txFree:    txFree,

		latScore:  latScore,
		errScore:  errScore,
		toutScore: toutScore,

		uptimeScore: uptimeScore,
	}
}

func calculateScore(endpoint ENDPOINT_TYPE, m normalizedMetrics) float64 {

	s := 0.05
	if m.httpFree < 0.05 {
		return s
	}

	switch endpoint {
	case EP_Query:
		s = 0.25*m.httpFree +
			0.35*m.readFree +
			0.05*m.writeFree +
			0.05*m.txFree +

			0.15*m.latScore +
			0.05*m.errScore +
			0.08*m.toutScore +

			0.02*m.uptimeScore
	case EP_Execute:
		s = 0.15*m.httpFree +
			0.05*m.readFree +
			0.35*m.writeFree +
			0.15*m.txFree +

			0.05*m.latScore +
			0.15*m.errScore +
			0.08*m.toutScore +

			0.02*m.uptimeScore
	case EP_BeginTx:
		s = 0.15*m.httpFree +
			0.05*m.readFree +
			0.15*m.writeFree +
			0.35*m.txFree +

			0.05*m.latScore +
			0.08*m.errScore +
			0.15*m.toutScore +

			0.02*m.uptimeScore
	}

	return s
}
