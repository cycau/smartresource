package cluster

import (
	"math"
	"strings"
	"sync"
	"time"

	"smartdatastream/server/global"
	. "smartdatastream/server/global"

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

	OpenConns    int `json:"openConns"`
	IdleConns    int `json:"idleConns"`
	RunningQuery int `json:"runningQuery"`
	RunningTx    int `json:"runningTx"`

	LatencyP95Ms  int       `json:"latencyP95Ms"`
	ErrorRate1m   float64   `json:"errorRate1m"`
	TimeoutRate1m float64   `json:"timeoutRate1m"`
	collectTime   time.Time `json:"-"`

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

/*****************************
 * initialize Balancer
 *****************************/
const STAT_WINDOW_TIME = 5 * time.Minute

func NewDatasourceInfo(config global.DatasourceConfig) *DatasourceInfo {
	var statLatency = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Objectives: map[float64]float64{0.95: 0.01},
		MaxAge:     STAT_WINDOW_TIME,
	}, []string{"latency"})

	return &DatasourceInfo{
		DatasourceID: config.DatasourceID,
		DatabaseName: config.DatabaseName,
		Active:       true,
		Readonly:     config.Readonly,
		MaxOpenConns: config.MaxOpenConns,
		MinIdleConns: config.MinIdleConns,
		MaxTxConns:   config.MaxTxConns,

		StatLatency:  statLatency,
		StatTotal:    ratecounter.NewRateCounter(STAT_WINDOW_TIME),
		StatErrors:   ratecounter.NewRateCounter(STAT_WINDOW_TIME),
		StatTimeouts: ratecounter.NewRateCounter(STAT_WINDOW_TIME),
		collectTime:  time.Now().Add(-STAT_WINDOW_TIME),
	}
}

// 必要の場合はatomic.Pointerでラップして使うこと
func (node *NodeInfo) Clone() NodeInfo {
	node.Mu.Lock()
	defer node.Mu.Unlock()

	datasources := make([]DatasourceInfo, len(node.HealthInfo.Datasources))
	for idx, dsInfo := range node.HealthInfo.Datasources {
		latencyP95Ms, errorRate1m, timeoutRate1m := collectStat(dsInfo)
		dsInfo.LatencyP95Ms = latencyP95Ms
		dsInfo.ErrorRate1m = errorRate1m
		dsInfo.TimeoutRate1m = timeoutRate1m
		datasources[idx] = dsInfo
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

/*****************************
 * スコア計算
 *****************************/
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

// 定数定義
type ENDPOINT_TYPE int

const (
	EP_Query ENDPOINT_TYPE = iota
	EP_Execute
	EP_BeginTx
	EP_Other
)

const (
	LAT_BAD_MS       = 3000.0 // 3秒を"かなり悪い"基準
	ERR_RATE_BAD     = 0.05   // 1分5%をかなり悪い
	TIMEOUT_RATE_BAD = 0.05   // 1分5%をかなり悪い
	UPTIME_OK        = 300.0  // サーバー起動から5分以上でOK
	TOP_K            = 3      // スコア上位TopK
	USAGE_THRESHOLD  = 0.8    // 使用率80%以下なら自分で処理、それ以外は他ノードとの協調で処理
)

// 正規化された指標を計算
type normalizedMetrics struct {
	httpFree    float64
	dbFree      float64
	txFree      float64
	idleScore   float64
	latScore    float64
	errScore    float64
	toutScore   float64
	uptimeScore float64
}

// Node score information
type ScoreWithWeight struct {
	score   float64
	weight  float64
	exIndex int
}

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

// clamp01 0〜1にクランプ
func clamp01(x float64) float64 {
	return math.Min(1.0, math.Max(0.0, x))
}

// スコア計算
func (node *NodeInfo) GetScore(dsIdx int, tarDbName string, endpoint ENDPOINT_TYPE) *ScoreWithWeight {

	if node.Status != SERVING {
		return nil
	}
	if node.HealthInfo.RunningHttp >= node.HealthInfo.MaxHttpQueue {
		return nil
	}

	dsInfo := node.HealthInfo.Datasources[dsIdx]

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
		weight = float64(dsInfo.MaxOpenConns - dsInfo.MaxTxConns)
	case EP_Execute:
		weight = float64(dsInfo.MaxOpenConns - dsInfo.MaxTxConns/2)
	case EP_BeginTx:
		weight = float64(dsInfo.MaxTxConns)
	default:
		weight = float64(dsInfo.MaxOpenConns)
	}

	return &ScoreWithWeight{score: score, weight: weight, exIndex: dsIdx}
}

const STAT_COLLECT_INTERVAL = 2 * time.Second

func collectStat(dsInfo DatasourceInfo) (latencyP95Ms int, errorRate1m float64, timeoutRate1m float64) {

	// 品質指標の正規化
	if dsInfo.StatLatency == nil {
		return dsInfo.LatencyP95Ms, dsInfo.ErrorRate1m, dsInfo.TimeoutRate1m
	}

	if time.Since(dsInfo.collectTime) < STAT_COLLECT_INTERVAL {
		return dsInfo.LatencyP95Ms, dsInfo.ErrorRate1m, dsInfo.TimeoutRate1m
	}

	// collecte when self node
	latency := &dto.Metric{}
	dsInfo.StatLatency.WithLabelValues("p95").(prometheus.Metric).Write(latency)
	p95 := latency.GetSummary().GetQuantile()[0].GetValue()
	if math.IsNaN(p95) {
		p95 = 16.0
	}

	total := float64(dsInfo.StatTotal.Rate())
	if total > 0 {
		errorRate1m = float64(dsInfo.StatErrors.Rate()) / total
		timeoutRate1m = float64(dsInfo.StatTimeouts.Rate()) / total
	}

	dsInfo.collectTime = time.Now()
	return int(p95), errorRate1m, timeoutRate1m
}

func calculateMetrics(node *NodeInfo, dsInfo DatasourceInfo) normalizedMetrics {

	httpUsage := float64(node.HealthInfo.RunningHttp) / float64(node.HealthInfo.MaxHttpQueue)
	dbUsage := float64(dsInfo.RunningQuery) / float64(dsInfo.MaxOpenConns)
	txUsage := 1.0
	if dsInfo.MaxTxConns > 0 {
		txUsage = float64(dsInfo.RunningTx) / float64(dsInfo.MaxTxConns)
	}

	httpFree := 1 - clamp01(httpUsage)
	dbFree := 1 - clamp01(dbUsage)
	txFree := 1 - clamp01(txUsage)
	idleScore := clamp01(float64(dsInfo.IdleConns) / float64(dsInfo.OpenConns))
	uptimeScore := clamp01(float64(time.Since(node.HealthInfo.UpTime).Seconds()) / UPTIME_OK)

	latencyP95Ms, errorRate1m, timeoutRate1m := collectStat(dsInfo)

	latScore := 1 - clamp01(math.Log1p(float64(latencyP95Ms))/math.Log1p(LAT_BAD_MS))
	errScore := 1 - clamp01(errorRate1m/ERR_RATE_BAD)
	toutScore := 1 - clamp01(timeoutRate1m/TIMEOUT_RATE_BAD)

	return normalizedMetrics{
		httpFree:    httpFree,
		dbFree:      dbFree,
		txFree:      txFree,
		idleScore:   idleScore,
		latScore:    latScore,
		errScore:    errScore,
		toutScore:   toutScore,
		uptimeScore: uptimeScore,
	}
}

func calculateScore(endpoint ENDPOINT_TYPE, m normalizedMetrics) float64 {

	s := 0.0
	if m.httpFree < 0.01 {
		return s
	}

	switch endpoint {
	case EP_Query:
		s = 0.25*m.dbFree +
			0.00*m.txFree +
			0.20*m.httpFree +
			0.12*m.idleScore +

			0.20*m.latScore +
			0.12*m.errScore +
			0.08*m.toutScore +

			0.03*m.uptimeScore
	case EP_Execute:
		s = 0.25*m.dbFree +
			0.05*m.txFree +
			0.15*m.httpFree +
			0.12*m.idleScore +

			0.10*m.latScore +
			0.15*m.errScore +
			0.15*m.toutScore +

			0.03*m.uptimeScore
	case EP_BeginTx:
		s = 0.25*m.dbFree +
			0.25*m.txFree +
			0.10*m.httpFree +
			0.12*m.idleScore +

			0.05*m.latScore +
			0.20*m.errScore +
			0.15*m.toutScore +

			0.03*m.uptimeScore
	}

	return s
}
