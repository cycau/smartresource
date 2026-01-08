package cluster

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

type Balancer struct {
	SelfNode   *NodeInfo  `json:"selfNode"`
	OtherNodes []NodeInfo `json:"otherNodes"`
	semaphore  chan struct{}
	mu         sync.RWMutex
}

/*****************************
 * インスタンス化
 *****************************/
func NewBalancer(selfNode *NodeInfo, otherNodes []NodeInfo) *Balancer {
	return &Balancer{
		SelfNode:   selfNode,
		OtherNodes: otherNodes,
		semaphore:  make(chan struct{}, selfNode.HealthInfo.MaxHttpSessions),
		mu:         sync.RWMutex{},
	}
}

/*****************************
 * 各ノードの health 情報を取得
 *****************************/
func (c *Balancer) CollectHealth() *Balancer {
	for i := range c.OtherNodes {
		node := &c.OtherNodes[i]

		go func(node *NodeInfo) {
			defer node.mu.Unlock()

			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, node.BaseURL+"/healz", nil)
			if err != nil {
				log.Printf("Failed to create request for %s: %v", node.NodeID, err)
				node.mu.Lock()
				node.Status = HEALZERR
				return
			}
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				log.Printf("Failed to do request for %s: %v", node.NodeID, err)
				node.mu.Lock()
				node.Status = HEALZERR
				return
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				log.Printf("Failed to read response body for %s: %v", node.NodeID, err)
				node.mu.Lock()
				node.Status = HEALZERR
				return
			}
			var nodeInfo NodeInfo
			err = json.Unmarshal(body, &nodeInfo)
			if err != nil {
				log.Printf("Failed to unmarshal health info from %s: %v", node.NodeID, err)
				node.mu.Lock()
				node.Status = HEALZERR
				return
			}

			node.mu.Lock()
			node.Status = nodeInfo.Status
			node.HealthInfo = nodeInfo.HealthInfo
		}(node)
	}
	return c
}

/*****************************
 * ノード選出
 *****************************/
func (b *Balancer) SelectNode(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		// エンドポイントタイプを判定
		path := r.URL.Path
		var endpoint endpointType
		if strings.HasSuffix(path, "/query") {
			endpoint = endpointQuery
		} else if strings.HasSuffix(path, "/execute") {
			endpoint = endpointExecute
		} else if strings.HasSuffix(path, "/tx/begin") {
			endpoint = endpointBeginTx
		} else {
			// 対象外のパスはそのまま処理
			next.ServeHTTP(w, r)
			return
		}

		// RunningHttp count
		b.SelfNode.mu.Lock()
		b.SelfNode.HealthInfo.RunningHttp++
		b.SelfNode.mu.Unlock()
		defer func() {
			b.SelfNode.mu.Lock()
			b.SelfNode.HealthInfo.RunningHttp--
			b.SelfNode.mu.Unlock()
		}()

		// トランザクション処理は常に受け入れる
		txID := extractTxId(r)
		if txID != nil {
			next.ServeHTTP(w, r)
			return
		}

		selfNode := b.SelfNode.Clone()

		dbName := extractDbName(r)
		datasources := matchedDatasources(selfNode.HealthInfo.Datasources, dbName)

		// 自分の余力をチェック
		hasCapacity := judgeCapacity(&selfNode, dbName, endpoint)
		if hasCapacity {
			// 余力がある場合は自分で処理
			next.ServeHTTP(w, r)
			return
		}

		otherNodes := make([]NodeInfo, len(b.OtherNodes))
		for i := range b.OtherNodes {
			otherNodes[i] = b.OtherNodes[i].Clone()
		}

		// 中央レイテンシを計算
		medianLatency := calculateMedianLatency(otherNodes)

		// スコア計算
		scores := make([]nodeScore, 0, len(otherNodes))
		for i := range otherNodes {
			node := &otherNodes[i]
			if node.Status != SERVING {
				continue
			}

			m := calculateNormalizedMetrics(node, dbName)

			// ゲート条件チェック
			if !checkGateForEndpoint(node, m, endpoint) {
				continue
			}

			// レイテンシ補正
			m.latScore = adjustLatencyScore(m.latScore, node.HealthInfo.Datasources[0].LatencyP95Ms, medianLatency)

			// スコア計算
			var score float64
			switch endpoint {
			case endpointQuery:
				score = calculateScoreQuery(m)
			case endpointExecute:
				score = calculateScoreExecute(m)
			case endpointBeginTx:
				score = calculateScoreBeginTx(m)
			}

			scores = append(scores, nodeScore{node: node, score: score})
		}

		// ノード選択
		selectedNode := switchNode(scores)
		if selectedNode == nil {
			log.Printf("No candidate nodes available, processing locally despite lack of capacity")
			http.Error(w, "No candidate nodes available and no capacity to process locally", http.StatusServiceUnavailable)
			return
		}

		// リダイレクト
		redirectURL := selectedNode.BaseURL + r.URL.Path
		if r.URL.RawQuery != "" {
			redirectURL += "?" + r.URL.RawQuery + "&dsId=" + b.SelfNode.NodeID
		} else {
			redirectURL += "?dsId=" + b.SelfNode.NodeID
		}

		// 307 Temporary Redirect
		w.Header().Set("Location", redirectURL)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}
	return http.HandlerFunc(fn)
}

// 指定されたdatasourceGroupにマッチするDatasourceInfoを抽出
func matchedDatasources(datasources []DatasourceInfo, dsGroup *string) []DatasourceInfo {
	matched := make([]DatasourceInfo, 0)
	for _, ds := range datasources {
		if ds.DatasourceGroup == *dsGroup {
			matched = append(matched, ds)
		}
	}
	return matched
}

// 自分の余力をチェック
func judgeCapacity(node *NodeInfo, dsId *string, endpoint endpointType) bool {
	if node.Status != "SERVING" {
		return false
	}

	// 正規化指標を計算
	m := calculateNormalizedMetrics(node, dsId)

	if m.dbFree <= 0 {
		return false
	}

	switch endpoint {
	case endpointBeginTx:
		if m.txFree < 0.05 {
			return false
		}
	case endpointQuery, endpointExecute:
		if m.errScore == 0 {
			return false
		}
		if m.latScore == 0 {
			return false
		}
	}
	return true
}

// ノード選択（TopK + Weighted Random）
func switchNode(scores []nodeScore) *NodeInfo {
	if len(scores) == 0 {
		return nil
	}

	// TopKを抽出
	k := TOP_K
	if len(scores) < k {
		k = len(scores)
	}

	// スコアでソート（降順）
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].score > scores[j].score
	})

	topK := scores[:k]

	// 重み付きランダム選択
	totalWeight := 0.0
	for _, ns := range topK {
		weight := ns.score
		if ns.node.Weight > 0 {
			weight *= ns.node.Weight
		}
		totalWeight += weight
	}

	if totalWeight <= 0 {
		return topK[0].node
	}

	r := rand.Float64() * totalWeight
	current := 0.0
	for _, ns := range topK {
		weight := ns.score
		if ns.node.Weight > 0 {
			weight *= ns.node.Weight
		}
		current += weight
		if r <= current {
			return ns.node
		}
	}

	return topK[0].node
}

/*****************************
 * スコア計算
 *****************************/
// 定数定義
const (
	LAT_BAD_MS = 2000.0 // 2秒を"かなり悪い"基準
	ERR_BAD    = 0.05   // 5%をかなり悪い
	TO_BAD     = 20.0   // 1分20回timeoutをかなり悪い
	WAIT_BAD   = 10.0
	UPTIME_OK  = 300.0
	TOP_K      = 3 // スコア上位TopK
)

// clamp01 0〜1にクランプ
func clamp01(x float64) float64 {
	return math.Min(1.0, math.Max(0.0, x))
}

// log1p log(1+x)
func log1p(x float64) float64 {
	return math.Log1p(x)
}

// 正規化された指標を計算
type normalizedMetrics struct {
	httpFree    float64
	dbFree      float64
	txFree      float64
	latScore    float64
	errScore    float64
	toScore     float64
	waitScore   float64
	idleScore   float64
	uptimeScore float64
}

func calculateNormalizedMetrics(node *NodeInfo, dsId *string) normalizedMetrics {
	node.mu.RLock()
	defer node.mu.RUnlock()

	var dsInfo DatasourceInfo
	for _, ds := range node.HealthInfo.Datasources {
		if dsId != nil && ds.DatasourceID == *dsId {
			// 指定されたdatasourceIdが存在する場合、その情報を使う
			dsInfo = ds
			break
		}
	}
	if dsId == nil {
		return normalizedMetrics{}
	}

	httpUsage := float64(node.HealthInfo.RunningHttp) / float64(node.HealthInfo.MaxHttpSessions)
	dbUsage := float64(dsInfo.OpenConns) / float64(dsInfo.MaxOpenConns)
	txUsage := float64(dsInfo.RunningTx) / float64(dsInfo.MaxTxConns)

	httpFree := 1 - clamp01(httpUsage)
	dbFree := 1 - clamp01(dbUsage)
	txFree := 1 - clamp01(txUsage)

	// 品質指標の正規化
	latScore := 1 - clamp01(log1p(float64(dsInfo.LatencyP95Ms))/log1p(LAT_BAD_MS))
	errScore := 1 - clamp01(dsInfo.ErrorRate1m/ERR_BAD)
	toScore := 1 - clamp01(float64(dsInfo.Timeouts1m)/TO_BAD)
	waitScore := 1 - clamp01(float64(dsInfo.WaitConns)/WAIT_BAD)
	idleScore := clamp01(float64(dsInfo.IdleConns) / float64(dsInfo.MaxOpenConns))
	uptimeScore := clamp01(float64(node.HealthInfo.UptimeSec) / UPTIME_OK)

	return normalizedMetrics{
		httpFree:    httpFree,
		dbFree:      dbFree,
		txFree:      txFree,
		latScore:    latScore,
		errScore:    errScore,
		toScore:     toScore,
		waitScore:   waitScore,
		idleScore:   idleScore,
		uptimeScore: uptimeScore,
	}
}

// 相対レイテンシ補正
func adjustLatencyScore(latScore float64, p95LatencyMs int, medianLatency float64) float64 {
	if medianLatency <= 0 {
		return latScore
	}
	latRatio := float64(p95LatencyMs) / medianLatency
	K := 2.0 // K = 1.5〜3の間で選択
	return 1.0 / (1.0 + (latRatio-1.0)*K)
}

// 用途別スコア計算
func calculateScoreQuery(m normalizedMetrics) float64 {
	return 0.22*m.dbFree +
		0.18*m.httpFree +
		0.10*m.txFree +
		0.20*m.latScore +
		0.12*m.errScore +
		0.08*m.toScore +
		0.06*m.waitScore +
		0.02*m.idleScore +
		0.02*m.uptimeScore
}

func calculateScoreExecute(m normalizedMetrics) float64 {
	return 0.30*m.dbFree +
		0.14*m.httpFree +
		0.08*m.txFree +
		0.14*m.latScore +
		0.14*m.errScore +
		0.10*m.toScore +
		0.08*m.waitScore +
		0.02*m.idleScore
}

func calculateScoreBeginTx(m normalizedMetrics) float64 {
	return 0.42*m.txFree +
		0.22*m.dbFree +
		0.08*m.httpFree +
		0.10*m.errScore +
		0.06*m.toScore +
		0.06*m.waitScore +
		0.04*m.latScore +
		0.02*m.idleScore
}

// ノードスコア情報
type nodeScore struct {
	node  *NodeInfo
	score float64
}

// 候補ノードの中央レイテンシを計算
func calculateMedianLatency(nodes []NodeInfo) float64 {
	if len(nodes) == 0 {
		return 0
	}
	latencies := make([]float64, 0, len(nodes))
	for _, node := range nodes {
		if node.HealthInfo.Datasources[0].LatencyP95Ms > 0 {
			latencies = append(latencies, float64(node.HealthInfo.Datasources[0].LatencyP95Ms))
		}
	}
	if len(latencies) == 0 {
		return 0
	}
	sort.Float64s(latencies)
	mid := len(latencies) / 2
	if len(latencies)%2 == 0 {
		return (latencies[mid-1] + latencies[mid]) / 2.0
	}
	return latencies[mid]
}

/*****************************
 * ゲート条件チェック
 *****************************/
type endpointType int

const (
	endpointQuery endpointType = iota
	endpointExecute
	endpointBeginTx
)

func checkGateForEndpoint(node *NodeInfo, m normalizedMetrics, endpoint endpointType) bool {
	if node.Status != "SERVING" {
		return false
	}
	if m.dbFree <= 0 {
		return false
	}

	switch endpoint {
	case endpointBeginTx:
		if m.txFree < 0.05 {
			return false
		}
		// if node.HealthInfo.Datasources[0].WaitConns >= 20 {
		// 	return false
		// }
	case endpointQuery, endpointExecute:
		if m.errScore == 0 {
			return false
		}
		if m.latScore == 0 {
			return false
		}
	}
	return true
}

/*****************************
 * リクエスト情報抽出
 *****************************/
// リクエストからtxIdを取得
func extractTxId(r *http.Request) *string {
	query := r.URL.Query()
	if txID := query.Get("txId"); txID != "" {
		return &txID
	}
	return nil
}

// リクエストからdatasourceIdを取得
func extractDsId(r *http.Request) *string {
	query := r.URL.Query()
	if dsId := query.Get("dsId"); dsId != "" {
		return &dsId
	}
	return nil
}

// リクエストからdatabaseNameを取得
func extractDbName(r *http.Request) *string {
	query := r.URL.Query()
	if dbName := query.Get("dbName"); dbName != "" {
		return &dbName
	}
	return nil
}
