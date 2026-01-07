package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// collectClusterHealthInfo 格納用
type Balancer struct {
	SelfNode   *NodeInfo  `json:"selfNode"`
	OtherNodes []NodeInfo `json:"otherNodes"`
	semaphore  chan struct{}
	mu         sync.RWMutex
}

func NewBalancer(selfNode *NodeInfo, otherNodes []NodeInfo) *Balancer {
	return &Balancer{
		SelfNode:   selfNode,
		OtherNodes: otherNodes,
		semaphore:  make(chan struct{}, selfNode.HealthInfo.MaxHttpSessions),
		mu:         sync.RWMutex{},
	}
}

// config.cluster から各ノードの health 情報を取得
func (c *Balancer) CollectHealth() *Balancer {
	c.mu.Lock()
	nodes := make([]NodeInfo, len(c.OtherNodes))
	copy(nodes, c.OtherNodes)
	c.mu.Unlock()

	for i := range nodes {
		node := &nodes[i]
		// TODO: 300ms超えたらエラーとする
		timeout := 300 * time.Millisecond // 300ms
		go func(timeout time.Duration, node *NodeInfo) {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, node.BaseURL+"/health", nil)
			if err != nil {
				log.Printf("Failed to create request for %s: %v", node.NodeID, err)
				return
			}
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				log.Printf("Failed to do request for %s: %v", node.NodeID, err)
				return
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				log.Printf("Failed to read response body for %s: %v", node.NodeID, err)
				return
			}
			var healthInfo NodeInfo
			err = json.Unmarshal(body, &healthInfo)
			if err != nil {
				log.Printf("Failed to unmarshal health info from %s: %v", node.NodeID, err)
				return
			}
			c.mu.Lock()
			for j := range c.OtherNodes {
				if c.OtherNodes[j].NodeID == node.NodeID {
					c.OtherNodes[j] = healthInfo
					break
				}
			}
			c.mu.Unlock()
		}(timeout, node)
	}
	return c
}

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

func calculateNormalizedMetrics(h NodeInfo, maxHttpSessions, maxOpenConns, maxTransactionConns int) normalizedMetrics {
	httpUsage := float64(h.HealthInfo.RunningHttp) / float64(maxHttpSessions)
	dbUsage := float64(h.HealthInfo.DatasourceInfo[0].OpenConns) / float64(maxOpenConns)
	txUsage := float64(h.HealthInfo.DatasourceInfo[0].RunningTx) / float64(maxTransactionConns)

	httpFree := 1 - clamp01(httpUsage)
	dbFree := 1 - clamp01(dbUsage)
	txFree := 1 - clamp01(txUsage)

	// 品質指標の正規化
	latScore := 1 - clamp01(log1p(float64(h.HealthInfo.DatasourceInfo[0].P95LatencyMs))/log1p(LAT_BAD_MS))
	errScore := 1 - clamp01(h.HealthInfo.DatasourceInfo[0].ErrorRate1m/ERR_BAD)
	toScore := 1 - clamp01(float64(h.HealthInfo.DatasourceInfo[0].Timeouts1m)/TO_BAD)
	waitScore := 1 - clamp01(float64(h.HealthInfo.DatasourceInfo[0].WaitConns)/WAIT_BAD)
	idleScore := clamp01(float64(h.HealthInfo.DatasourceInfo[0].IdleConns) / float64(maxOpenConns))
	uptimeScore := clamp01(float64(h.HealthInfo.UptimeSec) / UPTIME_OK)

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

// スコア計算（用途別）
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

// ゲート条件チェック
type endpointType int

const (
	endpointQuery endpointType = iota
	endpointExecute
	endpointBeginTx
)

func checkCommonGate(h NodeInfo, m normalizedMetrics) bool {
	if h.Status != "SERVING" {
		return false
	}
	if m.dbFree <= 0 {
		return false
	}
	return true
}

func checkGateForEndpoint(h NodeInfo, m normalizedMetrics, endpoint endpointType) bool {
	if !checkCommonGate(h, m) {
		return false
	}

	switch endpoint {
	case endpointBeginTx:
		if m.txFree < 0.05 {
			return false
		}
		if h.HealthInfo.DatasourceInfo[0].WaitConns >= 20 {
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

// ノードスコア情報
type nodeScore struct {
	node  NodeInfo
	score float64
}

// 候補ノードの中央レイテンシを計算
func calculateMedianLatency(nodes []NodeInfo) float64 {
	if len(nodes) == 0 {
		return 0
	}
	latencies := make([]float64, 0, len(nodes))
	for _, node := range nodes {
		if node.HealthInfo.DatasourceInfo[0].P95LatencyMs > 0 {
			latencies = append(latencies, float64(node.HealthInfo.DatasourceInfo[0].P95LatencyMs))
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

// ノード選択（TopK + Weighted Random）
func selectNode(scores []nodeScore) *NodeInfo {
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
		return &topK[0].node
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
			return &ns.node
		}
	}

	return &topK[0].node
}

// リクエストボディからTxIDを取得
func extractTxIDFromRequest(r *http.Request) (*string, error) {
	// リクエストボディを読み取る（後で再利用するため）
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	var reqBody map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &reqBody); err != nil {
		return nil, nil // JSONパースエラーは無視（TxIDがない可能性）
	}

	if txID, ok := reqBody["txId"].(string); ok && txID != "" {
		return &txID, nil
	}
	return nil, nil
}

// 自分の余力をチェック
func (b *Balancer) hasCapacity(selfHealth *NodeInfo, endpoint endpointType) bool {
	if selfHealth == nil {
		return false
	}

	m := calculateNormalizedMetrics(*selfHealth, selfHealth.HealthInfo.MaxHttpSessions, selfHealth.HealthInfo.DatasourceInfo[0].MaxOpenConns, selfHealth.HealthInfo.DatasourceInfo[0].MaxTxConns)

	// 簡単な余力チェック（ゲート条件を満たしているか）
	return checkGateForEndpoint(*selfHealth, m, endpoint)
}

func (b *Balancer) DoOrSwitchNode(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		// RunningHttp count
		b.SelfNode.HealthInfo.RunningHttp++
		defer func() {
			b.SelfNode.HealthInfo.RunningHttp--
		}()

		// トランザクション処理は常に受け入れる
		txID, err := extractTxIDFromRequest(r)
		if err == nil && txID != nil && *txID != "" {
			next.ServeHTTP(w, r)
			return
		}

		rebalanceCnt, err := strconv.Atoi(r.URL.Query().Get("rebalanceCnt"))
		if err != nil {
			rebalanceCnt = 0
		}
		if rebalanceCnt >= 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte("Too many redirects"))
			return
		}

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

		// 自分のヘルス情報を取得
		selfHealth := b.SelfNode

		// 自分の余力をチェック
		if b.hasCapacity(selfHealth, endpoint) {
			// 余力がある場合は自分で処理
			next.ServeHTTP(w, r)
			return
		}

		// 余力がない場合、他のノードを評価
		b.mu.RLock()
		candidates := make([]NodeInfo, 0, len(b.OtherNodes))
		for _, node := range b.OtherNodes {
			if node.NodeID != selfHealth.NodeID && node.NodeID != "" {
				candidates = append(candidates, node)
			}
		}
		b.mu.RUnlock()

		if len(candidates) == 0 {
			// 候補がない場合は自分で処理
			next.ServeHTTP(w, r)
			return
		}

		// 中央レイテンシを計算
		medianLatency := calculateMedianLatency(candidates)

		// スコア計算
		scores := make([]nodeScore, 0, len(candidates))
		for _, node := range candidates {
			m := calculateNormalizedMetrics(node, node.HealthInfo.MaxHttpSessions, node.HealthInfo.DatasourceInfo[0].MaxOpenConns, node.HealthInfo.DatasourceInfo[0].MaxTxConns)

			// ゲート条件チェック
			if !checkGateForEndpoint(node, m, endpoint) {
				continue
			}

			// レイテンシ補正
			adjustedLatScore := adjustLatencyScore(m.latScore, node.HealthInfo.DatasourceInfo[0].P95LatencyMs, medianLatency)
			m.latScore = adjustedLatScore

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

		if len(scores) == 0 {
			// 候補がない場合は自分で処理
			next.ServeHTTP(w, r)
			return
		}

		// ノード選択
		selectedNode := selectNode(scores)
		if selectedNode == nil {
			// 選択できない場合は自分で処理
			next.ServeHTTP(w, r)
			return
		}

		// リダイレクト
		redirectURL := selectedNode.BaseURL + r.URL.Path
		if r.URL.RawQuery != "" {
			redirectURL += "?" + r.URL.RawQuery
		}
		// rebalanceCount queryが存在したら削除
		if rebalanceCnt > 0 {
			queryString := "rebalanceCnt=" + strconv.Itoa(rebalanceCnt)
			idx := strings.Index(redirectURL, queryString)
			if idx != -1 {
				redirectURL = redirectURL[idx-1:] + redirectURL[:idx+len(queryString)]
			}
		}
		// 307 Temporary Redirect
		redirectURL += "&rebalanceCnt=" + strconv.Itoa(rebalanceCnt+1)
		w.Header().Set("Location", redirectURL)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}
	return http.HandlerFunc(fn)
}
