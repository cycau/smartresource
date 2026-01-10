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

// 定数定義
const (
	LAT_BAD_MS      = 2000.0 // 2秒を"かなり悪い"基準
	ERR_BAD         = 0.05   // 5%をかなり悪い
	TO_BAD          = 20.0   // 1分20回timeoutをかなり悪い
	WAIT_BAD        = 10.0
	UPTIME_OK       = 300.0
	TOP_K           = 3   // スコア上位TopK
	USAGE_THRESHOLD = 0.8 // 使用率80%以下なら自分で処理、それ以外は他ノードとの協調で処理
)

type ENDPOINT_TYPE int

const (
	EP_Query ENDPOINT_TYPE = iota
	EP_Execute
	EP_BeginTx
	EP_Other
)

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
		// RunningHttp count
		b.SelfNode.mu.Lock()
		b.SelfNode.HealthInfo.RunningHttp++
		if b.SelfNode.HealthInfo.RunningHttp > b.SelfNode.HealthInfo.MaxHttpSessions {
			b.SelfNode.mu.Unlock()
			http.Error(w, "Too many requests", http.StatusServiceUnavailable)
			return
		}
		b.SelfNode.mu.Unlock()
		defer func() {
			b.SelfNode.mu.Lock()
			b.SelfNode.HealthInfo.RunningHttp--
			b.SelfNode.mu.Unlock()
		}()

		endpoint, dbName, txId := extractRequest(r)

		// エンドポイントタイプを判定
		if *endpoint == EP_Other {
			// 対象外のパスはそのまま処理
			next.ServeHTTP(w, r)
			return
		}

		// トランザクション処理は常に受け入れる
		if txId != nil {
			next.ServeHTTP(w, r)
			return
		}

		selfNode := b.SelfNode.Clone()

		// 使用率80%以下なら自分で処理、それ以外は他ノードとの協調で処理
		selfDatasource, selfScore := selectDatasource(&selfNode, dbName, *endpoint)
		if selfDatasource != nil {
			next.ServeHTTP(w, r) // 他ノードとの協調で処理
			return
		}
		// Weight抽選で、自分のScoreの方が高い場合は自分で処理
		// それ以外はredirect、Max３回か、自分にリダイレクトされた場合は終了（Client側実装）

		// 他ノードのデータソース合計数を取得
		maxDatasourceNum := 0
		otherNodes := make([]NodeInfo, len(b.OtherNodes))
		for i := range b.OtherNodes {
			otherNodes[i] = b.OtherNodes[i].Clone()
			maxDatasourceNum += len(b.OtherNodes[i].HealthInfo.Datasources)
		}

		// 中央レイテンシを計算
		medianLatency := calculateMedianLatency(otherNodes, dbName)

		// スコア計算
		scores := make([]scoreInfo, 0, maxDatasourceNum)
		for nodeIdx := range otherNodes {
			node := &otherNodes[nodeIdx]
			if node.Status != SERVING {
				continue
			}

			for dsIdx := range node.HealthInfo.Datasources {
				if node.HealthInfo.Datasources[dsIdx].Active {
					continue
				}
				if node.HealthInfo.Datasources[dsIdx].DatabaseName != *dbName {
					continue
				}
				if node.HealthInfo.Datasources[dsIdx].Readonly && (*endpoint == EP_Execute || *endpoint == EP_BeginTx) {
					continue
				}

				m := calculateDatasourceMetrics(node, dsIdx)
				// ゲート条件チェック
				if !checkGate(m, *endpoint) {
					continue
				}
				// レイテンシ補正
				m.latScore = adjustLatencyScore(m.latScore, node.HealthInfo.Datasources[dsIdx].LatencyP95Ms, medianLatency)

				// スコア計算
				score := calculateScore(m, *endpoint)
				scores = append(scores, scoreInfo{score: score, weight: node.Weight, index: nodeIdx})
			}

		}

		// ノード選択
		_, weightedRandomNodeScore := selectBestWithWeight(scores)
		if weightedRandomNodeScore == nil {
			if selfScore.score > 0 {
				next.ServeHTTP(w, r)
				return
			}
			log.Printf("No candidate nodes available, processing locally despite lack of capacity")
			http.Error(w, "No candidate nodes available and no capacity to process locally", http.StatusServiceUnavailable)
			return
		}
		if selfScore.score > weightedRandomNodeScore.score {
			next.ServeHTTP(w, r)
			return
		}

		selectedNode := &otherNodes[weightedRandomNodeScore.index]

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

// 80%以下なら自分で処理、それ以外は他ノードとの協調を試みる
func selectDatasource(node *NodeInfo, databaseName *string, endpoint ENDPOINT_TYPE) (*DatasourceInfo, *scoreInfo) {
	if node.Status != SERVING {
		return nil, nil
	}
	if float64(node.HealthInfo.RunningHttp)/float64(node.HealthInfo.MaxHttpSessions) >= USAGE_THRESHOLD {
		return nil, nil
	}

	isWrite := false
	if endpoint == EP_Execute || endpoint == EP_BeginTx {
		isWrite = true
	}

	scores := make([]scoreInfo, 0, len(node.HealthInfo.Datasources))
	for idx, ds := range node.HealthInfo.Datasources {
		if !ds.Active {
			continue
		}
		if ds.DatabaseName != *databaseName {
			continue
		}
		if isWrite && ds.Readonly {
			continue
		}
		if endpoint != EP_BeginTx {
			if float64(ds.OpenConns)/float64(ds.MaxOpenConns) >= USAGE_THRESHOLD {
				continue
			}
		} else {
			if float64(ds.RunningTx)/float64(ds.MaxTxConns) >= USAGE_THRESHOLD {
				continue
			}
		}
		// 正規化指標を計算
		m := calculateDatasourceMetrics(node, idx)
		score := calculateScore(m, endpoint)
		weight := 0.0
		switch endpoint {
		case EP_Query:
			weight = float64(ds.MaxOpenConns)
		case EP_Execute:
			weight = float64(ds.MaxOpenConns)
		case EP_BeginTx:
			weight = float64(ds.MaxTxConns)
		}
		scores = append(scores, scoreInfo{score: score, weight: weight, index: idx})
	}

	_, weightedRandomScore := selectBestWithWeight(scores)
	return &node.HealthInfo.Datasources[weightedRandomScore.index], weightedRandomScore
}

// ノード選択（TopK + Weighted Random）
// return: BestScore, WeightedRandomScore
func selectBestWithWeight(scores []scoreInfo) (*scoreInfo, *scoreInfo) {
	if len(scores) == 0 {
		return nil, nil
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
	for _, sc := range topK {
		weight := sc.score
		if sc.weight > 0 {
			weight *= sc.weight
		}
		totalWeight += weight
	}

	if totalWeight <= 0 {
		return nil, nil
	}

	r := rand.Float64() * totalWeight
	current := 0.0
	for _, sc := range topK {
		weight := sc.score
		if sc.weight > 0 {
			weight *= sc.weight
		}
		current += weight
		if r <= current {
			return &topK[0], &sc
		}
	}

	return nil, nil
}

/*****************************
 * スコア計算
 *****************************/
// clamp01 0〜1にクランプ
func clamp01(x float64) float64 {
	return math.Min(1.0, math.Max(0.0, x))
}

// log1p log(1+x)
func log1p(x float64) float64 {
	return math.Log1p(x)
}

// 正規化された指標を計算
type datasourceMetrics struct {
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

func calculateDatasourceMetrics(node *NodeInfo, dsIdx int) datasourceMetrics {
	dsInfo := &node.HealthInfo.Datasources[dsIdx]
	if !dsInfo.Active {
		return datasourceMetrics{}
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
	uptimeScore := clamp01(float64(time.Since(node.HealthInfo.UpTime).Seconds()) / UPTIME_OK)

	return datasourceMetrics{
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
// latScore: 元のレイテンシスコア（0〜1の範囲）
// p95LatencyMs: このノードのP95レイテンシ（ミリ秒）
// medianLatency: 候補ノードの中央レイテンシ（ミリ秒）
// 返り値: 補正後のレイテンシスコア（0〜1の範囲を保証）
//
// 補正ロジック:
// - 中央値と同じレイテンシ → 補正係数1.0（変化なし）
// - 中央値より遅い → 補正係数 < 1.0（ペナルティ）
// - 中央値より速い → 補正係数 > 1.0（ボーナス、最大1.5倍まで）
// - 元のlatScoreに補正係数を乗算し、0〜1の範囲にクランプ
func adjustLatencyScore(latScore float64, p95LatencyMs int, medianLatency float64) float64 {
	// 中央値が無効な場合は元のスコアを返す
	if medianLatency <= 0 {
		return latScore
	}

	// P95レイテンシが0以下の場合は元のスコアを返す（異常値の可能性）
	if p95LatencyMs <= 0 {
		return latScore
	}

	// 相対レイテンシ比を計算
	latRatio := float64(p95LatencyMs) / medianLatency

	// 補正係数K（1.5〜3の間、現在は2.0）
	K := 2.0

	// 相対補正係数を計算
	// 元の式: 1.0 / (1.0 + (latRatio-1.0)*K)
	// この式は latRatio < 0.5 の場合に分母が負になる可能性がある
	//
	// 安全な計算: 分母が常に正になるようにする
	// latRatio < 0.5 の場合、1.0 + (latRatio-1.0)*K = 1.0 + (負の値)*K となり負になる可能性がある
	// 例: latRatio=0.4, K=2.0 → 1.0 + (0.4-1.0)*2.0 = 1.0 - 1.2 = -0.2

	// より安全な式: 分母を絶対値で保護し、最小値を設定
	denominator := 1.0 + (latRatio-1.0)*K

	// 分母が0以下になるのを防ぐ（異常に速いノードの場合）
	// この場合、最大ボーナス（1.5倍）を適用
	if denominator <= 0 {
		adjusted := latScore * 1.5
		return clamp01(adjusted)
	}

	// 通常の補正係数を計算
	relativeAdjustment := 1.0 / denominator

	// 補正係数の範囲を制限
	// - 最小値: 0.1（過度なペナルティを防ぐ）
	// - 最大値: 1.5（過度なボーナスを防ぐ）
	if relativeAdjustment < 0.1 {
		relativeAdjustment = 0.1
	} else if relativeAdjustment > 1.5 {
		relativeAdjustment = 1.5
	}

	// 元のlatScoreに相対補正を適用
	// 結果は0〜1の範囲にクランプ
	adjusted := latScore * relativeAdjustment
	return clamp01(adjusted)
}

// 用途別スコア計算
func calculateScore(m datasourceMetrics, endpoint ENDPOINT_TYPE) float64 {
	switch endpoint {
	case EP_Query:
		return 0.22*m.dbFree +
			0.18*m.httpFree +
			0.10*m.txFree +
			0.20*m.latScore +
			0.12*m.errScore +
			0.08*m.toScore +
			0.06*m.waitScore +
			0.02*m.idleScore +
			0.02*m.uptimeScore
	case EP_Execute:
		return 0.30*m.dbFree +
			0.14*m.httpFree +
			0.08*m.txFree +
			0.14*m.latScore +
			0.14*m.errScore +
			0.10*m.toScore +
			0.08*m.waitScore +
			0.02*m.idleScore
	case EP_BeginTx:
		return 0.42*m.txFree +
			0.22*m.dbFree +
			0.08*m.httpFree +
			0.10*m.errScore +
			0.06*m.toScore +
			0.06*m.waitScore +
			0.04*m.latScore +
			0.02*m.idleScore
	}
	return 0
}

// ノードスコア情報
type scoreInfo struct {
	score  float64
	weight float64
	index  int
}

// 候補ノードの中央レイテンシを計算
func calculateMedianLatency(nodes []NodeInfo, dbName *string) float64 {
	if len(nodes) == 0 {
		return 0
	}
	latencies := make([]float64, 0, len(nodes))
	for nodeIdx := range nodes {
		node := &nodes[nodeIdx]
		for dsIdx := range node.HealthInfo.Datasources {
			ds := &node.HealthInfo.Datasources[dsIdx]
			if !ds.Active {
				continue
			}
			if ds.DatabaseName != *dbName {
				continue
			}
			if ds.LatencyP95Ms > 0 {
				latencies = append(latencies, float64(ds.LatencyP95Ms))
			}
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
func checkGate(m datasourceMetrics, endpoint ENDPOINT_TYPE) bool {
	if m.dbFree <= 0 {
		return false
	}

	switch endpoint {
	case EP_BeginTx:
		if m.txFree < 0.05 {
			return false
		}
	case EP_Query, EP_Execute:
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
// get: endpoint, dbName, txId
func extractRequest(r *http.Request) (*ENDPOINT_TYPE, *string, *string) {

	path := r.URL.Path
	var endpoint ENDPOINT_TYPE
	if strings.HasSuffix(path, "/query") {
		endpoint = EP_Query
	} else if strings.HasSuffix(path, "/execute") {
		endpoint = EP_Execute
	} else if strings.HasSuffix(path, "/tx/begin") {
		endpoint = EP_BeginTx
	} else {
		endpoint = EP_Other
	}

	query := r.URL.Query()

	dbName := query.Get("dbName")
	txId := query.Get("txId")

	return &endpoint, &dbName, &txId
}
