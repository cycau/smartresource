package cluster

import (
	"context"
	"log"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"time"
)

type Balancer struct {
	SelfNode   *NodeInfo  `json:"selfNode"`
	OtherNodes []NodeInfo `json:"otherNodes"`
}

// 定数定義
const (
	LAT_BAD_MS      = 2000.0 // 2秒を"かなり悪い"基準
	ERR_BAD         = 0.05   // 5%をかなり悪い
	TIMEOUT_BAD     = 20.0   // 1分20回timeoutをかなり悪い
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
	}
}

/*****************************
 * ノード選出
 *****************************/
func (b *Balancer) SelectNode(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		// RunningHttp count
		b.SelfNode.Mu.Lock()
		b.SelfNode.HealthInfo.RunningHttp++
		if b.SelfNode.HealthInfo.RunningHttp > b.SelfNode.HealthInfo.MaxHttpSessions {
			b.SelfNode.Mu.Unlock()
			http.Error(w, "Too many requests", http.StatusServiceUnavailable)
			return
		}
		b.SelfNode.Mu.Unlock()
		defer func() {
			b.SelfNode.Mu.Lock()
			b.SelfNode.HealthInfo.RunningHttp--
			b.SelfNode.Mu.Unlock()
		}()

		endpoint, tarTbName, txId := parseRequest(r)

		// 対象外のパスはそのまま処理
		if endpoint == EP_Other {
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
		// Weight抽選で、自分のScoreの方が高い場合は自分で処理
		// それ以外はredirect、Max３回か、自分にリダイレクトされた場合は終了（Client側実装）
		selfBestScore, recommendedDatasource := selectSelfDatasource(&selfNode, tarTbName, endpoint)
		if recommendedDatasource != nil {
			// add to context
			ctx := context.WithValue(r.Context(), "DS_IDX", recommendedDatasource.index)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		totalDatasources := 0
		otherNodes := make([]NodeInfo, len(b.OtherNodes))
		for i := range b.OtherNodes {
			otherNodes[i] = b.OtherNodes[i].Clone()
			totalDatasources += len(b.OtherNodes[i].HealthInfo.Datasources)
		}

		isWrite := endpoint == EP_Execute || endpoint == EP_BeginTx
		// スコア計算
		scoresWithWeight := make([]scoreWithWeight, 0, totalDatasources)
		for nodeIdx := range otherNodes {
			node := &otherNodes[nodeIdx]
			if node.Status != SERVING {
				continue
			}

			for dsIdx := range node.HealthInfo.Datasources {
				ds := &node.HealthInfo.Datasources[dsIdx]
				if !ds.Active {
					continue
				}
				if ds.Readonly && isWrite {
					continue
				}
				if ds.DatabaseName != *tarTbName {
					continue
				}

				m := calculateMetrics(node, dsIdx)
				weight := 0.0
				switch endpoint {
				case EP_Query:
					weight = float64(ds.MaxOpenConns)
				case EP_Execute:
					weight = float64(ds.MaxOpenConns)
				case EP_BeginTx:
					weight = float64(ds.MaxTxConns)
				}

				// ゲート条件チェックは行わない
				// DB枯渇しても、Httpバッファリングが可能な場合は処理を継続させる

				// スコア計算
				score := calculateScore(m, endpoint)
				scoresWithWeight = append(scoresWithWeight, scoreWithWeight{score: score, weight: weight, index: nodeIdx})
			}
		}

		// ノード選択
		_, randomNodeScore := selectBestRandom(scoresWithWeight)
		if randomNodeScore == nil {
			// 自分にはまだ捌ける余地がある場合は自分で処理
			if selfBestScore.score > 0 {
				ctx := context.WithValue(r.Context(), "DS_IDX", selfBestScore.index)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
			log.Printf("No candidate nodes available, processing locally despite lack of capacity")
			http.Error(w, "No candidate nodes available and no capacity to process locally", http.StatusServiceUnavailable)
			return
		}
		// 自分のScoreの方が高い場合は自分で処理
		if selfBestScore.score > randomNodeScore.score {
			// さらに他ノードBestScoreとの比較はやめる、一瞬他ノードに集中させてしまう恐れあり
			ctx := context.WithValue(r.Context(), "DS_IDX", selfBestScore.index)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		// 他ノードにリダイレクト
		selectedNode := &otherNodes[randomNodeScore.index]

		// 307 Temporary Redirect
		w.Header().Set("Location", selectedNode.NodeID)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}
	return http.HandlerFunc(fn)
}

// 80%以下なら自分で処理、それ以外は他ノードとの協調を試みる
func selectSelfDatasource(node *NodeInfo, databaseName *string, endpoint ENDPOINT_TYPE) (*scoreWithWeight, *scoreWithWeight) {
	if node.Status != SERVING {
		return nil, nil
	}

	isWrite := endpoint == EP_Execute || endpoint == EP_BeginTx

	scoresWithWeight := make([]scoreWithWeight, 0, len(node.HealthInfo.Datasources))
	for dsIdx := range node.HealthInfo.Datasources {
		ds := &node.HealthInfo.Datasources[dsIdx]
		if !ds.Active {
			continue
		}
		if ds.Readonly && isWrite {
			continue
		}
		if ds.DatabaseName != *databaseName {
			continue
		}
		// 正規化指標を計算
		m := calculateMetrics(node, dsIdx)
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
		scoresWithWeight = append(scoresWithWeight, scoreWithWeight{score: score, weight: weight, index: dsIdx})
	}

	// ノード選択（TopK + Weighted Random）
	bestScore, weightedRandomScore := selectBestRandom(scoresWithWeight)
	if bestScore == nil {
		return nil, nil
	}

	// 使用率80%以下なら、他のノードとの協調を試みる
	if float64(node.HealthInfo.RunningHttp)/float64(node.HealthInfo.MaxHttpSessions) >= USAGE_THRESHOLD {
		return bestScore, nil
	}

	randomDs := &node.HealthInfo.Datasources[weightedRandomScore.index]
	if endpoint != EP_BeginTx {
		if float64(randomDs.OpenConns)/float64(randomDs.MaxOpenConns) >= USAGE_THRESHOLD {
			return bestScore, nil
		}
	} else {
		if float64(randomDs.RunningTx)/float64(randomDs.MaxTxConns) >= USAGE_THRESHOLD {
			return bestScore, nil
		}
	}

	return bestScore, weightedRandomScore
}

// ノード選択（TopK + Weighted Random）
// return: BestScore, WeightedRandomScore
func selectBestRandom(scores []scoreWithWeight) (*scoreWithWeight, *scoreWithWeight) {
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
type normalizedMetrics struct {
	httpFree    float64
	dbFree      float64
	txFree      float64
	latScore    float64
	errScore    float64
	toutScore   float64
	waitScore   float64
	idleScore   float64
	uptimeScore float64
}

func calculateMetrics(node *NodeInfo, dsIdx int) normalizedMetrics {
	dsInfo := &node.HealthInfo.Datasources[dsIdx]

	httpUsage := float64(node.HealthInfo.RunningHttp) / float64(node.HealthInfo.MaxHttpSessions)
	dbUsage := float64(dsInfo.OpenConns) / float64(dsInfo.MaxOpenConns)
	txUsage := float64(dsInfo.RunningTx) / float64(dsInfo.MaxTxConns)

	httpFree := 1 - clamp01(httpUsage)
	dbFree := 1 - clamp01(dbUsage)
	txFree := 1 - clamp01(txUsage)

	// 品質指標の正規化
	latScore := 1 - clamp01(log1p(float64(dsInfo.LatencyP95Ms))/log1p(LAT_BAD_MS))
	errScore := 1 - clamp01(dsInfo.ErrorRate1m/ERR_BAD)
	toScore := 1 - clamp01(float64(dsInfo.Timeouts1m)/TIMEOUT_BAD)
	waitScore := 1 - clamp01(float64(dsInfo.WaitConns)/WAIT_BAD)
	idleScore := clamp01(float64(dsInfo.IdleConns) / float64(dsInfo.MaxOpenConns))
	uptimeScore := clamp01(float64(time.Since(node.HealthInfo.UpTime).Seconds()) / UPTIME_OK)

	return normalizedMetrics{
		httpFree:    httpFree,
		dbFree:      dbFree,
		txFree:      txFree,
		latScore:    latScore,
		errScore:    errScore,
		toutScore:   toScore,
		waitScore:   waitScore,
		idleScore:   idleScore,
		uptimeScore: uptimeScore,
	}
}

// 用途別スコア計算
func calculateScore(m normalizedMetrics, endpoint ENDPOINT_TYPE) float64 {
	switch endpoint {
	case EP_Query:
		return 0.22*m.dbFree +
			0.18*m.httpFree +
			0.10*m.txFree +
			0.20*m.latScore +
			0.12*m.errScore +
			0.08*m.toutScore +
			0.06*m.waitScore +
			0.02*m.idleScore +
			0.02*m.uptimeScore
	case EP_Execute:
		return 0.30*m.dbFree +
			0.14*m.httpFree +
			0.08*m.txFree +
			0.14*m.latScore +
			0.14*m.errScore +
			0.10*m.toutScore +
			0.08*m.waitScore +
			0.02*m.idleScore
	case EP_BeginTx:
		return 0.42*m.txFree +
			0.22*m.dbFree +
			0.08*m.httpFree +
			0.10*m.errScore +
			0.06*m.toutScore +
			0.06*m.waitScore +
			0.04*m.latScore +
			0.02*m.idleScore
	}
	return 0
}

// ノードスコア情報
type scoreWithWeight struct {
	score  float64
	weight float64
	index  int
}

/*****************************
 * リクエスト情報抽出
 *****************************/
// get: endpoint, dbName, txId
func parseRequest(r *http.Request) (ENDPOINT_TYPE, *string, *string) {

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

	return endpoint, &dbName, &txId
}
