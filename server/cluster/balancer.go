package cluster

import (
	"context"
	"fmt"
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
	LAT_BAD_MS      = 3000.0 // 3秒を"かなり悪い"基準
	ERR_RATE_BAD    = 0.05   // 1分5%をかなり悪い
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
 * initialize Balancer
 *****************************/
func NewBalancer(selfNode *NodeInfo, otherNodes []NodeInfo) *Balancer {
	return &Balancer{
		SelfNode:   selfNode,
		OtherNodes: otherNodes,
	}
}

/*****************************
 * select Node
 *****************************/
func (b *Balancer) SelectNode(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/healz") {
			next.ServeHTTP(w, r)
			return
		}
		secretKey := r.Header.Get("X-Secret-Key")
		if secretKey != b.SelfNode.SecretKey {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("Unauthorized"))
			return
		}

		endpoint, tarDbName, txID, dsID := parseRequest(r)

		// 対象外のパスはそのまま処理
		if endpoint == EP_Other {
			next.ServeHTTP(w, r)
			return
		}

		// トランザクション処理は常に受け入れる
		if txID != "" {
			next.ServeHTTP(w, r)
			return
		}

		// RunningHttp count
		b.SelfNode.Mu.Lock()
		if b.SelfNode.HealthInfo.RunningHttp >= b.SelfNode.HealthInfo.MaxHttpSessions {
			b.SelfNode.Mu.Unlock()
			http.Error(w, "Too many requests", http.StatusServiceUnavailable)
			return
		}
		b.SelfNode.HealthInfo.RunningHttp++
		b.SelfNode.Mu.Unlock()
		defer func() {
			b.SelfNode.Mu.Lock()
			b.SelfNode.HealthInfo.RunningHttp--
			b.SelfNode.Mu.Unlock()
		}()

		// Datasource指定がある場合はそのまま処理
		if dsID != "" {
			for idx, ds := range b.SelfNode.HealthInfo.Datasources {
				if ds.DatasourceID == dsID {
					ctx := context.WithValue(r.Context(), "$S_IDX", idx)
					next.ServeHTTP(w, r.WithContext(ctx))
					return
				}
			}
			http.Error(w, `Invalid datasource ID [`+dsID+`]`, http.StatusBadRequest)
			return
		}

		/*** ノード選択処理 ***/
		selfNode := b.SelfNode.Clone()

		// 使用率80%以下なら自分で処理、それ以外は他ノードとの協調で処理
		// Weight抽選で、自分のScoreの方が高い場合は自分で処理
		// それ以外はredirect、Max３回か、自分にリダイレクトされた場合は終了（Client側実装）
		selfBestScore, selfBestRandomDs := selectSelfDatasource(&selfNode, tarDbName, endpoint)
		if selfBestRandomDs != nil {
			// add to context
			ctx := context.WithValue(r.Context(), "$S_IDX", selfBestRandomDs.index)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		// 他のノード選択
		selectedNodeScore, selectedNode := selectOtherDatasource(&b.OtherNodes, tarDbName, endpoint)

		if selectedNodeScore == nil {
			// 自分にはまだ捌ける余地がある場合は自分で処理
			if selfBestScore.score > 0 {
				ctx := context.WithValue(r.Context(), "$S_IDX", selfBestScore.index)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
			log.Printf("No candidate nodes available, processing locally despite lack of capacity")
			http.Error(w, "No candidate nodes available and no capacity to process locally", http.StatusServiceUnavailable)
			return
		}

		// 自分のScoreの方が高い場合は自分で処理
		if selfBestScore.score > selectedNodeScore.score {
			// さらに他ノードBestScoreとの比較はやめる、一瞬他ノードに集中させてしまう恐れあり
			ctx := context.WithValue(r.Context(), "$S_IDX", selfBestScore.index)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		// 307 Temporary Redirect
		w.Header().Set("Location", selectedNode.NodeID)
		w.WriteHeader(http.StatusTemporaryRedirect)
		fmt.Fprintf(w, "Redirecting to Node[%s] [%f -> %f]\n", selectedNode.NodeID, selfBestScore.score, selectedNodeScore.score)
	}
	return http.HandlerFunc(fn)
}

func selectSelfDatasource(node *NodeInfo, tarDbName string, endpoint ENDPOINT_TYPE) (bestScore *scoreWithWeight, bestRandomScore *scoreWithWeight) {
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
		if ds.DatabaseName != tarDbName {
			continue
		}
		// 正規化指標を計算
		metrics := calculateMetrics(node, dsIdx)
		score := calculateScore(metrics, endpoint)
		weight := 0.0
		switch endpoint {
		case EP_BeginTx:
			weight = float64(ds.MaxTxConns)
		default:
			weight = float64(ds.MaxOpenConns)
		}
		scoresWithWeight = append(scoresWithWeight, scoreWithWeight{score: score, weight: weight, index: dsIdx})
	}

	// ノード選択（TopK + Weighted Random）
	best, bestRandom := selectBestRandomScore(scoresWithWeight)
	if best == nil {
		return nil, nil
	}

	// 使用率80%以上なら、他のノードとの協調を試みる
	if float64(node.HealthInfo.RunningHttp)/float64(node.HealthInfo.MaxHttpSessions) >= USAGE_THRESHOLD {
		return best, nil
	}

	randomDs := &node.HealthInfo.Datasources[bestRandom.index]
	if endpoint != EP_BeginTx {
		if float64(randomDs.RunningSql)/float64(randomDs.MaxOpenConns) >= USAGE_THRESHOLD {
			return best, nil
		}
	} else {
		if float64(randomDs.RunningTx)/float64(randomDs.MaxTxConns) >= USAGE_THRESHOLD {
			return best, nil
		}
	}

	return best, bestRandom
}

func selectOtherDatasource(nodes *[]NodeInfo, tarDbName string, endpoint ENDPOINT_TYPE) (selectedNodeScore *scoreWithWeight, selectedNode *NodeInfo) {
	totalDatasources := 0
	otherNodes := make([]NodeInfo, len(*nodes))
	for i := range *nodes {
		otherNodes[i] = (*nodes)[i].Clone()
		totalDatasources += len((*nodes)[i].HealthInfo.Datasources)
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
			if ds.DatabaseName != tarDbName {
				continue
			}

			metrics := calculateMetrics(node, dsIdx)
			weight := 0.0
			switch endpoint {
			case EP_BeginTx:
				weight = float64(ds.MaxTxConns)
			default:
				weight = float64(ds.MaxOpenConns)
			}

			// ゲート条件チェックは行わない
			// DB枯渇しても、Httpバッファリングが可能な場合は処理を継続させる

			// スコア計算
			score := calculateScore(metrics, endpoint)
			scoresWithWeight = append(scoresWithWeight, scoreWithWeight{score: score, weight: weight, index: nodeIdx})
		}
	}

	// ノード選択
	_, bestRandomScore := selectBestRandomScore(scoresWithWeight)
	return bestRandomScore, &otherNodes[bestRandomScore.index]
}

// TopK + Weighted Random
func selectBestRandomScore(scores []scoreWithWeight) (bestScore *scoreWithWeight, bestRandomScore *scoreWithWeight) {
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
		if current >= r {
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

func calculateMetrics(node *NodeInfo, dsIdx int) normalizedMetrics {
	dsInfo := &node.HealthInfo.Datasources[dsIdx]

	httpUsage := float64(node.HealthInfo.RunningHttp) / float64(node.HealthInfo.MaxHttpSessions)
	dbUsage := float64(dsInfo.RunningSql) / float64(dsInfo.MaxOpenConns)
	txUsage := 1.0
	if dsInfo.MaxTxConns > 0 {
		txUsage = float64(dsInfo.RunningTx) / float64(dsInfo.MaxTxConns)
	}

	httpFree := 1 - clamp01(httpUsage)
	dbFree := 1 - clamp01(dbUsage)
	txFree := 1 - clamp01(txUsage)
	idleScore := clamp01(float64(dsInfo.IdleConns) / float64(dsInfo.OpenConns))

	// 品質指標の正規化
	latScore := 1 - clamp01(math.Log1p(float64(dsInfo.LatencyP95Ms))/math.Log1p(LAT_BAD_MS))
	errScore := 1 - clamp01(float64(dsInfo.ErrorRate1m)/ERR_RATE_BAD)
	toutScore := 1 - clamp01(float64(dsInfo.Timeouts1m)/TIMEOUT_BAD)

	uptimeScore := clamp01(float64(time.Since(node.HealthInfo.UpTime).Seconds()) / UPTIME_OK)

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

// 用途別スコア計算
func calculateScore(m normalizedMetrics, endpoint ENDPOINT_TYPE) float64 {
	if m.httpFree < 0.01 {
		return 0.0
	}

	switch endpoint {
	case EP_Query:
		return 0.25*m.dbFree +
			0.00*m.txFree +
			0.20*m.httpFree +
			0.12*m.idleScore +

			0.20*m.latScore +
			0.12*m.errScore +
			0.08*m.toutScore +

			0.03*m.uptimeScore
	case EP_Execute:
		return 0.25*m.dbFree +
			0.05*m.txFree +
			0.15*m.httpFree +
			0.12*m.idleScore +

			0.10*m.latScore +
			0.15*m.errScore +
			0.15*m.toutScore +

			0.03*m.uptimeScore
	case EP_BeginTx:
		return 0.25*m.dbFree +
			0.25*m.txFree +
			0.10*m.httpFree +
			0.12*m.idleScore +

			0.05*m.latScore +
			0.20*m.errScore +
			0.15*m.toutScore +

			0.03*m.uptimeScore
	}
	return 0
}

// Node score information
type scoreWithWeight struct {
	score  float64
	weight float64
	index  int
}

// parse request and return endpoint, database name, and transaction ID
func parseRequest(r *http.Request) (endpoint ENDPOINT_TYPE, dbName string, txID string, dsIDX string) {

	path := r.URL.Path
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

	dbName = query.Get("_DbName")
	txID = query.Get("_TxID")
	dsIDX = query.Get("_DsIDX")

	return endpoint, dbName, txID, dsIDX
}
