package cluster

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	. "smartdatastream/server/global"
	"sort"
	"strconv"
	"strings"
)

type Balancer struct {
	SelfNode   *NodeInfo
	OtherNodes []*NodeInfo
}

/*****************************
 * initialize Balancer
 *****************************/
func NewBalancer(selfNode *NodeInfo, otherNodes []*NodeInfo) *Balancer {
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
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		err, endpoint, tarDbName, txID, dsID, redirectCount := parseRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// 対象外のパスはそのまま処理
		if endpoint == EP_Other {
			next.ServeHTTP(w, r)
			return
		}

		// RunningHttp count
		b.SelfNode.Mu.Lock()
		b.SelfNode.RunningHttp++
		b.SelfNode.Mu.Unlock()
		defer func() {
			b.SelfNode.Mu.Lock()
			b.SelfNode.RunningHttp--
			b.SelfNode.Mu.Unlock()
		}()

		// トランザクション処理は常に受け入れる（優先処理）
		if txID != "" {
			next.ServeHTTP(w, r)
			return
		}

		// Datasource指定がある場合はそのまま処理
		if dsID != "" {
			for idx, ds := range b.SelfNode.Datasources {
				if ds.DatasourceID == dsID {
					next.ServeHTTP(w, PutCtxDsIdx(r, idx))
					return
				}
			}
			http.Error(w, `Invalid datasource ID [`+dsID+`]`, http.StatusBadRequest)
			return
		}

		/*** 自分から探す ***/

		// 使用率80%以下なら自分で処理、それ以外は他ノードとの協調で処理
		// Weight抽選で、自分のScoreの方が高い場合は自分で処理
		// それ以外はredirect、Max３回か、自分にリダイレクトされた場合は終了（Client側実装）
		selfBestScore, selfRecommendDs := selectSelfDatasource(b.SelfNode, tarDbName, endpoint)
		log.Printf("Self Best Score: %+v -> %+v", selfBestScore, selfRecommendDs)
		if selfRecommendDs != nil {
			next.ServeHTTP(w, PutCtxDsIdx(r, selfRecommendDs.exIndex))
			return
		}
		// Client側で、最後のリダイレクトの場合（リダイレクトを受け付けない場合）
		if redirectCount < 1 {
			if selfBestScore == nil {
				http.Error(w, fmt.Sprintf("No resource to process on this node. Node[%s], RedirectCount[%d]", b.SelfNode.NodeID, redirectCount), http.StatusServiceUnavailable)
				return
			}
			next.ServeHTTP(w, PutCtxDsIdx(r, selfBestScore.exIndex))
			return
		}

		/*** 他のノード選択 ***/
		recommendNodeScore, recommendNode := selectOtherNode(b.OtherNodes, tarDbName, endpoint)

		if recommendNode == nil {
			// ゲート条件チェックは行わない
			// DB枯渇しても、Httpバッファリングが可能な場合は処理を継続させる
			if selfBestScore != nil {
				next.ServeHTTP(w, PutCtxDsIdx(r, selfBestScore.exIndex))
				return
			}
			log.Printf("No candidate nodes available, processing locally despite lack of capacity")
			http.Error(w, "No candidate nodes available and no capacity to process locally", http.StatusServiceUnavailable)
			return
		}

		// 自分のScoreの方が高い場合は自分で処理
		if selfBestScore.score > recommendNodeScore.score {
			// さらに他ノードBestScoreとの比較はやめる、一瞬他ノードに集中させてしまう恐れあり
			next.ServeHTTP(w, PutCtxDsIdx(r, selfBestScore.exIndex))
			return
		}

		// 307 Temporary Redirect
		w.Header().Set("Location", recommendNode.NodeID)
		w.WriteHeader(http.StatusTemporaryRedirect)
		fmt.Fprintf(w, "Redirecting to Node[%s] [%f -> %f]\n", recommendNode.NodeID, selfBestScore.score, recommendNodeScore.score)
	}
	return http.HandlerFunc(fn)
}

func selectSelfDatasource(selfNode *NodeInfo, tarDbName string, endpoint ENDPOINT_TYPE) (bestScore *ScoreWithWeight, recommendScore *ScoreWithWeight) {
	selfNode.Mu.Lock()
	defer selfNode.Mu.Unlock()

	scores := make([]*ScoreWithWeight, 0, len(selfNode.Datasources))

	for dsIdx := range selfNode.Datasources {

		score := selfNode.GetScore(dsIdx, tarDbName, endpoint)
		if score == nil {
			continue
		}

		scores = append(scores, score)
	}

	// ノード選択（TopK + Weighted Random）
	best, bestRandom := selectBestRandomScore(scores)
	if best == nil {
		return nil, nil
	}

	// 使用率80%以上なら、他のノードとの協調を試みる
	if float64(selfNode.RunningHttp)/float64(selfNode.MaxHttpQueue) >= USAGE_THRESHOLD {
		return best, nil
	}

	randomDs := selfNode.Datasources[bestRandom.exIndex]
	switch endpoint {
	case EP_BeginTx:
		if float64(randomDs.RunningTx)/float64(randomDs.MaxTxConns) >= USAGE_THRESHOLD {
			return best, nil
		}
	default:
		if float64(randomDs.RunningQuery)/float64((randomDs.MaxOpenConns)) >= USAGE_THRESHOLD {
			return best, nil
		}
	}

	return best, bestRandom
}

func selectOtherNode(otherNodes []*NodeInfo, tarDbName string, endpoint ENDPOINT_TYPE) (recommendNodeScore *ScoreWithWeight, recommendNode *NodeInfo) {
	scores := make([]*ScoreWithWeight, 0, 255)

	for nodeIdx, node := range otherNodes {
		node.Mu.RLock()

		for dsIdx := range node.Datasources {
			score := node.GetScore(dsIdx, tarDbName, endpoint)
			if score == nil {
				continue
			}

			score.exIndex = nodeIdx
			scores = append(scores, score)
		}

		node.Mu.RUnlock()
	}

	// ノード選択
	_, bestRandomScore := selectBestRandomScore(scores)
	if bestRandomScore != nil {
		return bestRandomScore, otherNodes[bestRandomScore.exIndex]
	}
	return nil, nil
}

// TopK + Weighted Random
func selectBestRandomScore(scores []*ScoreWithWeight) (bestScore *ScoreWithWeight, bestRandomScore *ScoreWithWeight) {
	if len(scores) == 0 {
		return nil, nil
	}
	if len(scores) == 1 {
		return scores[0], scores[0]
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
		totalWeight += (sc.score * sc.weight)
	}

	if totalWeight <= 0 {
		return nil, nil
	}

	r := rand.Float64() * totalWeight
	current := 0.0
	for _, sc := range topK {
		if sc.weight <= 0 {
			continue
		}
		current += (sc.score * sc.weight)
		if current >= r {
			return topK[0], sc
		}
	}

	return nil, nil
}

// parse request and return endpoint, database name, and transaction ID
func parseRequest(r *http.Request) (err error, endpoint ENDPOINT_TYPE, dbName string, txID string, dsID string, redirectCount int) {

	query := r.URL.Query()
	dbName = query.Get(QUERYP_DB_NAME)
	txID = query.Get(QUERYP_TX_ID)
	dsID = query.Get(QUERYP_DS_ID)
	redirectCount, err = strconv.Atoi(query.Get(QUERYP_REDIRECT_COUNT))
	endpoint = GetEndpointType(r.URL.Path)

	switch endpoint {
	case EP_Query:
		if dbName == "" && txID == "" && dsID == "" {
			return fmt.Errorf("require query parameters [ _DbName ] [ _TxID ] [ _DsID ]"), endpoint, dbName, txID, dsID, redirectCount
		}
	case EP_Execute:
		if dbName == "" && txID == "" && dsID == "" {
			return fmt.Errorf("require query parameters [ _DbName ] [ _TxID ] [ _DsID ]"), endpoint, dbName, txID, dsID, redirectCount
		}
	case EP_BeginTx:
		if dbName == "" && dsID == "" {
			return fmt.Errorf("require query parameters [ _DbName ] [ _DsID ]"), endpoint, dbName, txID, dsID, redirectCount
		}
	}

	return nil, endpoint, dbName, txID, dsID, redirectCount
}
