package cluster

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"smartdatastream/server/global"
	. "smartdatastream/server/global"
	"sort"
	"strconv"
	"strings"
	"time"
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
		secretKey := r.Header.Get(HEADER_SECRET_KEY)
		if secretKey != b.SelfNode.SecretKey {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		err, endpoint, tarDbName, txID, redirectCount := parseRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
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

		// 対象外のパスはそのまま処理
		if endpoint == EP_Other {
			next.ServeHTTP(w, r)
			return
		}

		// トランザクション処理は常に受け入れる（優先処理）
		if txID != "" {
			dsIdx, err := global.GetDsIdxFromTxID(txID)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			b.runHandler(next, w, r, dsIdx)
			return
		}

		/*** 自分から探す ***/

		// 使用率80%以下なら自分で処理、それ以外は他ノードとの協調で処理
		// Weight抽選で、自分のScoreの方が高い場合は自分で処理
		// それ以外はredirect、Max３回か、自分にリダイレクトされた場合は終了（Client側実装）
		selfBestScore, selfRecommendDs := selectSelfDatasource(b.SelfNode, tarDbName, endpoint)
		log.Printf("###[Balancer] Self Best Score: %+v -> %+v", selfBestScore, selfRecommendDs)
		if selfRecommendDs != nil {
			log.Printf("###[Balancer] Serving on self node: %+v", selfRecommendDs.exIndex)
			b.runHandler(next, w, r, selfRecommendDs.exIndex)
			return
		}
		// Client側で、最後のリダイレクトの場合（リダイレクトを受け付けない場合）
		if redirectCount < 1 {
			if selfBestScore == nil {
				http.Error(w, fmt.Sprintf("No resource to process on this node. Node[%s], RedirectCount[%d]", b.SelfNode.NodeID, redirectCount), http.StatusServiceUnavailable)
				return
			}
			log.Printf("###[Balancer] Forced Serving on self node: %+v", selfBestScore.exIndex)
			b.runHandler(next, w, r, selfBestScore.exIndex)
			return
		}

		/*** 他のノード選択 ***/
		recommendNodeScore, recommendNode := selectOtherNode(b.OtherNodes, tarDbName, endpoint)
		log.Printf("###[Balancer] Recommend Node Score: %+v -> %+v", recommendNodeScore, recommendNode)
		if recommendNode == nil {
			// ゲート条件チェックは行わない
			// DB枯渇しても、Httpバッファリングが可能な場合は処理を継続させる
			if selfBestScore != nil {
				log.Printf("###[Balancer] No recommend node, Serving on self node: %+v", selfBestScore.exIndex)
				b.runHandler(next, w, r, selfBestScore.exIndex)
				return
			}
			log.Printf("###[Balancer] No candidate nodes available, processing locally despite lack of capacity")
			http.Error(w, "No candidate nodes available and no capacity to process locally", http.StatusServiceUnavailable)
			return
		}

		// 自分のScoreの方が高い場合は自分で処理
		if selfBestScore.score > recommendNodeScore.score {
			// さらに他ノードBestScoreとの比較はやめる、一瞬他ノードに集中させてしまう恐れあり
			log.Printf("###[Balancer] Self Best Score is higher than recommend node score, Serving on self node: %+v", selfBestScore.exIndex)
			b.runHandler(next, w, r, selfBestScore.exIndex)
			return
		}

		// 307 Temporary Redirect
		w.Header().Set("Location", recommendNode.NodeID)
		w.WriteHeader(http.StatusTemporaryRedirect)
		log.Printf("###[Balancer] Redirecting to Node[%s]", recommendNode.NodeID)
	}
	return http.HandlerFunc(fn)
}

func (b *Balancer) runHandler(next http.Handler, w http.ResponseWriter, r *http.Request, dsIdx int) {

	timeoutSecInt := b.SelfNode.Datasources[dsIdx].DefaultQueryTimeoutSec

	timeoutSec := r.Header.Get(HEADER_TIMEOUT_SEC)
	if timeoutSec != "" {
		timeoutSecHeader, err := strconv.Atoi(timeoutSec)
		if err == nil {
			if timeoutSecHeader > 0 {
				timeoutSecInt = timeoutSecHeader
			}
		}
	}
	deadline := time.Now().Add(time.Duration(timeoutSecInt) * time.Second)
	rc := http.NewResponseController(w)
	if err := rc.SetWriteDeadline(deadline); err != nil {
		log.Printf("SetWriteDeadline: %v", err)
	}
	r = r.WithContext(context.WithValue(r.Context(), CTX_DS_IDX, dsIdx))

	next.ServeHTTP(w, r)
}

func selectSelfDatasource(selfNode *NodeInfo, tarDbName string, endpoint ENDPOINT_TYPE) (bestScore *ScoreWithWeight, recommendScore *ScoreWithWeight) {

	scores := selfNode.GetScore(tarDbName, endpoint)
	for i, s := range scores {
		if s != nil {
			log.Printf("###[Balancer] Self Node Score[%d]: %+v", i, *s)
		}
	}

	// ノード選択（TopK + Weighted Random）
	best, bestRandom := selectBestRandomScore(scores)
	if best == nil {
		return nil, nil
	}

	// 使用率80%相当
	if best.score < 0.45 {
		return best, nil
	}

	return best, bestRandom
}

func selectOtherNode(otherNodes []*NodeInfo, tarDbName string, endpoint ENDPOINT_TYPE) (recommendNodeScore *ScoreWithWeight, recommendNode *NodeInfo) {
	scores := make([]*ScoreWithWeight, 0, 8)

	for nodeIdx, node := range otherNodes {
		nodeScores := node.GetScore(tarDbName, endpoint)
		for _, score := range nodeScores {
			score.exIndex = nodeIdx
		}
		scores = append(scores, nodeScores...)
	}
	for i, s := range scores {
		if s != nil {
			log.Printf("###[Balancer] Other Node Score[%d]: %+v", i, *s)
		}
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
func parseRequest(r *http.Request) (err error, endpoint ENDPOINT_TYPE, dbName string, txID string, redirectCount int) {

	endpoint = GetEndpointType(r.URL.Path)
	dbName = r.Header.Get(HEADER_DB_NAME)
	txID = r.Header.Get(HEADER_TX_ID)
	redirectCount, err = strconv.Atoi(r.Header.Get(HEADER_REDIRECT_COUNT))

	return nil, endpoint, dbName, txID, redirectCount
}
