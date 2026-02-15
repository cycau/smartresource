package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"smartdatastream/server/cluster"
	. "smartdatastream/server/global"
	"smartdatastream/server/rdb"
)

type Router struct {
	chi.Router
	balancer   *cluster.Balancer
	dsManager  *rdb.DsManager
	dmlHandler *rdb.DmlHandler
	txHandler  *rdb.TxHandler
}

func NewRouter(balancer *cluster.Balancer, dsManager *rdb.DsManager) *Router {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(balancer.SelectNode)

	router := &Router{
		Router:     r,
		balancer:   balancer,
		dsManager:  dsManager,
		dmlHandler: rdb.NewDmlHandler(dsManager),
		txHandler:  rdb.NewTxHandler(dsManager),
	}

	router.setupRoutes()
	return router
}

/*****************************
 * 各ノードの health 情報を取得
 *****************************/
func (r *Router) StartCollectHealthTicker() {
	// wait for http server to start
	time.Sleep(500 * time.Millisecond)
	// collect once at startup
	r.collectHealth(true)

	// remove self from other nodes
	otherNodes := make([]*cluster.NodeInfo, 0, len(r.balancer.OtherNodes))
	for _, node := range r.balancer.OtherNodes {
		if r.balancer.SelfNode.NodeID != node.NodeID {
			otherNodes = append(otherNodes, node)
		}
	}
	// r.balancer.OtherNodes = otherNodes // TODO: comment out for debug

	selfNode := r.balancer.SelfNode
	selfNode.Mu.Lock()
	selfNode.Status = cluster.SERVING
	selfNode.UpTime = time.Now()
	selfNode.Mu.Unlock()

	go func() {
		for {
			time.Sleep(time.Duration(2000+rand.Intn(2000)) * time.Millisecond) // 平均3(2-4)秒ランダム待ち
			r.collectHealth(false)
		}
	}()
}

const HEALZ_ERROR_INTERVAL = 15 * time.Second

func (r *Router) collectHealth(isSync bool) {
	var wg sync.WaitGroup
	for _, node := range r.balancer.OtherNodes {
		wg.Add(1)
		go func(node *cluster.NodeInfo) {
			defer wg.Done()

			if node.Status == cluster.HEALZERR {
				if time.Since(node.CheckTime) < HEALZ_ERROR_INTERVAL {
					return
				}
			}

			defer node.Mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, node.BaseURL+"/healz", nil)
			if err != nil {
				log.Printf("Failed to create request for %s: %v", node.NodeID, err)
				node.Mu.Lock()
				node.Status = cluster.HEALZERR
				return
			}
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				log.Printf("Failed to do request for %s: %v", node.NodeID, err)
				node.Mu.Lock()
				node.Status = cluster.HEALZERR
				node.CheckTime = time.Now()
				return
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				log.Printf("Failed to read response body for %s: %v", node.NodeID, err)
				node.Mu.Lock()
				node.Status = cluster.HEALZERR
				return
			}
			var responsedNodeInfo cluster.NodeInfo
			err = json.Unmarshal(body, &responsedNodeInfo)
			if err != nil {
				log.Printf("Failed to unmarshal health info from %s: %v", node.NodeID, err)
				node.Mu.Lock()
				node.Status = cluster.HEALZERR
				return
			}

			node.Mu.Lock()
			node.NodeID = responsedNodeInfo.NodeID
			node.Status = responsedNodeInfo.Status

			node.MaxHttpQueue = responsedNodeInfo.MaxHttpQueue
			node.RunningHttp = responsedNodeInfo.RunningHttp
			node.UpTime = responsedNodeInfo.UpTime
			node.CheckTime = responsedNodeInfo.CheckTime
			node.Datasources = responsedNodeInfo.Datasources
			node.CheckTime = time.Now()
		}(node)
	}

	if isSync {
		wg.Wait()
	}

	selfNode := r.balancer.SelfNode
	selfNode.Mu.Lock()
	for dsIdx := range selfNode.Datasources {
		runningRead, runningWrite, runningTx := r.dsManager.StatsGet(dsIdx)
		latencyP95Ms, errorRate1m, timeoutRate1m := r.dmlHandler.StatsGet(dsIdx)

		dsInfo := &selfNode.Datasources[dsIdx]
		dsInfo.RunningRead = runningRead
		dsInfo.RunningWrite = runningWrite
		dsInfo.RunningTx = runningTx
		dsInfo.LatencyP95Ms = latencyP95Ms
		dsInfo.ErrorRate1m = errorRate1m
		dsInfo.TimeoutRate1m = timeoutRate1m
	}
	selfNode.CheckTime = time.Now()
	selfNode.Mu.Unlock()
}

/*****************************
 * ルーティング設定
 *****************************/
func (r *Router) setupRoutes() {
	// Health check
	r.Get("/healz", func(nodeInfo *cluster.NodeInfo) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(nodeInfo.Clone())
		}
	}(r.balancer.SelfNode).ServeHTTP)

	// API v1 routes
	r.Route("/rdb", func(router chi.Router) {
		// Execute endpoint
		router.Post(EP_PATH_QUERY, r.dmlHandler.Query)
		router.Post(EP_PATH_EXECUTE, r.dmlHandler.Execute)

		// Transaction endpoints
		router.Route("/tx", func(txRouter chi.Router) {
			txRouter.Post(EP_PATH_TX_BEGIN, r.txHandler.BeginTx)
			txRouter.Post(EP_PATH_QUERY, r.dmlHandler.QueryTx)
			txRouter.Post(EP_PATH_EXECUTE, r.dmlHandler.ExecuteTx)
			txRouter.Put(EP_PATH_TX_COMMIT, r.txHandler.CommitTx)
			txRouter.Put(EP_PATH_TX_ROLLBACK, r.txHandler.RollbackTx)
			txRouter.Put(EP_PATH_TX_CLOSE, r.txHandler.CloseTx)
		})
	})
}
