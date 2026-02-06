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
	balancer  *cluster.Balancer
	txManager *rdb.TxManager
}

func NewRouter(balancer *cluster.Balancer, txManager *rdb.TxManager) *Router {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(balancer.SelectNode)

	router := &Router{
		Router:    r,
		balancer:  balancer,
		txManager: txManager,
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
	r.balancer.OtherNodes = otherNodes

	r.balancer.SelfNode.Mu.Lock()
	r.balancer.SelfNode.Status = cluster.SERVING
	r.balancer.SelfNode.HealthInfo.UpTime = time.Now()
	r.balancer.SelfNode.Mu.Unlock()

	go func() {
		for {
			time.Sleep(time.Duration(3000+rand.Intn(2000)) * time.Millisecond) // 3-5秒ランダム待ち
			r.collectHealth(false)
		}
	}()
}

func (r *Router) collectHealth(isSync bool) {
	var wg sync.WaitGroup

	for _, node := range r.balancer.OtherNodes {
		wg.Add(1)
		go func(node *cluster.NodeInfo) {
			defer wg.Done()
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
			node.HealthInfo = responsedNodeInfo.HealthInfo
			node.HealthInfo.CheckTime = time.Now()
		}(node)
	}

	if isSync {
		wg.Wait()
	}

	selfNode := r.balancer.SelfNode
	selfNode.Mu.Lock()
	defer selfNode.Mu.Unlock()

	for i := range selfNode.HealthInfo.Datasources {
		_, openConns, idleConns, runningTx := r.txManager.Stats(i)

		ds := &selfNode.HealthInfo.Datasources[i]
		ds.OpenConns = openConns
		ds.IdleConns = idleConns
		ds.RunningTx = runningTx
	}

	selfNode.HealthInfo.CheckTime = time.Now()
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
		execHandler := rdb.NewExecHandler(r.balancer.SelfNode, r.txManager)
		router.Post(EP_PATH_QUERY, execHandler.Query)
		router.Post(EP_PATH_EXECUTE, execHandler.Execute)

		// Transaction endpoints
		txHandler := rdb.NewTxHandler(r.balancer.SelfNode, r.txManager)
		router.Post(EP_PATH_BEGIN_TX, txHandler.BeginTx)
		router.Put(EP_PATH_COMMIT_TX, txHandler.CommitTx)
		router.Put(EP_PATH_ROLLBACK_TX, txHandler.RollbackTx)
		router.Put(EP_PATH_DONE_TX, txHandler.DoneTx)
	})
}
