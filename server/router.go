package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"smartdatastream/server/cluster"
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
func (r *Router) CollectHealth() {
	for i := range r.balancer.OtherNodes {
		node := &r.balancer.OtherNodes[i]

		go func(node *cluster.NodeInfo) {
			defer node.Mu.Unlock()

			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
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
			var requestedNodeInfo cluster.NodeInfo
			err = json.Unmarshal(body, &requestedNodeInfo)
			if err != nil {
				log.Printf("Failed to unmarshal health info from %s: %v", node.NodeID, err)
				node.Mu.Lock()
				node.Status = cluster.HEALZERR
				return
			}

			node.Mu.Lock()
			node.Status = requestedNodeInfo.Status
			node.HealthInfo = requestedNodeInfo.HealthInfo
			node.HealthInfo.CheckTime = time.Now()
		}(node)
	}

	selfNode := r.balancer.SelfNode
	selfNode.Mu.Lock()
	defer selfNode.Mu.Unlock()

	for i := range selfNode.HealthInfo.Datasources {
		_, openConns, idleConns, runningTx := r.txManager.Statistics(i)

		ds := &selfNode.HealthInfo.Datasources[i]
		//ds.Mu.Lock()
		ds.OpenConns = openConns
		ds.IdleConns = idleConns
		ds.RunningTx = runningTx
		//ds.Mu.Unlock()
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
		router.Post("/query", execHandler.Query)
		router.Post("/execute", execHandler.Execute)

		// Transaction endpoints
		txHandler := rdb.NewTxHandler(r.balancer.SelfNode, r.txManager)
		router.Route("/tx", func(txRouter chi.Router) {
			txRouter.Post("/begin", txHandler.BeginTx)
			txRouter.Post("/commit", txHandler.CommitTx)
			txRouter.Post("/rollback", txHandler.RollbackTx)
			txRouter.Post("/done", txHandler.DoneTx)
		})
	})
}
