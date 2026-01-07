package main

import (
	"encoding/json"
	"net/http"

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
	r.Use(balancer.DoOrSwitchNode)

	router := &Router{
		Router:    r,
		balancer:  balancer,
		txManager: txManager,
	}

	router.setupRoutes()
	return router
}

func (r *Router) setupRoutes() {
	// Health check
	r.Get("/health", func(nodeInfo *cluster.NodeInfo) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(nodeInfo)
		}
	}(r.balancer.SelfNode).ServeHTTP)

	// API v1 routes
	r.Route("/v1/rdb", func(router chi.Router) {
		// Execute endpoint
		executeHandler := rdb.NewSqlHandler(r.balancer.SelfNode, r.txManager)
		router.Post("/query", executeHandler.Query)
		router.Post("/execute", executeHandler.Execute)

		// Transaction endpoints
		txHandler := rdb.NewTxHandler(r.balancer.SelfNode, r.txManager)
		router.Route("/tx", func(txRouter chi.Router) {
			txRouter.Post("/begin", txHandler.BeginTx)
			txRouter.Post("/commit", txHandler.CommitTx)
			txRouter.Post("/rollback", txHandler.RollbackTx)
		})
	})
}
