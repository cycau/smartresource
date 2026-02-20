package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"smartdatastream/server/cluster"
	"smartdatastream/server/global"
	"smartdatastream/server/rdb"

	gonanoid "github.com/matoous/go-nanoid/v2"

	"gopkg.in/yaml.v3"
)

func runServer(config global.Config) {
	// Initialize cluster health info
	datasourceInfo := make([]cluster.DatasourceInfo, len(config.MyDatasources))
	for i := range config.MyDatasources {
		dsConfig := &config.MyDatasources[i]
		if dsConfig.MaxWriteConns > dsConfig.PoolConns {
			dsConfig.MaxWriteConns = dsConfig.PoolConns
		}
		if dsConfig.MinWriteConns > dsConfig.MaxWriteConns {
			dsConfig.MinWriteConns = dsConfig.MaxWriteConns
		}

		datasourceInfo[i] = *cluster.NewDatasourceInfo(*dsConfig)
	}

	// Initialize DsManager
	dsManager := rdb.NewDsManager(config.MyDatasources)

	// Set maxHttpQueue for HTTP connection limiting
	maxHttpQueue := config.MaxHttpQueue
	if maxHttpQueue <= 0 {
		maxHttpQueue = 1000 // default
	}
	log.Printf("HTTP connection limit: %d", maxHttpQueue)

	nodeId, _ := gonanoid.New(9)
	thisNode := &cluster.NodeInfo{
		NodeID:       fmt.Sprintf("%s-%s", config.NodeName, nodeId),
		Status:       cluster.STARTING,
		BaseURL:      "-",
		SecretKey:    config.SecretKey,
		MaxHttpQueue: maxHttpQueue,
		Datasources:  datasourceInfo,
	}

	// collect cluster health information
	clusterNodes := make([]*cluster.NodeInfo, 0, len(config.ClusterNodes))
	for _, nodeURL := range config.ClusterNodes {
		clusterNodes = append(clusterNodes, &cluster.NodeInfo{
			Status:  cluster.STARTING,
			BaseURL: nodeURL,
		})
	}
	balancer := cluster.NewBalancer(thisNode, clusterNodes)

	// Create router
	router := NewRouter(balancer, dsManager)

	// Create HTTP server
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", config.NodePort),
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Start server in a goroutine
	go func() {
		log.Printf("Server starting on port %d", config.NodePort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	router.StartHealthTicker()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")
	thisNode.Mu.Lock()
	thisNode.Status = cluster.STOPPING
	thisNode.Mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	// Shutdown DsManager
	dsManager.Shutdown()

	log.Println("Server exited")
}

func main() {
	// Load configuration
	configPath := "config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
		return
	}

	var config global.Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		log.Fatalf("Failed to parse config file: %v", err)
		return
	}

	runServer(config)
}
