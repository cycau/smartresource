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

		if dsConfig.MaxWriteConns == 1 {
			dsConfig.MaxWriteConns = 2
		}
		dsConfig.MaxConns = max(3, dsConfig.MaxConns)
		dsConfig.MaxWriteConns = min(dsConfig.MaxWriteConns, dsConfig.MaxConns-1)
		dsConfig.MinWriteConns = min(dsConfig.MinWriteConns, dsConfig.MaxWriteConns)

		datasourceInfo[i] = *cluster.NewDatasourceInfo(*dsConfig)
	}

	// Initialize DsManager
	dsManager := rdb.NewDsManager(config.MyDatasources)

	// Set maxHttpQueue for HTTP connection limiting
	maxHttpQueue := config.MaxHttpQueue
	if maxHttpQueue <= 0 {
		maxHttpQueue = 1000 // default
	}

	nodeId, _ := gonanoid.New(9)
	thisNode := &cluster.NodeInfo{
		NodeID:       fmt.Sprintf("%s-%s", config.NodeName, nodeId),
		Status:       cluster.STARTING,
		BaseURL:      "-",
		SecretKey:    config.SecretKey,
		MaxHttpQueue: maxHttpQueue,
		Datasources:  datasourceInfo,
	}

	log.Printf("### [Server] Node ID: %s", thisNode.NodeID)
	log.Printf("### [Server] Node Name: %s", config.NodeName)
	log.Printf("### [Server] Datasources: %v", datasourceInfo)
	log.Printf("### [Server] Max Client HTTP Queue: %d", maxHttpQueue)

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
		log.Printf("### [Server] Starting on port: %d", config.NodePort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("### [Error] Server failed to start: %v", err)
		}
	}()

	router.StartHealthTicker()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("### [Server] Shutting down server...")
	thisNode.Mu.Lock()
	thisNode.Status = cluster.STOPPING
	thisNode.Mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("### [Error] Server forced to shutdown: %v", err)
	}

	// Shutdown DsManager
	dsManager.Shutdown()

	log.Println("### [Server] Exited successfully.")
}

func main() {
	// Load configuration
	configPath := "config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		log.Fatalf("### [Error] Failed to read config file: %v", err)
		return
	}

	var config global.Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		log.Fatalf("### [Error] Failed to parse config file: %v", err)
		return
	}

	if len(config.MyDatasources) < 1 {
		log.Printf("### [Error] Wrong configuration file: %s", configPath)
		return
	}

	runServer(config)
}
