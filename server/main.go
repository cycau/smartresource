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

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

const (
	STAT_INTERVAL = 5 * time.Minute
)

func loadConfig(path string) (*global.Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config global.Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &config, nil
}

func UUID() string {
	return uuid.New().String()
}

func main() {
	// Load configuration
	configPath := "config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	config, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize cluster health info
	datasourceInfo := make([]cluster.DatasourceInfo, len(config.MyDatasources))
	for i, dsConfig := range config.MyDatasources {
		if dsConfig.Readonly {
			dsConfig.MaxTxConns = 0
		}

		datasourceInfo[i] = *cluster.NewDatasourceInfo(dsConfig)
	}

	// Initialize TxManager
	txManager := rdb.NewTxManager(config.MyDatasources)

	// Set maxHttpQueue for HTTP connection limiting
	maxHttpQueue := config.MaxHttpQueue
	if maxHttpQueue <= 0 {
		maxHttpQueue = 1000 // default
	}
	log.Printf("HTTP connection limit: %d", maxHttpQueue)

	nodeId, _ := gonanoid.New(9)
	thisNode := &cluster.NodeInfo{
		NodeID:    fmt.Sprintf("%s-%s", config.NodeName, nodeId),
		Status:    cluster.STARTING,
		BaseURL:   "-",
		SecretKey: config.SecretKey,
		HealthInfo: cluster.HealthInfo{
			MaxHttpQueue: maxHttpQueue,
			Datasources:  datasourceInfo,
		},
	}

	// collect cluster health information
	clusterNodes := make([]cluster.NodeInfo, 0, len(config.ClusterNodes))
	for _, nodeURL := range config.ClusterNodes {
		clusterNodes = append(clusterNodes, cluster.NodeInfo{
			Status:  cluster.STARTING,
			BaseURL: nodeURL,
		})
	}
	balancer := cluster.NewBalancer(thisNode, clusterNodes)

	// Create router
	router := NewRouter(balancer, txManager)

	// Create HTTP server
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", config.NodePort),
		Handler: router,
	}

	// Start server in a goroutine
	go func() {
		log.Printf("Server starting on port %d", config.NodePort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	router.StartCollectHealthTicker()

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

	// Shutdown TxManager
	txManager.Shutdown()

	log.Println("Server exited")
}
