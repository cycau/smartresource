package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"smartdatastream/server/cluster"
	"smartdatastream/server/rdb"

	"gopkg.in/yaml.v3"
)

type Config struct {
	NodeID          string              `yaml:"nodeId"`
	NodePort        int                 `yaml:"nodePort"`
	MaxHttpSessions int                 `yaml:"maxHttpSessions"`
	Cluster         []ClusterNodeConfig `yaml:"cluster"`
	Datasources     []DatasourceConfig  `yaml:"datasources"`
}

type ClusterNodeConfig struct {
	NodeID  string `yaml:"nodeId"`
	BaseURL string `yaml:"baseUrl"`
}

type DatasourceConfig struct {
	DatasourceID                 string `yaml:"datasourceId"`
	DatabaseName                 string `yaml:"databaseName"`
	Driver                       string `yaml:"driver"`
	DSN                          string `yaml:"dsn"`
	MaxOpenConns                 int    `yaml:"maxOpenConns"`
	MinIdleConns                 int    `yaml:"minIdleConns"`
	MaxTransactionConns          int    `yaml:"maxTransactionConns"`
	ConnMaxLifetimeSeconds       int    `yaml:"connMaxLifetimeSeconds"`
	DefaultExecuteTimeoutSeconds int    `yaml:"defaultExecuteTimeoutSeconds"`
	Readonly                     bool   `yaml:"readonly"`
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &config, nil
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
	// Initialize datasources
	dsConfigs := make([]rdb.Config, len(config.Datasources))
	for i, ds := range config.Datasources {
		dsConfigs[i] = rdb.Config{
			DatasourceID:                 ds.DatasourceID,
			DatabaseName:                 ds.DatabaseName,
			Driver:                       ds.Driver,
			DSN:                          ds.DSN,
			MaxOpenConns:                 ds.MaxOpenConns,
			MinIdleConns:                 ds.MinIdleConns,
			MaxTransactionConns:          ds.MaxTransactionConns,
			ConnMaxLifetimeSeconds:       ds.ConnMaxLifetimeSeconds,
			DefaultExecuteTimeoutSeconds: ds.DefaultExecuteTimeoutSeconds,
			Readonly:                     ds.Readonly,
		}
	}
	datasources, err := rdb.Initialize(dsConfigs)
	if err != nil {
		log.Fatalf("Failed to initialize datasources: %v", err)
	}
	log.Printf("Initialized %d datasource(s)", len(config.Datasources))

	// Initialize TxIDGenerator
	txIDGen := rdb.NewTxIDGenerator()

	// Initialize TxManager
	txManager := rdb.NewTxManager(datasources, txIDGen)

	// Set maxHttpSessions for HTTP connection limiting
	maxHttpSessions := config.MaxHttpSessions
	if maxHttpSessions <= 0 {
		maxHttpSessions = 100 // default
	}
	log.Printf("HTTP connection limit: %d", maxHttpSessions)

	selfNode := &cluster.NodeInfo{
		NodeID:  config.NodeID,
		Status:  cluster.STARTING,
		BaseURL: fmt.Sprintf("http://localhost:%d", config.NodePort),
		HealthInfo: cluster.HealthInfo{
			MaxHttpSessions: config.MaxHttpSessions,
			Datasources:     []cluster.DatasourceInfo{},
		},
	}

	// collect cluster health information
	otherNodes := make([]cluster.NodeInfo, len(config.Cluster)-1)
	for i, node := range config.Cluster {
		if node.NodeID == config.NodeID {
			continue
		}
		otherNodes[i] = cluster.NodeInfo{
			NodeID:  node.NodeID,
			Status:  cluster.STARTING,
			BaseURL: node.BaseURL,
		}
	}
	balancer := cluster.NewBalancer(selfNode, otherNodes)

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

	go func() {
		for {
			time.Sleep(time.Duration(rand.Intn(2000)+2000) * time.Millisecond) // 2-4秒ランダム待ち
			router.CollectHealth()
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	// Shutdown TxManager
	txManager.Shutdown()

	// Close datasources
	if err := datasources.CloseAll(); err != nil {
		log.Printf("Error closing datasources: %v", err)
	}

	log.Println("Server exited")
}
