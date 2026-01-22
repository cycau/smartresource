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
	SecretKey       string              `yaml:"secretKey"`
	MaxHttpSessions int                 `yaml:"maxHttpSessions"`
	Cluster         []ClusterNodeConfig `yaml:"cluster"`
	Datasources     []DatasourceConfig  `yaml:"datasources"`
}

type ClusterNodeConfig struct {
	NodeID  string `yaml:"nodeId"`
	BaseURL string `yaml:"baseUrl"`
}

type DatasourceConfig struct {
	DatasourceID            string `yaml:"datasourceId"`
	DatabaseName            string `yaml:"databaseName"`
	Driver                  string `yaml:"driver"`
	DSN                     string `yaml:"dsn"`
	MaxOpenConns            int    `yaml:"maxOpenConns"`
	MinIdleConns            int    `yaml:"minIdleConns"`
	MaxTransactionConns     int    `yaml:"maxTransactionConns"`
	ConnMaxLifetimeSec      int    `yaml:"connMaxLifetimeSec"`
	DefaultQueryTimeoutSec  int    `yaml:"defaultQueryTimeoutSec"`
	DefaultTxIdleTimeoutSec int    `yaml:"defaultTxIdleTimeoutSec"`
	Readonly                bool   `yaml:"readonly"`
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
	hzDatasources := make([]cluster.DatasourceInfo, len(config.Datasources))
	for i, ds := range config.Datasources {
		dsConfigs[i] = rdb.Config{
			DatasourceID:            ds.DatasourceID,
			DatabaseName:            ds.DatabaseName,
			Driver:                  ds.Driver,
			DSN:                     ds.DSN,
			MaxOpenConns:            ds.MaxOpenConns,
			MinIdleConns:            ds.MinIdleConns,
			MaxTransactionConns:     ds.MaxTransactionConns,
			ConnMaxLifetimeSec:      ds.ConnMaxLifetimeSec,
			DefaultQueryTimeoutSec:  ds.DefaultQueryTimeoutSec,
			DefaultTxIdleTimeoutSec: ds.DefaultTxIdleTimeoutSec,
			Readonly:                ds.Readonly,
		}

		hzDatasources[i] = cluster.DatasourceInfo{
			DatasourceID: ds.DatasourceID,
			DatabaseName: ds.DatabaseName,
			Active:       true,
			Readonly:     ds.Readonly,
			MaxOpenConns: ds.MaxOpenConns,
			MinIdleConns: ds.MinIdleConns,
			MaxTxConns:   ds.MaxTransactionConns,
		}
	}

	// Initialize TxManager
	txManager := rdb.NewTxManager(dsConfigs)

	// Set maxHttpSessions for HTTP connection limiting
	maxHttpSessions := config.MaxHttpSessions
	if maxHttpSessions <= 0 {
		maxHttpSessions = 1000 // default
	}
	log.Printf("HTTP connection limit: %d", maxHttpSessions)

	selfNode := &cluster.NodeInfo{
		NodeID:    config.NodeID,
		Status:    cluster.STARTING,
		BaseURL:   "-",
		SecretKey: config.SecretKey,
		HealthInfo: cluster.HealthInfo{
			MaxHttpSessions: config.MaxHttpSessions,
			Datasources:     hzDatasources,
		},
	}

	// collect cluster health information
	otherNodes := make([]cluster.NodeInfo, 0, len(config.Cluster))
	for _, node := range config.Cluster {
		if node.NodeID == config.NodeID {
			continue
		}
		otherNodes = append(otherNodes, cluster.NodeInfo{
			NodeID:  node.NodeID,
			Status:  cluster.STARTING,
			BaseURL: node.BaseURL,
		})
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

	// collect once at startup
	router.CollectHealth()
	time.Sleep(1000 * time.Millisecond) // wait for collection
	selfNode.Mu.Lock()
	selfNode.Status = cluster.SERVING
	selfNode.HealthInfo.UpTime = time.Now()
	selfNode.Mu.Unlock()

	go func() {
		for {
			time.Sleep(time.Duration(3000+rand.Intn(2000)) * time.Millisecond) // 3-5秒ランダム待ち
			router.CollectHealth()
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")
	selfNode.Mu.Lock()
	selfNode.Status = cluster.STOPPING
	selfNode.Mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	// Shutdown TxManager
	txManager.Shutdown()

	log.Println("Server exited")
}
