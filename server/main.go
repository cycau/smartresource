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
	"smartdatastream/server/rdb"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type Config struct {
	NodeName        string             `yaml:"nodeName"`
	NodePort        int                `yaml:"nodePort"`
	SecretKey       string             `yaml:"secretKey"`
	MaxHttpSessions int                `yaml:"maxHttpSessions"`
	Datasources     []DatasourceConfig `yaml:"datasources"`
	ClusterNodes    []string           `yaml:"clusterNodes"`
}

type DatasourceConfig struct {
	DatasourceID           string `yaml:"datasourceId"`
	DatabaseName           string `yaml:"databaseName"`
	Driver                 string `yaml:"driver"`
	DSN                    string `yaml:"dsn"`
	MaxOpenConns           int    `yaml:"maxOpenConns"`
	MinIdleConns           int    `yaml:"minIdleConns"`
	MaxConnLifetimeSec     int    `yaml:"maxConnLifetimeSec"`
	MaxTxConns             int    `yaml:"maxTxConns"`
	MaxTxIdleTimeoutSec    int    `yaml:"maxTxIdleTimeoutSec"`
	DefaultQueryTimeoutSec int    `yaml:"defaultQueryTimeoutSec"`
	Readonly               bool   `yaml:"readonly"`
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
	// Initialize datasources
	dsConfigs := make([]rdb.Config, len(config.Datasources))
	hzDatasources := make([]cluster.DatasourceInfo, len(config.Datasources))
	for i, ds := range config.Datasources {
		if ds.Readonly {
			ds.MaxTxConns = 0
		}

		dsConfigs[i] = rdb.Config{
			DatasourceID:           ds.DatasourceID,
			DatabaseName:           ds.DatabaseName,
			Driver:                 ds.Driver,
			DSN:                    ds.DSN,
			MaxOpenConns:           ds.MaxOpenConns,
			MinIdleConns:           ds.MinIdleConns,
			MaxConnLifetimeSec:     ds.MaxConnLifetimeSec,
			MaxTxConns:             ds.MaxTxConns,
			DefaultQueryTimeoutSec: ds.DefaultQueryTimeoutSec,
			MaxTxIdleTimeoutSec:    ds.MaxTxIdleTimeoutSec,
			Readonly:               ds.Readonly,
		}

		hzDatasources[i] = cluster.DatasourceInfo{
			DatasourceID: ds.DatasourceID,
			DatabaseName: ds.DatabaseName,
			Active:       true,
			Readonly:     ds.Readonly,
			MaxOpenConns: ds.MaxOpenConns,
			MinIdleConns: ds.MinIdleConns,
			MaxTxConns:   ds.MaxTxConns,
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
		NodeID:    fmt.Sprintf("%s-%s", config.NodeName, UUID()),
		Status:    cluster.STARTING,
		BaseURL:   "-",
		SecretKey: config.SecretKey,
		HealthInfo: cluster.HealthInfo{
			MaxHttpSessions: config.MaxHttpSessions,
			Datasources:     hzDatasources,
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
	balancer := cluster.NewBalancer(selfNode, clusterNodes)

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
