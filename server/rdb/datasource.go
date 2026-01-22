package rdb

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// DatasourceConfig represents configuration for a single datasource
type Config struct {
	DatasourceID            string
	DatabaseName            string
	Driver                  string
	DSN                     string
	MaxOpenConns            int
	MinIdleConns            int
	MaxTransactionConns     int
	ConnMaxLifetimeSec      int
	DefaultQueryTimeoutSec  int
	DefaultTxIdleTimeoutSec int
	Readonly                bool
}

type Datasource struct {
	DatasourceID            string
	DatabaseName            string
	Driver                  string
	MaxTxConns              int
	DefaultQueryTimeoutSec  int
	DefaultTxIdleTimeoutSec int
	DB                      *sql.DB
	Readonly                bool
}

// Initialize initializes datasources from configuration
func NewDatasource(cfg Config) (*Datasource, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	driverName := cfg.Driver
	if driverName == "postgres" {
		driverName = "pgx"
	}

	db, err := sql.Open(driverName, cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open datasource %s: %w", cfg.DatasourceID, err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MinIdleConns)
	if cfg.ConnMaxLifetimeSec > 0 {
		db.SetConnMaxLifetime(time.Duration(cfg.ConnMaxLifetimeSec) * time.Second)
	}

	// Ping to verify connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping datasource %s: %w", cfg.DatasourceID, err)
	}

	return &Datasource{
		DatasourceID:            cfg.DatasourceID,
		DatabaseName:            cfg.DatabaseName,
		Driver:                  driverName,
		MaxTxConns:              cfg.MaxTransactionConns,
		DefaultQueryTimeoutSec:  cfg.DefaultQueryTimeoutSec,
		DefaultTxIdleTimeoutSec: cfg.DefaultTxIdleTimeoutSec,
		DB:                      db,
		Readonly:                cfg.Readonly,
	}, nil
}

// Close closes all database connections
func (d *Datasource) Close() error {
	if err := d.DB.Close(); err != nil {
		return err
	}
	return nil
}
