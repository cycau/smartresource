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
	DatasourceID                 string
	DatabaseName                 string
	Driver                       string
	DSN                          string
	MaxOpenConns                 int
	MinIdleConns                 int
	MaxTransactionConns          int
	ConnMaxLifetimeSeconds       int
	DefaultExecuteTimeoutSeconds int
	Readonly                     bool
}

type Datasource struct {
	DatasourceID                 string
	DatabaseName                 string
	Driver                       string
	DefaultExecuteTimeoutSeconds int
	DB                           *sql.DB
	Readonly                     bool
}

// Datasources holds a map of datasource ID to *sql.DB
type Datasources struct {
	dss     []*Datasource
	dsIdMap map[string]uint16 // datasourceId -> index
}

// Initialize initializes datasources from configuration
func Initialize(configs []Config) (*Datasources, error) {
	d := &Datasources{
		dss:     make([]*Datasource, 0, len(configs)),
		dsIdMap: make(map[string]uint16, len(configs)),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Build datasource short ID mapping (temporary implementation: index + 1)
	for i, cfg := range configs {
		d.dsIdMap[cfg.DatasourceID] = uint16(i)

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
		if cfg.ConnMaxLifetimeSeconds > 0 {
			db.SetConnMaxLifetime(time.Duration(cfg.ConnMaxLifetimeSeconds) * time.Second)
		}

		// Ping to verify connection
		if err := db.PingContext(ctx); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to ping datasource %s: %w", cfg.DatasourceID, err)
		}

		d.dss[i] = &Datasource{
			DatasourceID:                 cfg.DatasourceID,
			DatabaseName:                 cfg.DatabaseName,
			Driver:                       driverName,
			DefaultExecuteTimeoutSeconds: cfg.DefaultExecuteTimeoutSeconds,
			DB:                           db,
			Readonly:                     cfg.Readonly,
		}
	}

	return d, nil
}

// GetDatasourceShort returns the short ID for a datasource ID
func (d *Datasources) Get(datasourceIdx int) (*Datasource, bool) {
	if datasourceIdx < 0 || datasourceIdx >= len(d.dss) {
		return nil, false
	}
	return d.dss[datasourceIdx], true
}

// Get returns a *sql.DB for the given datasource ID
func (d *Datasources) GetById(datasourceID string) (*Datasource, bool) {
	ds, ok := d.dsIdMap[datasourceID]
	return d.dss[ds], ok
}

// Close closes all database connections
func (d *Datasources) CloseAll() error {
	var firstErr error
	for _, db := range d.dss {
		if err := db.DB.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
