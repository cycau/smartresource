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
type DatasourceConfig struct {
	DatasourceID                 string
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
	shortID                      uint16
	Driver                       string
	DefaultExecuteTimeoutSeconds int
	DB                           *sql.DB
	Readonly                     bool
}

// Datasources holds a map of datasource ID to *sql.DB
type Datasources struct {
	dsMap          map[string]*Datasource
	dsShortMap     map[string]uint16 // datasourceId -> short ID
	dsShortReverse map[uint16]string // short ID -> datasourceId
}

// NewDatasources creates a new Datasources instance
func NewDatasources() *Datasources {
	return &Datasources{
		dsMap:          make(map[string]*Datasource),
		dsShortMap:     make(map[string]uint16),
		dsShortReverse: make(map[uint16]string),
	}
}

// Initialize initializes datasources from configuration
func (d *Datasources) Initialize(configs []DatasourceConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Build datasource short ID mapping (temporary implementation: index + 1)
	for i, cfg := range configs {
		shortID := uint16(i + 1)
		d.dsShortMap[cfg.DatasourceID] = shortID
		d.dsShortReverse[shortID] = cfg.DatasourceID

		driverName := cfg.Driver
		if driverName == "postgres" {
			driverName = "pgx"
		}

		db, err := sql.Open(driverName, cfg.DSN)
		if err != nil {
			return fmt.Errorf("failed to open datasource %s: %w", cfg.DatasourceID, err)
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
			return fmt.Errorf("failed to ping datasource %s: %w", cfg.DatasourceID, err)
		}

		d.dsMap[cfg.DatasourceID] = &Datasource{
			DatasourceID:                 cfg.DatasourceID,
			shortID:                      shortID,
			Driver:                       driverName,
			DefaultExecuteTimeoutSeconds: cfg.DefaultExecuteTimeoutSeconds,
			DB:                           db,
			Readonly:                     cfg.Readonly,
		}
	}

	return nil
}

// Get returns a *sql.DB for the given datasource ID
func (d *Datasources) Get(datasourceID string) (*Datasource, bool) {
	ds, ok := d.dsMap[datasourceID]
	return ds, ok
}

// Set sets a *sql.DB for the given datasource ID
func (d *Datasources) Set(datasourceID string, db *sql.DB) {
	d.dsMap[datasourceID] = &Datasource{
		DatasourceID: datasourceID,
		DB:           db,
	}
}

// GetDatasourceShort returns the short ID for a datasource ID
func (d *Datasources) GetDatasourceShort(datasourceID string) (uint16, bool) {
	shortID, ok := d.dsShortMap[datasourceID]
	return shortID, ok
}

// GetDatasourceID returns the datasource ID for a short ID
func (d *Datasources) GetDatasourceID(shortID uint16) (string, bool) {
	datasourceID, ok := d.dsShortReverse[shortID]
	return datasourceID, ok
}

// Close closes all database connections
func (d *Datasources) Close() error {
	var firstErr error
	for _, db := range d.dsMap {
		if err := db.DB.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
