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
	DatasourceID           string
	DatabaseName           string
	Driver                 string
	DSN                    string
	MaxOpenConns           int
	MinIdleConns           int
	MaxConnLifetimeSec     int
	MaxTxConns             int
	MaxTxIdleTimeoutSec    int
	DefaultQueryTimeoutSec int
	Readonly               bool
}

type Datasource struct {
	DatasourceID        string
	DatabaseName        string
	Driver              string
	MaxTxConns          int
	MaxTxIdleTimeout    time.Duration
	DefaultQueryTimeout time.Duration
	DB                  *sql.DB
	Readonly            bool
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
	if cfg.MaxConnLifetimeSec > 0 {
		db.SetConnMaxLifetime(time.Duration(cfg.MaxConnLifetimeSec) * time.Second)
	}

	// Ping to verify connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping datasource %s: %w", cfg.DatasourceID, err)
	}

	return &Datasource{
		DatasourceID:        cfg.DatasourceID,
		DatabaseName:        cfg.DatabaseName,
		Driver:              driverName,
		MaxTxConns:          cfg.MaxTxConns,
		MaxTxIdleTimeout:    time.Duration(cfg.MaxTxIdleTimeoutSec) * time.Second,
		DefaultQueryTimeout: time.Duration(cfg.DefaultQueryTimeoutSec) * time.Second,
		DB:                  db,
		Readonly:            cfg.Readonly,
	}, nil
}

func (d *Datasource) newTx(isolationLevel sql.IsolationLevel) (*sql.Conn, *sql.Tx, error) {
	conn, err := d.DB.Conn(context.Background())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get connection: %w", err)
	}

	// Begin transaction
	tx, err := conn.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: isolationLevel,
	})
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	return conn, tx, nil
}

func (d *Datasource) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := d.DB.QueryContext(ctx, query, args...)
	return rows, err
}

func (d *Datasource) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	result, err := d.DB.ExecContext(ctx, query, args...)
	return result, err
}

// Close closes all database connections
func (d *Datasource) Close() error {
	if err := d.DB.Close(); err != nil {
		return err
	}
	return nil
}
