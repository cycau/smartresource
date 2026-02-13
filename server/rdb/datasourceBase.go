package rdb

import (
	"context"
	"database/sql"
	"fmt"
	"smartdatastream/server/global"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type Datasource struct {
	DatasourceID        string
	DatabaseName        string
	Driver              string
	DB                  *sql.DB
	Readonly            bool
	DefaultQueryTimeout time.Duration
}

// Initialize initializes datasources from configuration
func NewDatasource(config global.DatasourceConfig) (*Datasource, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	driverName := config.Driver
	if driverName == "postgres" {
		driverName = "pgx"
	}

	db, err := sql.Open(driverName, config.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open datasource %s: %w", config.DatasourceID, err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(config.MaxOpenConns)
	db.SetMaxIdleConns(config.MaxIdleConns)
	if config.MaxConnLifetimeSec > 0 {
		db.SetConnMaxLifetime(time.Duration(config.MaxConnLifetimeSec) * time.Second)
	}

	// Ping to verify connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping datasource %s: %w", config.DatasourceID, err)
	}

	return &Datasource{
		DatasourceID:        config.DatasourceID,
		DatabaseName:        config.DatabaseName,
		Driver:              driverName,
		DB:                  db,
		DefaultQueryTimeout: time.Duration(config.DefaultQueryTimeoutSec) * time.Second,
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

func (d *Datasource) Close() error {
	if err := d.DB.Close(); err != nil {
		return err
	}
	return nil
}
