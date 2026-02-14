package rdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"smartdatastream/server/global"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

// errAsBadConn wraps an error so that errors.Is(err, driver.ErrBadConn) is true (e.g. for pgx ConnectError).
type errAsBadConn struct{ err error }

func (e *errAsBadConn) Error() string   { return e.err.Error() }
func (e *errAsBadConn) Unwrap() error   { return e.err }
func (e *errAsBadConn) Is(target error) bool { return target == driver.ErrBadConn }

// connectErrorToErrBadConnConnector wraps a driver.Connector and converts *pgconn.ConnectError to an error that Is(driver.ErrBadConn).
// So handlers can rely only on driver.ErrBadConn and need not import pgconn.
type connectErrorToErrBadConnConnector struct {
	driver.Connector
}

func (c *connectErrorToErrBadConnConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		var connectErr *pgconn.ConnectError
		if errors.As(err, &connectErr) {
			return nil, &errAsBadConn{err: err}
		}
		return nil, err
	}
	return conn, nil
}

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

	var db *sql.DB
	if driverName == "pgx" {
		connConfig, err := pgx.ParseConfig(config.DSN)
		if err != nil {
			return nil, fmt.Errorf("failed to parse pgx config for %s: %w", config.DatasourceID, err)
		}
		db = sql.OpenDB(&connectErrorToErrBadConnConnector{Connector: stdlib.GetConnector(*connConfig)})
	} else {
		var err error
		db, err = sql.Open(driverName, config.DSN)
		if err != nil {
			return nil, fmt.Errorf("failed to open datasource %s: %w", config.DatasourceID, err)
		}
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
