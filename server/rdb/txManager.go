package rdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrTxNotFound = errors.New("transaction not found")
	ErrTxExpired  = errors.New("transaction expired")
)

// TxEntry represents a transaction entry
type TxEntry struct {
	TxID           string
	dsIdx          int
	idleTimeoutSec int
	ExpiresAt      time.Time
	Conn           *sql.Conn
	Tx             *sql.Tx
}

// Touch updates the expiration time and last touch time
func (e *TxEntry) touch() {
	e.ExpiresAt = time.Now().Add(time.Duration(e.idleTimeoutSec) * time.Second)
}

type TxDatasource struct {
	ds      *Datasource
	entries map[string]*TxEntry
	mu      sync.RWMutex
}

// TxManager manages transactions
type TxManager struct {
	tars        []*TxDatasource
	txIDGen     *TxIDGenerator
	stopCleanup chan struct{}
	wg          sync.WaitGroup
}

func (tx *TxManager) close(entry *TxEntry) {
	tar := tx.tars[entry.dsIdx]
	tar.mu.Lock()
	defer tar.mu.Unlock()

	entry.Conn.Close()
	delete(tar.entries, entry.TxID)
}

// NewTxManager creates a new TxManager
func NewTxManager(configs []Config) *TxManager {
	tars := make([]*TxDatasource, len(configs))

	for i, cfg := range configs {
		ds, err := NewDatasource(cfg)
		if err != nil {
			panic(fmt.Sprintf("failed to initialize datasource %s: %v", cfg.DatasourceID, err))
		}

		tars[i] = &TxDatasource{
			ds:      ds,
			entries: make(map[string]*TxEntry),
		}
	}
	tm := &TxManager{
		tars:        tars,
		txIDGen:     NewTxIDGenerator(),
		stopCleanup: make(chan struct{}),
	}

	// Start background cleanup goroutine
	tm.wg.Add(1)
	go tm.cleanupExpired()

	return tm
}

// Begin starts a new transaction
func (tm *TxManager) Begin(clientNodeIndex int, datasourceIdx int, isolationLevel sql.IsolationLevel, timeoutSec *int) (*TxEntry, error) {
	// Get database connection
	tar := tm.tars[datasourceIdx]
	if tar == nil {
		return nil, fmt.Errorf("datasource not found: %s", tar.ds.DatasourceID)
	}

	timeout := time.Duration(tar.ds.DefaultTxIdleTimeoutSec) * time.Second
	if timeoutSec != nil {
		timeout = time.Duration(*timeoutSec) * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, err := tar.ds.DB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}

	// Begin transaction
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{
		Isolation: isolationLevel,
	})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Generate transaction ID
	txID, err := tm.txIDGen.Generate(clientNodeIndex, datasourceIdx)
	if err != nil {
		tx.Rollback()
		conn.Close()
		return nil, fmt.Errorf("failed to generate txid: %w", err)
	}

	// Create entry
	entry := &TxEntry{
		TxID:           txID,
		dsIdx:          datasourceIdx,
		idleTimeoutSec: int(timeout.Seconds()),
		ExpiresAt:      time.Now().Add(timeout),
		Conn:           conn,
		Tx:             tx,
	}

	// Register entry
	tar.mu.Lock()
	tar.entries[txID] = entry
	tar.mu.Unlock()

	return entry, nil
}

// Get retrieves a transaction entry and touches it
func (tm *TxManager) getTx(txID string) (entry *TxEntry, dsIdx int, err error) {
	txInfo, err := tm.txIDGen.VerifyAndParse(txID)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to verify and parse txID: %w", err)
	}

	tar := tm.tars[txInfo.DsIndex]
	tar.mu.RLock()
	entry, ok := tar.entries[txID]
	tar.mu.RUnlock()

	if !ok {
		return nil, 0, ErrTxNotFound
	}

	// Check if expired
	if time.Now().After(entry.ExpiresAt) {
		// Remove expired entry
		tar.mu.Lock()
		delete(tar.entries, txID)
		tar.mu.Unlock()
		// Cleanup connection
		go func() {
			entry.Tx.Rollback()
			entry.Conn.Close()
		}()
		return nil, 0, ErrTxExpired
	}

	return entry, txInfo.DsIndex, nil
}

// Commit commits a transaction
func (tm *TxManager) Commit(txID string) error {
	entry, _, err := tm.getTx(txID)
	if err != nil {
		return err
	}

	// Commit transaction
	err = entry.Tx.Commit()

	tm.close(entry)

	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Rollback rolls back a transaction
func (tm *TxManager) Rollback(txID string) error {
	entry, _, err := tm.getTx(txID)
	if err != nil {
		return err
	}

	// Rollback transaction
	err = entry.Tx.Rollback()

	tm.close(entry)

	if err != nil {
		return fmt.Errorf("failed to rollback transaction: %w", err)
	}

	return nil
}

// QueryContext queries the database
func (tm *TxManager) Query(ctx context.Context, timeoutSec *int, datasourceIdx int, sql string, parameters ...any) (*sql.Rows, error) {
	tar := tm.tars[datasourceIdx]
	if tar == nil {
		return nil, fmt.Errorf("datasource not found: %d", datasourceIdx)
	}
	if timeoutSec == nil {
		timeoutSec = &tar.ds.DefaultQueryTimeoutSec
	}
	ctx, _ = context.WithTimeout(ctx, time.Duration(*timeoutSec)*time.Second)

	return tar.ds.DB.QueryContext(ctx, sql, parameters...)
}

// QueryContext queries the database
func (tm *TxManager) QueryTx(ctx context.Context, timeoutSec *int, txId string, sql string, parameters ...any) (*sql.Rows, error) {
	entry, _, err := tm.getTx(txId)
	if err != nil {
		return nil, err
	}

	if timeoutSec == nil {
		timeoutSec = &tm.tars[entry.dsIdx].ds.DefaultQueryTimeoutSec
	}
	ctx, _ = context.WithTimeout(ctx, time.Duration(*timeoutSec)*time.Second)

	rows, err := entry.Tx.QueryContext(ctx, sql, parameters...)
	entry.touch()

	return rows, err
}

// ExecContext executes the database
func (tm *TxManager) Exec(ctx context.Context, timeoutSec *int, datasourceIdx int, sql string, parameters ...any) (sql.Result, error) {
	tar := tm.tars[datasourceIdx]
	if tar == nil {
		return nil, fmt.Errorf("datasource not found: %d", datasourceIdx)
	}
	if timeoutSec == nil {
		timeoutSec = &tar.ds.DefaultQueryTimeoutSec
	}
	ctx, _ = context.WithTimeout(ctx, time.Duration(*timeoutSec)*time.Second)

	return tar.ds.DB.ExecContext(ctx, sql, parameters...)
}

// ExecContext executes the database
func (tm *TxManager) ExecTx(ctx context.Context, timeoutSec *int, txId string, sql string, parameters ...any) (sql.Result, error) {
	entry, _, err := tm.getTx(txId)
	if err != nil {
		if err == ErrTxNotFound || err == ErrTxExpired {
			return nil, fmt.Errorf("Transaction not found or expired: %v", err)
		}
		return nil, fmt.Errorf("Failed to get transaction: %v", err)
	}

	if timeoutSec == nil {
		timeoutSec = &tm.tars[entry.dsIdx].ds.DefaultQueryTimeoutSec
	}
	ctx, _ = context.WithTimeout(ctx, time.Duration(*timeoutSec)*time.Second)

	result, err := entry.Tx.ExecContext(ctx, sql, parameters...)
	entry.touch()

	return result, err
}

func (tm *TxManager) Statistics(datasourceIdx int) (error, int, int, int) {
	tar := tm.tars[datasourceIdx]
	if tar == nil {
		return fmt.Errorf("datasource not found: %d", datasourceIdx), 0, 0, 0
	}
	tar.mu.RLock()
	defer tar.mu.RUnlock()

	stats := tar.ds.DB.Stats()
	openConns := stats.OpenConnections
	idleConns := stats.Idle
	//waitConns := int(stats.WaitCount)

	return nil, openConns, idleConns, len(tar.entries)
}

// cleanupExpired periodically cleans up expired transactions
func (tm *TxManager) cleanupExpired() {
	defer tm.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-tm.stopCleanup:
			return
		case <-ticker.C:
			now := time.Now()
			var expired []*TxEntry

			for _, tar := range tm.tars {
				tar.mu.Lock()
				for txID, entry := range tar.entries {
					if now.After(entry.ExpiresAt) {
						expired = append(expired, entry)
						delete(tar.entries, txID)
					}
				}
				tar.mu.Unlock()
			}
			// Cleanup expired entries
			for _, entry := range expired {
				entry.Tx.Rollback()
				entry.Conn.Close()
			}
		}
	}
}

// Shutdown stops the cleanup goroutine
func (tm *TxManager) Shutdown() {
	close(tm.stopCleanup)
	tm.wg.Wait()

	// Rollback all remaining transactions
	for _, tar := range tm.tars {
		tar.mu.Lock()
		for _, entry := range tar.entries {
			entry.Tx.Rollback()
			entry.Conn.Close()
		}

		tar.ds.Close()
		tar.mu.Unlock()
	}
}
