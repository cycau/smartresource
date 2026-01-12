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
	TxID      string
	DsIdx     int
	Conn      *sql.Conn
	Tx        *sql.Tx
	ExpiresAt time.Time
	LastTouch time.Time
	mu        sync.RWMutex
}

// Touch updates the expiration time and last touch time
func (e *TxEntry) Touch(ttl time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now()
	e.ExpiresAt = now.Add(ttl)
	e.LastTouch = now
}

// IsExpired checks if the transaction is expired
func (e *TxEntry) IsExpired() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return time.Now().After(e.ExpiresAt)
}

// TxManager manages transactions
type TxManager struct {
	datasources *Datasources
	txIDGen     *TxIDGenerator
	entries     map[string]*TxEntry
	stopCleanup chan struct{}
	mu          sync.RWMutex
	wg          sync.WaitGroup
}

// NewTxManager creates a new TxManager
func NewTxManager(datasources *Datasources, txIDGen *TxIDGenerator) *TxManager {
	tm := &TxManager{
		datasources: datasources,
		txIDGen:     txIDGen,
		entries:     make(map[string]*TxEntry),
		stopCleanup: make(chan struct{}),
	}

	// Start background cleanup goroutine
	tm.wg.Add(1)
	go tm.cleanupExpired()

	return tm
}

// Begin starts a new transaction
func (tm *TxManager) Begin(reqCtx context.Context, nodeIndex int, datasourceIdx int, isolationLevel sql.IsolationLevel, executeTimeoutMs *int) (*TxEntry, error) {
	// Get database connection
	ds, ok := tm.datasources.Get(datasourceIdx)
	if !ok {
		return nil, fmt.Errorf("datasource not found: %s", ds.DatasourceID)
	}

	timeout := time.Duration(ds.DefaultExecuteTimeoutSeconds) * time.Second
	if executeTimeoutMs != nil && *executeTimeoutMs > 0 {
		timeout = time.Duration(*executeTimeoutMs)
	}
	expiresAt := time.Now().Add(timeout)
	ctx, cancel := context.WithTimeout(reqCtx, timeout)
	defer cancel()

	conn, err := ds.DB.Conn(ctx)
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
	txID, err := tm.txIDGen.Generate(nodeIndex, datasourceIdx)
	if err != nil {
		tx.Rollback()
		conn.Close()
		return nil, fmt.Errorf("failed to generate txid: %w", err)
	}

	// Create entry
	now := time.Now()
	entry := &TxEntry{
		TxID:      txID,
		DsIdx:     datasourceIdx,
		Conn:      conn,
		Tx:        tx,
		ExpiresAt: expiresAt,
		LastTouch: now,
	}

	// Register entry
	tm.mu.Lock()
	tm.entries[txID] = entry
	tm.mu.Unlock()

	return entry, nil
}

// Get retrieves a transaction entry and touches it
func (tm *TxManager) Get(txID string) (*TxEntry, error) {
	tm.mu.RLock()
	entry, ok := tm.entries[txID]
	tm.mu.RUnlock()

	if !ok {
		return nil, ErrTxNotFound
	}

	// Check if expired
	if entry.IsExpired() {
		// Remove expired entry
		tm.mu.Lock()
		delete(tm.entries, txID)
		tm.mu.Unlock()
		// Cleanup connection
		go func() {
			entry.Tx.Rollback()
			entry.Conn.Close()
		}()
		return nil, ErrTxExpired
	}

	// Touch to extend expiration
	entry.Touch(time.Until(entry.ExpiresAt))

	return entry, nil
}

// Commit commits a transaction
func (tm *TxManager) Commit(txID string) error {
	tm.mu.Lock()
	entry, ok := tm.entries[txID]
	if ok {
		delete(tm.entries, txID)
	}
	tm.mu.Unlock()

	if !ok {
		return ErrTxNotFound
	}

	// Commit transaction
	err := entry.Tx.Commit()

	// Always close connection
	entry.Conn.Close()

	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Rollback rolls back a transaction
func (tm *TxManager) Rollback(txID string) error {
	tm.mu.Lock()
	entry, ok := tm.entries[txID]
	if ok {
		delete(tm.entries, txID)
	}
	tm.mu.Unlock()

	if !ok {
		return ErrTxNotFound
	}

	// Rollback transaction
	err := entry.Tx.Rollback()

	// Always close connection
	entry.Conn.Close()

	if err != nil {
		return fmt.Errorf("failed to rollback transaction: %w", err)
	}

	return nil
}

// QueryContext queries the database
func (tm *TxManager) QueryContext(ctx context.Context, datasourceIdx int, sql string, args ...interface{}) (*sql.Rows, error) {
	ds, ok := tm.datasources.Get(datasourceIdx)
	if !ok {
		return nil, fmt.Errorf("datasource not found: %d", datasourceIdx)
	}
	return ds.DB.QueryContext(ctx, sql, args...)
}

// ExecContext executes the database
func (tm *TxManager) ExecContext(ctx context.Context, datasourceIdx int, sql string, args ...interface{}) (sql.Result, error) {
	ds, ok := tm.datasources.Get(datasourceIdx)
	if !ok {
		return nil, fmt.Errorf("datasource not found: %d", datasourceIdx)
	}
	return ds.DB.ExecContext(ctx, sql, args...)
}

func (tm *TxManager) Statistics(datasourceIdx int) (int, int, int) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	ds, _ := tm.datasources.Get(datasourceIdx)
	stats := ds.DB.Stats()
	openConns := stats.OpenConnections
	idleConns := stats.Idle
	//waitConns := int(stats.WaitCount)

	runningTx := 0
	for _, entry := range tm.entries {
		if entry.DsIdx == datasourceIdx {
			runningTx++
		}
	}

	return openConns, idleConns, runningTx
}

// cleanupExpired periodically cleans up expired transactions
func (tm *TxManager) cleanupExpired() {
	defer tm.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tm.cleanup()
		case <-tm.stopCleanup:
			return
		}
	}
}

// cleanup removes expired transactions
func (tm *TxManager) cleanup() {
	now := time.Now()
	var expired []*TxEntry

	tm.mu.Lock()
	for txID, entry := range tm.entries {
		if now.After(entry.ExpiresAt) {
			expired = append(expired, entry)
			delete(tm.entries, txID)
		}
	}
	tm.mu.Unlock()

	// Cleanup expired entries
	for _, entry := range expired {
		entry.Tx.Rollback()
		entry.Conn.Close()
	}
}

// GetDatasources returns the datasources instance
func (tm *TxManager) GetDatasources() *Datasources {
	return tm.datasources
}

// Shutdown stops the cleanup goroutine
func (tm *TxManager) Shutdown() {
	close(tm.stopCleanup)
	tm.wg.Wait()

	// Rollback all remaining transactions
	tm.mu.Lock()
	entries := make([]*TxEntry, 0, len(tm.entries))
	for _, entry := range tm.entries {
		entries = append(entries, entry)
	}
	tm.mu.Unlock()

	for _, entry := range entries {
		entry.Tx.Rollback()
		entry.Conn.Close()
	}
}
