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
	TxID        string
	dsIdx       int
	ongoing     bool
	ExpiresAt   time.Time
	idleTimeout *time.Duration
	Conn        *sql.Conn
	Tx          *sql.Tx
}

// Touch updates the expiration time and last touch time
func (e *TxEntry) touch() {
	if e.idleTimeout == nil {
		return
	}
	e.ExpiresAt = time.Now().Add(*e.idleTimeout)
}

func (e *TxEntry) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := e.Tx.QueryContext(ctx, query, args...)
	e.touch()
	return rows, err
}

func (e *TxEntry) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	result, err := e.Tx.ExecContext(ctx, query, args...)
	e.touch()
	return result, err
}

func (e *TxEntry) commit() error {
	err := e.Tx.Commit()
	e.touch()
	if err != nil {
		return err
	}
	return nil
}

func (e *TxEntry) rollback() error {
	err := e.Tx.Rollback()
	e.touch()
	if err != nil {
		return err
	}
	return nil
}

func (e *TxEntry) cleanup() {
	e.Tx.Rollback()
	e.Conn.Close()
}

type TxDatasource struct {
	Datasource
	entries map[string]*TxEntry
	mu      sync.Mutex
	cond    *sync.Cond
}

func (ds *TxDatasource) reserveEntry(txID string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	for len(ds.entries) >= ds.MaxTxConns {
		ds.cond.Wait()
	}
	ds.entries[txID] = nil
}

func (ds *TxDatasource) registerEntry(entry *TxEntry) {
	ds.mu.Lock()
	ds.entries[entry.TxID] = entry
	ds.mu.Unlock()
}

func (ds *TxDatasource) getEntry(txID string) (*TxEntry, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	entry, ok := ds.entries[txID]
	if !ok {
		return nil, ErrTxNotFound
	}

	// Check if expired
	if time.Now().After(entry.ExpiresAt) {
		// Remove expired entry
		delete(ds.entries, txID)
		ds.cond.Signal() // notify waiting goroutines

		// Cleanup connection
		go entry.rollback()
		return nil, ErrTxExpired
	}

	entry.ongoing = true
	return entry, nil
}

func (ds *TxDatasource) givebackEntry(txID string) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	entry, ok := ds.entries[txID]
	if !ok {
		return ErrTxNotFound
	}

	entry.ongoing = false
	entry.touch()
	return nil
}

func (ds *TxDatasource) closeEntry(entry *TxEntry) {
	entry.Conn.Close()

	ds.mu.Lock()
	delete(ds.entries, entry.TxID)
	ds.cond.Signal() // notify waiting goroutines
	ds.mu.Unlock()
}

// TxManager manages transactions
type TxManager struct {
	dss         []*TxDatasource
	txIDGen     *TxIDGenerator
	stopCleanup chan struct{}
	wg          sync.WaitGroup
}

// NewTxManager creates a new TxManager
func NewTxManager(configs []Config) *TxManager {
	dss := make([]*TxDatasource, len(configs))

	for i, cfg := range configs {
		ds, err := NewDatasource(cfg)
		if err != nil {
			panic(fmt.Sprintf("failed to initialize datasource %s: %v", cfg.DatasourceID, err))
		}

		txDs := &TxDatasource{
			Datasource: *ds,
			entries:    make(map[string]*TxEntry),
		}
		txDs.cond = sync.NewCond(&txDs.mu)
		dss[i] = txDs
	}
	tm := &TxManager{
		dss:         dss,
		txIDGen:     NewTxIDGenerator(),
		stopCleanup: make(chan struct{}),
	}

	// Start background cleanup goroutine
	tm.wg.Add(1)
	go tm.cleanupExpired()

	return tm
}

// Begin starts a new transaction
func (tm *TxManager) Begin(datasourceIdx int, isolationLevel sql.IsolationLevel, timeoutSec *int, clientNodeIndex int) (*TxEntry, error) {
	txID, err := tm.txIDGen.Generate(datasourceIdx, clientNodeIndex)
	if err != nil {
		return nil, err
	}

	ds := tm.dss[datasourceIdx]
	if ds == nil {
		return nil, fmt.Errorf("datasource not found: %d", datasourceIdx)
	}

	ds.reserveEntry(txID)

	conn, tx, err := ds.newTx(isolationLevel)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}

	idleTimeout := &ds.MaxTxIdleTimeout
	expiresAt := time.Now().Add(ds.MaxTxIdleTimeout)
	if timeoutSec != nil {
		expiresAt = time.Now().Add(time.Duration(*timeoutSec) * time.Second)
		idleTimeout = nil
	}
	// Create entry
	entry := &TxEntry{
		TxID:        txID,
		dsIdx:       datasourceIdx,
		idleTimeout: idleTimeout,
		ExpiresAt:   expiresAt,
		Conn:        conn,
		Tx:          tx,
	}

	// Register entry
	ds.registerEntry(entry)
	fmt.Printf("Registered transaction: %s, total entries: %d\n", txID, len(ds.entries))

	return entry, nil
}

// Get retrieves a transaction entry and touches it
func (tm *TxManager) getTx(txID string) (entry *TxEntry, srcDs *TxDatasource, err error) {
	dsIdx, err := tm.txIDGen.GetDatasourceIndex(txID)
	if err != nil {
		return nil, nil, err
	}

	ds := tm.dss[dsIdx]
	entry, err = ds.getEntry(txID)
	if err != nil {
		return nil, nil, err
	}

	return entry, ds, nil
}

// Commit commits a transaction^
func (tm *TxManager) Commit(txID string) error {
	entry, ds, err := tm.getTx(txID)
	if err != nil {
		return err
	}

	err = entry.commit()

	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	ds.closeEntry(entry)

	return nil
}

// Rollback rolls back a transaction
func (tm *TxManager) Rollback(txID string) error {
	entry, ds, err := tm.getTx(txID)
	if err != nil {
		return err
	}

	err = entry.rollback()

	if err != nil {
		return fmt.Errorf("failed to rollback transaction: %w", err)
	}

	ds.closeEntry(entry)

	return nil
}

// QueryContext queries the database
func (tm *TxManager) Query(ctx context.Context, timeoutSec *int, datasourceIdx int, sql string, parameters ...any) (*sql.Rows, context.CancelFunc, error) {
	ds := tm.dss[datasourceIdx]
	if ds == nil {
		return nil, nil, fmt.Errorf("datasource not found: %d", datasourceIdx)
	}
	timeout := ds.DefaultQueryTimeout
	if timeoutSec != nil {
		timeout = time.Duration(*timeoutSec) * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)

	rows, err := ds.QueryContext(ctx, sql, parameters...)

	return rows, cancel, err
}

// QueryContext queries the database
func (tm *TxManager) QueryTx(ctx context.Context, timeoutSec *int, txId string, sql string, parameters ...any) (*sql.Rows, context.CancelFunc, error) {
	entry, ds, err := tm.getTx(txId)
	if err != nil {
		return nil, nil, err
	}

	timeout := ds.DefaultQueryTimeout
	if timeoutSec != nil {
		timeout = time.Duration(*timeoutSec) * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)

	rows, err := entry.QueryContext(ctx, sql, parameters...)
	entry.touch()

	return rows, cancel, err
}

// ExecContext executes the database
func (tm *TxManager) Exec(ctx context.Context, timeoutSec *int, datasourceIdx int, sql string, parameters ...any) (sql.Result, context.CancelFunc, error) {
	ds := tm.dss[datasourceIdx]
	if ds == nil {
		return nil, nil, fmt.Errorf("datasource not found: %d", datasourceIdx)
	}

	timeout := ds.DefaultQueryTimeout
	if timeoutSec != nil {
		timeout = time.Duration(*timeoutSec) * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)

	result, err := ds.ExecContext(ctx, sql, parameters...)

	return result, cancel, err
}

// ExecContext executes the database
func (tm *TxManager) ExecTx(ctx context.Context, timeoutSec *int, txId string, sql string, parameters ...any) (sql.Result, context.CancelFunc, error) {
	entry, ds, err := tm.getTx(txId)
	if err != nil {
		if err == ErrTxNotFound || err == ErrTxExpired {
			return nil, nil, fmt.Errorf("Transaction not found or expired: %v", err)
		}
		return nil, nil, fmt.Errorf("Failed to get transaction: %v", err)
	}

	timeout := ds.DefaultQueryTimeout
	if timeoutSec != nil {
		timeout = time.Duration(*timeoutSec) * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)

	result, err := entry.ExecContext(ctx, sql, parameters...)
	entry.touch()

	return result, cancel, err
}

func (tm *TxManager) Statistics(datasourceIdx int) (error, int, int, int) {
	ds := tm.dss[datasourceIdx]
	if ds == nil {
		return fmt.Errorf("datasource not found: %d", datasourceIdx), 0, 0, 0
	}
	ds.mu.Lock()
	defer ds.mu.Unlock()

	stats := ds.DB.Stats()
	openConns := stats.OpenConnections
	idleConns := stats.Idle
	//waitConns := int(stats.WaitCount)

	return nil, openConns, idleConns, len(ds.entries)
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

			for _, ds := range tm.dss {
				ds.mu.Lock()
				removedCount := 0
				for txID, entry := range ds.entries {
					if now.After(entry.ExpiresAt) {
						expired = append(expired, entry)
						delete(ds.entries, txID)
						removedCount++
					}
				}
				if removedCount > 0 {
					ds.cond.Broadcast() // 複数のエントリが削除された可能性があるので、すべての待機中のgoroutineに通知
				}
				ds.mu.Unlock()
			}
			// Cleanup expired entries
			for _, entry := range expired {
				go entry.cleanup()
			}
		}
	}
}

// Shutdown stops the cleanup goroutine
func (tm *TxManager) Shutdown() {
	close(tm.stopCleanup)
	tm.wg.Wait()

	// Rollback all remaining transactions
	for _, ds := range tm.dss {
		ds.mu.Lock()
		for _, entry := range ds.entries {
			entry.cleanup()
		}

		ds.Close()
		ds.mu.Unlock()
	}
}
