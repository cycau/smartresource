package rdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"smartdatastream/server/global"
	"sync"
	"time"
)

var (
	ErrTxNotFound = errors.New("transaction not found")
	ErrTxExpired  = errors.New("transaction expired")
)

/************************************************************
 * TxEntry manages a transaction entry
 ************************************************************/
type TxEntry struct {
	TxID        string
	dsIdx       int
	executing   bool
	ExpiresAt   time.Time
	idleTimeout *time.Duration
	Conn        *sql.Conn
	Tx          *sql.Tx
}

func (e *TxEntry) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	rows, err := e.Tx.QueryContext(ctx, query, args...)
	return rows, err
}

func (e *TxEntry) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	result, err := e.Tx.ExecContext(ctx, query, args...)
	return result, err
}

func (e *TxEntry) commit() error {
	err := e.Tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (e *TxEntry) rollback() error {
	err := e.Tx.Rollback()
	if err != nil {
		return err
	}
	return nil
}

func (e *TxEntry) cleanup() {
	e.Tx.Rollback()
	e.Conn.Close()
}

/************************************************************
 * TxDatasource manages transactions for a single datasource
 ************************************************************/
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

func (ds *TxDatasource) getEntry(txID string, executing bool) (*TxEntry, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	entry, ok := ds.entries[txID]
	if !ok {
		return nil, ErrTxNotFound
	}

	// Check if expired
	if time.Now().After(entry.ExpiresAt) {
		// Remove expired entry
		delete(ds.entries, entry.TxID)
		ds.cond.Signal() // notify waiting goroutines
		go entry.cleanup()

		return nil, ErrTxExpired
	}

	entry.executing = executing
	if entry.idleTimeout != nil {
		entry.ExpiresAt = time.Now().Add(*entry.idleTimeout)
	}

	return entry, nil
}

type releaseResourceFunc func()

func (ds *TxDatasource) givebackEntry(entry *TxEntry) {
	ds.mu.Lock()

	entry.executing = false
	if entry.idleTimeout != nil {
		entry.ExpiresAt = time.Now().Add(*entry.idleTimeout)
	}

	ds.mu.Unlock()
}

func (ds *TxDatasource) closeEntry(entry *TxEntry) {
	ds.mu.Lock()

	delete(ds.entries, entry.TxID)
	ds.cond.Signal()
	ds.mu.Unlock()

	go entry.Conn.Close()
}

/************************************************************
 * TxManager manages transactions for all datasources
 ************************************************************/
type TxManager struct {
	dss               []*TxDatasource
	txIDGen           *TxIDGenerator
	stopCleanupTicker chan struct{}
	wg                sync.WaitGroup
}

// NewTxManager creates a new TxManager
func NewTxManager(configs []global.DatasourceConfig) *TxManager {
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
		dss:               dss,
		txIDGen:           NewTxIDGenerator(),
		stopCleanupTicker: make(chan struct{}),
	}

	// Start background cleanup goroutine
	tm.wg.Add(1)
	go tm.startCleanupTicker()

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
		executing:   false,
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
func (tm *TxManager) getTx(txID string, executing bool) (entry *TxEntry, srcDs *TxDatasource, err error) {
	dsIdx, err := tm.txIDGen.GetDatasourceIndex(txID)
	if err != nil {
		return nil, nil, err
	}

	ds := tm.dss[dsIdx]
	entry, err = ds.getEntry(txID, executing)
	if err != nil {
		return nil, nil, err
	}

	return entry, ds, nil
}

// Commit commits a transaction
func (tm *TxManager) Commit(txID string) error {
	entry, _, err := tm.getTx(txID, false)
	if err != nil {
		return err
	}

	err = entry.commit()

	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Rollback rolls back a transaction
func (tm *TxManager) Rollback(txID string) error {
	entry, _, err := tm.getTx(txID, false)
	if err != nil {
		return err
	}

	err = entry.rollback()

	if err != nil {
		return fmt.Errorf("failed to rollback transaction: %w", err)
	}

	return nil
}

// Rollback rolls back a transaction
func (tm *TxManager) Close(txID string) error {
	entry, ds, err := tm.getTx(txID, false)
	if err != nil {
		return err
	}

	ds.closeEntry(entry)

	return nil
}

// QueryContext queries the database
func (tm *TxManager) Query(ctx context.Context, timeoutSec *int, datasourceIdx int, sql string, parameters ...any) (*sql.Rows, releaseResourceFunc, error) {
	ds := tm.dss[datasourceIdx]
	if ds == nil {
		return nil, nil, fmt.Errorf("datasource not found: %d", datasourceIdx)
	}

	timeout := ds.DefaultQueryTimeout
	if timeoutSec != nil {
		timeout = time.Duration(*timeoutSec) * time.Second
	}
	ctx, cancelCtx := context.WithTimeout(ctx, timeout)

	rows, err := ds.QueryContext(ctx, sql, parameters...)

	releaseResource := func() {
		cancelCtx()
	}

	return rows, releaseResource, err
}

// QueryContext queries the database
func (tm *TxManager) QueryTx(ctx context.Context, timeoutSec *int, txID string, sql string, parameters ...any) (*sql.Rows, releaseResourceFunc, error) {
	entry, ds, err := tm.getTx(txID, true)
	if err != nil {
		return nil, nil, err
	}

	timeout := ds.DefaultQueryTimeout
	if timeoutSec != nil {
		timeout = time.Duration(*timeoutSec) * time.Second
	}
	ctx, cancelCtx := context.WithTimeout(ctx, timeout)

	rows, err := entry.QueryContext(ctx, sql, parameters...)

	releaseResource := func() {
		cancelCtx()
		ds.givebackEntry(entry)
	}

	return rows, releaseResource, err
}

// ExecContext executes the database
func (tm *TxManager) Exec(ctx context.Context, timeoutSec *int, datasourceIdx int, sql string, parameters ...any) (sql.Result, releaseResourceFunc, error) {
	ds := tm.dss[datasourceIdx]
	if ds == nil {
		return nil, nil, fmt.Errorf("datasource not found: %d", datasourceIdx)
	}

	timeout := ds.DefaultQueryTimeout
	if timeoutSec != nil {
		timeout = time.Duration(*timeoutSec) * time.Second
	}
	ctx, cancelCtx := context.WithTimeout(ctx, timeout)

	result, err := ds.ExecContext(ctx, sql, parameters...)

	releaseResource := func() {
		cancelCtx()
	}

	return result, releaseResource, err
}

// ExecContext executes the database
func (tm *TxManager) ExecTx(ctx context.Context, timeoutSec *int, txId string, sql string, parameters ...any) (sql.Result, releaseResourceFunc, error) {
	entry, ds, err := tm.getTx(txId, true)
	if err != nil {
		return nil, nil, err
	}

	timeout := ds.DefaultQueryTimeout
	if timeoutSec != nil {
		timeout = time.Duration(*timeoutSec) * time.Second
	}
	ctx, cancelCtx := context.WithTimeout(ctx, timeout)

	result, err := entry.ExecContext(ctx, sql, parameters...)

	releaseResource := func() {
		cancelCtx()
		ds.givebackEntry(entry)
	}

	return result, releaseResource, err
}

/************************************************************
 * Statistics returns the statistics of a datasource
 ************************************************************/
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

/************************************************************
 * cleanupExpired periodically cleans up expired transactions
 ************************************************************/
func (tm *TxManager) startCleanupTicker() {
	defer tm.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-tm.stopCleanupTicker: // stop cleanup
			return
		case <-ticker.C: // cleanup expired transactions
			now := time.Now()
			var expired []*TxEntry

			for _, ds := range tm.dss {
				ds.mu.Lock()
				removedCount := 0

				for txID, entry := range ds.entries {
					if entry == nil {
						continue
					}
					if entry.executing {
						continue
					}
					if now.After(entry.ExpiresAt) {
						expired = append(expired, entry)
						delete(ds.entries, txID)
						removedCount++
					}
				}

				if removedCount > 0 {
					ds.cond.Broadcast()
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

/************************************************************
 * Shutdown stops the cleanup goroutine
 ************************************************************/
func (tm *TxManager) Shutdown() {
	close(tm.stopCleanupTicker)
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
