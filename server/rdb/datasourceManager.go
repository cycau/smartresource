package rdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"smartdatastream/server/global"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
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

/************************************************************
 * TxDatasource manages transactions for a single datasource
 ************************************************************/
type TxDatasource struct {
	Datasource
	MaxTxIdleTimeout time.Duration

	mu       sync.Mutex
	semRead  *semaphore.Weighted
	semWrite *semaphore.Weighted
	entries  map[string]*TxEntry

	runningRead  int
	runningWrite int
	runningTx    int
}

func (ds *TxDatasource) registerEntry(entry *TxEntry) {
	ds.mu.Lock()

	ds.entries[entry.TxID] = entry

	ds.mu.Unlock()
}

type ResourceType int

const (
	RESOURCE_TYPE_READ ResourceType = iota
	RESOURCE_TYPE_WRITE
	RESOURCE_TYPE_TX
)

func (ds *TxDatasource) releaseResource(resourceType ResourceType) {
	ds.mu.Lock()
	switch resourceType {
	case RESOURCE_TYPE_READ:
		ds.runningRead--
	case RESOURCE_TYPE_WRITE:
		ds.runningWrite--
	case RESOURCE_TYPE_TX:
		ds.runningTx--
	}
	ds.mu.Unlock()

	switch resourceType {
	case RESOURCE_TYPE_READ:
		ds.semRead.Release(1)
	case RESOURCE_TYPE_WRITE:
		ds.semWrite.Release(1)
	case RESOURCE_TYPE_TX:
		ds.semWrite.Release(1)
	}
}

func (ds *TxDatasource) cleanupEntry(entry *TxEntry) {

	delete(ds.entries, entry.TxID)
	ds.runningTx--
	ds.semWrite.Release(1)

	go func() {
		entry.Tx.Rollback()
		entry.Conn.Close()
	}()
}

func (ds *TxDatasource) getEntry(txID string, executing bool) (*TxEntry, error) {
	ds.mu.Lock()
	entry, ok := ds.entries[txID]
	ds.mu.Unlock()

	if !ok {
		return nil, ErrTxNotFound
	}

	// Check if expired
	if time.Now().After(entry.ExpiresAt) {
		// Remove expired entry
		ds.mu.Unlock()
		ds.cleanupEntry(entry)
		ds.mu.Lock()

		return nil, ErrTxExpired
	}

	entry.executing = executing
	if entry.idleTimeout != nil {
		entry.ExpiresAt = time.Now().Add(*entry.idleTimeout)
	}

	return entry, nil
}

type ReleaseResourceFunc func()

/************************************************************
 * TxManager manages transactions for all datasources
 ************************************************************/
type DsManager struct {
	dss               []*TxDatasource
	txIDGen           *TxIDGenerator
	stopCleanupTicker chan struct{}
	wg                sync.WaitGroup
}

// NewTxManager creates a new TxManager
func NewTxManager(configs []global.DatasourceConfig) *DsManager {
	dss := make([]*TxDatasource, len(configs))

	for i, config := range configs {
		ds, err := NewDatasource(config)
		if err != nil {
			panic(fmt.Sprintf("failed to initialize datasource %s: %v", config.DatasourceID, err))
		}

		txDs := &TxDatasource{
			Datasource:       *ds,
			MaxTxIdleTimeout: time.Duration(config.MaxTxIdleTimeoutSec) * time.Second,

			semRead:  semaphore.NewWeighted(int64(config.MaxOpenConns - config.MinWriteConns)),
			semWrite: semaphore.NewWeighted(int64(config.MaxWriteConns)),
			entries:  make(map[string]*TxEntry),

			runningRead:  0,
			runningWrite: 0,
			runningTx:    0,
		}
		dss[i] = txDs
	}
	dm := &DsManager{
		dss:               dss,
		txIDGen:           NewTxIDGenerator(),
		stopCleanupTicker: make(chan struct{}),
	}

	// Start background cleanup goroutine
	dm.wg.Add(1)
	go dm.startCleanupTicker()

	return dm
}

func (dm *DsManager) allocateResource(datasourceIdx int, resourceType ResourceType) (*TxDatasource, error) {
	ds := dm.dss[datasourceIdx]
	if ds == nil {
		return nil, fmt.Errorf("datasource %d not found", datasourceIdx)
	}

	ds.mu.Lock()
	switch resourceType {
	case RESOURCE_TYPE_READ:
		ds.runningRead++
	case RESOURCE_TYPE_WRITE:
		ds.runningWrite++
	case RESOURCE_TYPE_TX:
		ds.runningTx++
	}
	ds.mu.Unlock()

	switch resourceType {
	case RESOURCE_TYPE_READ:
		err := ds.semRead.Acquire(context.Background(), 1)
		if err != nil {
			return nil, fmt.Errorf("failed to acquire read semaphore: %w", err)
		}
	case RESOURCE_TYPE_WRITE:
		err := ds.semWrite.Acquire(context.Background(), 1)
		if err != nil {
			return nil, fmt.Errorf("failed to acquire write semaphore: %w", err)
		}
	case RESOURCE_TYPE_TX:
		err := ds.semWrite.Acquire(context.Background(), 1)
		if err != nil {
			return nil, fmt.Errorf("failed to acquire transaction semaphore: %w", err)
		}
	}

	return ds, nil
}

func (dm *DsManager) StatsGet(datasourceIdx int) (runningRead int, runningWrite int, runningTx int) {
	ds := dm.dss[datasourceIdx]
	if ds == nil {
		return 99999, 99999, 99999
	}

	ds.mu.Lock()
	runningRead = ds.runningRead
	runningWrite = ds.runningWrite
	runningTx = ds.runningTx
	ds.mu.Unlock()

	return runningRead, runningWrite, runningTx
}

// Begin starts a new transaction
func (dm *DsManager) BeginTx(datasourceIdx int, isolationLevel sql.IsolationLevel, timeoutSec *int) (*TxEntry, error) {
	txID, err := dm.txIDGen.Generate(datasourceIdx)
	if err != nil {
		return nil, err
	}

	ds, err := dm.allocateResource(datasourceIdx, RESOURCE_TYPE_TX)
	if err != nil {
		return nil, err
	}

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
func (dm *DsManager) getTx(txID string, executing bool) (entry *TxEntry, srcDs *TxDatasource, err error) {
	dsIdx, err := dm.txIDGen.GetDatasourceIndex(txID)
	if err != nil {
		return nil, nil, err
	}

	ds := dm.dss[dsIdx]
	if ds == nil {
		return nil, nil, fmt.Errorf("datasource not found: %d", dsIdx)
	}

	entry, err = ds.getEntry(txID, executing)
	if err != nil {
		return nil, nil, err
	}

	return entry, ds, nil
}

// Commit commits a transaction
func (dm *DsManager) CommitTx(txID string) error {
	entry, _, err := dm.getTx(txID, false)
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
func (dm *DsManager) RollbackTx(txID string) error {
	entry, _, err := dm.getTx(txID, false)
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
func (dm *DsManager) CloseTx(txID string) error {
	entry, ds, err := dm.getTx(txID, false)
	if err != nil {
		return err
	}

	ds.mu.Lock()
	delete(ds.entries, entry.TxID)
	ds.mu.Unlock()
	go entry.Conn.Close()

	ds.releaseResource(RESOURCE_TYPE_TX)
	return nil
}

// QueryContext queries the database
func (dm *DsManager) Query(ctx context.Context, timeoutSec *int, datasourceIdx int, sql string, parameters ...any) (*sql.Rows, ReleaseResourceFunc, error) {
	ds, err := dm.allocateResource(datasourceIdx, RESOURCE_TYPE_READ)
	if err != nil {
		return nil, nil, err
	}

	timeout := ds.DefaultQueryTimeout
	if timeoutSec != nil {
		timeout = time.Duration(*timeoutSec) * time.Second
	}
	ctx, cancelCtx := context.WithTimeout(ctx, timeout)

	rows, err := ds.QueryContext(ctx, sql, parameters...)

	releaseResourceFunc := func() {
		cancelCtx()
		ds.releaseResource(RESOURCE_TYPE_READ)
	}

	return rows, releaseResourceFunc, err
}

// QueryContext queries the database
func (dm *DsManager) QueryTx(ctx context.Context, timeoutSec *int, txID string, sql string, parameters ...any) (*sql.Rows, ReleaseResourceFunc, error) {
	entry, ds, err := dm.getTx(txID, true)
	if err != nil {
		return nil, nil, err
	}

	timeout := ds.DefaultQueryTimeout
	if timeoutSec != nil {
		timeout = time.Duration(*timeoutSec) * time.Second
	}
	ctx, cancelCtx := context.WithTimeout(ctx, timeout)

	rows, err := entry.QueryContext(ctx, sql, parameters...)

	releaseResourceFunc := func() {
		cancelCtx()
		entry.executing = false
		if entry.idleTimeout != nil {
			entry.ExpiresAt = time.Now().Add(*entry.idleTimeout)
		}
	}

	return rows, releaseResourceFunc, err
}

// ExecContext executes the database
func (dm *DsManager) Execute(ctx context.Context, timeoutSec *int, datasourceIdx int, sql string, parameters ...any) (sql.Result, ReleaseResourceFunc, error) {
	ds, err := dm.allocateResource(datasourceIdx, RESOURCE_TYPE_WRITE)
	if err != nil {
		return nil, nil, err
	}

	timeout := ds.DefaultQueryTimeout
	if timeoutSec != nil {
		timeout = time.Duration(*timeoutSec) * time.Second
	}
	ctx, cancelCtx := context.WithTimeout(ctx, timeout)

	result, err := ds.ExecContext(ctx, sql, parameters...)

	releaseResourceFunc := func() {
		cancelCtx()
		ds.releaseResource(RESOURCE_TYPE_WRITE)
	}

	return result, releaseResourceFunc, err
}

// ExecContext executes the database
func (dm *DsManager) ExecuteTx(ctx context.Context, timeoutSec *int, txId string, sql string, parameters ...any) (sql.Result, ReleaseResourceFunc, error) {
	entry, ds, err := dm.getTx(txId, true)
	if err != nil {
		return nil, nil, err
	}

	timeout := ds.DefaultQueryTimeout
	if timeoutSec != nil {
		timeout = time.Duration(*timeoutSec) * time.Second
	}
	ctx, cancelCtx := context.WithTimeout(ctx, timeout)

	result, err := entry.ExecContext(ctx, sql, parameters...)

	releaseResourceFunc := func() {
		cancelCtx()
		entry.executing = false
		if entry.idleTimeout != nil {
			entry.ExpiresAt = time.Now().Add(*entry.idleTimeout)
		}
	}

	return result, releaseResourceFunc, err
}

/************************************************************
 * Periodically cleans up expired transactions
 ************************************************************/
func (dm *DsManager) startCleanupTicker() {
	defer dm.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-dm.stopCleanupTicker: // stop cleanup
			return
		case <-ticker.C: // cleanup expired transactions
			now := time.Now()

			for _, ds := range dm.dss {
				expiredEntries := make([]*TxEntry, 0)
				ds.mu.Lock()

				for _, entry := range ds.entries {
					if entry == nil {
						continue
					}
					if entry.executing {
						continue
					}
					if now.Before(entry.ExpiresAt) {
						continue
					}
					expiredEntries = append(expiredEntries, entry)
				}

				for _, entry := range expiredEntries {
					ds.cleanupEntry(entry)
				}

				ds.mu.Unlock()
			}
		}
	}
}

/************************************************************
 * Shutdown stops the background cleanup goroutine
 ************************************************************/
func (dm *DsManager) Shutdown() {
	close(dm.stopCleanupTicker)
	dm.wg.Wait()

	// Rollback all remaining transactions
	for _, ds := range dm.dss {
		ds.mu.Lock()

		for _, entry := range ds.entries {
			entry.Tx.Rollback()
			entry.Conn.Close()
		}

		ds.Close()
		ds.mu.Unlock()
	}
}
