package btree

// DB is the public API for the embedded key-value database.
// It manages namespaces, transactions, and the underlying pager/WAL.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
)

// Options configures the database.
type Options struct {
	PageSize  uint32 // Page size in bytes (default: 4096)
	CacheSize int    // Maximum number of cached pages (default: 2000)
	InProcess bool   // Use in-process locking only (faster, but single-process access only)
	NoSync    bool   // Skip fsync on WAL commit (like SQLite synchronous=normal in WAL mode)
}

// DefaultOptions returns default database options.
func DefaultOptions() Options {
	return Options{
		PageSize:  DefaultPageSize,
		CacheSize: defaultCacheSize,
	}
}

// DB represents an open database.
type DB struct {
	mu     sync.RWMutex
	pager  *pager
	path   string
	opts   Options
	closed bool

	// Namespace root pages are stored in a master table on page 1.
	// Format: each cell in the master B-tree maps namespace name -> root page number (4 bytes).
	masterBT *btree
}

// Open opens or creates a database at the given path.
func Open(path string, opts Options) (*DB, error) {
	if opts.PageSize == 0 {
		opts.PageSize = DefaultPageSize
	}
	if opts.PageSize < MinPageSize || opts.PageSize > MaxPageSize {
		return nil, errors.New("btree: invalid page size")
	}
	// Page size must be a power of 2
	if opts.PageSize&(opts.PageSize-1) != 0 {
		return nil, errors.New("btree: page size must be a power of 2")
	}
	if opts.CacheSize <= 0 {
		opts.CacheSize = defaultCacheSize
	}

	p := newPager(path, opts.PageSize, opts.CacheSize)
	p.inProcess = opts.InProcess
	p.noSync = opts.NoSync
	if err := p.open(); err != nil {
		return nil, err
	}

	db := &DB{
		pager: p,
		path:  path,
		opts:  opts,
		masterBT: &btree{
			pager:    p,
			rootPage: 1,
		},
	}

	return db, nil
}

// Close closes the database.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	db.closed = true
	return db.pager.close()
}

// Path returns the database file path.
func (db *DB) Path() string {
	return db.path
}

// BeginRead starts a read-only transaction.
func (db *DB) BeginRead() (*ReadTx, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrClosed
	}

	if err := db.pager.beginRead(); err != nil {
		db.mu.RUnlock()
		return nil, err
	}

	return &ReadTx{
		db:    db,
		pager: db.pager,
	}, nil
}

// BeginWrite starts a read-write transaction. Only one write transaction
// can be active at a time (single-writer semantics).
func (db *DB) BeginWrite() (*WriteTx, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrClosed
	}

	if err := db.pager.beginRead(); err != nil {
		db.mu.RUnlock()
		return nil, err
	}

	if err := db.pager.beginWrite(); err != nil {
		db.pager.endRead()
		db.mu.RUnlock()
		return nil, err
	}

	return &WriteTx{
		ReadTx: ReadTx{
			db:    db,
			pager: db.pager,
		},
	}, nil
}

// Checkpoint triggers a WAL checkpoint, writing committed WAL frames
// back to the database file.
func (db *DB) Checkpoint() error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}
	return db.pager.checkpoint()
}

// CreateNamespace creates a new namespace. Must be called within a write transaction.
func (db *DB) CreateNamespace(tx *WriteTx, name string) error {
	if tx.closed {
		return ErrTxClosed
	}

	// Check if namespace already exists
	nameKey := []byte(name)
	pg, err := db.pager.getPage(1)
	if err != nil {
		return err
	}
	_, found := searchLeafPage(pg, nameKey)
	db.pager.releasePage(pg)
	if found {
		return ErrNamespaceExists
	}

	// Allocate a new page for the namespace's B-tree root
	rootPg, err := db.pager.allocatePage()
	if err != nil {
		return err
	}

	// Initialize as empty leaf page
	hdrOff := 0
	rootPg.header.pageType = pageTypeLeafIdx
	rootPg.header.cellCount = 0
	rootPg.header.cellContentOff = uint16(db.pager.pageSize)
	rootPg.header.serialize(rootPg.data[hdrOff:])
	db.pager.releasePage(rootPg)

	// Store namespace -> root page mapping in master table
	var rootPgBuf [4]byte
	binary.BigEndian.PutUint32(rootPgBuf[:], rootPg.pgno)

	masterPg, err := db.pager.getWritablePage(1)
	if err != nil {
		return err
	}

	bt := &btree{pager: db.pager, rootPage: 1}
	err = bt.insertIntoLeaf(masterPg, nameKey, rootPgBuf[:])
	db.pager.releasePage(masterPg)
	return err
}

// DeleteNamespace deletes a namespace. Must be called within a write transaction.
func (db *DB) DeleteNamespace(tx *WriteTx, name string) error {
	if tx.closed {
		return ErrTxClosed
	}

	nameKey := []byte(name)
	masterPg, err := db.pager.getWritablePage(1)
	if err != nil {
		return err
	}

	idx, found := searchLeafPage(masterPg, nameKey)
	if !found {
		db.pager.releasePage(masterPg)
		return ErrNamespaceNotFound
	}

	cells := db.masterBT.collectLeafCells(masterPg)
	cells = append(cells[:idx], cells[idx+1:]...)
	db.masterBT.rebuildLeafPage(masterPg, cells)
	db.pager.releasePage(masterPg)
	return nil
}

// GetNamespace returns a Namespace handle for the given name.
func (db *DB) GetNamespace(name string) (*Namespace, error) {
	if err := db.pager.beginRead(); err != nil {
		return nil, err
	}
	defer db.pager.endRead()

	return db.getNamespaceLocked(name)
}

// getNamespaceLocked returns a Namespace handle (caller must hold read lock).
func (db *DB) getNamespaceLocked(name string) (*Namespace, error) {
	nameKey := []byte(name)
	pg, err := db.pager.getPage(1)
	if err != nil {
		return nil, err
	}
	defer db.pager.releasePage(pg)

	idx, found := searchLeafPage(pg, nameKey)
	if !found {
		return nil, ErrNamespaceNotFound
	}

	off := pg.getCellOffset(idx)
	cell, _ := parseLeafCell(pg.data, int(off))
	if len(cell.value) < 4 {
		return nil, ErrCorrupt
	}
	rootPage := binary.BigEndian.Uint32(cell.value)

	return &Namespace{
		name:     name,
		rootPage: rootPage,
		db:       db,
	}, nil
}

// ListNamespaces returns the names of all namespaces.
func (db *DB) ListNamespaces() ([]string, error) {
	if err := db.pager.beginRead(); err != nil {
		return nil, err
	}
	defer db.pager.endRead()

	pg, err := db.pager.getPage(1)
	if err != nil {
		return nil, err
	}
	defer db.pager.releasePage(pg)

	n := int(pg.header.cellCount)
	names := make([]string, 0, n)
	for i := range n {
		off := pg.getCellOffset(i)
		cell, _ := parseLeafCell(pg.data, int(off))
		names = append(names, string(bytes.Clone(cell.key)))
	}
	return names, nil
}

// Namespace represents a named key-value namespace (separate B-tree).
type Namespace struct {
	name     string
	rootPage uint32
	db       *DB
}

// Name returns the namespace name.
func (ns *Namespace) Name() string {
	return ns.name
}

// RootPage returns the root page number (for internal use).
func (ns *Namespace) RootPage() uint32 {
	return ns.rootPage
}

// ReadTx is a read-only transaction.
type ReadTx struct {
	db     *DB
	pager  *pager
	closed bool
}

// Get retrieves a value by key from the given namespace.
// The returned slice points directly into the page buffer and is only valid
// until the transaction ends or any write operation occurs.
func (tx *ReadTx) Get(ns *Namespace, key []byte) ([]byte, error) {
	if tx.closed {
		return nil, ErrTxClosed
	}
	bt := &btree{pager: tx.pager, rootPage: ns.rootPage}
	pg, err := tx.pager.getPage(bt.rootPage)
	if err != nil {
		return nil, err
	}

	// Search without starting a new read tx (we're already in one)
	for {
		if pg.header.isLeaf() {
			idx, found := searchLeafPage(pg, key)
			if !found {
				tx.pager.releasePage(pg)
				return nil, ErrKeyNotFound
			}
			off := pg.getCellOffset(idx)
			cell, _ := parseLeafCell(pg.data, int(off))
			tx.pager.releasePage(pg)
			return cell.value, nil
		}
		childPgno, _ := searchInteriorPage(pg, key)
		tx.pager.releasePage(pg)
		pg, err = tx.pager.getPage(childPgno)
		if err != nil {
			return nil, err
		}
	}
}

// Has checks if a key exists in the given namespace.
func (tx *ReadTx) Has(ns *Namespace, key []byte) (bool, error) {
	_, err := tx.Get(ns, key)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// NewCursor creates a cursor for iterating over the namespace.
func (tx *ReadTx) NewCursor(ns *Namespace) *Cursor {
	bt := &btree{pager: tx.pager, rootPage: ns.rootPage}
	return bt.NewCursor()
}

// Rollback ends the read transaction (for ReadTx, this is the same as commit).
func (tx *ReadTx) Rollback() error {
	if tx.closed {
		return ErrTxClosed
	}
	tx.closed = true
	tx.pager.endRead()
	tx.db.mu.RUnlock()
	return nil
}

// WriteTx is a read-write transaction.
type WriteTx struct {
	ReadTx
}

// Put inserts or updates a key-value pair in the given namespace.
func (tx *WriteTx) Put(ns *Namespace, key, value []byte) error {
	if tx.closed {
		return ErrTxClosed
	}
	bt := &btree{pager: tx.pager, rootPage: ns.rootPage}
	return bt.Put(key, value)
}

// Delete removes a key from the given namespace.
func (tx *WriteTx) Delete(ns *Namespace, key []byte) error {
	if tx.closed {
		return ErrTxClosed
	}
	bt := &btree{pager: tx.pager, rootPage: ns.rootPage}
	return bt.Delete(key)
}

// AutoCheckpointThreshold is the number of WAL frames after which an
// automatic passive checkpoint is triggered. Set to 0 to disable.
// Default: 10000 frames.
var AutoCheckpointThreshold uint32 = 10000

// Commit commits the transaction, writing all changes to the WAL.
func (tx *WriteTx) Commit() error {
	if tx.closed {
		return ErrTxClosed
	}
	tx.closed = true
	err := tx.pager.commit()
	threshold := AutoCheckpointThreshold
	needCheckpoint := threshold > 0 && tx.pager.wal.nFrame >= threshold
	tx.pager.endRead()
	tx.db.mu.RUnlock()

	// Auto-checkpoint after all locks are released to prevent WAL bloat.
	if err == nil && needCheckpoint {
		_ = tx.db.Checkpoint()
	}
	return err
}

// Rollback discards all changes in the transaction.
func (tx *WriteTx) Rollback() error {
	if tx.closed {
		return ErrTxClosed
	}
	tx.closed = true
	err := tx.pager.rollback()
	tx.pager.endRead()
	tx.db.mu.RUnlock()
	return err
}

// Savepoint creates a savepoint within the transaction.
// Returns a savepoint ID that can be used with RollbackToSavepoint or ReleaseSavepoint.
func (tx *WriteTx) Savepoint() (int, error) {
	if tx.closed {
		return 0, ErrTxClosed
	}
	return tx.pager.savepoint()
}

// RollbackToSavepoint rolls back all changes made since the given savepoint.
func (tx *WriteTx) RollbackToSavepoint(id int) error {
	if tx.closed {
		return ErrTxClosed
	}
	return tx.pager.rollbackToSavepoint(id)
}

// ReleaseSavepoint releases a savepoint, merging its changes into the parent.
func (tx *WriteTx) ReleaseSavepoint(id int) error {
	if tx.closed {
		return ErrTxClosed
	}
	return tx.pager.releaseSavepoint(id)
}

// CreateNamespace creates a new namespace within this transaction.
func (tx *WriteTx) CreateNamespace(name string) (*Namespace, error) {
	if tx.closed {
		return nil, ErrTxClosed
	}
	if err := tx.db.CreateNamespace(tx, name); err != nil {
		return nil, err
	}
	return tx.db.getNamespaceLocked(name)
}

// DeleteNamespace deletes a namespace within this transaction.
func (tx *WriteTx) DeleteNamespace(name string) error {
	if tx.closed {
		return ErrTxClosed
	}
	return tx.db.DeleteNamespace(tx, name)
}
