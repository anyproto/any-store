package btree

// DB is the public API for the embedded key-value database.
// It manages namespaces, transactions, and the underlying pager/WAL.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
)

// Options configures the database.
type Options struct {
	PageSize              uint32 // Page size in bytes (default: 4096)
	CacheSize             int    // Maximum number of cached pages (default: 2000)
	AutoCheckpointAfter   uint32 // WAL frames before auto-checkpoint (0 = use default 10000)
	DisableAutoCheckpoint bool   // Disable auto-checkpoint entirely (manual Checkpoint() only)

	// InProcess uses heap-backed shared memory for the WAL index instead of
	// mmap'd files with POSIX fcntl locks. Faster, but restricts access to a
	// single OS process. Equivalent to SQLite's PRAGMA locking_mode=EXCLUSIVE
	// (WAL_HEAPMEMORY_MODE): SHM locks become no-ops, memory barriers are
	// skipped, and no .db-wal-shm file is created.
	//
	// Forced to true automatically on platforms without mmap SHM support
	// (e.g. Windows) and when InMemory is true.
	InProcess bool

	// NoCommitSync skips fdatasync on WAL commit. WAL frames are still written
	// to the WAL file on disk, but durability is deferred until checkpoint.
	// Equivalent to SQLite's PRAGMA synchronous=NORMAL in WAL mode:
	//   - false (default) = synchronous=FULL  — fsync every WAL commit
	//   - true            = synchronous=NORMAL — fsync only on checkpoint
	NoCommitSync bool

	// InMemory keeps the entire database in memory with no files on disk.
	// WAL frames are stored in a heap-backed slice (memFrames) instead of
	// being written to a WAL file. Checkpoint moves frames into the in-memory
	// page cache (which is made non-purgeable so pages are never evicted).
	// The database does not survive process crashes.
	//
	// When InMemory is true, InProcess and NoCommitSync are forced to true
	// automatically (heap SHM, no fsync — both meaningless for in-memory).
	// The path argument to Open is ignored and can be any string.
	InMemory bool
}

// DefaultOptions returns default database options.
func DefaultOptions() Options {
	return Options{
		PageSize:            DefaultPageSize,
		CacheSize:           defaultCacheSize,
		AutoCheckpointAfter: AutoCheckpointThreshold,
	}
}

// DB represents an open database.
type DB struct {
	mu      sync.RWMutex
	writeMu sync.Mutex // serializes write transactions
	pager   *pager
	path    string
	opts    Options
	closing atomic.Bool // set to reject new transactions
	closed  atomic.Bool // set when Close() is actually called

	// Namespace root pages are stored in a master table on page 1.
	// Format: each cell in the master B-tree maps namespace name -> root page number (4 bytes).
	masterBT *btree

	// Local counter cache for multi-process staleness detection.
	// Accessed atomically: written by Commit (under writeMu + mu.RLock)
	// and read by BeginRead/BeginWrite (under mu.RLock). Since both
	// paths hold mu.RLock (not exclusive), atomics are required.
	localFileChangeCounter atomic.Uint32
	localSchemaCookie      atomic.Uint32

	readTxPool  sync.Pool
	writeTxPool sync.Pool
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
	if opts.AutoCheckpointAfter == 0 && !opts.DisableAutoCheckpoint {
		opts.AutoCheckpointAfter = AutoCheckpointThreshold
	}

	if opts.InMemory {
		opts.InProcess = true
		opts.NoCommitSync = true
	}
	if !hasMmapShm {
		// Platform lacks mmap SHM (e.g. Windows); force heap SHM.
		opts.InProcess = true
	}

	p := newPager(path, opts.PageSize, opts.CacheSize, !opts.InMemory)
	p.inProcess = opts.InProcess
	p.noCommitSync = opts.NoCommitSync
	p.inMemory = opts.InMemory
	if err := p.open(); err != nil {
		return nil, err
	}

	// Reject databases created with an older schema format.
	// Schema format 5 introduced unified leaf cell overflow (key+value as a
	// single payload blob). Older formats stored key fully on-page and only
	// allowed value overflow, which panics on large keys.
	if p.header.SchemaFormat != 0 && p.header.SchemaFormat < 5 {
		_ = p.close()
		return nil, ErrOldFormat
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

	// Initialize local counters from the on-disk state (reading through WAL).
	maxFrame, slot, err := p.beginRead()
	if err != nil {
		p.close()
		return nil, err
	}
	fcc, sc, err := p.readHeaderCounters(maxFrame)
	p.endRead(slot)
	if err != nil {
		p.close()
		return nil, err
	}
	db.localFileChangeCounter.Store(fcc)
	db.localSchemaCookie.Store(sc)

	return db, nil
}

// Close closes the database.
func (db *DB) Close() error {
	if !db.closed.CompareAndSwap(false, true) {
		return ErrClosed
	}
	db.closing.Store(true)
	// Wait for write mutex to ensure no writer is active
	db.writeMu.Lock()
	db.writeMu.Unlock()
	// Wait for all active readers to finish
	db.mu.Lock()
	db.mu.Unlock()
	return db.pager.close()
}

// SetClosing marks the database as closing, causing new transactions to fail.
func (db *DB) SetClosing() {
	db.closing.Store(true)
}

// Path returns the database file path.
func (db *DB) Path() string {
	return db.path
}

// BeginRead starts a read-only transaction.
func (db *DB) BeginRead() (*ReadTx, error) {
	if db.closing.Load() {
		return nil, ErrClosed
	}
	db.mu.RLock()
	if db.closing.Load() {
		db.mu.RUnlock()
		return nil, ErrClosed
	}

	maxFrame, slot, err := db.pager.beginRead()
	if err != nil {
		db.mu.RUnlock()
		return nil, err
	}

	// Read on-disk counters for staleness detection.
	fcc, sc, err := db.pager.readHeaderCounters(maxFrame)
	if err != nil {
		db.pager.endRead(slot)
		db.mu.RUnlock()
		return nil, err
	}

	tx := db.getReadTx()
	tx.db = db
	tx.pager = db.pager
	tx.closed = false
	tx.walMaxFrame = maxFrame
	tx.walSlot = slot
	tx.diskFileChangeCounter = fcc
	tx.diskSchemaCookie = sc
	tx.localFileChangeCounter = db.localFileChangeCounter.Load()
	tx.localSchemaCookie = db.localSchemaCookie.Load()
	return tx, nil
}

// BeginWrite starts a read-write transaction. Only one write transaction
// can be active at a time (single-writer semantics). Blocks until any
// existing write transaction completes.
func (db *DB) BeginWrite() (*WriteTx, error) {
	if db.closing.Load() {
		return nil, ErrClosed
	}
	db.writeMu.Lock()
	if db.closing.Load() {
		db.writeMu.Unlock()
		return nil, ErrClosed
	}
	db.mu.RLock()
	if db.closing.Load() {
		db.mu.RUnlock()
		db.writeMu.Unlock()
		return nil, ErrClosed
	}

	maxFrame, slot, err := db.pager.beginRead()
	if err != nil {
		db.mu.RUnlock()
		db.writeMu.Unlock()
		return nil, err
	}

	// Read on-disk counters for staleness detection.
	fcc, sc, err := db.pager.readHeaderCounters(maxFrame)
	if err != nil {
		db.pager.endRead(slot)
		db.mu.RUnlock()
		db.writeMu.Unlock()
		return nil, err
	}

	if err := db.pager.beginWrite(); err != nil {
		db.pager.endRead(slot)
		db.mu.RUnlock()
		db.writeMu.Unlock()
		return nil, err
	}

	tx := db.getWriteTx()
	tx.ReadTx.db = db
	tx.ReadTx.pager = db.pager
	tx.ReadTx.closed = false
	tx.ReadTx.walMaxFrame = maxFrame
	tx.ReadTx.walSlot = slot
	tx.ReadTx.writable = true
	tx.ReadTx.diskFileChangeCounter = fcc
	tx.ReadTx.diskSchemaCookie = sc
	tx.ReadTx.localFileChangeCounter = db.localFileChangeCounter.Load()
	tx.ReadTx.localSchemaCookie = db.localSchemaCookie.Load()
	tx.dataChanged = false   // pool reuse safety
	tx.schemaChanged = false // pool reuse safety
	return tx, nil
}

func (db *DB) getReadTx() *ReadTx {
	if tx, ok := db.readTxPool.Get().(*ReadTx); ok {
		return tx
	}
	return &ReadTx{}
}

func (db *DB) putReadTx(tx *ReadTx) {
	db.readTxPool.Put(tx)
}

func (db *DB) getWriteTx() *WriteTx {
	if tx, ok := db.writeTxPool.Get().(*WriteTx); ok {
		return tx
	}
	return &WriteTx{}
}

func (db *DB) putWriteTx(tx *WriteTx) {
	db.writeTxPool.Put(tx)
}

// Checkpoint triggers a WAL checkpoint with the specified mode, writing
// committed WAL frames back to the database file.
func (db *DB) Checkpoint(mode CheckpointMode) error {
	if db.closing.Load() {
		return ErrClosed
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closing.Load() {
		return ErrClosed
	}
	return db.pager.checkpointWithMode(mode)
}

// UpdateLocalCounters manually sets the local counter cache. This is used by
// a process that has detected staleness and rebuilt its in-memory state: after
// rebuilding, call this to record the new baseline so subsequent transactions
// no longer report as stale. Uses atomic stores, safe to call concurrently.
func (db *DB) UpdateLocalCounters(fileChangeCounter, schemaCookie uint32) {
	db.localFileChangeCounter.Store(fileChangeCounter)
	db.localSchemaCookie.Store(schemaCookie)
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
	_, found, serr := searchLeafPage(pg, nameKey)
	db.pager.releasePage(pg)
	if serr != nil {
		return serr
	}
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
	rootPg.header.cellContentOff = uint16(db.pager.pageSize) - uint16(db.pager.header.ReservedSpace)
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
// All pages belonging to the namespace's B-tree are freed to the freelist.
func (db *DB) DeleteNamespace(tx *WriteTx, name string) error {
	if tx.closed {
		return ErrTxClosed
	}

	nameKey := []byte(name)
	masterPg, err := db.pager.getWritablePage(1)
	if err != nil {
		return err
	}

	idx, found, serr := searchLeafPage(masterPg, nameKey)
	if serr != nil {
		db.pager.releasePage(masterPg)
		return serr
	}
	if !found {
		db.pager.releasePage(masterPg)
		return ErrNamespaceNotFound
	}

	// Get the root page number before removing the entry
	off := masterPg.getCellOffset(idx)
	cell, _, cerr := parseLeafCell(masterPg.data, int(off))
	if cerr != nil {
		db.pager.releasePage(masterPg)
		return cerr
	}
	var rootPage uint32
	if len(cell.value) >= 4 {
		rootPage = binary.BigEndian.Uint32(cell.value)
	}

	cells := db.masterBT.collectLeafCells(masterPg)
	cells = append(cells[:idx], cells[idx+1:]...)
	if err := db.masterBT.rebuildLeafPage(masterPg, cells); err != nil {
		db.pager.releasePage(masterPg)
		return err
	}
	db.pager.releasePage(masterPg)

	// Free all pages in the namespace's B-tree
	if rootPage != 0 {
		return db.freeTreePages(rootPage)
	}
	return nil
}

// freeTreePages recursively frees all pages in a B-tree,
// including any overflow page chains attached to leaf cells.
func (db *DB) freeTreePages(pgno uint32) error {
	pg, err := db.pager.getPage(pgno)
	if err != nil {
		return err
	}

	if pg.header.isInterior() {
		// Collect child page numbers before freeing
		n := int(pg.header.cellCount)
		cpOff := pg.cellPointerOffset()
		children := make([]uint32, 0, n+1)
		for i := range n {
			off := int(binary.BigEndian.Uint16(pg.data[cpOff+i*2:]))
			childPgno := binary.BigEndian.Uint32(pg.data[off : off+4])
			children = append(children, childPgno)
		}
		children = append(children, pg.header.rightChild)
		db.pager.releasePage(pg)

		// Recurse into children first (free leaves before interior)
		for _, child := range children {
			if err := db.freeTreePages(child); err != nil {
				return err
			}
		}
	} else {
		// Leaf page: free any overflow chains
		usableSize := int(db.pager.pageSize) - int(db.pager.header.ReservedSpace)
		n := int(pg.header.cellCount)
		for i := range n {
			off := pg.getCellOffset(i)
			cell, _, cerr := parseLeafCellWithSize(pg.data, int(off), usableSize)
			if cerr != nil {
				db.pager.releasePage(pg)
				return cerr
			}
			if cell.overflowPg != 0 {
				db.pager.releasePage(pg)
				if err := db.pager.freeOverflowChain(cell.overflowPg); err != nil {
					return err
				}
				// Re-get page since we released it
				pg, err = db.pager.getPage(pgno)
				if err != nil {
					return err
				}
			}
		}
		db.pager.releasePage(pg)
	}

	// Free this page
	return db.pager.freePage(pgno)
}

// GetNamespace returns a Namespace handle for the given name.
// If a write transaction is active (pager in writer state), this uses the
// writer path to see uncommitted dirty pages. This is safe because
// GetNamespace is called from the writer goroutine when inside a write tx.
func (db *DB) GetNamespace(name string) (*Namespace, error) {
	if pagerState(db.pager.state.Load()) == pagerWriter {
		return db.getNamespaceLocked(name)
	}
	maxFrame, slot, err := db.pager.beginRead()
	if err != nil {
		return nil, err
	}
	defer db.pager.endRead(slot)

	return db.getNamespaceAt(name, maxFrame)
}

// getNamespaceLocked returns a Namespace handle (caller must hold read lock).
// Uses pager.getPage which reads from writePages — safe only when called from
// the writer goroutine (e.g. WriteTx.CreateNamespace after modifying page 1).
func (db *DB) getNamespaceLocked(name string) (*Namespace, error) {
	nameKey := []byte(name)
	pg, err := db.pager.getPage(1)
	if err != nil {
		return nil, err
	}
	defer db.pager.releasePage(pg)

	return db.resolveNamespace(name, nameKey, pg)
}

// getNamespaceAt returns a Namespace handle using readPageMVCC for snapshot
// isolation. Safe to call from any goroutine (readers or writer) because
// it does not access pager.writePages, and readPageMVCC returns an uncached
// copy when the page is dirty (avoiding races with the writer).
func (db *DB) getNamespaceAt(name string, walMaxFrame uint32) (*Namespace, error) {
	nameKey := []byte(name)
	pg, err := db.pager.readPageMVCC(1, walMaxFrame)
	if err != nil {
		return nil, err
	}
	defer db.pager.releasePage(pg)

	return db.resolveNamespace(name, nameKey, pg)
}

// resolveNamespace searches page 1 (the master table) for the given namespace
// and returns a Namespace handle.
func (db *DB) resolveNamespace(name string, nameKey []byte, pg *page) (*Namespace, error) {
	idx, found, serr := searchLeafPage(pg, nameKey)
	if serr != nil {
		return nil, serr
	}
	if !found {
		return nil, ErrNamespaceNotFound
	}

	off := pg.getCellOffset(idx)
	cell, _, cerr := parseLeafCell(pg.data, int(off))
	if cerr != nil {
		return nil, cerr
	}
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
	maxFrame, slot, err := db.pager.beginRead()
	if err != nil {
		return nil, err
	}
	defer db.pager.endRead(slot)

	pg, err := db.pager.readPageMVCC(1, maxFrame)
	if err != nil {
		return nil, err
	}
	defer db.pager.releasePage(pg)

	n := int(pg.header.cellCount)
	names := make([]string, 0, n)
	for i := range n {
		off := pg.getCellOffset(i)
		cell, _, cerr := parseLeafCell(pg.data, int(off))
		if cerr != nil {
			return nil, cerr
		}
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
	db          *DB
	pager       *pager
	closed      bool
	walMaxFrame uint32 // WAL snapshot for this transaction
	walSlot     int    // reader slot number (for endRead)
	writable    bool   // true when embedded in a WriteTx (MVCC: allows seeing dirty pages)

	// Disk counters from page 1 at transaction start (for staleness detection).
	diskFileChangeCounter  uint32
	diskSchemaCookie       uint32
	localFileChangeCounter uint32 // snapshot of DB's local value
	localSchemaCookie      uint32 // snapshot of DB's local value
}

// txGetPage fetches a page respecting MVCC snapshot isolation.
// For write transactions, dirty pages from writePages are returned directly.
// For read transactions, readPageMVCC bypasses dirty pages from uncommitted writers.
func (tx *ReadTx) txGetPage(pgno uint32) (*page, error) {
	if tx.writable {
		if pg := tx.pager.writePages[pgno]; pg != nil {
			pg.pinCount++
			return pg, nil
		}
		return tx.pager.getPageAt(pgno, tx.walMaxFrame)
	}
	return tx.pager.readPageMVCC(pgno, tx.walMaxFrame)
}

// readOverflow reads overflow chain data using the correct isolation level.
// Writers use the shared cache (to see their own dirty pages).
// Readers bypass the cache to avoid polluting it with stale snapshot data
// that the writer could later read, causing on-disk corruption.
func (tx *ReadTx) readOverflow(firstPgno uint32, buf []byte) error {
	if tx.writable {
		return tx.pager.readOverflowChainAt(firstPgno, buf, tx.walMaxFrame)
	}
	return tx.pager.readOverflowChainMVCC(firstPgno, buf, tx.walMaxFrame)
}

// AppendValue retrieves a value by key from the given namespace, appending it to buf.
// Pass nil for buf to allocate a new slice (equivalent to Get).
func (tx *ReadTx) AppendValue(ns *Namespace, key []byte, buf []byte) ([]byte, error) {
	if tx.closed {
		return buf, ErrTxClosed
	}
	pg, err := tx.txGetPage(ns.rootPage)
	if err != nil {
		return buf, err
	}

	// Search without starting a new read tx (we're already in one)
	usableSize := int(tx.pager.pageSize) - int(tx.pager.header.ReservedSpace)
	mvcc := !tx.writable
	for {
		if pg.header.isLeaf() {
			idx, found, serr := searchLeafWithOverflow(pg, key, usableSize, tx.pager, tx.walMaxFrame, mvcc)
			if serr != nil {
				tx.pager.releasePage(pg)
				return buf, serr
			}
			if !found {
				tx.pager.releasePage(pg)
				return buf, ErrKeyNotFound
			}
			off := pg.getCellOffset(idx)
			cell, _, cerr := parseLeafCellWithSize(pg.data, int(off), usableSize)
			if cerr != nil {
				tx.pager.releasePage(pg)
				return buf, cerr
			}
			if cell.overflowPg != 0 {
				// Unified payload format: read keyLen, valLen, compute nLocal
				pos := int(off)
				keyLen, kn, verr := getVarintSafe(pg.data[pos:])
				if verr != nil {
					tx.pager.releasePage(pg)
					return buf, ErrCorrupt
				}
				pos += kn
				valLen, _, verr := getVarintSafe(pg.data[pos:])
				if verr != nil {
					tx.pager.releasePage(pg)
					return buf, ErrCorrupt
				}

				totalPayload := int(keyLen) + int(valLen)
				nLocal := localPayloadSize(totalPayload, usableSize)
				localKeyBytes := min(nLocal, int(keyLen))
				localValBytes := nLocal - localKeyBytes
				keyOverflow := int(keyLen) - localKeyBytes
				overflowSize := totalPayload - nLocal

				start := len(buf)
				buf = append(buf, make([]byte, int(valLen))...)
				fullVal := buf[start:]
				if localValBytes > 0 {
					copy(fullVal, cell.value)
				}
				if overflowSize > 0 {
					overflowBuf := make([]byte, overflowSize)
					if err := tx.readOverflow(cell.overflowPg, overflowBuf); err != nil {
						tx.pager.releasePage(pg)
						return buf[:start], err
					}
					valOverflow := int(valLen) - localValBytes
					if valOverflow > 0 {
						copy(fullVal[localValBytes:], overflowBuf[keyOverflow:])
					}
				}
				tx.pager.releasePage(pg)
				return buf, nil
			}
			buf = append(buf, cell.value...)
			tx.pager.releasePage(pg)
			return buf, nil
		}
		childPgno, _, serr := searchInteriorWithOverflow(pg, key, usableSize, tx.pager, tx.walMaxFrame, mvcc)
		if serr != nil {
			tx.pager.releasePage(pg)
			return buf, serr
		}
		tx.pager.releasePage(pg)
		pg, err = tx.txGetPage(childPgno)
		if err != nil {
			return buf, err
		}
	}
}

// Get retrieves a value by key from the given namespace.
// The returned slice is a copy and is safe to retain after pages are released.
func (tx *ReadTx) Get(ns *Namespace, key []byte) ([]byte, error) {
	return tx.AppendValue(ns, key, nil)
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
	bt := &btree{pager: tx.pager, rootPage: ns.rootPage, walMaxFrame: tx.walMaxFrame, writable: tx.writable}
	return bt.NewCursor()
}

// Count returns the total number of key-value pairs in the namespace.
// This is a lightweight operation that only reads page headers without parsing cell data.
func (tx *ReadTx) Count(ns *Namespace) (int, error) {
	if tx.closed {
		return 0, ErrTxClosed
	}
	bt := &btree{pager: tx.pager, rootPage: ns.rootPage, walMaxFrame: tx.walMaxFrame, writable: tx.writable}
	return bt.Count()
}

// GetNamespace returns a Namespace handle for the given name.
// Uses the transaction's WAL snapshot via getPageAt, which is safe to call
// concurrently with writer goroutines (does not access pager.writePages).
func (tx *ReadTx) GetNamespace(name string) (*Namespace, error) {
	if tx.closed {
		return nil, ErrTxClosed
	}
	return tx.db.getNamespaceAt(name, tx.walMaxFrame)
}

// IsDataStale returns true if the on-disk FileChangeCount differs from the
// locally cached value, indicating another process has committed data changes
// since this DB last committed or synced counters.
func (tx *ReadTx) IsDataStale() bool {
	return tx.diskFileChangeCounter != tx.localFileChangeCounter
}

// IsSchemaStale returns true if the on-disk SchemaCookie differs from the
// locally cached value, indicating another process has committed schema changes.
func (tx *ReadTx) IsSchemaStale() bool {
	return tx.diskSchemaCookie != tx.localSchemaCookie
}

// DiskFileChangeCounter returns the FileChangeCount read from page 1 at
// the start of this transaction.
func (tx *ReadTx) DiskFileChangeCounter() uint32 {
	return tx.diskFileChangeCounter
}

// DiskSchemaCookie returns the SchemaCookie read from page 1 at the start
// of this transaction.
func (tx *ReadTx) DiskSchemaCookie() uint32 {
	return tx.diskSchemaCookie
}

// Rollback ends the read transaction (for ReadTx, this is the same as commit).
func (tx *ReadTx) Rollback() error {
	if tx.closed {
		return ErrTxClosed
	}
	tx.closed = true
	tx.pager.endRead(tx.walSlot)
	db := tx.db
	db.mu.RUnlock()
	db.putReadTx(tx)
	return nil
}

// WriteTx is a read-write transaction.
type WriteTx struct {
	ReadTx
	dataChanged   bool // set by MarkDataChanged; causes FileChangeCount++ on commit
	schemaChanged bool // set by MarkSchemaChanged; causes SchemaCookie++ on commit
}

// GetNamespace returns a Namespace handle for the given name.
// Uses pager.getPage which sees dirty pages from the current write transaction.
func (tx *WriteTx) GetNamespace(name string) (*Namespace, error) {
	if tx.closed {
		return nil, ErrTxClosed
	}
	return tx.db.getNamespaceLocked(name)
}

// MarkDataChanged signals that this transaction modifies data, causing
// FileChangeCount to be incremented on commit.
func (tx *WriteTx) MarkDataChanged() {
	tx.dataChanged = true
}

// MarkSchemaChanged signals that this transaction modifies schema, causing
// SchemaCookie to be incremented on commit.
func (tx *WriteTx) MarkSchemaChanged() {
	tx.schemaChanged = true
}

// Put inserts or updates a key-value pair in the given namespace.
func (tx *WriteTx) Put(ns *Namespace, key, value []byte) error {
	if tx.closed {
		return ErrTxClosed
	}
	bt := &btree{pager: tx.pager, rootPage: ns.rootPage, walMaxFrame: tx.walMaxFrame, writable: true}
	return bt.Put(key, value)
}

// Delete removes a key from the given namespace.
func (tx *WriteTx) Delete(ns *Namespace, key []byte) error {
	if tx.closed {
		return ErrTxClosed
	}
	bt := &btree{pager: tx.pager, rootPage: ns.rootPage, walMaxFrame: tx.walMaxFrame, writable: true}
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
	nFrame, newFCC, newSC, err := tx.pager.commit(tx.dataChanged, tx.schemaChanged)
	if err == nil {
		tx.db.localFileChangeCounter.Store(newFCC)
		tx.db.localSchemaCookie.Store(newSC)
	}
	threshold := tx.db.opts.AutoCheckpointAfter
	needCheckpoint := threshold > 0 && nFrame >= threshold
	tx.pager.endRead(tx.walSlot)

	// Auto-checkpoint before releasing db.mu.RLock to avoid deadlock with Close().
	// Checkpoint does NOT block readers — it only blocks new writers.
	if err == nil && needCheckpoint {
		_ = tx.pager.tryCheckpoint()
	}
	db := tx.db
	db.mu.RUnlock()
	db.writeMu.Unlock()
	db.putWriteTx(tx)
	return err
}

// Rollback discards all changes in the transaction.
func (tx *WriteTx) Rollback() error {
	if tx.closed {
		return ErrTxClosed
	}
	tx.closed = true
	err := tx.pager.rollback()
	tx.pager.endRead(tx.walSlot)
	db := tx.db
	db.mu.RUnlock()
	db.writeMu.Unlock()
	db.putWriteTx(tx)
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
