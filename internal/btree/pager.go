package btree

// Pager manages page I/O, the page cache, and coordinates with the WAL.
// Modeled after SQLite's pager.c.
//
// The pager is the layer between the B-tree and the file system. It provides:
//   - Reading pages from the database file or WAL
//   - Writing dirty pages through the WAL
//   - Page cache management
//   - Transaction commit/rollback coordination

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// pagerState represents the pager's state machine.
type pagerState int32

const (
	pagerOpen   pagerState = iota // File opened, no transaction
	pagerReader                   // Read transaction active
	pagerWriter                   // Write transaction active
	pagerError                    // Error state, requires rollback
)

// spillFlag constants control when the pagerStress callback is inhibited.
// Modeled after SQLite's SPILLFLAG_* constants (pager.c:447-449).
const (
	spillFlagOff      uint8 = 0x01 // Never spill cache (user request)
	spillFlagRollback uint8 = 0x02 // Currently rolling back, suppress spill
)

// pager manages database pages, cache, and WAL interaction.
type pager struct {
	mu       sync.RWMutex
	file     fileHandle
	wal      *wal
	cache    *pcache
	header       dbHeader
	path         string
	pageSize     uint32
	usableSize_  int // pageSize - ReservedSpace; immutable after open, safe for concurrent reads
	dbSize   atomic.Uint32 // database size in pages (atomic: writer increments, readers bounds-check)
	state    atomic.Int32 // pagerState

	// Savepoint support: snapshots of dirty pages at savepoint boundaries
	savepoints []savepointState

	// savedHeader is a snapshot of the database header at the start of the
	// write transaction, used to restore p.header on rollback (fix 5.2).
	savedHeader dbHeader

	// WAL snapshot for the current write transaction (set atomically).
	// Readers use per-tx walMaxFrame instead.
	walMaxFrame atomic.Uint32

	// Write-transaction page map: bypasses pcache lock for hot pages during writes.
	// Only accessed by the single writer goroutine — readers must NOT read this
	// map (use getPageAt or readPageMVCC instead to avoid data races).
	writePages map[uint32]*page

	// savedWalFrame is the WAL frame count at beginWrite() time.
	// Used to roll back spilled frames on transaction rollback.
	savedWalFrame uint32

	// savedWalCksum1/2 are the WAL cumulative checksums at beginWrite() time.
	// Must be restored on rollback so the next transaction's frames have
	// correct checksums when overwriting spill positions.
	savedWalCksum1 uint32
	savedWalCksum2 uint32

	// doNotSpill is a bitmask of spillFlag* constants that inhibit the
	// pagerStress callback. Modeled after SQLite's Pager.doNotSpill (pager.c:648).
	doNotSpill uint8

	// Reusable slice for collecting dirty pages during commit
	dirtyBuf []*page

	// dontWritePages tracks pages that were dirtied but whose content doesn't
	// need to be persisted (e.g., freed leaf pages added to a freelist trunk).
	// Matches SQLite's PGHDR_DONT_WRITE flag (pager.c:6283). We use a map
	// because we cannot modify the page struct in page.go.
	dontWritePages map[uint32]bool

	// hasContent tracks pages that were freed as freelist leaf pages during
	// the current write transaction. When such a page is re-allocated from
	// the freelist, it must NOT use the NOCONTENT optimization because the
	// page's prior content may need to be preserved for savepoint rollback.
	// Matches SQLite's BtShared.pHasContent bitvec (btree.c:617-685).
	//
	// Without this, a page freed and re-allocated within the same transaction
	// after a savepoint would lose its original content: freePage marks it as
	// dontWrite (skipping the WAL write), and allocateFromFreelist would use
	// getPageNoContent (skipping savepoint journaling). On savepoint rollback
	// the page content is not restored, leading to corrupt overflow chains.
	// This is the exact bug from SQLite ticket 7f7f8026eda387d544b.
	hasContent map[uint32]bool

	// pagePool recycles page objects with their data buffers for uncached
	// (MVCC snapshot) pages, avoiding per-read-transaction heap allocations.
	// Inspired by SQLite's pcache1 free-list recycling (pcache1.c:429-465).
	pagePool sync.Pool

	// inProcess uses heap-backed shm (faster, single-process only)
	inProcess bool

	// noCommitSync skips fdatasync on WAL commit (deferred durability)
	noCommitSync bool

	// inMemory keeps the entire database in memory with no files on disk
	inMemory bool

	// writerWalSlot is the writer's WAL reader slot number, stored here
	// so Close can release it when force-rolling back an abandoned WriteTx.
	// Written by BeginWrite before pager.beginWrite() (which stores
	// pagerWriter atomically), so it is visible to Close after observing
	// pagerWriter via state.Load() (Go memory model: sequenced-before →
	// happens-before the atomic store, synchronized-with the atomic load).
	writerWalSlot int
}

// savepointState captures the state needed to rollback to a savepoint.
type savepointState struct {
	id       int
	dbSize   uint32
	pages    map[uint32][]byte // pgno -> copy of page data before modification
	walFrame   uint32            // WAL frame count at savepoint time
	walCksum1  uint32            // WAL cumulative checksum-1 at savepoint time
	walCksum2  uint32            // WAL cumulative checksum-2 at savepoint time
	header     dbHeader          // snapshot of database header at savepoint time (fix 9.3)
}

// newPager creates a new pager for the given database path.
// purgeable controls whether the page cache can evict pages (false for InMemory databases).
func newPager(path string, pageSize uint32, cacheSize int, purgeable bool) *pager {
	p := &pager{
		path:     path,
		pageSize: pageSize,
		cache:    newPcache(int(pageSize), cacheSize, purgeable),
	}
	p.cache.xStress = p.pagerStress
	return p
}

// open opens the database file, initializes the WAL, and recovers if needed.
func (p *pager) open() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.inMemory {
		// In-memory database: no file on disk
		return p.initNewDB()
	}

	f, err := osOpenFile(p.path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	p.file = f

	info, err := f.Stat()
	if err != nil {
		return err
	}

	if info.Size() == 0 {
		// New database - initialize
		return p.initNewDB()
	}

	// Existing database - read header
	readSize := p.pageSize
	if readSize == 0 {
		readSize = DefaultPageSize
	}
	buf := make([]byte, readSize)
	n, err := f.ReadAt(buf, 0)
	if err != nil && n < dbHeaderSize {
		return fmt.Errorf("btree: failed to read database header: %w", err)
	}

	if err := p.header.deserialize(buf[:dbHeaderSize]); err != nil {
		return err
	}

	p.pageSize = p.header.PageSize
	p.usableSize_ = int(p.pageSize) - int(p.header.ReservedSpace)
	p.dbSize.Store(p.header.DatabaseSize)
	p.cache = newPcache(int(p.pageSize), p.cache.maxPages, p.cache.purgeable)
	p.cache.xStress = p.pagerStress

	// Open WAL
	p.wal = newWal(p.path+"-wal", p.pageSize)
	p.wal.inProcess = p.inProcess
	p.wal.noCommitSync = p.noCommitSync
	p.wal.inMemory = p.inMemory
	p.wal.busyHandler = DefaultBusyTimeout(5 * time.Second)
	if err := p.wal.open(); err != nil {
		return err
	}

	// After WAL recovery, refresh the full header from WAL's page 1.
	// The DB file header may be stale (e.g. freelist pointers) if a crash
	// occurred before checkpoint. Without this, commit() would serialize
	// stale p.header fields back into page 1, corrupting the freelist.
	if p.wal.index.maxFrame.Load() > 0 {
		frame := p.wal.index.get(1, p.wal.index.maxFrame.Load())
		if frame > 0 {
			walBuf := make([]byte, p.pageSize)
			if err := p.wal.readFrame(frame, walBuf); err == nil {
				p.header.deserialize(walBuf[:dbHeaderSize])
			}
		}
		// Use max of DB file's DatabaseSize and WAL's maxPage.
		// After checkpoint + WAL reset, the DB file may have a larger
		// DatabaseSize than what survives in the WAL after recovery.
		if p.wal.index.maxPage.Load() > p.dbSize.Load() {
			p.dbSize.Store(p.wal.index.maxPage.Load())
		}
	}

	p.state.Store(int32(pagerOpen))
	return nil
}

// initNewDB initializes a brand new database file.
func (p *pager) initNewDB() error {
	if p.pageSize == 0 {
		p.pageSize = DefaultPageSize
	}

	p.header = dbHeader{
		PageSize:         p.pageSize,
		WriteVersion:     2, // WAL mode
		ReadVersion:      2,
		ReservedSpace:    0,
		FileChangeCount:  1,
		DatabaseSize:     1, // Just page 1
		SchemaFormat:     5,
		DefaultCacheSize: defaultCacheSize,
		TextEncoding:     1, // UTF-8
	}
	p.usableSize_ = int(p.pageSize) - int(p.header.ReservedSpace)

	// Create page 1 with the database header and an empty leaf table page
	buf := make([]byte, p.pageSize)
	p.header.serialize(buf[:dbHeaderSize])

	// Initialize page 1 as a leaf index b-tree page (fix 3.5/4.6).
	// Our B-tree is an index B-tree (key-value pairs), not a table B-tree
	// (integer-keyed rows). The correct type is pageTypeLeafIdx (10) =
	// PTF_ZERODATA | PTF_LEAF, not pageTypeLeafTbl (13).
	hdrOff := dbHeaderSize
	buf[hdrOff] = pageTypeLeafIdx // page type
	buf[hdrOff+1] = 0             // first free block (high byte)
	buf[hdrOff+2] = 0             // first free block (low byte)
	buf[hdrOff+3] = 0             // cell count (high byte)
	buf[hdrOff+4] = 0             // cell count (low byte)
	usable := uint16(p.usableSize())
	buf[hdrOff+5] = byte(usable >> 8) // cell content offset (high byte)
	buf[hdrOff+6] = byte(usable)      // cell content offset (low byte)
	buf[hdrOff+7] = 0                 // fragmented free bytes

	if p.file != nil {
		if _, err := p.file.WriteAt(buf, 0); err != nil {
			return err
		}
		if err := fdatasync(p.file); err != nil {
			return err
		}
	}

	p.dbSize.Store(1)
	p.cache = newPcache(int(p.pageSize), p.cache.maxPages, p.cache.purgeable)
	p.cache.xStress = p.pagerStress

	// For inMemory mode, pre-populate page 1 in pcache so reads find it
	if p.inMemory {
		pg := p.cache.create(1)
		copy(pg.data, buf)
		off := dbHeaderSize
		pg.header.deserialize(pg.data[off:])
		p.cache.release(pg)
	}

	// Open WAL
	p.wal = newWal(p.path+"-wal", p.pageSize)
	p.wal.inProcess = p.inProcess
	p.wal.noCommitSync = p.noCommitSync
	p.wal.inMemory = p.inMemory
	p.wal.busyHandler = DefaultBusyTimeout(5 * time.Second)
	if err := p.wal.open(); err != nil {
		return err
	}

	p.state.Store(int32(pagerOpen))
	return nil
}

// beginRead starts a read transaction, taking a WAL snapshot.
// Returns the WAL max frame for snapshot isolation and the reader slot number.
func (p *pager) beginRead() (maxFrame uint32, slot int, err error) {
	p.mu.RLock()
	if pagerState(p.state.Load()) == pagerError {
		p.mu.RUnlock()
		return 0, 0, ErrCorrupt
	}
	maxFrame, slot, err = p.wal.beginRead()
	if err != nil {
		p.mu.RUnlock()
		return 0, 0, err
	}
	// Update pager's walMaxFrame monotonically: never decrease, since a reader
	// with an older snapshot must not overwrite a newer value set by the writer.
	// The writer's getWritablePage → getPage uses p.walMaxFrame.Load() for pages
	// not yet in writePages; a stale value would read an old page version.
	for {
		old := p.walMaxFrame.Load()
		if maxFrame <= old {
			break
		}
		if p.walMaxFrame.CompareAndSwap(old, maxFrame) {
			break
		}
	}
	return maxFrame, slot, nil
}

// endRead ends a read transaction for the given reader slot.
func (p *pager) endRead(slot int) {
	p.wal.endRead(slot)
	p.mu.RUnlock()
}

// beginWrite starts a write transaction (must hold a read transaction first).
func (p *pager) beginWrite() error {
	if err := p.wal.beginWrite(); err != nil {
		return err
	}
	p.state.Store(int32(pagerWriter))
	// Save a snapshot of the database header so rollback can restore it (fix 5.2).
	p.savedHeader = p.header
	// Save WAL frame count and checksums so rollback can clean up spilled frames
	// and restore the checksum chain for the next transaction.
	p.savedWalFrame = p.wal.nFrame.Load()
	p.savedWalCksum1 = p.wal.cksum1
	p.savedWalCksum2 = p.wal.cksum2
	if p.writePages == nil {
		p.writePages = make(map[uint32]*page, 64)
	}
	return nil
}

// getPage returns the page with the given page number, reading from WAL or disk as needed.
// Uses the pager's walMaxFrame (set during beginRead for the current writer).
// If this is a write transaction, dirty pages from writePages are returned
// directly, bypassing the MVCC snapshot check in getPageAt. This allows the
// writer to see its own uncommitted changes while readers bypass dirty pages.
func (p *pager) getPage(pgno uint32) (*page, error) {
	// Fast path for writer: return its own dirty pages directly.
	// writePages is only populated during a write transaction and is
	// only accessed by the single writer goroutine, so no lock is needed.
	if pg := p.writePages[pgno]; pg != nil {
		pg.pinCount++
		return pg, nil
	}
	return p.getPageWriter(pgno, p.walMaxFrame.Load())
}

// getPageAt returns the page with the given page number, using the specified
// walMaxFrame for snapshot isolation. This allows different readers to have
// different WAL snapshots. Uses createNoStress: reader goroutines must not
// trigger pagerStress which accesses writer-only unsynchronized fields.
func (p *pager) getPageAt(pgno, walMaxFrame uint32) (*page, error) {
	return p.getPageAtImpl(pgno, walMaxFrame, true)
}

// getPageWriter returns the page for the writer context. Like getPageAt but
// uses create() which allows the stress callback (pagerStress) to spill dirty
// pages when the cache is full. This ensures update-heavy transactions on
// existing pages also trigger cache spill, not just new page allocations.
func (p *pager) getPageWriter(pgno, walMaxFrame uint32) (*page, error) {
	return p.getPageAtImpl(pgno, walMaxFrame, false)
}

func (p *pager) getPageAtImpl(pgno, walMaxFrame uint32, noStress bool) (*page, error) {
	if pgno == 0 {
		return nil, ErrInvalidPage
	}

	// Check cache first. SQLite returns cached pages directly without any
	// WAL lookup (pager.c:5568-5573 getPageNormal). The shared cache is only
	// accessed by the writer goroutine (readers use readPageMVCC which
	// bypasses the cache), so cached pages are always valid for the writer's
	// snapshot. Cache is cleared on checkpoint/WAL restart.
	if pg := p.cache.fetch(pgno); pg != nil {
		return pg, nil
	}

	// Cache miss: create a new cached page.
	// noStress=true (reader path): uses createNoStress to avoid invoking
	// pagerStress which accesses writer-only unsynchronized fields.
	// noStress=false (writer path): uses create() which allows the stress
	// callback to spill dirty pages, keeping cache size bounded during
	// update-heavy transactions on existing pages.
	var pg *page
	if noStress {
		pg = p.cache.createNoStress(pgno)
	} else {
		pg = p.cache.create(pgno)
	}

	// Try to read from WAL first
	if walMaxFrame > 0 {
		frame := p.wal.index.get(pgno, walMaxFrame)
		if frame > 0 {
			if err := p.wal.readFrame(frame, pg.data); err == nil {
				// Parse page header
				off := 0
				if pgno == 1 {
					off = dbHeaderSize
				}
				pg.header.deserialize(pg.data[off:])
				return pg, nil
			}
			// readFrame can fail if the WAL was reset (checkpointed and
			// truncated) between the index.get lookup and now. In this case,
			// the page data has been written to the database file by the
			// checkpoint, so we fall through to reading from disk.
		}
	}

	// Read from database file
	if p.file != nil {
		offset := int64(pgno-1) * int64(p.pageSize)
		_, err := p.file.ReadAt(pg.data, offset)
		if err != nil {
			// If page is beyond current file but within dbSize, it's a new page
			if pgno <= p.dbSize.Load() {
				p.cache.release(pg)
				return nil, fmt.Errorf("btree: failed to read page %d: %w", pgno, err)
			}
			// Zero-fill new pages
			clear(pg.data)
		}
	} else {
		// InMemory: no file; zero-fill new pages (existing pages should
		// have been found in cache above or in the WAL)
		clear(pg.data)
	}

	// Parse page header
	off := 0
	if pgno == 1 {
		off = dbHeaderSize
	}
	if pg.data[off] != 0 { // only parse if page type is set
		pg.header.deserialize(pg.data[off:])
	}

	return pg, nil
}

// readPageUncached reads a page directly from WAL or disk into a standalone
// page object that is NOT stored in the shared cache. This is used for MVCC
// snapshot isolation when the cache holds a newer version of the page than
// what the reader's snapshot should see.
func (p *pager) readPageUncached(pgno, walMaxFrame uint32) (*page, error) {
	pg := p.acquireTempPage()
	pg.pgno = pgno
	pg.pinCount = 1
	pg.uncached = true

	// Try to read from WAL first
	if walMaxFrame > 0 {
		frame := p.wal.index.get(pgno, walMaxFrame)
		if frame > 0 {
			if err := p.wal.readFrame(frame, pg.data); err == nil {
				off := 0
				if pgno == 1 {
					off = dbHeaderSize
				}
				pg.header.deserialize(pg.data[off:])
				return pg, nil
			}
			// readFrame can fail if the WAL was reset (checkpointed and
			// truncated) between the index.get lookup and now. In this case,
			// the page data has been written to the database file by the
			// checkpoint, so we fall through to reading from disk.
		}
	}

	// Read from database file
	if p.file != nil {
		offset := int64(pgno-1) * int64(p.pageSize)
		_, err := p.file.ReadAt(pg.data, offset)
		if err != nil {
			if pgno <= p.dbSize.Load() {
				p.recycleTempPage(pg)
				return nil, fmt.Errorf("btree: failed to read page %d: %w", pgno, err)
			}
			clear(pg.data)
		}
	} else {
		// InMemory: no file; try pcache for checkpointed data (copy to preserve MVCC isolation)
		if cached := p.cache.fetch(pgno); cached != nil {
			copy(pg.data, cached.data)
			p.cache.release(cached)
		} else {
			clear(pg.data)
		}
	}

	off := 0
	if pgno == 1 {
		off = dbHeaderSize
	}
	if pg.data[off] != 0 {
		pg.header.deserialize(pg.data[off:])
	}

	return pg, nil
}

// readPageMVCC returns a page with snapshot isolation for committed data.
// Always returns an uncached copy to avoid data races with the writer goroutine,
// which may dirty and modify cached pages at any time. The uncached page reads
// from the WAL (at the reader's snapshot point) or from disk.
func (p *pager) readPageMVCC(pgno, walMaxFrame uint32) (*page, error) {
	if pgno == 0 {
		return nil, ErrInvalidPage
	}
	return p.readPageUncached(pgno, walMaxFrame)
}

// getPageNoContent returns a page without reading from disk/WAL (fix 5.4).
// If the page is already in cache, it's returned as-is (matching SQLite's
// behavior where PAGER_GET_NOCONTENT still returns cached pages). If not in
// cache, a new blank page is created. This is used when allocating pages from
// the freelist or growing the database, where the old content is irrelevant.
// Modeled after SQLite's PAGER_GET_NOCONTENT flag in pager.c:5507.
func (p *pager) getPageNoContent(pgno uint32) (*page, error) {
	if pgno == 0 {
		return nil, ErrInvalidPage
	}
	// Cache hit: return as-is (the content may be stale but the caller will overwrite it)
	if pg := p.cache.fetch(pgno); pg != nil {
		return pg, nil
	}
	// Cache miss: create a blank page without any disk/WAL read
	pg := p.cache.create(pgno)
	clear(pg.data)
	pg.header = pageHeader{}
	return pg, nil
}

// getWritablePage returns a page ready for writing. It marks the page as dirty
// and saves a copy for savepoint rollback if needed.
func (p *pager) getWritablePage(pgno uint32) (*page, error) {
	if pagerState(p.state.Load()) != pagerWriter {
		return nil, ErrReadOnly
	}

	// Fast path: check write-transaction page map (no lock needed)
	if pg := p.writePages[pgno]; pg != nil {
		// Clear dontWrite flag: the page is being re-acquired for writing,
		// so its content is meaningful again (fix 5.4). Matches SQLite's
		// pcache.c:596-597 where PGHDR_DONT_WRITE is cleared by makeDirty.
		delete(p.dontWritePages, pgno)
		// Re-dirty pages that were made clean by pagerStress (spill).
		// Without this, post-spill modifications would be lost at commit
		// because appendDirtyPages only collects dirty pages. Also re-registers
		// the page in pcache if it was evicted after the spill.
		if !pg.dirty {
			p.cache.reinsertDirty(pg)
		}
		// Save copy for savepoint rollback if needed (lazy copy-on-write)
		if len(p.savepoints) > 0 {
			sp := &p.savepoints[len(p.savepoints)-1]
			if _, exists := sp.pages[pgno]; !exists {
				dataCopy := make([]byte, len(pg.data))
				copy(dataCopy, pg.data)
				sp.pages[pgno] = dataCopy
			}
		}
		pg.pinCount++
		return pg, nil
	}

	pg, err := p.getPage(pgno)
	if err != nil {
		return nil, err
	}

	// Save copy for savepoint rollback if we have active savepoints
	if len(p.savepoints) > 0 && !pg.dirty {
		sp := &p.savepoints[len(p.savepoints)-1]
		if _, exists := sp.pages[pgno]; !exists {
			dataCopy := make([]byte, len(pg.data))
			copy(dataCopy, pg.data)
			sp.pages[pgno] = dataCopy
		}
	}

	p.cache.makeDirty(pg)
	p.writePages[pgno] = pg
	return pg, nil
}

// allocatePage allocates a new page and returns it.
// It first checks the freelist for reusable pages before growing the database.
func (p *pager) allocatePage() (*page, error) {
	if pagerState(p.state.Load()) != pagerWriter {
		return nil, ErrReadOnly
	}

	// Check freelist first
	if p.header.FirstFreelistPg != 0 {
		pg, err := p.allocateFromFreelist()
		if err == nil {
			return pg, nil
		}
		// Fall through to grow database if freelist read fails
	}

	pgno := p.dbSize.Add(1)

	// Use getPageNoContent: new pages have no existing content to read (fix 5.4).
	pg, err := p.getPageNoContent(pgno)
	if err != nil {
		p.dbSize.Add(^uint32(0)) // decrement
		return nil, err
	}
	clear(pg.data)
	p.cache.makeDirty(pg)
	p.writePages[pgno] = pg
	return pg, nil
}

// Freelist format (SQLite-compatible trunk/leaf linked list):
//
//	Trunk page:
//	  Offset 0:  4 bytes - next trunk page number (0 = last trunk)
//	  Offset 4:  4 bytes - number of leaf page numbers on this trunk
//	  Offset 8+: 4 bytes each - leaf page numbers
//
// Max leaves per trunk = (pageSize - 8) / 4

// usableSize returns the usable page size (total page size minus reserved space).
// This must be used for all cell/content calculations. The full pageSize is only
// used for I/O operations (file reads/writes, buffer allocation, WAL frame sizes).
// Safe for concurrent use: returns an immutable value set at open time.
func (p *pager) usableSize() int {
	return p.usableSize_
}

// freelistMaxLeaves returns the max number of leaf entries per trunk page.
func (p *pager) freelistMaxLeaves() int {
	return (p.usableSize() - 8) / 4
}

// freePage adds a page to the freelist.
func (p *pager) freePage(pgno uint32) error {
	if pagerState(p.state.Load()) != pagerWriter {
		return ErrReadOnly
	}
	if pgno == 0 || pgno == 1 {
		return ErrInvalidPage
	}
	// Bounds check: page number must be within database size (fix 5.1).
	if pgno > p.dbSize.Load() {
		return ErrCorrupt
	}

	trunkPgno := p.header.FirstFreelistPg

	if trunkPgno != 0 {
		// Validate trunk page number (fix 5.1).
		if trunkPgno > p.dbSize.Load() {
			return ErrCorrupt
		}
		// Read the current trunk page
		trunkPg, err := p.getWritablePage(trunkPgno)
		if err != nil {
			return err
		}
		leafCount := int(binary.BigEndian.Uint32(trunkPg.data[4:8]))
		maxLeaves := p.freelistMaxLeaves()

		// Validate leaf count (fix 5.1).
		if leafCount < 0 || leafCount > maxLeaves {
			p.releasePage(trunkPg)
			return ErrCorrupt
		}

		if leafCount < maxLeaves {
			// Trunk has room — append as leaf entry
			binary.BigEndian.PutUint32(trunkPg.data[8+leafCount*4:], pgno)
			binary.BigEndian.PutUint32(trunkPg.data[4:8], uint32(leafCount+1))
			p.releasePage(trunkPg)
			p.header.TotalFreelistPgs++
			// Mark the freed page as dontWrite if it's dirty (fix 5.4).
			// The page content is now irrelevant since it's a freelist leaf.
			// Only done when adding as leaf to trunk, NOT when becoming a trunk
			// (trunk page content is meaningful -- it holds freelist structure).
			// Matches SQLite's freePage2() (btree.c:6920).
			if p.writePages[pgno] != nil {
				p.dontWrite(pgno)
			}
			// Track that this page had content before being freed, so that if
			// it is re-allocated from the freelist within the same transaction,
			// its content will be properly journaled for savepoint rollback.
			// Matches SQLite's btreeSetHasContent() (btree.c:6922).
			p.setHasContent(pgno)
			return nil
		}
		p.releasePage(trunkPg)
	}

	// No trunk or trunk is full — freed page becomes new trunk.
	// Must use getWritablePage to ensure savepoint copy is saved for potential
	// rollback. Falls back to cache.create only when no savepoints are active
	// (since no rollback can occur).
	newTrunkPg, err := p.getWritablePage(pgno)
	if err != nil {
		if len(p.savepoints) > 0 {
			// With active savepoints, we MUST have a savepoint copy for rollback.
			// If getWritablePage fails, propagate the error rather than silently
			// creating a page without a savepoint copy (which would cause Bug 9).
			if debugTrace {
				trace("freePage: getWritablePage(%d) failed with savepoints active: %v", pgno, err)
			}
			return err
		}
		// No savepoints: safe to use cache.create (no rollback possible).
		if debugTrace {
			trace("freePage: getWritablePage(%d) failed: %v — using cache.create (no savepoints)", pgno, err)
		}
		newTrunkPg = p.cache.create(pgno)
		p.cache.makeDirty(newTrunkPg)
		p.writePages[pgno] = newTrunkPg
	} else {
		if debugTrace {
			trace("freePage: pg=%d becomes new trunk (old trunk=%d), savepoint copy saved via getWritablePage", pgno, trunkPgno)
		}
	}
	clear(newTrunkPg.data)
	binary.BigEndian.PutUint32(newTrunkPg.data[0:4], trunkPgno) // next trunk = old trunk
	binary.BigEndian.PutUint32(newTrunkPg.data[4:8], 0)         // leaf count = 0
	newTrunkPg.header = pageHeader{}                            // clear parsed header
	p.releasePage(newTrunkPg)

	p.header.FirstFreelistPg = pgno
	p.header.TotalFreelistPgs++
	return nil
}

// allocateFromFreelist pops a page from the freelist and returns it.
func (p *pager) allocateFromFreelist() (*page, error) {
	trunkPgno := p.header.FirstFreelistPg
	if trunkPgno == 0 {
		return nil, ErrInvalidPage
	}
	// Validate trunk page number (fix 5.1).
	if trunkPgno > p.dbSize.Load() {
		return nil, ErrCorrupt
	}

	trunkPg, err := p.getWritablePage(trunkPgno)
	if err != nil {
		return nil, err
	}

	leafCount := int(binary.BigEndian.Uint32(trunkPg.data[4:8]))
	maxLeaves := p.freelistMaxLeaves()

	// Validate leaf count (fix 5.1): must be in range [0, maxLeaves].
	if leafCount < 0 || leafCount > maxLeaves {
		p.releasePage(trunkPg)
		return nil, ErrCorrupt
	}

	if leafCount > 0 {
		// Pop the last leaf page number
		leafPgno := binary.BigEndian.Uint32(trunkPg.data[8+(leafCount-1)*4:])

		// Validate leaf page number (fix 5.1).
		if leafPgno < 2 || leafPgno > p.dbSize.Load() {
			p.releasePage(trunkPg)
			return nil, ErrCorrupt
		}

		binary.BigEndian.PutUint32(trunkPg.data[4:8], uint32(leafCount-1))
		p.releasePage(trunkPg)

		p.header.TotalFreelistPgs--

		// Check if this page had meaningful content before being freed
		// in the current transaction. If so, we must NOT use the NOCONTENT
		// optimization — the page must go through getWritablePage so its
		// content is saved in the savepoint journal for potential rollback.
		// Matches SQLite's btreeGetHasContent() check (btree.c:6725):
		//   noContent = !btreeGetHasContent(pBt, *pPgno)? PAGER_GET_NOCONTENT : 0;
		//   rc = btreeGetUnusedPage(pBt, *pPgno, ppPage, noContent);
		//   if( rc==SQLITE_OK ){
		//     rc = sqlite3PagerWrite((*ppPage)->pDbPage);
		//   }
		if p.getHasContent(leafPgno) {
			// Page was freed in this transaction — fetch with content so
			// savepoint journaling captures the pre-free data.
			if debugTrace {
				trace("allocateFromFreelist: leaf pg=%d hasContent=true → getWritablePage (savepoint copy)", leafPgno)
			}
			pg, err := p.getWritablePage(leafPgno)
			if err != nil {
				return nil, err
			}
			clear(pg.data)
			pg.header = pageHeader{}
			delete(p.dontWritePages, leafPgno)
			return pg, nil
		}

		// When savepoints are active, MUST use getWritablePage so the page's
		// pre-allocation state is saved for potential rollback. This matches
		// SQLite which always calls sqlite3PagerWrite() after btreeGetUnusedPage(),
		// ensuring the page is journaled for savepoint rollback regardless of
		// the NOCONTENT flag. Without this, rolling back a savepoint would leave
		// the page with its new data while the freelist header is restored to
		// reference it — causing corruption (Bug 9).
		if len(p.savepoints) > 0 {
			if debugTrace {
				trace("allocateFromFreelist: leaf pg=%d hasContent=false but savepoints=%d → getWritablePage (savepoint safety)", leafPgno, len(p.savepoints))
			}
			pg, err := p.getWritablePage(leafPgno)
			if err != nil {
				return nil, err
			}
			clear(pg.data)
			pg.header = pageHeader{}
			delete(p.dontWritePages, leafPgno)
			return pg, nil
		}

		// No savepoints: use getPageNoContent since old freelist leaf content
		// is irrelevant and doesn't need savepoint journaling (fix 5.4).
		if debugTrace {
			trace("allocateFromFreelist: leaf pg=%d hasContent=false, no savepoints → getPageNoContent", leafPgno)
		}
		pg, err := p.getPageNoContent(leafPgno)
		if err != nil {
			return nil, err
		}
		clear(pg.data)
		pg.header = pageHeader{}
		p.cache.makeDirty(pg)
		p.writePages[leafPgno] = pg
		// Clear dontWrite flag: when a page is freed and then re-allocated
		// within the same transaction, the freePage() call may have marked it
		// dontWrite. Now that it's being reused, its content is meaningful
		// and must be written to WAL on commit. Matches SQLite's pcache.c
		// makeDirty() which clears PGHDR_DONT_WRITE.
		delete(p.dontWritePages, leafPgno)
		return pg, nil
	}

	// Trunk has no leaves — use the trunk page itself
	nextTrunk := binary.BigEndian.Uint32(trunkPg.data[0:4])

	// Validate next trunk page number (fix 5.1): 0 means end of list.
	if nextTrunk != 0 && nextTrunk > p.dbSize.Load() {
		p.releasePage(trunkPg)
		return nil, ErrCorrupt
	}

	p.header.FirstFreelistPg = nextTrunk
	p.header.TotalFreelistPgs--

	// Reuse the trunk page
	clear(trunkPg.data)
	trunkPg.header = pageHeader{}
	return trunkPg, nil
}

// acquireTempPage returns a page from the pool or allocates a new one.
// The returned page has a valid data buffer but all other fields are unset.
func (p *pager) acquireTempPage() *page {
	if v := p.pagePool.Get(); v != nil {
		return v.(*page)
	}
	return &page{
		data: make([]byte, p.pageSize),
	}
}

// recycleTempPage returns an uncached page to the pool for reuse.
func (p *pager) recycleTempPage(pg *page) {
	pg.pgno = 0
	pg.dirty = false
	pg.uncached = false
	pg.pinCount = 0
	pg.header = pageHeader{}
	pg.next = nil
	pg.prev = nil
	p.pagePool.Put(pg)
}

// releasePage unpins a page. For uncached (MVCC snapshot) pages, the page is
// recycled to the pool immediately. This is safe because:
//   - Cursors keep pages pinned until movement or Close()
//   - Get() clones the value before releasing
//   - Count() only reads header.cellCount before releasing
func (p *pager) releasePage(pg *page) {
	if pg == nil {
		return
	}
	if pg.uncached {
		p.recycleTempPage(pg)
		return
	}
	p.cache.release(pg)
}

// dontWrite marks a page so that it will be skipped during WAL writes on commit
// (fix 5.4). This is used for freed pages added as leaves to a freelist trunk:
// their content is irrelevant and need not be persisted.
//
// Matches SQLite's sqlite3PagerDontWrite() (pager.c:6283). The flag is only set
// when no savepoints are active, matching SQLite's condition (pPager->nSavepoint==0).
// With savepoints, the page data may need to be preserved for rollback.
func (p *pager) dontWrite(pgno uint32) {
	if len(p.savepoints) > 0 {
		if debugTrace {
			trace("dontWrite: SKIPPED pg=%d (savepoints=%d active)", pgno, len(p.savepoints))
		}
		return
	}
	if debugTrace {
		trace("dontWrite: marking pg=%d (no savepoints)", pgno)
	}
	if p.dontWritePages == nil {
		p.dontWritePages = make(map[uint32]bool)
	}
	p.dontWritePages[pgno] = true
}

// setHasContent marks a page as having had meaningful content before being
// freed as a freelist leaf page. When this page is later re-allocated from
// the freelist, the NOCONTENT optimization must be skipped so that the page's
// prior content is properly saved in the savepoint journal.
// Matches SQLite's btreeSetHasContent() (btree.c:651-664).
func (p *pager) setHasContent(pgno uint32) {
	if p.hasContent == nil {
		p.hasContent = make(map[uint32]bool)
	}
	p.hasContent[pgno] = true
}

// getHasContent returns true if the page was freed as a freelist leaf within
// the current transaction and may contain content needed for savepoint rollback.
// Matches SQLite's btreeGetHasContent() (btree.c:673-676).
func (p *pager) getHasContent(pgno uint32) bool {
	return p.hasContent[pgno]
}

// readHeaderCounters reads the FileChangeCount and SchemaCookie from page 1.
// It checks the SHM header to discover the true WAL state (including frames
// written by other processes), then uses shmHashGet + direct WAL file read to
// get the latest page 1 data. Falls back to the database file if page 1 is
// not in the WAL. This bypasses both the page cache and the in-process WAL
// index to ensure cross-process visibility.
//
// For inProcess mode (heap SHM), the in-process Go map (walIndex.get) is used
// instead of shmHashGet to avoid data races on the raw SHM byte regions.
// The SHM hash tables are written without synchronization (best-effort for
// cross-process readers), but in single-process mode the Go map with its
// RWMutex provides safe concurrent access.
func (p *pager) readHeaderCounters(walMaxFrame uint32) (fileChangeCount, schemaCookie uint32, err error) {
	// Determine the effective max frame by checking the SHM header,
	// which reflects writes from ALL processes sharing this database.
	// For inProcess mode, the SHM header is not updated by writers
	// (writeFrames skips writeHeader when inProcess=true), so we use
	// the in-process walIndex.maxFrame directly.
	effectiveMaxFrame := walMaxFrame
	if p.inProcess {
		// Use mxCommitFrame (not maxFrame) so spilled uncommitted frames are invisible to readers.
		if mf := p.wal.index.mxCommitFrame.Load(); mf > effectiveMaxFrame {
			effectiveMaxFrame = mf
		}
	} else if hdr, valid := p.wal.index.readHeader(); valid && hdr.mxFrame > effectiveMaxFrame {
		effectiveMaxFrame = hdr.mxFrame
	}

	// Look up page 1's latest frame.
	// For inProcess mode, use the Go map (walIndex.get) which is protected by
	// a RWMutex, avoiding data races on the unsynchronized SHM byte regions.
	// For multi-process mode, use shmHashGet which reads the cross-process SHM.
	if effectiveMaxFrame > 0 {
		var frame uint32
		if p.inProcess {
			frame = p.wal.index.get(1, effectiveMaxFrame)
		} else {
			frame = p.wal.index.shmHashGet(1, effectiveMaxFrame)
		}
		if frame > 0 {
			buf := make([]byte, dbHeaderSize)
			if err := p.readWalFrameData(frame, buf); err == nil {
				return binary.BigEndian.Uint32(buf[24:28]), binary.BigEndian.Uint32(buf[40:44]), nil
			}
		}
	}

	// No WAL frame for page 1; read from database file.
	if p.file == nil {
		// InMemory: page 1 is in pcache; read header from there.
		if pg := p.cache.fetch(1); pg != nil {
			fcc := binary.BigEndian.Uint32(pg.data[24:28])
			sc := binary.BigEndian.Uint32(pg.data[40:44])
			p.cache.release(pg)
			return fcc, sc, nil
		}
		return p.header.FileChangeCount, p.header.SchemaCookie, nil
	}
	buf := make([]byte, dbHeaderSize)
	if _, err := p.file.ReadAt(buf, 0); err != nil {
		return 0, 0, err
	}
	return binary.BigEndian.Uint32(buf[24:28]), binary.BigEndian.Uint32(buf[40:44]), nil
}

// readWalFrameData reads the first `len(buf)` bytes of page data from a WAL
// frame. Unlike wal.readFrame, this does not check the in-process nFrame
// counter, making it safe for cross-process reads where another process wrote
// the frame. For InMemory mode (memFrames), it reads from the in-memory frames.
func (p *pager) readWalFrameData(frame uint32, buf []byte) error {
	// Use the immutable inMemory flag instead of checking the mutable memFrames
	// slice header to avoid a data race with writeFramesMem's append.
	if p.wal.inMemory {
		p.wal.mu.RLock()
		defer p.wal.mu.RUnlock()
		idx := frame - 1
		if idx < uint32(len(p.wal.memFrames)) {
			copy(buf, p.wal.memFrames[idx].data)
			return nil
		}
		return ErrWALCorrupt
	}
	if p.wal.file == nil {
		return ErrWALCorrupt
	}
	frameSize := int64(walFrameSize) + int64(p.pageSize)
	offset := int64(walHeaderSize) + int64(frame-1)*frameSize + walFrameSize
	_, err := p.wal.file.ReadAt(buf, offset)
	return err
}

// pagerStress is the pcache stress callback invoked when the cache is full
// and all clean pages are exhausted. It spills a single dirty page to the WAL
// without committing, making it clean and evictable.
// Modeled after SQLite's pagerStress() (pager.c:4609-4681).
func (p *pager) pagerStress(pg *page) error {
	// Defense-in-depth: verify pager is in writer state. The primary guard
	// is that getPageAt uses createNoStress (readers never invoke xStress).
	// This check catches any unexpected caller path.
	// DRIFT from SQLite: SQLite's pagerStress always has a writer context;
	// our shared cache means readers could trigger xStress via create().
	st := pagerState(p.state.Load())
	if st == pagerError || st != pagerWriter {
		return nil
	}

	// Do not spill if OFF or ROLLBACK flags are set (SQLite pager.c:4636-4641).
	if p.doNotSpill&(spillFlagOff|spillFlagRollback) != 0 {
		return nil
	}

	// DRIFT from SQLite: SQLite does not explicitly check pgno==1 in pagerStress.
	// Instead, page 1 is structurally protected: it stays pinned (referenced)
	// throughout the transaction, so pcache never selects it as a spill victim.
	// We add an explicit guard because page 1 may become unpinned between
	// b-tree operations in our implementation.
	if pg.pgno == 1 {
		return nil
	}

	// DRIFT from SQLite: SQLite's pagerStress in WAL mode writes DONT_WRITE
	// pages to WAL anyway (the data is irrelevant but the frame is still
	// written). We skip the WAL write and just mark them clean, avoiding
	// unnecessary I/O. Safe because dontWrite page data is never read back.
	// We must still make them clean so they become evictable — without this,
	// the cache grows unbounded when freed pages are the only dirty victims.
	if p.dontWritePages[pg.pgno] {
		p.cache.makeClean(pg)
		return nil
	}

	// subjournalPageIfRequired equivalent (SQLite pager.c:4647):
	// Save page data for savepoint rollback before spilling.
	if len(p.savepoints) > 0 {
		sp := &p.savepoints[len(p.savepoints)-1]
		if _, exists := sp.pages[pg.pgno]; !exists {
			dataCopy := make([]byte, len(pg.data))
			copy(dataCopy, pg.data)
			sp.pages[pg.pgno] = dataCopy
		}
	}

	// Write the page to WAL without commit (SQLite pager.c:4649).
	if err := p.wal.writeFrames([]*page{pg}, false, 0); err != nil {
		return err
	}

	// Mark the page as clean so it becomes evictable (SQLite pager.c:4677).
	p.cache.makeClean(pg)

	return nil
}

// commit writes all dirty pages to WAL and commits the transaction.
// dataChanged/schemaChanged control whether FileChangeCount/SchemaCookie are
// incremented. Returns the WAL frame count and the new counter values.
func (p *pager) commit(dataChanged, schemaChanged bool) (nFrame, newFCC, newSC uint32, err error) {
	if pagerState(p.state.Load()) != pagerWriter {
		return 0, 0, 0, ErrReadOnly
	}

	if debugTrace {
		trace("commit: dbSize=%d savepoints=%d writePages=%d dontWritePages=%d hasContent=%d",
			p.dbSize.Load(), len(p.savepoints), len(p.writePages), len(p.dontWritePages), len(p.hasContent))
	}

	// Update the in-memory header with current database size.
	p.header.DatabaseSize = p.dbSize.Load()

	// Collect dirty pages first to determine if there are real changes.
	p.dirtyBuf = p.cache.appendDirtyPages(p.dirtyBuf[:0])

	// Filter out dontWrite pages before WAL write (fix 5.4).
	// These are freed leaf pages whose content is irrelevant.
	if len(p.dontWritePages) > 0 {
		if debugTrace {
			trace("commit: filtering %d dontWrite pages from %d dirty pages savepoints=%d",
				len(p.dontWritePages), len(p.dirtyBuf), len(p.savepoints))
			// Bug 13 diagnostic: check if any dontWrite page has non-zero content
			for pgno := range p.dontWritePages {
				if pg := p.writePages[pgno]; pg != nil {
					allZero := true
					for _, b := range pg.data {
						if b != 0 {
							allZero = false
							break
						}
					}
					trace("BUG13-DIAG commit: dontWrite pg=%d allZero=%v dirty=%v",
						pgno, allZero, pg.dirty)
				}
			}
		}
		n := 0
		for _, pg := range p.dirtyBuf {
			if p.dontWritePages[pg.pgno] {
				if debugTrace {
					trace("commit: dontWrite filtering pg=%d (skipping WAL write)", pg.pgno)
				}
				p.cache.makeClean(pg)
			} else {
				p.dirtyBuf[n] = pg
				n++
			}
		}
		p.dirtyBuf = p.dirtyBuf[:n]
		clear(p.dontWritePages)
	}

	// Determine if there are real changes: dirty data pages, header
	// modifications (freelist, dbSize changes), or spilled pages. Counter
	// increments are deferred until we confirm there's something to commit.
	// The nFrame check catches transactions where all dirty pages were spilled
	// (making dirtyBuf empty) but a commit frame is still needed.
	hasRealChanges := len(p.dirtyBuf) > 0 || p.header != p.savedHeader ||
		p.writePages[1] != nil || p.wal.nFrame.Load() > p.savedWalFrame

	if !hasRealChanges {
		// Empty transaction — counters not incremented.
		p.state.Store(int32(pagerOpen))
		p.savepoints = p.savepoints[:0]
		clear(p.writePages)
		clear(p.hasContent)
		p.wal.endWrite()
		return 0, p.header.FileChangeCount, p.header.SchemaCookie, nil
	}

	// There are real changes — apply counter increments if flagged.
	if dataChanged {
		p.header.FileChangeCount++
	}
	if schemaChanged {
		p.header.SchemaCookie++
	}

	// Ensure page 1 is always written with the updated header (fix 5.3).
	pg1, err := p.getWritablePage(1)
	if err != nil {
		p.pagerError()
		return 0, 0, 0, err
	}
	p.header.serialize(pg1.data[:dbHeaderSize])
	p.releasePage(pg1)

	// Re-collect dirty pages since page 1 may be newly dirty.
	p.dirtyBuf = p.cache.appendDirtyPages(p.dirtyBuf[:0])

	// Detect zero-content pages about to be written to WAL (Bug 9 detection)
	if debugTrace {
		trace("commit: writing %d dirty pages to WAL", len(p.dirtyBuf))
		for _, pg := range p.dirtyBuf {
			allZero := true
			for _, b := range pg.data {
				if b != 0 {
					allZero = false
					break
				}
			}
			if allZero && pg.pgno != 1 {
				// Check if it's a freelist trunk page (first 8 bytes = nextTrunk + leafCount)
				isFreelistTrunk := pg.pgno == p.header.FirstFreelistPg
				trace("commit: WARNING zero-content page %d about to be written to WAL! isFreelistTrunk=%v firstFreelist=%d",
					pg.pgno, isFreelistTrunk, p.header.FirstFreelistPg)
			}
		}
	}

	// Write all dirty pages to WAL
	if err := p.wal.writeFrames(p.dirtyBuf, true, p.dbSize.Load()); err != nil {
		p.pagerError()
		return 0, 0, 0, err
	}

	// Capture nFrame atomically for checkpoint threshold decision.
	nFrame = p.wal.nFrame.Load()

	// Mark all pages as clean
	for _, pg := range p.dirtyBuf {
		p.cache.makeClean(pg)
	}

	p.savepoints = p.savepoints[:0]
	clear(p.writePages)
	clear(p.dontWritePages)
	clear(p.hasContent)
	p.state.Store(int32(pagerOpen))
	p.wal.endWrite()

	return nFrame, p.header.FileChangeCount, p.header.SchemaCookie, nil
}

// rollback discards all changes in the current write transaction.
func (p *pager) rollback() error {
	st := pagerState(p.state.Load())
	if st != pagerWriter && st != pagerError {
		return nil
	}

	// Suppress spills during rollback (SQLite pager.c:2457).
	p.doNotSpill |= spillFlagRollback

	// Discard all dirty pages from cache
	dirtyPages := p.cache.dirtyPages()
	for _, pg := range dirtyPages {
		p.cache.discard(pg.pgno)
	}

	// Discard spilled (clean) pages from cache. Spilled pages were written
	// to WAL mid-transaction and marked clean by pagerStress, so they won't
	// appear in dirtyPages. Their cached content is stale after rollback.
	for pgno := range p.writePages {
		p.cache.discard(pgno)
	}

	// Roll back spilled frames in the WAL index. Spilled frames in the WAL
	// file are harmless (no commit marker), but pageMap entries and maxFrame
	// must be restored to the pre-transaction state.
	p.wal.index.rollbackToFrame(p.savedWalFrame)
	// Restore nFrame so the next transaction overwrites the dead spill frames
	// instead of writing past them. Also restore cumulative checksums so the
	// checksum chain is correct when frames are overwritten.
	p.wal.nFrame.Store(p.savedWalFrame)
	p.wal.cksum1 = p.savedWalCksum1
	p.wal.cksum2 = p.savedWalCksum2

	// Truncate in-memory WAL frames to match restored nFrame. Without this,
	// writeFramesMem appends past the stale entries, and readFrame(N) reads
	// memFrames[N-1] which points to rolled-back data instead of new data.
	if p.wal.inMemory {
		p.wal.mu.Lock()
		p.wal.memFrames = p.wal.memFrames[:p.savedWalFrame]
		p.wal.mu.Unlock()
	}

	// Restore the database header from the snapshot saved at beginWrite (fix 5.2).
	// This ensures FirstFreelistPg, TotalFreelistPgs, and DatabaseSize are
	// reverted to their pre-transaction values after dirty pages are discarded.
	p.header = p.savedHeader
	p.dbSize.Store(p.header.DatabaseSize)

	p.doNotSpill &^= spillFlagRollback
	p.savepoints = p.savepoints[:0]
	clear(p.writePages)
	clear(p.dontWritePages)
	clear(p.hasContent)
	p.state.Store(int32(pagerOpen))
	p.wal.endWrite()
	return nil
}

// pagerError transitions the pager to the error state. It ensures the WAL
// write lock is released (fix 2.2) so that other connections are not blocked
// indefinitely. Modeled after SQLite's pager_error() + pager_unlock() which
// calls sqlite3WalEndWriteTransaction() when in PAGER_ERROR state.
//
// Recovery path: the cache is purged and the header is restored from the
// saved snapshot. The next call to rollback() (or beginRead which checks
// for pagerError) will transition back to pagerOpen.
// DRIFT from SQLite: SQLite's pager_error() only sets errCode and transitions
// to PAGER_ERROR, deferring cleanup to the subsequent sqlite3PagerRollback().
// We perform eager cleanup here (cache purge, WAL rollback, lock release,
// transition to pagerOpen) to avoid leaving the WAL write lock held, which
// would block other writers in our concurrent goroutine model.
func (p *pager) pagerError() {
	p.state.Store(int32(pagerError))

	// Purge the cache — its contents cannot be trusted after an error.
	dirtyPages := p.cache.dirtyPages()
	for _, pg := range dirtyPages {
		p.cache.discard(pg.pgno)
	}
	p.cache.clear()

	// Roll back spilled frames in the WAL index and restore nFrame/checksums.
	// Without this, pageMap and nFrame remain inflated after error recovery,
	// causing checkpoint to copy uncommitted spill data to the database.
	p.wal.index.rollbackToFrame(p.savedWalFrame)
	p.wal.nFrame.Store(p.savedWalFrame)
	p.wal.cksum1 = p.savedWalCksum1
	p.wal.cksum2 = p.savedWalCksum2

	// Restore mxCommitFrame: the commit path advances mxCommitFrame before
	// writeHeader (wal.go), so a writeHeader failure leaves mxCommitFrame
	// ahead of the rolled-back WAL state. At beginWrite() time,
	// mxCommitFrame == nFrame == savedWalFrame, so restore to that value.
	p.wal.index.mxCommitFrame.Store(p.savedWalFrame)

	// Truncate in-memory WAL frames to match restored nFrame.
	if p.wal.inMemory {
		p.wal.mu.Lock()
		p.wal.memFrames = p.wal.memFrames[:p.savedWalFrame]
		p.wal.mu.Unlock()
	}

	// Restore the database header to pre-transaction state.
	p.header = p.savedHeader
	p.dbSize.Store(p.header.DatabaseSize)

	p.savepoints = p.savepoints[:0]
	clear(p.writePages)
	clear(p.dontWritePages)
	clear(p.hasContent)

	// Release the WAL write lock so other writers are not blocked (fix 2.2).
	p.wal.endWrite()

	// Transition back to open state now that we have cleaned up.
	// This mirrors SQLite's pager_unlock() which transitions from
	// PAGER_ERROR -> PAGER_OPEN after clearing the error.
	p.state.Store(int32(pagerOpen))
}

// savepoint creates a new savepoint and returns its ID.
// Page copies are saved lazily in getWritablePage when pages are actually modified.
func (p *pager) savepoint() (int, error) {
	if pagerState(p.state.Load()) != pagerWriter {
		return 0, ErrReadOnly
	}

	id := len(p.savepoints)
	dbSz := p.dbSize.Load()
	walFr := p.wal.nFrame.Load()
	if debugTrace {
		trace("savepoint: creating id=%d dbSize=%d walFrame=%d writePages=%d dontWritePages=%d hasContent=%d",
			id, dbSz, walFr, len(p.writePages), len(p.dontWritePages), len(p.hasContent))
	}
	p.savepoints = append(p.savepoints, savepointState{
		id:        id,
		dbSize:    dbSz,
		pages:     make(map[uint32][]byte),
		walFrame:  walFr,
		walCksum1: p.wal.cksum1,
		walCksum2: p.wal.cksum2,
		header:    p.header, // snapshot header for rollback (fix 9.3)
	})
	return id, nil
}

// rollbackToSavepoint rolls back to the given savepoint, restoring pages.
func (p *pager) rollbackToSavepoint(id int) error {
	if pagerState(p.state.Load()) != pagerWriter {
		return ErrReadOnly
	}
	if id < 0 || id >= len(p.savepoints) {
		return ErrInvalidSavepoint
	}

	sp := &p.savepoints[id]

	if debugTrace {
		trace("rollbackToSavepoint: id=%d spDbSize=%d currentDbSize=%d numSavepoints=%d writePages=%d",
			id, sp.dbSize, p.dbSize.Load(), len(p.savepoints), len(p.writePages))
	}

	// Suppress spills during savepoint rollback (SQLite pager.c:2457).
	p.doNotSpill |= spillFlagRollback

	// Discard pages allocated after the savepoint
	for pgno := range p.writePages {
		if pgno > sp.dbSize {
			if debugTrace {
				trace("rollbackToSavepoint: discard pg=%d (> spDbSize=%d)", pgno, sp.dbSize)
			}
			p.cache.discard(pgno)
			delete(p.writePages, pgno)
		}
	}

	// Roll back spilled frames in the WAL index to the savepoint's WAL position.
	p.wal.index.rollbackToFrame(sp.walFrame)
	// Restore nFrame and cumulative checksums so the next write overwrites
	// the dead spill frames with a correct checksum chain.
	p.wal.nFrame.Store(sp.walFrame)
	p.wal.cksum1 = sp.walCksum1
	p.wal.cksum2 = sp.walCksum2

	// Truncate in-memory WAL frames to match restored nFrame.
	if p.wal.inMemory {
		p.wal.mu.Lock()
		p.wal.memFrames = p.wal.memFrames[:sp.walFrame]
		p.wal.mu.Unlock()
	}

	// Restore saved page copies from all savepoints being rolled back.
	// Iterate from NEWEST to OLDEST (fix 9.1): this ensures that when a page
	// has copies at multiple savepoint levels, the oldest (correct) copy is
	// written last and wins. This is analogous to SQLite's pDone bitvec that
	// skips pages already restored — our reverse iteration achieves the same
	// result by letting the oldest copy overwrite newer ones.
	for i := len(p.savepoints) - 1; i >= id; i-- {
		if debugTrace {
			trace("rollbackToSavepoint: restoring sp[%d] pages (%d entries)", i, len(p.savepoints[i].pages))
		}
		for pgno, data := range p.savepoints[i].pages {
			// Always check writePages first. When a page was spilled and
			// evicted, a concurrent reader (e.g., integrity check) may have
			// created a different cache entry for this pgno via getPageAt.
			// Using cache.fetch alone could return the reader-created page,
			// leaving writePages pointing to a stale object and causing
			// data loss on subsequent writes via getWritablePage.
			var pg *page
			if wp := p.writePages[pgno]; wp != nil {
				// Ensure the writer's page is in the cache (replaces any
				// reader-created entry). reinsertDirty handles dirty list
				// cleanup for displaced pages.
				p.cache.reinsertDirty(wp)
				pg = p.cache.fetch(pgno) // pin under pcache mutex to avoid data race on pinCount
			} else {
				pg = p.cache.fetch(pgno)
			}
			if pg != nil {
				copy(pg.data, data)
				off := 0
				if pgno == 1 {
					off = dbHeaderSize
				}
				if pg.data[off] != 0 {
					pg.header.deserialize(pg.data[off:])
				}
				// Also restore the database header if this is page 1 (fix 9.3).
				if pgno == 1 {
					p.header.deserialize(pg.data[:dbHeaderSize])
					p.dbSize.Store(p.header.DatabaseSize)
				}
				// Re-dirty pages that were made clean by pagerStress spill.
				// Without this, spilled pages remain clean after data
				// restoration, and their content is lost at commit because
				// appendDirtyPages only collects dirty pages.
				if !pg.dirty {
					p.cache.makeDirty(pg)
				}
				p.cache.release(pg)
			}
		}
	}

	// Restore database header from savepoint snapshot (fix 9.3).
	// This covers the case where page 1 was not in any savepoint's page map
	// but the header was modified in memory (e.g., freelist changes).
	p.header = sp.header
	p.dbSize.Store(sp.dbSize)

	p.doNotSpill &^= spillFlagRollback

	// Remove savepoints above the target but keep the target itself.
	// This matches SQLite's behavior (pager.c:7025): ROLLBACK TO keeps the
	// savepoint active so it can be released or rolled back again later.
	p.savepoints = p.savepoints[:id+1]

	if debugTrace {
		// Bug 13 diagnostic: log residual dontWritePages/hasContent after rollback
		if len(p.dontWritePages) > 0 || len(p.hasContent) > 0 {
			trace("BUG13-DIAG rollbackToSavepoint: RESIDUAL dontWritePages=%d hasContent=%d after rollback id=%d",
				len(p.dontWritePages), len(p.hasContent), id)
			for pg := range p.dontWritePages {
				trace("BUG13-DIAG   residual dontWrite pg=%d inWritePages=%v", pg, p.writePages[pg] != nil)
			}
		}
	}

	return nil
}

// releaseSavepoint releases a savepoint and all savepoints above it,
// merging their changes into the parent savepoint (or transaction).
func (p *pager) releaseSavepoint(id int) error {
	if pagerState(p.state.Load()) != pagerWriter {
		return ErrReadOnly
	}
	if id < 0 || id >= len(p.savepoints) {
		return ErrInvalidSavepoint
	}

	if debugTrace {
		trace("releaseSavepoint: id=%d numSavepoints=%d", id, len(p.savepoints))
	}
	// Merge page copies down to parent savepoint
	if id > 0 {
		parent := &p.savepoints[id-1]
		for i := id; i < len(p.savepoints); i++ {
			for pgno, data := range p.savepoints[i].pages {
				if _, exists := parent.pages[pgno]; !exists {
					parent.pages[pgno] = data
				}
			}
		}
	}

	p.savepoints = p.savepoints[:id]
	return nil
}

// checkpointWithMode runs a WAL checkpoint with the specified mode.
// Does NOT take pager.mu.Lock — readers can continue during checkpoint.
// The WAL's busy handler is used for FULL/RESTART/TRUNCATE modes to wait
// for readers that block progress, matching SQLite's behavior.
func (p *pager) checkpointWithMode(mode CheckpointMode) error {
	return p.wal.checkpointWithMode(p.file, p.cache, mode, p.wal.busyHandler)
}

// tryCheckpoint attempts a passive checkpoint for auto-checkpoint.
// Uses PASSIVE mode to avoid blocking writers or readers, matching SQLite.
func (p *pager) tryCheckpoint() error {
	return p.wal.checkpointPassive(p.file, p.cache)
}

// writeOverflowChain writes data to a chain of overflow pages and returns
// the first page number in the chain.
func (p *pager) writeOverflowChain(data []byte) (uint32, error) {
	if pagerState(p.state.Load()) != pagerWriter {
		return 0, ErrReadOnly
	}

	usable := overflowPageUsable(p.usableSize())
	var firstPgno uint32
	var prevPg *page
	origDataLen := len(data)

	for len(data) > 0 {
		pg, err := p.allocatePage()
		if err != nil {
			return 0, err
		}
		if firstPgno == 0 {
			firstPgno = pg.pgno
		}
		if prevPg != nil {
			// Set next pointer on previous page
			binary.BigEndian.PutUint32(prevPg.data[0:4], pg.pgno)
			p.releasePage(prevPg)
		}

		// Write next pointer (0 = last page for now) and data
		binary.BigEndian.PutUint32(pg.data[0:4], 0)
		chunk := usable
		if chunk > len(data) {
			chunk = len(data)
		}
		copy(pg.data[4:4+chunk], data[:chunk])
		data = data[chunk:]
		prevPg = pg
	}
	if prevPg != nil {
		p.releasePage(prevPg)
	}
	if debugTrace {
		trace("writeOverflowChain: firstPg=%d totalDataLen=%d", firstPgno, origDataLen)
	}
	return firstPgno, nil
}

// readOverflowChainAt reads data from a chain of overflow pages into buf
// using the specified walMaxFrame for snapshot isolation.
// Uses getPageAt (shared cache) — suitable for the writer who needs to see
// its own dirty pages. Readers should use readOverflowChainMVCC instead to
// avoid populating the cache with stale snapshot data.
func (p *pager) readOverflowChainAt(firstPgno uint32, buf []byte, walMaxFrame uint32) error {
	return p.readOverflowChainInternal(firstPgno, buf, walMaxFrame, false)
}

// readOverflowChainMVCC reads overflow data bypassing the shared page cache.
// This prevents readers from polluting the cache with old-snapshot data that
// the writer could later read as current, causing on-disk corruption.
func (p *pager) readOverflowChainMVCC(firstPgno uint32, buf []byte, walMaxFrame uint32) error {
	return p.readOverflowChainInternal(firstPgno, buf, walMaxFrame, true)
}

func (p *pager) readOverflowChainInternal(firstPgno uint32, buf []byte, walMaxFrame uint32, mvcc bool) error {
	usable := overflowPageUsable(p.usableSize())
	pgno := firstPgno
	off := 0

	// Compute max iterations to prevent infinite loops on circular chains (fix 8.2).
	// The maximum number of overflow pages needed is ceil(len(buf) / usable).
	maxIter := len(buf)/usable + 2
	if maxIter < 10 {
		maxIter = 10
	}
	iter := 0

	dbSize := p.dbSize.Load()

	for pgno != 0 && off < len(buf) {
		// Bounds checking (fix 8.2): page numbers must be >= 2 and <= dbSize.
		// Page 0 is invalid and page 1 is the database header page.
		if pgno < 2 || pgno > dbSize {
			return ErrCorrupt
		}

		// Max iteration counter to prevent infinite loops on circular chains (fix 8.2).
		iter++
		if iter > maxIter {
			return ErrCorrupt
		}

		var pg *page
		var err error
		if mvcc {
			pg, err = p.readPageUncached(pgno, walMaxFrame)
		} else {
			pg, err = p.getPage(pgno)
		}
		if err != nil {
			return err
		}
		chunk := usable
		if chunk > len(buf)-off {
			chunk = len(buf) - off
		}
		copy(buf[off:off+chunk], pg.data[4:4+chunk])
		pgno = binary.BigEndian.Uint32(pg.data[0:4])
		p.releasePage(pg)
		off += chunk
	}
	return nil
}

// freeOverflowChain frees all pages in an overflow chain.
func (p *pager) freeOverflowChain(firstPgno uint32) error {
	if debugTrace {
		trace("freeOverflowChain: start firstPg=%d", firstPgno)
	}
	pgno := firstPgno

	// Max iteration counter to prevent infinite loops on circular chains (fix 8.2).
	// Use dbSize as an upper bound — a chain cannot have more pages than the database.
	maxIter := int(p.dbSize.Load())
	if maxIter < 10 {
		maxIter = 10
	}
	iter := 0

	for pgno != 0 {
		// Bounds checking (fix 8.2): page numbers must be >= 2 and <= dbSize.
		if pgno < 2 || pgno > p.dbSize.Load() {
			return ErrCorrupt
		}

		// Max iteration counter (fix 8.2).
		iter++
		if iter > maxIter {
			return ErrCorrupt
		}

		pg, err := p.getPage(pgno)
		if err != nil {
			return err
		}
		nextPgno := binary.BigEndian.Uint32(pg.data[0:4])
		p.releasePage(pg)
		if err := p.freePage(pgno); err != nil {
			return err
		}
		pgno = nextPgno
	}
	return nil
}

// close closes the pager, WAL, and database file.
// Matches SQLite's sqlite3PagerClose() -> sqlite3WalClose(): checkpoint the WAL,
// then truncate the WAL file to zero bytes before closing.
func (p *pager) close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.wal != nil {
		if p.inMemory {
			// InMemory: just reset WAL state, nothing to checkpoint to disk
		} else {
			// Checkpoint before closing. Only truncate WAL if all frames
			// were copied to the database file. A partial PASSIVE checkpoint
			// (due to active readers) returns ErrBusy; truncating the WAL
			// in that case would destroy uncopied frames and corrupt the DB.
			// Matches SQLite's sqlite3WalClose(): walLimitSize only called
			// when rc==SQLITE_OK.
			if debugTrace {
				trace("close: starting passive checkpoint before WAL truncation, dbSize=%d", p.dbSize.Load())
			}
			cpErr := p.wal.checkpointPassive(p.file, p.cache)
			if cpErr != nil {
				if debugTrace {
					trace("close: checkpointPassive incomplete or failed: %v", cpErr)
				}
			}
			if cpErr == nil {
				p.wal.truncateFile()
			}
		}
		_ = p.wal.close()
	}

	p.cache.clear()

	if p.file != nil {
		err := p.file.Close()
		p.file = nil
		return err
	}
	return nil
}
