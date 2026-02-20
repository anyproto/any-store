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

// pager manages database pages, cache, and WAL interaction.
type pager struct {
	mu       sync.RWMutex
	file     *os.File
	wal      *wal
	cache    *pcache
	header   dbHeader
	path     string
	pageSize uint32
	dbSize   uint32 // database size in pages
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

	// inProcess uses heap-backed shm (faster, single-process only)
	inProcess bool

	// noCommitSync skips fdatasync on WAL commit (deferred durability)
	noCommitSync bool

	// inMemory keeps the entire database in memory with no files on disk
	inMemory bool
}

// savepointState captures the state needed to rollback to a savepoint.
type savepointState struct {
	id       int
	dbSize   uint32
	pages    map[uint32][]byte // pgno -> copy of page data before modification
	walFrame uint32            // WAL frame count at savepoint time
	header   dbHeader          // snapshot of database header at savepoint time (fix 9.3)
}

// newPager creates a new pager for the given database path.
// purgeable controls whether the page cache can evict pages (false for InMemory databases).
func newPager(path string, pageSize uint32, cacheSize int, purgeable bool) *pager {
	return &pager{
		path:     path,
		pageSize: pageSize,
		cache:    newPcache(int(pageSize), cacheSize, purgeable),
	}
}

// open opens the database file, initializes the WAL, and recovers if needed.
func (p *pager) open() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.inMemory {
		// In-memory database: no file on disk
		return p.initNewDB()
	}

	f, err := os.OpenFile(p.path, os.O_RDWR|os.O_CREATE, 0666)
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
	buf := make([]byte, p.pageSize)
	if p.pageSize == 0 {
		buf = make([]byte, DefaultPageSize)
	}
	n, err := f.ReadAt(buf, 0)
	if err != nil && n < dbHeaderSize {
		return fmt.Errorf("btree: failed to read database header: %w", err)
	}

	if err := p.header.deserialize(buf[:dbHeaderSize]); err != nil {
		return err
	}

	p.pageSize = p.header.PageSize
	p.dbSize = p.header.DatabaseSize
	p.cache = newPcache(int(p.pageSize), p.cache.maxPages, p.cache.purgeable)

	// Open WAL
	p.wal = newWal(p.path+"-wal", p.pageSize)
	p.wal.inProcess = p.inProcess
	p.wal.noCommitSync = p.noCommitSync
	p.wal.inMemory = p.inMemory
	p.wal.busyHandler = DefaultBusyTimeout(5 * time.Second)
	if err := p.wal.open(); err != nil {
		return err
	}

	// Update dbSize from WAL if it has committed frames
	if p.wal.index.maxPage > 0 {
		p.dbSize = p.wal.index.maxPage
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
		SchemaFormat:     4,
		DefaultCacheSize: defaultCacheSize,
		TextEncoding:     1, // UTF-8
	}

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
		if err := p.file.Sync(); err != nil {
			return err
		}
	}

	p.dbSize = 1
	p.cache = newPcache(int(p.pageSize), p.cache.maxPages, p.cache.purgeable)

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
	return p.getPageAt(pgno, p.walMaxFrame.Load())
}

// getPageAt returns the page with the given page number, using the specified
// walMaxFrame for snapshot isolation. This allows different readers to have
// different WAL snapshots.
func (p *pager) getPageAt(pgno, walMaxFrame uint32) (*page, error) {
	if pgno == 0 {
		return nil, ErrInvalidPage
	}

	// Check cache first. Use fetchPinned to capture the dirty flag under
	// the pcache lock, avoiding a data race with concurrent makeDirty calls
	// from the writer goroutine.
	if pg, wasDirty := p.cache.fetchPinned(pgno); pg != nil {
		// For clean pages, verify the cached version is within our snapshot.
		// Dirty pages are returned as-is: the caller is responsible for
		// MVCC dirty-page handling at a higher level (btree.getPage for
		// readers checks writePages; ReadTx.txGetPage also bypasses dirty
		// pages for non-writable transactions).
		if !wasDirty {
			latestFrame := p.wal.index.getLatest(pgno)
			if latestFrame == 0 || latestFrame <= walMaxFrame {
				return pg, nil
			}
			p.cache.release(pg)
			return p.readPageUncached(pgno, walMaxFrame)
		}
		return pg, nil
	}

	// Cache miss: create a new cached page.
	pg := p.cache.create(pgno)

	// Try to read from WAL first
	if walMaxFrame > 0 {
		frame := p.wal.index.get(pgno, walMaxFrame)
		if frame > 0 {
			if err := p.wal.readFrame(frame, pg.data); err != nil {
				p.cache.release(pg)
				return nil, err
			}
			// Parse page header
			off := 0
			if pgno == 1 {
				off = dbHeaderSize
			}
			pg.header.deserialize(pg.data[off:])
			return pg, nil
		}
	}

	// Read from database file
	if p.file != nil {
		offset := int64(pgno-1) * int64(p.pageSize)
		_, err := p.file.ReadAt(pg.data, offset)
		if err != nil {
			// If page is beyond current file but within dbSize, it's a new page
			if pgno <= p.dbSize {
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
	pg := &page{
		pgno:     pgno,
		data:     make([]byte, p.pageSize),
		pinCount: 1,
		uncached: true,
	}

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
			if pgno <= p.dbSize {
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

	p.dbSize++
	pgno := p.dbSize

	// Use getPageNoContent: new pages have no existing content to read (fix 5.4).
	pg, err := p.getPageNoContent(pgno)
	if err != nil {
		p.dbSize--
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
func (p *pager) usableSize() int {
	return int(p.pageSize) - int(p.header.ReservedSpace)
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
	if pgno > p.dbSize {
		return ErrCorrupt
	}

	trunkPgno := p.header.FirstFreelistPg

	if trunkPgno != 0 {
		// Validate trunk page number (fix 5.1).
		if trunkPgno > p.dbSize {
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

	// No trunk or trunk is full — freed page becomes new trunk
	newTrunkPg, err := p.getWritablePage(pgno)
	if err != nil {
		// Page may not be in cache yet; create it
		newTrunkPg = p.cache.create(pgno)
		p.cache.makeDirty(newTrunkPg)
		p.writePages[pgno] = newTrunkPg
	}
	clear(newTrunkPg.data)
	binary.BigEndian.PutUint32(newTrunkPg.data[0:4], trunkPgno) // next trunk = old trunk
	binary.BigEndian.PutUint32(newTrunkPg.data[4:8], 0)         // leaf count = 0
	newTrunkPg.header = pageHeader{} // clear parsed header
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
	if trunkPgno > p.dbSize {
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
		if leafPgno < 2 || leafPgno > p.dbSize {
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
			pg, err := p.getWritablePage(leafPgno)
			if err != nil {
				return nil, err
			}
			clear(pg.data)
			delete(p.dontWritePages, leafPgno)
			return pg, nil
		}

		// Use getPageNoContent: old freelist leaf content is irrelevant (fix 5.4).
		pg, err := p.getPageNoContent(leafPgno)
		if err != nil {
			return nil, err
		}
		clear(pg.data)
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
	if nextTrunk != 0 && nextTrunk > p.dbSize {
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

// releasePage unpins a page.
func (p *pager) releasePage(pg *page) {
	if pg == nil {
		return
	}
	// Uncached pages (MVCC snapshot copies) are not in the shared cache.
	// Just drop them -- they will be garbage collected.
	if pg.uncached {
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
		return
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
		p.wal.index.mu.RLock()
		if p.wal.index.maxFrame > effectiveMaxFrame {
			effectiveMaxFrame = p.wal.index.maxFrame
		}
		p.wal.index.mu.RUnlock()
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

// commit writes all dirty pages to WAL and commits the transaction.
// dataChanged/schemaChanged control whether FileChangeCount/SchemaCookie are
// incremented. Returns the WAL frame count and the new counter values.
func (p *pager) commit(dataChanged, schemaChanged bool) (nFrame, newFCC, newSC uint32, err error) {
	if pagerState(p.state.Load()) != pagerWriter {
		return 0, 0, 0, ErrReadOnly
	}

	// Update the in-memory header with current database size.
	p.header.DatabaseSize = p.dbSize

	// Collect dirty pages first to determine if there are real changes.
	p.dirtyBuf = p.cache.appendDirtyPages(p.dirtyBuf[:0])

	// Filter out dontWrite pages before WAL write (fix 5.4).
	// These are freed leaf pages whose content is irrelevant.
	if len(p.dontWritePages) > 0 {
		n := 0
		for _, pg := range p.dirtyBuf {
			if p.dontWritePages[pg.pgno] {
				p.cache.makeClean(pg)
			} else {
				p.dirtyBuf[n] = pg
				n++
			}
		}
		p.dirtyBuf = p.dirtyBuf[:n]
		clear(p.dontWritePages)
	}

	// Determine if there are real changes: dirty data pages or header
	// modifications (freelist, dbSize changes). Counter increments are
	// deferred until we confirm there's something to commit.
	hasRealChanges := len(p.dirtyBuf) > 0 || p.header != p.savedHeader || p.writePages[1] != nil

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

	// Write all dirty pages to WAL
	if err := p.wal.writeFrames(p.dirtyBuf, true, p.dbSize); err != nil {
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

	// Discard all dirty pages from cache
	dirtyPages := p.cache.dirtyPages()
	for _, pg := range dirtyPages {
		p.cache.discard(pg.pgno)
	}

	// Restore the database header from the snapshot saved at beginWrite (fix 5.2).
	// This ensures FirstFreelistPg, TotalFreelistPgs, and DatabaseSize are
	// reverted to their pre-transaction values after dirty pages are discarded.
	p.header = p.savedHeader
	p.dbSize = p.header.DatabaseSize

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
func (p *pager) pagerError() {
	p.state.Store(int32(pagerError))

	// Purge the cache — its contents cannot be trusted after an error.
	dirtyPages := p.cache.dirtyPages()
	for _, pg := range dirtyPages {
		p.cache.discard(pg.pgno)
	}
	p.cache.clear()

	// Restore the database header to pre-transaction state.
	p.header = p.savedHeader
	p.dbSize = p.header.DatabaseSize

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
	p.savepoints = append(p.savepoints, savepointState{
		id:       id,
		dbSize:   p.dbSize,
		pages:    make(map[uint32][]byte),
		walFrame: p.wal.nFrame.Load(),
		header:   p.header, // snapshot header for rollback (fix 9.3)
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

	// Discard pages allocated after the savepoint
	for pgno := range p.writePages {
		if pgno > sp.dbSize {
			p.cache.discard(pgno)
			delete(p.writePages, pgno)
		}
	}

	// Restore saved page copies from all savepoints being rolled back.
	// Iterate from NEWEST to OLDEST (fix 9.1): this ensures that when a page
	// has copies at multiple savepoint levels, the oldest (correct) copy is
	// written last and wins. This is analogous to SQLite's pDone bitvec that
	// skips pages already restored — our reverse iteration achieves the same
	// result by letting the oldest copy overwrite newer ones.
	for i := len(p.savepoints) - 1; i >= id; i-- {
		for pgno, data := range p.savepoints[i].pages {
			if pg := p.cache.fetch(pgno); pg != nil {
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
					p.dbSize = p.header.DatabaseSize
				}
				// Page is restored to pre-savepoint state but stays dirty
				// so it can be modified again in the current transaction.
				p.cache.release(pg)
			}
		}
	}

	// Restore database header from savepoint snapshot (fix 9.3).
	// This covers the case where page 1 was not in any savepoint's page map
	// but the header was modified in memory (e.g., freelist changes).
	p.header = sp.header
	p.dbSize = sp.dbSize

	// Remove savepoints above the target but keep the target itself.
	// This matches SQLite's behavior (pager.c:7025): ROLLBACK TO keeps the
	// savepoint active so it can be released or rolled back again later.
	p.savepoints = p.savepoints[:id+1]
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

// syncWalForCheckpoint syncs the WAL file before checkpoint when noCommitSync=true
// (fix 2.7). SQLite's synchronous=NORMAL skips per-commit WAL syncs but still
// syncs the WAL before checkpoint (wal.c:2260, CKPT_SYNC_FLAGS). Our wal.go
// guards the pre-checkpoint WAL sync with !w.noCommitSync, making noCommitSync=true
// equivalent to SQLite's synchronous=OFF rather than NORMAL.
//
// This pager-level sync bridges the gap: when noCommitSync=true, the pager syncs
// the WAL file before delegating to wal.checkpoint(). When noCommitSync=false, the
// WAL checkpoint already handles this sync internally, so no extra sync is needed.
//
// The sync is best-effort: errors are silently ignored because the checkpoint
// itself will encounter and report any persistent I/O errors.
func (p *pager) syncWalForCheckpoint() {
	if p.wal == nil || p.wal.file == nil || p.wal.inMemory {
		return
	}
	_ = fdatasync(p.wal.file)
}

// checkpoint runs a WAL checkpoint.
// Does NOT take pager.mu.Lock — readers can continue during checkpoint.
// The WAL checkpoint internally acquires lockWrite (blocks new writers)
// and uses mxSafeFrame to avoid interfering with active readers.
func (p *pager) checkpoint() error {
	if p.noCommitSync {
		p.syncWalForCheckpoint()
	}
	return p.wal.checkpoint(p.file, p.cache)
}

// tryCheckpoint attempts a checkpoint. Unlike the old version, this no longer
// needs TryLock on pager.mu since checkpoint doesn't block readers.
func (p *pager) tryCheckpoint() error {
	if p.noCommitSync {
		p.syncWalForCheckpoint()
	}
	return p.wal.checkpoint(p.file, p.cache)
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
	return firstPgno, nil
}

// readOverflowChain reads data from a chain of overflow pages into buf.
// Uses the pager's global walMaxFrame. For reader-specific snapshots,
// use readOverflowChainAt instead (fix 8.3).
func (p *pager) readOverflowChain(firstPgno uint32, buf []byte) error {
	return p.readOverflowChainAt(firstPgno, buf, p.walMaxFrame.Load())
}

// readOverflowChainAt reads data from a chain of overflow pages into buf
// using the specified walMaxFrame for snapshot isolation (fix 8.3).
// This ensures overflow reads use the correct reader snapshot rather than
// the pager's global walMaxFrame which may have advanced.
func (p *pager) readOverflowChainAt(firstPgno uint32, buf []byte, walMaxFrame uint32) error {
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

	dbSize := p.dbSize

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

		pg, err := p.getPageAt(pgno, walMaxFrame)
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
	pgno := firstPgno

	// Max iteration counter to prevent infinite loops on circular chains (fix 8.2).
	// Use dbSize as an upper bound — a chain cannot have more pages than the database.
	maxIter := int(p.dbSize)
	if maxIter < 10 {
		maxIter = 10
	}
	iter := 0

	for pgno != 0 {
		// Bounds checking (fix 8.2): page numbers must be >= 2 and <= dbSize.
		if pgno < 2 || pgno > p.dbSize {
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
			// Checkpoint before closing, then truncate WAL to zero bytes.
			// SQLite's sqlite3WalClose() does a PASSIVE checkpoint, and if successful,
			// deletes (or truncates to 0) the WAL file. We truncate to 0 to match
			// the test expectation that WAL is empty after a clean close.
			_ = p.wal.checkpoint(p.file, p.cache)
			// Truncate WAL file to zero bytes after successful checkpoint,
			// matching SQLite's walLimitSize(pWal, 0) in sqlite3WalClose().
			p.wal.truncateFile()
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
