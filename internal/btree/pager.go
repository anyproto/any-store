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

	// WAL snapshot for the current write transaction (set atomically).
	// Readers use per-tx walMaxFrame instead.
	walMaxFrame atomic.Uint32

	// Write-transaction page map: bypasses pcache lock for hot pages during writes.
	// Only accessed by the single writer goroutine, so no lock needed.
	writePages map[uint32]*page

	// Reusable slice for collecting dirty pages during commit
	dirtyBuf []*page

	// inProcess uses heap-backed shm (faster, single-process only)
	inProcess bool

	// noSync skips fdatasync on WAL commit (deferred durability)
	noSync bool
}

// savepointState captures the state needed to rollback to a savepoint.
type savepointState struct {
	id       int
	dbSize   uint32
	pages    map[uint32][]byte // pgno -> copy of page data before modification
	walFrame uint32            // WAL frame count at savepoint time
}

// newPager creates a new pager for the given database path.
func newPager(path string, pageSize uint32, cacheSize int) *pager {
	return &pager{
		path:     path,
		pageSize: pageSize,
		cache:    newPcache(int(pageSize), cacheSize),
	}
}

// open opens the database file, initializes the WAL, and recovers if needed.
func (p *pager) open() error {
	p.mu.Lock()
	defer p.mu.Unlock()

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
	p.cache = newPcache(int(p.pageSize), p.cache.maxPages)

	// Open WAL
	p.wal = newWal(p.path+"-wal", p.pageSize)
	p.wal.inProcess = p.inProcess
	p.wal.noSync = p.noSync
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

	// Initialize page 1 as a leaf table b-tree page (for the master namespace table)
	hdrOff := dbHeaderSize
	buf[hdrOff] = pageTypeLeafTbl // page type
	buf[hdrOff+1] = 0             // first free block (high byte)
	buf[hdrOff+2] = 0             // first free block (low byte)
	buf[hdrOff+3] = 0             // cell count (high byte)
	buf[hdrOff+4] = 0             // cell count (low byte)
	usable := uint16(p.pageSize)
	buf[hdrOff+5] = byte(usable >> 8) // cell content offset (high byte)
	buf[hdrOff+6] = byte(usable)      // cell content offset (low byte)
	buf[hdrOff+7] = 0                 // fragmented free bytes

	if _, err := p.file.WriteAt(buf, 0); err != nil {
		return err
	}
	if err := p.file.Sync(); err != nil {
		return err
	}

	p.dbSize = 1
	p.cache = newPcache(int(p.pageSize), p.cache.maxPages)

	// Open WAL
	p.wal = newWal(p.path+"-wal", p.pageSize)
	p.wal.inProcess = p.inProcess
	p.wal.noSync = p.noSync
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
	p.walMaxFrame.Store(maxFrame) // for internal pager operations (e.g. getPage in write path)
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
	if p.writePages == nil {
		p.writePages = make(map[uint32]*page, 64)
	}
	return nil
}

// getPage returns the page with the given page number, reading from WAL or disk as needed.
// Uses the pager's walMaxFrame (set during beginRead for the current writer).
func (p *pager) getPage(pgno uint32) (*page, error) {
	return p.getPageAt(pgno, p.walMaxFrame.Load())
}

// getPageAt returns the page with the given page number, using the specified
// walMaxFrame for snapshot isolation. This allows different readers to have
// different WAL snapshots.
func (p *pager) getPageAt(pgno, walMaxFrame uint32) (*page, error) {
	if pgno == 0 {
		return nil, ErrInvalidPage
	}

	// Check cache first
	if pg := p.cache.fetch(pgno); pg != nil {
		return pg, nil
	}

	// Create a new cached page
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

// getWritablePage returns a page ready for writing. It marks the page as dirty
// and saves a copy for savepoint rollback if needed.
func (p *pager) getWritablePage(pgno uint32) (*page, error) {
	if pagerState(p.state.Load()) != pagerWriter {
		return nil, ErrReadOnly
	}

	// Fast path: check write-transaction page map (no lock needed)
	if pg := p.writePages[pgno]; pg != nil {
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

	pg := p.cache.create(pgno)
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

// freelistMaxLeaves returns the max number of leaf entries per trunk page.
func (p *pager) freelistMaxLeaves() int {
	return (int(p.pageSize) - 8) / 4
}

// freePage adds a page to the freelist.
func (p *pager) freePage(pgno uint32) error {
	if pagerState(p.state.Load()) != pagerWriter {
		return ErrReadOnly
	}
	if pgno == 0 || pgno == 1 {
		return ErrInvalidPage
	}

	trunkPgno := p.header.FirstFreelistPg

	if trunkPgno != 0 {
		// Read the current trunk page
		trunkPg, err := p.getWritablePage(trunkPgno)
		if err != nil {
			return err
		}
		leafCount := int(binary.BigEndian.Uint32(trunkPg.data[4:8]))
		maxLeaves := p.freelistMaxLeaves()

		if leafCount < maxLeaves {
			// Trunk has room — append as leaf entry
			binary.BigEndian.PutUint32(trunkPg.data[8+leafCount*4:], pgno)
			binary.BigEndian.PutUint32(trunkPg.data[4:8], uint32(leafCount+1))
			p.releasePage(trunkPg)
			p.header.TotalFreelistPgs++
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

	trunkPg, err := p.getWritablePage(trunkPgno)
	if err != nil {
		return nil, err
	}

	leafCount := int(binary.BigEndian.Uint32(trunkPg.data[4:8]))

	if leafCount > 0 {
		// Pop the last leaf page number
		leafPgno := binary.BigEndian.Uint32(trunkPg.data[8+(leafCount-1)*4:])
		binary.BigEndian.PutUint32(trunkPg.data[4:8], uint32(leafCount-1))
		p.releasePage(trunkPg)

		p.header.TotalFreelistPgs--

		// Create the page in cache
		pg := p.cache.create(leafPgno)
		clear(pg.data)
		p.cache.makeDirty(pg)
		p.writePages[leafPgno] = pg
		return pg, nil
	}

	// Trunk has no leaves — use the trunk page itself
	nextTrunk := binary.BigEndian.Uint32(trunkPg.data[0:4])
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
	p.cache.release(pg)
}

// commit writes all dirty pages to WAL and commits the transaction.
// Returns the WAL frame count at commit time (for auto-checkpoint decisions).
func (p *pager) commit() (nFrame uint32, err error) {
	if pagerState(p.state.Load()) != pagerWriter {
		return 0, ErrReadOnly
	}

	p.dirtyBuf = p.cache.appendDirtyPages(p.dirtyBuf[:0])
	if len(p.dirtyBuf) == 0 {
		p.state.Store(int32(pagerOpen))
		p.savepoints = p.savepoints[:0]
		clear(p.writePages)
		p.wal.endWrite()
		return 0, nil
	}

	// Update the database header on page 1
	if pg := p.writePages[1]; pg != nil {
		p.header.DatabaseSize = p.dbSize
		p.header.FileChangeCount++
		p.header.serialize(pg.data[:dbHeaderSize])
	} else if pg := p.cache.fetch(1); pg != nil {
		p.header.DatabaseSize = p.dbSize
		p.header.FileChangeCount++
		p.header.serialize(pg.data[:dbHeaderSize])
		p.cache.release(pg)
	}

	// Write all dirty pages to WAL
	if err := p.wal.writeFrames(p.dirtyBuf, true, p.dbSize); err != nil {
		p.state.Store(int32(pagerError))
		return 0, err
	}

	// Capture nFrame under w.mu for happens-before ordering with checkpoint
	p.wal.mu.Lock()
	nFrame = p.wal.nFrame
	p.wal.mu.Unlock()

	// Mark all pages as clean
	for _, pg := range p.dirtyBuf {
		p.cache.makeClean(pg)
	}

	p.savepoints = p.savepoints[:0]
	clear(p.writePages)
	p.state.Store(int32(pagerOpen))
	p.wal.endWrite()

	return nFrame, nil
}

// rollback discards all changes in the current write transaction.
func (p *pager) rollback() error {
	if pagerState(p.state.Load()) != pagerWriter {
		return nil
	}

	// Discard all dirty pages from cache
	dirtyPages := p.cache.dirtyPages()
	for _, pg := range dirtyPages {
		p.cache.discard(pg.pgno)
	}

	p.savepoints = p.savepoints[:0]
	clear(p.writePages)
	p.state.Store(int32(pagerOpen))
	p.wal.endWrite()
	return nil
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
		walFrame: p.wal.nFrame,
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
	// Process from oldest to newest so that the oldest copy wins.
	for i := id; i < len(p.savepoints); i++ {
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
				// Page is restored to pre-savepoint state but stays dirty
				// so it can be modified again in the current transaction.
				p.cache.release(pg)
			}
		}
	}

	p.dbSize = sp.dbSize

	// Remove savepoints above the target (but keep the target)
	p.savepoints = p.savepoints[:id]
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

// checkpoint runs a WAL checkpoint.
// Does NOT take pager.mu.Lock — readers can continue during checkpoint.
// The WAL checkpoint internally acquires lockWrite (blocks new writers)
// and uses mxSafeFrame to avoid interfering with active readers.
func (p *pager) checkpoint() error {
	return p.wal.checkpoint(p.file)
}

// tryCheckpoint attempts a checkpoint. Unlike the old version, this no longer
// needs TryLock on pager.mu since checkpoint doesn't block readers.
func (p *pager) tryCheckpoint() error {
	return p.wal.checkpoint(p.file)
}

// writeOverflowChain writes data to a chain of overflow pages and returns
// the first page number in the chain.
func (p *pager) writeOverflowChain(data []byte) (uint32, error) {
	if pagerState(p.state.Load()) != pagerWriter {
		return 0, ErrReadOnly
	}

	usable := overflowPageUsable(int(p.pageSize))
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
func (p *pager) readOverflowChain(firstPgno uint32, buf []byte) error {
	usable := overflowPageUsable(int(p.pageSize))
	pgno := firstPgno
	off := 0

	for pgno != 0 && off < len(buf) {
		pg, err := p.getPage(pgno)
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
	for pgno != 0 {
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
func (p *pager) close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Checkpoint before closing
	if p.wal != nil {
		_ = p.wal.checkpoint(p.file)
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
