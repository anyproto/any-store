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
	"errors"
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

// masterStore is the authoritative page store for InMemory databases.
// It replaces the database file as the "disk" backing, holding checkpointed
// page data that has been flushed from the WAL. Protected by a RWMutex so
// readers can access checkpointed pages concurrently with checkpoint writes.
type masterStore struct {
	mu    sync.RWMutex
	pages map[uint32][]byte // pgno -> page data copy
}

// readPageInto copies the page data for pgno into dst. Returns true if found.
func (ms *masterStore) readPageInto(pgno uint32, dst []byte) bool {
	ms.mu.RLock()
	src, ok := ms.pages[pgno]
	if ok {
		copy(dst, src)
	}
	ms.mu.RUnlock()
	return ok
}

// writePage stores a copy of src as the page data for pgno.
func (ms *masterStore) writePage(pgno uint32, src []byte) {
	ms.mu.Lock()
	if existing, ok := ms.pages[pgno]; ok {
		copy(existing, src)
	} else {
		data := make([]byte, len(src))
		copy(data, src)
		ms.pages[pgno] = data
	}
	ms.mu.Unlock()
}

// pager manages database pages, cache, and WAL interaction.
type pager struct {
	mu          sync.RWMutex
	file        fileHandle
	wal         *wal
	writerCache *pcache

	// writerOpMu serializes pager write operations (commit, rollback) so
	// that DB.Close can safely force-rollback an abandoned transaction
	// without racing with a concurrent commit. Per-connection pcache has
	// no mutex, so this pager-level lock is the serialization point.
	writerOpMu  sync.Mutex
	master      *masterStore // InMemory "disk" — holds checkpointed page data
	header      dbHeader
	path        string
	pageSize    uint32
	usableSize_ int           // pageSize - ReservedSpace; immutable after open, safe for concurrent reads
	dbSize      atomic.Uint32 // database size in pages (atomic: writer increments, readers bounds-check)
	state       atomic.Int32  // pagerState

	// closeInvalidated is set by rollbackForClose when DB.Close force-rolls
	// back an in-flight write transaction. Writer-state guards consult it via
	// writeStateErr so the abandoned writer goroutine observes ErrClosed
	// rather than ErrReadOnly. Never cleared: once Close has invalidated the
	// writer, the database is closing for good.
	closeInvalidated atomic.Bool

	// balanceQuickDispatchCount counts dispatches into splitLeafRightmostAppend.
	// Test-only: production code never reads it (atomic load is ~ns). Used to
	// verify the dispatch guard exercises both branches in the test matrix.
	balanceQuickDispatchCount atomic.Int64

	// balanceNonrootDispatchCount counts dispatches into balanceNonroot (the
	// general 3-sibling redistribution path, ported from SQLite balance_nonroot
	// at btree.c:8248-9030). Test-only, mirrors balanceQuickDispatchCount: lets
	// the fill-factor test prove the new path actually fired and was not
	// silently swallowed by balance_quick.
	balanceNonrootDispatchCount atomic.Int64

	// lastBalanceNOld / lastBalanceNNew record the nOld / nNew of the most
	// recent balanceNonroot call. Test-only (TestBalanceNonroot_SiblingGather):
	// they pin the NB=3 sibling gather and the k ∈ {nOld-1, nOld, nOld+1}
	// invariant. Production code never reads them.
	lastBalanceNOld atomic.Int64
	lastBalanceNNew atomic.Int64

	// deleteRebalanceDispatchCount counts dispatches into the delete-side merge
	// driver (deleteRebalanceLeaf / deleteRebalanceInterior): the underfull page
	// is funnelled through the same balanceNonroot port as the insert path but
	// with inject.active==false — SQLite drives delete-merge through balance() →
	// balance_nonroot() the same way (btree.c:10005-10010). Mirrors
	// balanceNonrootDispatchCount; lets the fill-factor test prove the
	// delete-rebalance path actually fired.
	deleteRebalanceDispatchCount atomic.Int64

	// Savepoint support: snapshots of dirty pages at savepoint boundaries
	savepoints []savepointState

	// savedHeader is a snapshot of the database header at the start of the
	// write transaction, used to restore p.header on rollback (fix 5.2).
	savedHeader dbHeader

	// WAL snapshot for the current write transaction (set atomically).
	// Readers use per-tx walMaxFrame instead.
	walMaxFrame atomic.Uint32

	// savedWalFrame is the WAL frame count at beginWrite() time.
	// Used to roll back spilled frames on transaction rollback.
	// Atomic because rollbackForClose reads it without the writer lock
	// while beginWrite may be writing it concurrently.
	savedWalFrame atomic.Uint32

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

	// cellBufScratch holds reusable temp buffers for collectLeafCells /
	// collectInteriorCells, matching SQLite's scratch space (Pager.pTmpSpace /
	// sqlite3ScratchMalloc, pager.c). Each buffer grows to at most the content
	// size of one page, so retained RAM is ~cellScratchMax*pageSize. See
	// scratchPool for the free-list rationale and bounds.
	cellBufScratch scratchPool[byte]

	// cellSliceScratch is the matching free-list of reusable []cellData backing
	// slices (SQLite apCell[]). clearOnPut releases stale cellData headers so
	// they do not pin old cellBuf backing arrays in the GC. Besides the
	// collect* results, balanceNonroot's pooled cell array (b.cells) and
	// rewriteParentAfterBalance's spliced parent cell list draw from this pool.
	cellSliceScratch scratchPool[cellData]

	// Additional balance scratch pools, same bound and single-writer rationale
	// as cellBufScratch. They eliminate the per-balance make() churn that
	// dominated write-path allocation profiles (balanceNonroot +
	// rewriteParentAfterBalance were ~70-80% of allocated bytes):
	//   szScratch   — cellArray.sz (SQLite szCell[])
	//   keyScratch  — rewriteParentAfterBalance's oldDivs/newDivs key splices
	//   pgnoScratch — rewriteParentAfterBalance's oldChildren/newChildren
	szScratch   scratchPool[int]
	keyScratch  scratchPool[[]byte]
	pgnoScratch scratchPool[uint32]

	// dontWritePages tracks pages that were dirtied but whose content doesn't
	// need to be persisted (e.g., freed leaf pages added to a freelist trunk).
	// Matches SQLite's PGHDR_DONT_WRITE flag (pager.c:6283). We use a map
	// because we cannot modify the page struct in page.go.
	dontWritePages map[uint32]bool

	// backups is the list of in-flight online backups reading from this
	// pager. ~ Pager.pBackup (singly-linked list head in SQLite). Go slice
	// is simpler since we never insert from callback context.
	backups   []*Backup
	backupsMu sync.Mutex

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

	// openDev / openIno capture the (device, inode) identity of the live DB
	// file at open time. openIdentOK reports whether that identity could be
	// obtained from the OS (false on non-unix / VFS-injected files where no
	// real inode is available). databaseIsUnmoved compares the current
	// on-disk identity against these to detect a file renamed/unlinked/
	// relinked out from under the open fd, mirroring SQLite's
	// SQLITE_FCNTL_HAS_MOVED check (databaseIsUnmoved pager.c:4142-4159 →
	// fileHasMoved os_unix.c:1623-1632). Captured once in pager.open; never
	// mutated afterwards, so no synchronization is needed (close holds p.mu).
	openDev     uint64
	openIno     uint64
	openIdentOK bool

	// useSlab is set once by btree.Open from Options.SlabPages.
	// Local bool — no atomic/global reads on hot path.
	useSlab bool

	// mmapSize caps the DB-file mmap region. 0 disables. Populated
	// from Options.MmapSize at open time; the dbMmap fetcher reads
	// this via newDBMmap. See mmap_db.go.
	mmapSize int64

	// dbMmap is the optional mmap-backed DB-file reader. nil or
	// disabled on platforms without mmap support / when mmapSize==0.
	// See mmap_db.go.
	dbMmap *dbMmap

	// writerWalSlot is the writer's WAL reader slot number, stored here
	// so Close can release it when force-rolling back an abandoned WriteTx.
	// Written by BeginWrite before pager.beginWrite() (which stores
	// pagerWriter atomically), so it is visible to Close after observing
	// pagerWriter via state.Load() (Go memory model: sequenced-before →
	// happens-before the atomic store, synchronized-with the atomic load).
	writerWalSlot int

	// BEGIN ENCRYPTION
	// codec, when non-nil, encrypts pages before file/WAL writes and
	// decrypts them after reads. Installed once at Open and immutable
	// thereafter. Safe for concurrent use across the writer and readers.
	codec Codec
	// codecScratch is a reusable page-sized scratch buffer used on the
	// writer side (initNewDB, getPageWriter, wal.writeFrame) to hold
	// ciphertext while the source plaintext stays live in the pcache.
	// Allocated once in installCodec; reused for the pager's lifetime.
	// Safe to share because all three users are writer-serialized
	// (single-writer invariant). Reader paths cannot use this buffer
	// because multiple read transactions may run concurrently.
	// Modeled after SQLCipher's codec_ctx->buffer (crypto_impl.c).
	codecScratch []byte
	// codecAEAD holds the nonce/AAD scratch for writer-side codec calls.
	// Heap-allocated as part of the pager struct; passed by pointer so
	// the crypto/cipher.AEAD interface calls don't force per-call escape.
	codecAEAD aeadScratch
	// END ENCRYPTION
}

// savepointState captures the state needed to rollback to a savepoint.
type savepointState struct {
	id     int
	dbSize uint32
	pages  map[uint32][]byte // pgno -> copy of page data before modification
	// walHdr captures the WAL frame count + cumulative checksums at savepoint
	// time. Only three fields of the hdr are consulted on rollback:
	//   - mxFrame: truncate-back target for spill frames
	//   - aFrameCksum[0], aFrameCksum[1]: running checksum chain restoration
	// The remaining fields (nPage, salt, aCksum, isInit, ...) match the
	// enclosing transaction's hdr and are stored for completeness/debugging
	// only; rollbackToSavepoint does not read them. This widening from the
	// earlier scalar `walFrame/walCksum1/walCksum2` triplet keeps the
	// savepoint's WAL snapshot a peer of the tx-level walHdr — see
	// rollbackToSavepoint in pager.go.
	walHdr WalIndexHdr
	header dbHeader // snapshot of database header at savepoint time (fix 9.3)
}

// newPager creates a new pager for the given database path.
// purgeable controls whether the page cache can evict pages (false for InMemory databases).
// DRIFT: no mxPgno / SQLITE_FULL max-page-count guard in page getters See docs/btree/NOTES.md#drift-8-max-page-count-sqlite-full-enforcement-absent
func newPager(path string, pageSize uint32, cacheSize int, purgeable bool) *pager {
	p := &pager{
		path:     path,
		pageSize: pageSize,
		// useSlab defaults to false; set by btree.Open from Options.SlabPages.
		writerCache: newPcache(int(pageSize), cacheSize, purgeable),
		// cellData and [][]byte scratch entries hold pointers into recycled
		// buffers; clear them on put so stale headers do not pin backing
		// arrays in the GC (see scratchPool.clearOnPut).
		cellSliceScratch: scratchPool[cellData]{clearOnPut: true},
		keyScratch:       scratchPool[[]byte]{clearOnPut: true},
	}
	p.writerCache.xStress = p.pagerStress
	return p
}

// cellScratchMax bounds how many cell buffers / cellData slices the pager
// retains in its scratch free-lists. The balance path's peak concurrent demand
// is NB sibling buffers (nbMaxOut covers the gathered run plus output pages); a
// couple of spares absorb the rewriteParentAfterBalance parent read overlapping
// a cascade up the tree. Retained RAM is bounded by cellScratchMax*pageSize.
const cellScratchMax = nbMaxOut + 3

// takeCellBuf removes and returns a reusable cell buffer from the free-list
// whose capacity is >= minCap. Returns nil if none qualifies (the caller then
// allocates its own, which recycleCellBuf folds back into the pool). The caller
// owns the returned slice until it recycles it.
func (p *pager) takeCellBuf(minCap int) []byte {
	return p.cellBufScratch.take(minCap)
}

// readDBPage fills buf with the contents of pgno from the DB file.
// Tries the mmap fetcher first; on miss (disabled, or offset outside
// the current window), falls back to ReadAt. Matches SQLite's
// getPageMMap → getPageNormal gate (pager.c:5670-5710).
//
// Callers preserve the legacy ReadAt error semantics: short reads
// past EOF return the underlying io.ErrUnexpectedEOF / fs error from
// ReadAt; the mmap path returns nil on success because the slice is
// exactly len(buf) (checked in dbMmap.fetch).
func (p *pager) readDBPage(pgno uint32, buf []byte) error {
	if p.file == nil {
		return fmt.Errorf("btree: db file closed")
	}
	offset := int64(pgno-1) * int64(p.pageSize)

	if p.dbMmap.enabled() {
		if p.dbMmap.readAt(buf, offset) {
			return nil
		}
		// Miss: mapping not yet created or offset beyond current
		// window. Try a remap to cover this page, then retry.
		need := offset + int64(len(buf))
		if err := p.dbMmap.remap(need); err == nil {
			if p.dbMmap.readAt(buf, offset) {
				return nil
			}
		} else if debugTrace {
			// Silent mmap failure loses the fast path in production;
			// log it under debug so operators can diagnose.
			trace("readDBPage: dbMmap.remap(%d) failed: %v", need, err)
		}
		// Still a miss (mapping cap reached or mmap failed). Fall
		// through to ReadAt — matches SQLite's "continue accessing
		// using xRead()" pattern (os_unix.c:5579-5582).
	}

	_, err := p.file.ReadAt(buf, offset)
	return err
}

// recycleCellBuf returns a byte buffer to the pager's free-list for reuse. If
// the pool is full it keeps the larger buffers (replacing the smallest when the
// incoming one is bigger), so the pool converges on page-content-sized buffers
// and never grows past cellScratchMax entries.
func (p *pager) recycleCellBuf(buf []byte) {
	p.cellBufScratch.put(buf)
}

// takeCellSlice removes and returns a reusable cellData slice from the
// free-list whose capacity is >= minCap. Returns nil if none qualifies (the
// caller then allocates its own, which recycleCellSlice folds back into the
// pool).
func (p *pager) takeCellSlice(minCap int) []cellData {
	return p.cellSliceScratch.take(minCap)
}

// recycleCellSlice returns a cellData slice to the pager's free-list for reuse.
// Clears all entries first (clearOnPut) to release references to cellBuf data,
// preventing stale slice headers from pinning old buffers in the GC. When the
// pool is full it keeps the larger slices, bounded at cellScratchMax entries.
func (p *pager) recycleCellSlice(s []cellData) {
	p.cellSliceScratch.put(s)
}

// initCellScratch (re-)seeds the cell-scratch free-list. It pre-allocates
// nbSiblings page-content-sized buffers so the very first balance does not have
// to make() its gathered-sibling buffers; the pool then self-tunes within the
// cellScratchMax bound. Called from the pager init / re-open paths where the
// usable page size is known. Replaces the former single-buffer pre-allocation.
func (p *pager) initCellScratch() {
	p.cellBufScratch.reset()
	p.cellSliceScratch.reset()
	p.szScratch.reset()
	p.keyScratch.reset()
	p.pgnoScratch.reset()
	for i := 0; i < nbSiblings; i++ {
		p.cellBufScratch.put(make([]byte, 0, p.usableSize_))
	}
}

// open opens the database file, initializes the WAL, and recovers if needed.
func (p *pager) open() (err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.inMemory {
		// In-memory database: no file on disk.
		// Create masterStore to hold checkpointed page data (replaces the DB file).
		p.master = &masterStore{pages: make(map[uint32][]byte)}
		return p.initNewDB()
	}

	f, err := osOpenFile(p.path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	p.file = f

	// Release the file/mmap/WAL on any error after this point. Open() returns
	// the error to its caller and discards this pager, so without explicit
	// cleanup the opened fds leak: a leaked *os.File is closed by the GC
	// finalizer at an arbitrary later time, and on Linux that close can land on
	// an fd the runtime has since recycled to an unrelated file — corrupting it
	// (observed as spurious "bad file descriptor" errors in later operations).
	defer func() {
		if err != nil {
			if p.dbMmap != nil {
				_ = p.dbMmap.unmap()
				p.dbMmap = nil
			}
			if p.wal != nil {
				_ = p.wal.close(false)
				p.wal = nil
			}
			if p.file != nil {
				_ = p.file.Close()
				p.file = nil
			}
		}
	}()

	// Initialize the optional mmap reader. No syscalls here — the
	// underlying syscall.Mmap is deferred until the first fetch() to
	// match SQLite's lazy initialization in unixFetch (os_unix.c:5727).
	p.dbMmap = newDBMmap(f, p.mmapSize)

	// Acquire a DB-file flock, held for the lifetime of this pager and released
	// when p.file is closed.
	//
	// Multi-process mmap mode takes a SHARED lock: concurrent processes are
	// allowed and coordinated through the mmap'd -shm; any closer upgrading to
	// exclusive BUSY-retries while we hold shared, serializing shm unlink against
	// new-opener races (SQLite's sqlite3PagerSharedLock + wal.c:2508 invariant).
	//
	// In-process mode uses heap-backed, process-local SHM and is therefore
	// single-process only, so it takes an EXCLUSIVE lock instead — a second
	// opener (another process, or another handle in this process) is rejected
	// with a clear error rather than silently corrupting via a separate heap SHM.
	// (In-memory already returned above, so here p.inProcess implies file-backed.)
	if !p.inProcess {
		for attempt := 0; attempt < 100; attempt++ {
			lockErr := acquireSharedDBLock(f)
			if lockErr == nil {
				break
			}
			if !errors.Is(lockErr, ErrBusy) {
				_ = f.Close()
				p.file = nil
				return fmt.Errorf("btree: acquire DB-file shared lock: %w", lockErr)
			}
			if attempt == 99 {
				_ = f.Close()
				p.file = nil
				return fmt.Errorf("btree: DB-file lock still busy after retries (peer mid-close?)")
			}
			time.Sleep(10 * time.Millisecond)
		}
	} else {
		if lockErr := acquireExclusiveDBLock(f); lockErr != nil {
			if errors.Is(lockErr, ErrBusy) {
				// Derive the registry key BEFORE closing f (inode identity needs
				// the open fd). A same-process second open is already tracked in
				// openDBs (the first handle registered after its own open) -> use
				// the established ErrDatabaseOpen; a lock held by a DIFFERENT
				// process is reported as ErrInProcessLocked.
				key := registryKeyForOpenFile(f, p.path)
				_ = f.Close()
				p.file = nil
				if _, here := openDBs.Load(key); here {
					return ErrDatabaseOpen
				}
				return ErrInProcessLocked
			}
			_ = f.Close()
			p.file = nil
			return fmt.Errorf("btree: acquire DB-file exclusive lock: %w", lockErr)
		}
	}

	info, err := f.Stat()
	if err != nil {
		return err
	}

	if info.Size() == 0 {
		// New database - initialize. initNewDB writes page 1 to the file
		// (and fdatasyncs), so re-stat once afterwards to capture the
		// identity of the now-materialized inode. Cheap: one extra Stat on
		// the create path only.
		if err := p.initNewDB(); err != nil {
			return err
		}
		if info2, err := f.Stat(); err == nil {
			p.openDev, p.openIno, p.openIdentOK = statIdentity(info2)
		}
		return nil
	}

	// Existing database - reuse the already-fetched info (no extra syscall)
	// to capture the file identity for the close-time databaseIsUnmoved guard.
	p.openDev, p.openIno, p.openIdentOK = statIdentity(info)

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

	// VersionValidFor integrity sentinel: when non-zero, must equal
	// FileChangeCount — a divergence means a writer crashed mid-update
	// and the header on disk is half-new / half-old. Treat
	// VersionValidFor==0 as "not initialized by this writer" (legacy
	// DBs, pre-P3.3 test fixtures) and skip the check rather than
	// reject. Matches SQLite's defensive-read-only interpretation.
	if p.header.VersionValidFor != 0 && p.header.VersionValidFor != p.header.FileChangeCount {
		return fmt.Errorf("%w: VersionValidFor=%d FileChangeCount=%d", ErrCorrupt, p.header.VersionValidFor, p.header.FileChangeCount)
	}

	// Validate the on-disk header page size, mirroring C lockBtree
	// (btree.c:3377-3385): it must be a power of two within
	// [MinPageSize, MaxPageSize], else SQLITE_NOTADB. The on-disk size is
	// adopted as authoritative below (p.pageSize = p.header.PageSize), so
	// reject a corrupt/non-DB value here instead of trusting it into the
	// engine.
	if p.header.PageSize < MinPageSize || p.header.PageSize > MaxPageSize ||
		(p.header.PageSize&(p.header.PageSize-1)) != 0 {
		return fmt.Errorf("%w: invalid on-disk page size %d", ErrCorrupt, p.header.PageSize)
	}

	// Reconcile the on-disk page size against the process-global page buffer
	// pool. Open already keyed the pool to opts.PageSize (db.go), so this
	// returns ErrPageBufferPoolSizeMismatch when the on-disk size differs;
	// propagate it rather than draw mis-sized buffers from the shared pool.
	// (C allocates per-pager pTmpSpace/PCache at the file's actual page size,
	// pager.c:5016-5028, and so never mixes sizes the way this pool would.)
	if err := initPageBufferPool(p.header.PageSize); err != nil {
		return err
	}

	p.pageSize = p.header.PageSize
	p.usableSize_ = int(p.pageSize) - int(p.header.ReservedSpace)

	// Mirror C lockBtree (btree.c:3422-3424): the usable size (pageSize minus
	// the on-disk reserved-space byte) is not allowed to be less than 480 — at
	// page size 512 the reserved space cannot exceed 32. A larger ReservedSpace
	// byte would otherwise feed degenerate geometry into maxLocalPayload /
	// minLocalPayload / overflowPageUsable and the cell-content math. This
	// floors only the on-disk-open path; the codec-install and new-DB paths
	// derive usableSize from process-chosen Options, not untrusted bytes.
	if p.usableSize_ < 480 {
		return fmt.Errorf("%w: usable size %d < 480", ErrCorrupt, p.usableSize_)
	}

	// Reconcile the header page count against the physical file size,
	// mirroring C lockBtree (btree.c:3304-3308). C derives nPageFile via
	// sqlite3PagerPagecount and falls back to it when the header count is
	// zero or the change-counter sentinel (FileChangeCount bytes 24-27 vs
	// VersionValidFor bytes 92-95) mismatches. The VersionValidFor!=0
	// mismatch is already rejected above; this fallback arm covers the
	// nPage==0 / legacy-sentinel (VersionValidFor==0) case. When the header
	// count is untrusted, clamp dbSize to the file page count (C sets
	// nPage=nPageFile); a trusted-but-too-large header is rejected below
	// after the WAL is opened, not here.
	fileNPage := uint32((info.Size() + int64(p.pageSize) - 1) / int64(p.pageSize))
	headerNPageTrusted := p.header.DatabaseSize != 0 && p.header.VersionValidFor == p.header.FileChangeCount
	if headerNPageTrusted {
		p.dbSize.Store(p.header.DatabaseSize)
	} else {
		p.dbSize.Store(fileNPage)
	}
	p.initCellScratch()
	p.writerCache = newPcache(int(p.pageSize), p.writerCache.maxPages, p.writerCache.purgeable)
	p.writerCache.useSlab = p.useSlab
	p.writerCache.xStress = p.pagerStress

	// Open WAL
	p.wal = newWal(p.path+"-wal", p.pageSize)
	p.wal.inProcess = p.inProcess
	p.wal.noCommitSync = p.noCommitSync
	p.wal.inMemory = p.inMemory
	p.wal.busyHandler = DefaultBusyTimeout(5 * time.Second)
	// BEGIN ENCRYPTION
	// Propagate codec to the WAL so frame writes/reads use the same
	// cipher as the DB file.
	p.wal.codec = p.codec
	// END ENCRYPTION
	if err := p.wal.open(); err != nil {
		return err
	}

	// After WAL recovery, refresh the full header from WAL's page 1.
	// The DB file header may be stale (e.g. freelist pointers) if a crash
	// occurred before checkpoint. Without this, commit() would serialize
	// stale p.header fields back into page 1, corrupting the freelist.
	if p.wal.index.maxFrame.Load() > 0 {
		frame, err := p.wal.index.get(1, p.wal.index.maxFrame.Load(), p.wal.index.liveMinFrame())
		if err != nil {
			return err
		}
		if frame > 0 {
			// A failure here must not be swallowed: skipping the refresh would
			// leave stale freelist pointers in p.header, and the next commit
			// would serialize them back into page 1 (double-allocated pages).
			// Matches SQLite, where lockBtree reads page 1 through the
			// WAL-aware pager (btree.c lockBtree → sqlite3PagerGet(1)) and any
			// read error fails the open. Unlike the steady-state read paths,
			// open holds recovery exclusively — there is no benign concurrent
			// checkpoint race to fall through for.
			walBuf := make([]byte, p.pageSize)
			if err := p.wal.readFrame(frame, walBuf, nil, nil); err != nil {
				return err
			}
			if err := p.header.deserialize(walBuf[:dbHeaderSize]); err != nil {
				return err
			}
		}
		// Use max of DB file's DatabaseSize and WAL's maxPage.
		// After checkpoint + WAL reset, the DB file may have a larger
		// DatabaseSize than what survives in the WAL after recovery.
		if p.wal.index.maxPage.Load() > p.dbSize.Load() {
			p.dbSize.Store(p.wal.index.maxPage.Load())
		}
	}

	// Reject a trusted header whose DatabaseSize exceeds the available pages,
	// mirroring C lockBtree (btree.c:3411-3417): outside writable-schema mode
	// (which any-store never enables) nPage>nPageFile is SQLITE_CORRUPT, not a
	// clamp. nPageFile here is the file page count, but C's pagerPagecount
	// (pager.c:3292) prefers the WAL-committed size when larger, so account
	// for pages committed to the WAL but not yet checkpointed into the DB file
	// to avoid false-positively rejecting WAL-extended databases.
	if headerNPageTrusted {
		walNPage := p.wal.index.maxPage.Load()
		effectiveFileN := fileNPage
		if walNPage > effectiveFileN {
			effectiveFileN = walNPage
		}
		if p.header.DatabaseSize > effectiveFileN {
			return fmt.Errorf("%w: header DatabaseSize=%d exceeds file pages=%d wal=%d", ErrCorrupt, p.header.DatabaseSize, fileNPage, walNPage)
		}
	}

	p.state.Store(int32(pagerOpen))
	return nil
}

// BEGIN ENCRYPTION

// installCodec attaches a page codec to the pager. Must be called before
// any I/O. Sets the on-disk ReservedSpace header byte and the in-memory
// usableSize_ so the btree cell code respects the codec's per-page
// overhead. Calling with nil is a no-op (unencrypted mode).
func (p *pager) installCodec(c Codec) {
	if c == nil {
		return
	}
	p.codec = c
	p.header.ReservedSpace = uint8(c.Overhead())
	p.usableSize_ = int(p.pageSize) - int(p.header.ReservedSpace)
	// Pre-allocate one writer-side scratch buffer. Reused by all writer
	// paths (single-writer invariant); avoids per-call allocPageBuffer /
	// freePageBuffer churn that turned sync.Pool.Put's eface box into a
	// hot allocation site under encryption.
	p.codecScratch = make([]byte, p.pageSize)
}

// encryptPage transforms plaintext page bytes for writing to disk or WAL.
// When no codec is installed, returns src unchanged (pass-through). When
// a codec is installed, encrypts into scratch and returns the ciphertext
// slice. scratch must have len >= len(src) and must not alias src.
//
// Page 1 is special: the first dbHeaderSize (100) bytes are the database
// header, which includes the KDF salt and must remain plaintext so Open
// can read it without the key. Pages > 1 are encrypted in full.
//
// This is the any-store equivalent of SQLCipher's CODEC2 macro
// (pager.c:412), unified into one function.
func (p *pager) encryptPage(scratch, src []byte, pgno uint32) ([]byte, error) {
	return encryptPageWithCodec(p.codec, scratch, src, pgno, &p.codecAEAD)
}

// decryptPage is the inverse. Returns src unchanged when no codec is
// installed; otherwise decrypts into scratch and returns the plaintext
// slice. On integrity failure returns an error wrapping
// ErrPageIntegrity (the inner detail distinguishes cksum mismatch vs
// AEAD auth fail). decryptPage uses the pager's writer-side AEAD
// scratch. Reader-path callers must NOT go through this; they should
// call decryptPageWithCodec directly with their own *aeadScratch.
func (p *pager) decryptPage(scratch, src []byte, pgno uint32) ([]byte, error) {
	return decryptPageWithCodec(p.codec, scratch, src, pgno, &p.codecAEAD)
}

// END ENCRYPTION

// initNewDB initializes a brand new database file.
// DRIFT: new-DB page 1 written+fdatasynced direct to file, bypassing WAL (C defers to WAL) See docs/btree/NOTES.md#drift-63-new-db-page-1-written-directly-to-file-bypassing-wal
// DRIFT: SchemaFormat=5 is a Go-specific value (unified key||value overflow), unrelated to SQLite's See docs/btree/NOTES.md#old-drift-schema-format-5-custom-not-sqlite-fmt4
func (p *pager) initNewDB() error {
	if p.pageSize == 0 {
		p.pageSize = DefaultPageSize
	}

	// BEGIN ENCRYPTION
	// Preserve salt and ReservedSpace that may have been set by installCodec
	// before initNewDB was called.
	savedSalt := p.header.Salt
	savedReserve := p.header.ReservedSpace
	// END ENCRYPTION

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
	// BEGIN ENCRYPTION
	if p.codec != nil {
		p.header.ReservedSpace = savedReserve
		p.header.Salt = savedSalt
	}
	// END ENCRYPTION
	p.usableSize_ = int(p.pageSize) - int(p.header.ReservedSpace)
	p.initCellScratch()

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
		// BEGIN ENCRYPTION
		// When a codec is installed, encrypt page 1 before writing to disk.
		// The plaintext 100-byte dbHeader (carrying the salt) is preserved
		// by encryptPage; only bytes >= dbHeaderSize are encrypted.
		writeBuf := buf
		if p.codec != nil {
			ct, err := p.encryptPage(p.codecScratch, buf, 1)
			if err != nil {
				return err
			}
			writeBuf = ct
		}
		if _, err := p.file.WriteAt(writeBuf, 0); err != nil {
			return err
		}
		// END ENCRYPTION
		if err := fdatasync(p.file); err != nil {
			return err
		}
	}

	p.dbSize.Store(1)
	p.writerCache = newPcache(int(p.pageSize), p.writerCache.maxPages, p.writerCache.purgeable)
	p.writerCache.useSlab = p.useSlab
	p.writerCache.xStress = p.pagerStress

	// For inMemory mode, pre-populate page 1 in masterStore so reads find it.
	// Note: masterStore stores plaintext (encryption is a file-at-rest concern
	// per encryption.md §7; InMemory has no "at rest").
	if p.inMemory && p.master != nil {
		p.master.writePage(1, buf)
	}

	// Open WAL
	p.wal = newWal(p.path+"-wal", p.pageSize)
	p.wal.inProcess = p.inProcess
	p.wal.noCommitSync = p.noCommitSync
	p.wal.inMemory = p.inMemory
	p.wal.busyHandler = DefaultBusyTimeout(5 * time.Second)
	// BEGIN ENCRYPTION
	// Propagate codec to the WAL so frame writes/reads use the same
	// cipher as the DB file.
	p.wal.codec = p.codec
	// END ENCRYPTION
	if err := p.wal.open(); err != nil {
		return err
	}

	p.state.Store(int32(pagerOpen))
	return nil
}

// beginRead starts a read transaction, taking a WAL snapshot.
// Returns the WAL max frame for snapshot isolation and the reader slot number.
func (p *pager) beginRead() (maxFrame uint32, slot int, err error) {
	_, maxFrame, _, slot, err = p.beginReadHdr()
	return maxFrame, slot, err
}

// beginReadHdr is beginRead plus the exact WAL header snapshot used to claim
// the reader slot. Callers that may escalate to a write transaction should use
// this so BUSY_SNAPSHOT compares against the same snapshot the read lock used.
func (p *pager) beginReadHdr() (hdr WalIndexHdr, maxFrame, minFrame uint32, slot int, err error) {
	p.mu.RLock()
	if pagerState(p.state.Load()) == pagerError {
		p.mu.RUnlock()
		return WalIndexHdr{}, 0, 0, 0, ErrCorrupt
	}
	hdr, maxFrame, minFrame, slot, err = p.wal.beginReadHdr()
	if err != nil {
		p.mu.RUnlock()
		return WalIndexHdr{}, 0, 0, 0, err
	}
	// Update pager's walMaxFrame monotonically: never decrease, since a reader
	// with an older snapshot must not overwrite a newer value set by the writer.
	for {
		old := p.walMaxFrame.Load()
		if maxFrame <= old {
			break
		}
		if p.walMaxFrame.CompareAndSwap(old, maxFrame) {
			break
		}
	}
	return hdr, maxFrame, minFrame, slot, nil
}

// endRead ends a read transaction for the given reader slot.
func (p *pager) endRead(slot int) {
	p.wal.endRead(slot)
	p.mu.RUnlock()
}

// beginWrite starts a write transaction (must hold a read transaction
// first). readSnap is the caller's WAL hdr from BeginRead (per-tx
// walHdr); BUSY_SNAPSHOT compares it against live SHM inside
// wal.beginWriteWithSnapshot. The snapshot is required — there is
// deliberately no zero-arg form, because a WalIndexHdr{} snapshot
// silently disables the BUSY_SNAPSHOT check and is a multi-process
// correctness hazard (docs/btree/NOTES.md P0.2 drift, resolved).
// DRIFT: beginWrite refreshes page-1 header/dbSize on stateChanged (C does it on read path) See docs/btree/NOTES.md#drift-76-beginwrite-re-reads-page-1-header-on-state-change
func (p *pager) beginWrite(readSnap WalIndexHdr) error {
	stateChanged, err := p.wal.beginWriteWithSnapshot(readSnap)
	if err != nil {
		return err
	}
	// If another process changed WAL state (committed or checkpointed),
	// our writerCache has stale pages and must be cleared. We must also
	// refresh p.header and p.dbSize from the new page 1 — without this,
	// commit() would serialize our stale header back into page 1
	// (corrupting the freelist / DatabaseSize) and allocatePage would
	// hand out pgno slots the other process already wrote to.
	//
	// Mirrors SQLite's sqlite3PagerSharedLock pattern: on state change,
	// reset cache and re-read page 1 (pager.c:5390-5449). Uses the same
	// lookup pattern as readHeaderCounters so cross-process SHM hash
	// tables are consulted in multi-process mode.
	if stateChanged {
		p.writerCache.clear()
		// If the page-1 re-read fails at both the WAL frame and the DB file
		// (double I/O failure), abort beginWrite rather than continuing with a
		// stale header/dbSize that commit() would later serialize over the
		// peer's page 1. Mirrors SQLite propagating the page-1 read error out
		// of the begin/shared-lock path. We release the exclusive WAL write
		// lock that beginWriteWithSnapshot acquired (the pager never reached
		// pagerWriter, so the caller's error path only ends the read tx via
		// endRead and would otherwise leak the write lock); the caller's read
		// transaction itself is left intact.
		if err := p.refreshHeaderFromPage1(); err != nil {
			p.wal.endWrite()
			return err
		}
	}
	p.state.Store(int32(pagerWriter))
	// Save a snapshot of the database header so rollback can restore it (fix 5.2).
	p.savedHeader = p.header
	// Save WAL frame count and checksums so rollback can clean up spilled frames
	// and restore the checksum chain for the next transaction.
	p.savedWalFrame.Store(p.wal.nFrame.Load())
	p.savedWalCksum1 = p.wal.cksum1
	p.savedWalCksum2 = p.wal.cksum2
	return nil
}

// getPage returns the page with the given page number, reading from WAL or disk as needed.
// Uses the current WAL nFrame (not walMaxFrame) so that spilled pages written
// to WAL beyond walMaxFrame during this write transaction are visible.
// DRIFT: no mxPgno / SQLITE_FULL max-page-count guard in page getters See docs/btree/NOTES.md#drift-8-max-page-count-sqlite-full-enforcement-absent
func (p *pager) getPage(pgno uint32) (*page, error) {
	return p.getPageWriter(pgno, p.wal.nFrame.Load())
}

// getPageWriter returns a page using the writer's cache, reading from
// WAL or disk on cache miss. Used by the writer goroutine only.
// DRIFT: no mxPgno / SQLITE_FULL max-page-count guard in page getters See docs/btree/NOTES.md#drift-8-max-page-count-sqlite-full-enforcement-absent
func (p *pager) getPageWriter(pgno, walMaxFrame uint32) (*page, error) {
	if pgno == 0 {
		return nil, ErrInvalidPage
	}

	// Check writer cache first. SQLite returns cached pages directly without
	// any WAL lookup (pager.c:5568-5573 getPageNormal). The writer cache is
	// only accessed by the writer goroutine, so cached pages are always valid
	// for the writer's snapshot. Cache is cleared on checkpoint/WAL restart.
	if pg := p.writerCache.fetch(pgno); pg != nil {
		return pg, nil
	}

	// Cache miss: create a new cached page (hard create — writer always succeeds).
	pg := p.writerCache.create(pgno, 2)

	// Decide read-vs-zero up front, matching C getPageNormal
	// (pager.c:5590,5615): a page whose number exceeds the logical database
	// size is never read from WAL or disk — it is memset(0)ed. p.dbSize is the
	// writer's authoritative same-process allocation counter (refreshed via
	// beginWrite -> refreshHeaderFromPage1 on cross-process state change), so a
	// pgno above it cannot be live; reading WAL/disk could resurrect stale
	// trailing bytes from a since-truncated region (truncateTo defers the
	// physical ftruncate). Same idiom as getPageNoContent.
	if pgno > p.dbSize.Load() {
		clear(pg.data)
		pg.header = pageHeader{}
		return pg, nil
	}

	// Try to read from WAL first
	if walMaxFrame > 0 {
		// Writer path: the write lock excludes a concurrent restart, so the
		// live checkpoint frontier is a valid minFrame.
		frame, err := p.wal.index.get(pgno, walMaxFrame, p.wal.index.liveMinFrame())
		if err != nil {
			// ErrCorrupt from a full/over-probed SHM hash chain: fail the
			// read (matching C's walFindFrame SQLITE_CORRUPT_BKPT at
			// wal.c:3593) instead of falling through to the DB file, which
			// would silently serve a pre-commit page and mask the corruption.
			p.writerCache.discard(pg.pgno)
			return nil, err
		}
		if frame > 0 {
			// The resolved frame holds the page's only current version; the
			// DB-file copy is older. A readFrame failure here is always a
			// genuine I/O or codec-integrity error, never a benign WAL-reset
			// race: this path holds the write lock, which excludes a
			// concurrent restart (checkpointWithMode takes lockWrite
			// exclusive for non-PASSIVE modes; PASSIVE never resets,
			// checkpointPost). Propagate instead of falling through to a
			// stale disk read, matching C readDbPage: with iFrame!=0 the
			// DB-file read sits in the unreachable else branch and a WAL
			// read error becomes the page-get error (pager.c:3035-3046).
			// Reader paths (readTempPage, getPageReader) share this
			// contract via the reader-slot lock — see readTempPage.
			if err := p.wal.readFrame(frame, pg.data, p.codecScratch, &p.codecAEAD); err != nil {
				p.writerCache.discard(pg.pgno)
				return nil, fmt.Errorf("btree: failed to read page %d (WAL frame %d): %w", pgno, frame, err)
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

	// Read from database file. Reached only when pgno <= p.dbSize (the upfront
	// guard above returned for any out-of-bounds page), so a read failure here
	// is for an in-bounds page and is always a hard error — matching C, where
	// readDbPage runs only in the in-bounds else branch (pager.c:5617-5623).
	if p.file != nil {
		if err := p.readDBPage(pgno, pg.data); err != nil {
			p.writerCache.discard(pg.pgno)
			return nil, fmt.Errorf("btree: failed to read page %d: %w", pgno, err)
		} else {
			// BEGIN ENCRYPTION
			if p.codec != nil {
				plain, derr := p.decryptPage(p.codecScratch, pg.data, pgno)
				if derr != nil {
					p.writerCache.discard(pg.pgno)
					return nil, fmt.Errorf("btree: page %d: %w", pgno, derr)
				}
				copy(pg.data, plain)
			}
			// END ENCRYPTION
		}
	} else if p.master != nil {
		// InMemory: read from masterStore (replaces the DB file)
		if !p.master.readPageInto(pgno, pg.data) {
			clear(pg.data)
		}
	} else {
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

// readTempPage reads a page into a standalone temporary page object that is
// NOT stored in any cache. Used by getPageReader when no reader cache is
// available or admission control refused a cache slot.
//
// codecBuf / codecAEAD are caller-owned scratch buffers for the codec's
// decrypt call; when non-nil they avoid a per-page allocPageBuffer round-
// trip (which boxes the []byte into an eface on sync.Pool.Put) and a per-
// call aad/nonce escape. Callers that have a reader pcache should pass
// cache.codecScratch / &cache.codecAEAD. Pass nil/nil for the rare nil-
// cache path (admission-control refusal, integrity checks); the codec
// falls back to allocPageBuffer in that case.
// DRIFT: short DB-file read on in-bounds page is hard error in Go, zero-pad OK in C See docs/btree/NOTES.md#drift-7-short-db-file-read-treated-as-hard-error
func (p *pager) readTempPage(pgno, walMaxFrame, walMinFrame, dbSizeBound uint32, codecBuf []byte, codecAEAD *aeadScratch) (*page, error) {
	pg := p.acquireTempPage()
	pg.pgno = pgno
	pg.pinCount = 1
	pg.uncached = true

	// Decide read-vs-zero up front, matching C getPageNormal
	// (pager.c:5590,5615). dbSizeBound is the reader's per-snapshot page count
	// (effectiveReaderDbSize → hdr.nPage in multi-process mode) or p.dbSize on
	// the uncached/writer path; a page beyond it is not part of this snapshot,
	// so it is zeroed rather than read from WAL or disk. WAL-then-disk still
	// runs for pgno <= dbSizeBound, so a peer-committed page in
	// (p.dbSize, snapshotBound] living only in the WAL is still served.
	if pgno > dbSizeBound {
		clear(pg.data)
		pg.header = pageHeader{}
		return pg, nil
	}

	// Try to read from WAL first.
	if walMaxFrame > 0 {
		frame, err := p.wal.index.get(pgno, walMaxFrame, walMinFrame)
		if err != nil {
			// ErrCorrupt from a full/over-probed SHM hash chain: fail the read
			// (C walFindFrame SQLITE_CORRUPT_BKPT, wal.c:3593) rather than
			// serving a stale pre-commit page from disk.
			p.recycleTempPage(pg)
			return nil, err
		}
		if frame > 0 {
			// Propagate readFrame failures (C readDbPage, pager.c:3035-3046)
			// — never fall through to the older DB-file copy. Callers reach
			// readTempPage only via getPageReader, whose read tx holds a
			// reader-slot shared lock: a WAL reset needs slots 1-4 exclusive
			// (tryResetWALWithBusy), and slot-0 snapshots carry
			// minFrame = mxFrame+1 so the lookup above never resolves a
			// frame for them (C readLock==0 short-circuit). The failure is
			// therefore always genuine. See getPageWriter for the writer-
			// path variant of this argument.
			if err := p.wal.readFrame(frame, pg.data, codecBuf, codecAEAD); err != nil {
				p.recycleTempPage(pg)
				return nil, fmt.Errorf("btree: failed to read page %d (WAL frame %d): %w", pgno, frame, err)
			}
			off := 0
			if pgno == 1 {
				off = dbHeaderSize
			}
			pg.header.deserialize(pg.data[off:])
			return pg, nil
		}
	}

	// Read from database file. Reached only when pgno <= dbSizeBound (the
	// upfront guard returned for any out-of-snapshot page), so a read failure
	// here is for an in-bounds page and is always a hard error — matching C.
	if p.file != nil {
		if err := p.readDBPage(pgno, pg.data); err != nil {
			p.recycleTempPage(pg)
			return nil, fmt.Errorf("btree: failed to read page %d: %w", pgno, err)
		} else {
			// BEGIN ENCRYPTION
			if p.codec != nil {
				scratch := codecBuf
				ownScratch := false
				if scratch == nil {
					scratch = allocPageBuffer(int(p.pageSize), false)
					ownScratch = true
				}
				aeadS := codecAEAD
				if aeadS == nil {
					aeadS = new(aeadScratch)
				}
				plain, derr := decryptPageWithCodec(p.codec, scratch, pg.data, pgno, aeadS)
				if derr != nil {
					if ownScratch {
						freePageBuffer(scratch, false)
					}
					p.recycleTempPage(pg)
					return nil, fmt.Errorf("btree: page %d: %w", pgno, derr)
				}
				copy(pg.data, plain)
				if ownScratch {
					freePageBuffer(scratch, false)
				}
			}
			// END ENCRYPTION
		}
	} else if p.master != nil {
		if !p.master.readPageInto(pgno, pg.data) {
			clear(pg.data)
		}
	} else {
		clear(pg.data)
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

// getPageReader returns a page using the reader's private cache for snapshot
// isolation. On cache hit the page is returned directly (all pages in a reader
// cache were populated during this transaction, so they're valid for this
// snapshot). On cache miss the page is read from WAL/disk/masterStore and
// stored in the reader cache for subsequent lookups within the same transaction.
// DRIFT: no mxPgno / SQLITE_FULL max-page-count guard in page getters See docs/btree/NOTES.md#drift-8-max-page-count-sqlite-full-enforcement-absent
func (p *pager) getPageReader(pgno, walMaxFrame uint32, cache *pcache) (*page, error) {
	if pgno == 0 {
		return nil, ErrInvalidPage
	}
	// If no reader cache provided, read an uncached temporary page.
	// Never fall back to writerCache — that would race with the writer goroutine.
	if cache == nil {
		return p.readTempPage(pgno, walMaxFrame, p.wal.index.liveMinFrame(), p.readerDbSizeBound(nil), nil, nil)
	}

	// Check reader cache.
	if pg := cache.fetch(pgno); pg != nil {
		return pg, nil
	}

	// Cache miss: create a page in the reader cache (soft create — may be
	// refused under memory pressure). Reader caches have no xStress callback.
	// Matches SQLite pcache.c:486 — readers use createFlag=1.
	pg := cache.create(pgno, 1)
	if pg == nil {
		// Admission control refused the allocation. Fall back to an uncached
		// temporary page read so the query still succeeds. We still have the
		// cache struct — hand its scratch buffers down so repeated fallbacks
		// don't allocate per call.
		// BEGIN ENCRYPTION
		if p.codec != nil && cache.codecScratch == nil {
			cache.codecScratch = make([]byte, p.pageSize)
		}
		// END ENCRYPTION
		return p.readTempPage(pgno, walMaxFrame, p.readerMinFrame(cache), p.readerDbSizeBound(cache), cache.codecScratch, &cache.codecAEAD)
	}

	// Decide read-vs-zero up front, matching C getPageNormal
	// (pager.c:5590,5615). The reader bounds on the per-snapshot page count
	// (readerDbSizeBound → pcache.dbSize → effectiveReaderDbSize, hdr.nPage in
	// multi-process mode), NOT p.dbSize: a peer can commit frames for pages with
	// pgno > p.dbSize that are still within this reader's snapshot, and they
	// live in the WAL, so WAL-then-disk must still run for pgno <= snapshotBound.
	// A page truly beyond the snapshot is zeroed instead of reading stale bytes.
	if pgno > p.readerDbSizeBound(cache) {
		clear(pg.data)
		pg.header = pageHeader{}
		return pg, nil
	}

	// Try to read from WAL first.
	if walMaxFrame > 0 {
		frame, err := p.wal.index.get(pgno, walMaxFrame, p.readerMinFrame(cache))
		if err != nil {
			// ErrCorrupt from a full/over-probed SHM hash chain: fail the read
			// (C walFindFrame SQLITE_CORRUPT_BKPT, wal.c:3593) rather than
			// serving a stale pre-commit page from disk/masterStore.
			cache.discard(pg.pgno)
			return nil, err
		}
		if frame > 0 {
			// BEGIN ENCRYPTION
			if p.codec != nil && cache.codecScratch == nil {
				cache.codecScratch = make([]byte, p.pageSize)
			}
			// END ENCRYPTION
			// Propagate readFrame failures — the reader-slot lock held by
			// this read tx makes them always genuine; see readTempPage for
			// the full argument (C readDbPage, pager.c:3035-3046).
			if err := p.wal.readFrame(frame, pg.data, cache.codecScratch, &cache.codecAEAD); err != nil {
				cache.discard(pg.pgno)
				return nil, fmt.Errorf("btree: failed to read page %d (WAL frame %d): %w", pgno, frame, err)
			}
			off := 0
			if pgno == 1 {
				off = dbHeaderSize
			}
			pg.header.deserialize(pg.data[off:])
			return pg, nil
		}
	}

	// Read from database file. Reached only when pgno <= snapshotBound (the
	// upfront guard returned for any out-of-snapshot page), so a read failure
	// here is for an in-bounds page and is always a hard error — matching C.
	if p.file != nil {
		if err := p.readDBPage(pgno, pg.data); err != nil {
			cache.discard(pg.pgno)
			return nil, fmt.Errorf("btree: failed to read page %d: %w", pgno, err)
		} else {
			// BEGIN ENCRYPTION
			if p.codec != nil {
				if cache.codecScratch == nil {
					cache.codecScratch = make([]byte, p.pageSize)
				}
				plain, derr := decryptPageWithCodec(p.codec, cache.codecScratch, pg.data, pgno, &cache.codecAEAD)
				if derr != nil {
					cache.discard(pg.pgno)
					return nil, fmt.Errorf("btree: page %d: %w", pgno, derr)
				}
				copy(pg.data, plain)
			}
			// END ENCRYPTION
		}
	} else if p.master != nil {
		// InMemory: read from masterStore.
		if !p.master.readPageInto(pgno, pg.data) {
			clear(pg.data)
		}
	} else {
		clear(pg.data)
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

// getPageNoContent returns a page without reading from disk/WAL (fix 5.4).
// If the page is already in cache, it's returned as-is (matching SQLite's
// behavior where PAGER_GET_NOCONTENT still returns cached pages). If not in
// cache, a new blank page is created. This is used when allocating pages from
// the freelist or growing the database, where the old content is irrelevant.
// Modeled after SQLite's PAGER_GET_NOCONTENT flag in pager.c:5507.
// DRIFT: no mxPgno / SQLITE_FULL max-page-count guard in page getters See docs/btree/NOTES.md#drift-8-max-page-count-sqlite-full-enforcement-absent
// DRIFT: no btreeGetUnusedPage refcount>1 in-use ErrCorrupt check See docs/btree/NOTES.md#drift-10-missing-refcount-greater-than-one-in-use-page-corruption-det
func (p *pager) getPageNoContent(pgno uint32) (*page, error) {
	if pgno == 0 {
		return nil, ErrInvalidPage
	}
	// Cache hit: re-zero before returning. SQLite's NOCONTENT path always
	// re-zeroes even on a cache hit (pager.c:5567-5618: the cache-hit shortcut
	// is gated on `pPg->pPager && !noContent`, so with noContent set control
	// always falls through to memset(pPg->pData, 0, pPager->pageSize)). Match
	// that so a NOCONTENT fetch never hands back stale residual bytes.
	if pg := p.writerCache.fetch(pgno); pg != nil {
		clear(pg.data)
		pg.header = pageHeader{}
		return pg, nil
	}
	// Cache miss: create a blank page without any disk/WAL read (hard create — writer).
	pg := p.writerCache.create(pgno, 2)
	clear(pg.data)
	pg.header = pageHeader{}
	return pg, nil
}

// BEGIN ENCRYPTION

// readRawPage returns the raw on-disk bytes of pgno without invoking the
// codec. Used by the VerifyIntegrity sweep, which needs the post-codec-Encrypt
// bytes to verify the trailer (cksum) or attempt decrypt (AEAD).
//
// Reads from the WAL when a frame for pgno exists at or below walMaxFrame,
// otherwise from the main DB file. For the master-store (InMemory mode)
// returns the raw stored bytes. Returns a fresh page-sized buffer; caller
// owns it.
//
// Caller must hold a read lock (read transaction or equivalent) — same
// concurrency contract as getPage.
func (p *pager) readRawPage(pgno, walMaxFrame uint32) ([]byte, error) {
	if pgno == 0 {
		return nil, ErrInvalidPage
	}
	buf := make([]byte, p.pageSize)
	if walMaxFrame > 0 && p.wal != nil {
		frame, err := p.wal.index.get(pgno, walMaxFrame, p.wal.index.liveMinFrame())
		if err != nil {
			// ErrCorrupt from a full/over-probed SHM hash chain: fail the read
			// (C walFindFrame SQLITE_CORRUPT_BKPT, wal.c:3593) rather than
			// serving a stale pre-commit page from disk/masterStore.
			return nil, err
		}
		if frame > 0 {
			// Propagate readFrameRaw failures — the caller's read lock (see
			// the contract above) makes them always genuine; see
			// readTempPage for the full argument (C readDbPage,
			// pager.c:3035-3046).
			if err := p.wal.readFrameRaw(frame, buf); err != nil {
				return nil, fmt.Errorf("btree: readRawPage: page %d (WAL frame %d): %w", pgno, frame, err)
			}
			return buf, nil
		}
	}
	if p.file != nil {
		if err := p.readDBPage(pgno, buf); err != nil {
			return nil, fmt.Errorf("btree: readRawPage: page %d: %w", pgno, err)
		}
		return buf, nil
	}
	if p.master != nil {
		if !p.master.readPageInto(pgno, buf) {
			return nil, fmt.Errorf("btree: readRawPage: page %d not in masterStore", pgno)
		}
		return buf, nil
	}
	return nil, fmt.Errorf("btree: readRawPage: no backing storage for page %d", pgno)
}

// END ENCRYPTION

// writeStateErr returns the error for a write operation attempted while the
// pager is not in pagerWriter state: ErrClosed when the write transaction was
// force-rolled-back by DB.Close (the writer goroutine lost its transaction to
// Close, not to a read-only misuse), ErrReadOnly otherwise.
func (p *pager) writeStateErr() error {
	if p.closeInvalidated.Load() {
		return ErrClosed
	}
	return ErrReadOnly
}

// getWritablePage returns a page ready for writing. It marks the page as dirty
// and saves a copy for savepoint rollback if needed.
func (p *pager) getWritablePage(pgno uint32) (*page, error) {
	if pagerState(p.state.Load()) != pagerWriter {
		return nil, p.writeStateErr()
	}

	// Check writer cache first. Spilled pages (clean but still cached)
	// are re-dirtied here. Evicted pages fall through to getPage().
	if pg := p.writerCache.fetch(pgno); pg != nil {
		// Clear dontWrite flag: the page is being re-acquired for writing,
		// so its content is meaningful again (fix 5.4). Matches SQLite's
		// pcache.c:596-597 where PGHDR_DONT_WRITE is cleared by makeDirty.
		delete(p.dontWritePages, pgno)
		// Re-dirty pages that were made clean by pagerStress (spill).
		if !pg.dirty {
			p.writerCache.makeDirty(pg)
		}
		// Save copy for savepoint rollback if needed (lazy copy-on-write)
		if len(p.savepoints) > 0 {
			sp := &p.savepoints[len(p.savepoints)-1]
			if _, exists := sp.pages[pgno]; !exists {
				dataCopy := allocPageBuffer(int(p.pageSize), false)
				copy(dataCopy, pg.data)
				sp.pages[pgno] = dataCopy
			}
		}
		return pg, nil
	}

	pg, err := p.getPage(pgno)
	if err != nil {
		return nil, err
	}

	// If getPage returned a temp page (stale-cache path in getPageWriter),
	// adopt it into the writer so releasePage routes to writerCache.release
	// instead of recycleTempPage, which would corrupt the dirty list.
	// We must also register it in writerCache.pages so that discard() can
	// find it during rollback — otherwise the page stays on the dirty list
	// as a "ghost" that contaminates the next transaction's commit.
	if pg.uncached {
		pg.uncached = false
		pg.cache = p.writerCache
		// Discard any stale cache entry before adopting the temp page.
		// The stale page may still be on the LRU list (released at
		// getPageWriter:434); without this, a later evictOne on the stale
		// page would hashRemove(pgno) and unlink the NEW adopted page from
		// the cache, creating a ghost dirty-list entry.
		if old := p.writerCache.hashFind(pgno); old != nil {
			p.writerCache.discard(pgno)
		}
		// Link the adopted (already-pinned) temp page directly into the hash.
		// hashInsert sets pg.inCache so a later release routes through
		// writerCache.release, and bumps nPage. The page did not pass through
		// create(), so this is the analogue of the former pc.pages[pgno] = pg.
		p.writerCache.hashInsert(pg)
	}

	// Save copy for savepoint rollback if we have active savepoints
	if len(p.savepoints) > 0 && !pg.dirty {
		sp := &p.savepoints[len(p.savepoints)-1]
		if _, exists := sp.pages[pgno]; !exists {
			dataCopy := allocPageBuffer(int(p.pageSize), false)
			copy(dataCopy, pg.data)
			sp.pages[pgno] = dataCopy
		}
	}

	p.writerCache.makeDirty(pg)
	return pg, nil
}

// allocatePage allocates a new page and returns it. Equivalent to
// allocatePageNear(0) — no locality hint. Most callers (btree splits,
// root allocation) don't have a meaningful hint and use this form.
func (p *pager) allocatePage() (*page, error) {
	return p.allocatePageNear(0)
}

// allocatePageNear allocates a new page, preferring one close to the
// given pgno when possible. Nearby == 0 disables the hint (same as
// allocatePage). Matches SQLite's `allocateBtreePage(pBt, ..., nearby,
// BTALLOC_ANY)` at btree.c:6499-6505 — used for overflow chains to
// keep the chain pages contiguous on disk (btree.c:7197 fillInCell).
//
// Hint semantics: within the freelist trunk selected, the leaf with
// minimum |leafPgno - nearby| is popped. If the freelist is empty we
// fall through to growing the DB file; grow is always monotonic and
// a hint has no meaning there.
func (p *pager) allocatePageNear(nearby uint32) (*page, error) {
	if pagerState(p.state.Load()) != pagerWriter {
		return nil, p.writeStateErr()
	}

	// Check freelist first. Matches SQLite allocateBtreePage
	// (btree.c:6543 freelist branch): when the freelist is non-empty we
	// allocate from it and PROPAGATE any error (including ErrCorrupt)
	// rather than swallowing it and growing the DB. The grow-the-DB
	// fallback (btree.c:6758-6815 else branch) only runs when the
	// freelist is empty (n==0, here FirstFreelistPg == 0).
	if p.header.FirstFreelistPg != 0 {
		return p.allocateFromFreelist(nearby)
	}

	pgno := p.dbSize.Add(1)

	// Use getPageNoContent: new pages have no existing content to read (fix 5.4).
	pg, err := p.getPageNoContent(pgno)
	if err != nil {
		p.dbSize.Add(^uint32(0)) // decrement
		return nil, err
	}
	clear(pg.data)
	p.writerCache.makeDirty(pg)
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
// DRIFT: trunk fills to usableSize/4-2 (corruption ceiling) not SQLite's conservative -8; pre-3.6.0 See docs/btree/NOTES.md#old-drift-freelist-trunk-fill-corruption-ceiling
// DRIFT: freelist trunk fills to usableSize/4-2 (corruption ceiling), not SQLite's usableSize/4-8 See docs/btree/NOTES.md#old-drift-freelist-trunk-fill-bound-corruption-ceiling
func (p *pager) freelistMaxLeaves() int {
	return (p.usableSize() - 8) / 4
}

// freePage adds a page to the freelist.
// DRIFT: freePage2 trunk decision uses offset 32 not nFree; freed-leaf isInit not cleared See docs/btree/NOTES.md#drift-73-freepage2-trunk-decision-and-page-invalidation-drifts
// DRIFT: secure_delete page-zeroing on free unsupported and undocumented See docs/btree/NOTES.md#drift-74-secure-delete-page-zeroing-on-free-unsupported
// DRIFT: freelist trunk fills to usableSize/4-2 (corruption ceiling), not SQLite's usableSize/4-8 See docs/btree/NOTES.md#old-drift-freelist-trunk-fill-bound-corruption-ceiling
func (p *pager) freePage(pgno uint32) error {
	if pagerState(p.state.Load()) != pagerWriter {
		return p.writeStateErr()
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
			if p.writerCache.hashFind(pgno) != nil {
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
		newTrunkPg = p.writerCache.create(pgno, 2)
		p.writerCache.makeDirty(newTrunkPg)
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
// If nearby > 0, the leaf with minimum |leafPgno - nearby| is selected
// instead of the last leaf — matches SQLite btree.c:6678-6699 in
// BTALLOC_ANY mode. Callers that don't care about locality pass 0.
// DRIFT: only BTALLOC_ANY; EXACT/LE absent (any-store has no auto-vacuum, their sole consumer) See docs/btree/NOTES.md#old-drift-no-btalloc-exact-le-modes
func (p *pager) allocateFromFreelist(nearby uint32) (*page, error) {
	trunkPgno := p.header.FirstFreelistPg
	if trunkPgno == 0 {
		return nil, ErrInvalidPage
	}
	// Aggregate freelist-count corruption guard. C allocateBtreePage rejects the
	// whole allocation if the recorded freelist page count is >= the DB page count
	// (btree.c:6538-6542, n>=mxPage). page-1 offset 36 == header.TotalFreelistPgs.
	if p.header.TotalFreelistPgs >= p.dbSize.Load() {
		return nil, ErrCorrupt
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
		// Pick the leaf index to allocate. When nearby == 0, pop the
		// last leaf (legacy behavior: O(1), cheap). When nearby > 0,
		// walk the leaves and pick the one with minimum absolute
		// distance to nearby — matches SQLite btree.c:6678-6699 in
		// BTALLOC_ANY mode. O(leafCount) per allocation; for a 4 KiB
		// page trunk holds ~1018 leaves → sub-µs scan on modern CPUs,
		// dominated by page I/O anyway.
		pickIdx := leafCount - 1
		if nearby > 0 {
			bestDist := int64(-1)
			for i := 0; i < leafCount; i++ {
				lp := binary.BigEndian.Uint32(trunkPg.data[8+i*4:])
				d := int64(lp) - int64(nearby)
				if d < 0 {
					d = -d
				}
				if bestDist < 0 || d < bestDist {
					bestDist = d
					pickIdx = i
				}
			}
		}
		leafPgno := binary.BigEndian.Uint32(trunkPg.data[8+pickIdx*4:])

		// Validate leaf page number (fix 5.1).
		if leafPgno < 2 || leafPgno > p.dbSize.Load() {
			p.releasePage(trunkPg)
			return nil, ErrCorrupt
		}

		// Compact: move the last leaf into the picked slot, then shrink.
		// When pickIdx == leafCount-1 (legacy last-leaf case), the copy is
		// a self-copy and degenerates into just the decrement.
		if pickIdx != leafCount-1 {
			lastLeaf := binary.BigEndian.Uint32(trunkPg.data[8+(leafCount-1)*4:])
			binary.BigEndian.PutUint32(trunkPg.data[8+pickIdx*4:], lastLeaf)
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
				trace("allocateFromFreelist: leaf pg=%d hasContent=false but savepoints=%d → getPageNoContent + savepoint copy", leafPgno, len(p.savepoints))
			}
			// hasContent==false: the page has no meaningful on-disk content, and
			// for a page near dbSize whose content was never materialised to the
			// DB file (grown+freed within uncommitted/un-checkpointed churn) a real
			// read would hit EOF. Use a NOCONTENT fetch (no disk read) — matching
			// SQLite's `noContent = !btreeGetHasContent(...)` path (btree.c:6725) —
			// then journal the no-content state into the innermost savepoint so a
			// rollback restores the page consistently with the freelist header
			// (Bug 9). The previous getWritablePage here READ the page and failed
			// with EOF, leaking the already-popped leaf: the caller
			// (allocatePageNear) swallows the error and grows instead, leaving the
			// page neither on the freelist nor referenced ("page N: never used").
			pg, err := p.getPageNoContent(leafPgno)
			if err != nil {
				return nil, err
			}
			sp := &p.savepoints[len(p.savepoints)-1]
			if _, exists := sp.pages[leafPgno]; !exists {
				dataCopy := allocPageBuffer(int(p.pageSize), false)
				copy(dataCopy, pg.data)
				sp.pages[leafPgno] = dataCopy
			}
			clear(pg.data)
			pg.header = pageHeader{}
			p.writerCache.makeDirty(pg)
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
		p.writerCache.makeDirty(pg)
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
// The returned page has a valid data buffer from the slab/pool.
func (p *pager) acquireTempPage() *page {
	if v := p.pagePool.Get(); v != nil {
		pg := v.(*page)
		// Pooled pages may have had their data buffer returned to slab/pool.
		if pg.data == nil {
			pg.data = allocPageBuffer(int(p.pageSize), p.useSlab)
		}
		return pg
	}
	return &page{
		data: allocPageBuffer(int(p.pageSize), p.useSlab),
	}
}

// recycleTempPage returns an uncached page to the pool for reuse.
// The page's data buffer is returned to the slab/pool and a fresh buffer
// will be allocated on next acquireTempPage.
func (p *pager) recycleTempPage(pg *page) {
	// Return data buffer to slab/pool before pooling the page struct.
	freePageBuffer(pg.data, p.useSlab)
	pg.data = nil
	pg.pgno = 0
	pg.dirty = false
	pg.uncached = false
	pg.cache = nil
	pg.pinCount = 0
	pg.header = pageHeader{}
	pg.next = nil
	pg.prev = nil
	// Clear pcache hash-chain membership so a pooled page reused as a temp page
	// (or adopted into the writer cache via getWritablePage) never carries a
	// stale bucket link or inCache flag. Temp pages never enter apHash directly,
	// but resetting here keeps the pagePool population disjoint from the cache.
	pg.hashNext = nil
	pg.inCache = false
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
	// Route via the page's owning cache if set (supports per-connection caches).
	// Fall back to the pager's writer cache for pages without a backpointer.
	if pg.cache != nil {
		pg.cache.release(pg)
		return
	}
	p.writerCache.release(pg)
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

// effectiveReaderDbSize returns the database size in pages for a reader's
// captured snapshot header. It returns hdr.nPage (the committed page count
// recorded in the WAL-index header at the reader's snapshot, visible cross-
// process via SHM), falling back to the process-local p.dbSize when the
// header carries no committed size — hdr not initialized (in-process mode,
// where writers don't publish the SHM header) or an empty/checkpointed WAL.
//
// Mirrors SQLite's sqlite3WalDbsize (wal.c:3672 — returns pWal->hdr.nPage)
// plus pagerPagecount's fallback when WalDbsize returns 0 (pager.c:3292 call,
// 3299-3305 fallback). SQLite falls back there to the file size; we fall back
// to p.dbSize, which is equivalent: the WAL header lacks a committed size only
// in in-process mode (writers don't publish the SHM header, and p.dbSize is the
// authoritative same-process counter) or for a genuinely empty DB.
// Readers must never bound-check against p.dbSize directly: that atomic is the
// writer's allocation counter and is only refreshed on the write path
// (beginWrite -> refreshHeaderFromPage1), so a pure reader would reject pages
// a peer allocated after this process opened.
func (p *pager) effectiveReaderDbSize(hdr WalIndexHdr) uint32 {
	if hdr.isInit != 0 && hdr.nPage != 0 {
		return hdr.nPage
	}
	return p.dbSize.Load()
}

// readerDbSizeBound returns the page-count upper bound to validate pgno
// against on a read path: the snapshot bound carried on the reader cache
// (pcache.dbSize), or the process-local p.dbSize when there is no reader
// cache (writer / uncached paths) or the cache bound is unset (0). This is
// the single reader-vs-writer rule for the page and overflow bound checks;
// an unset cache bound degrades safely to the previous p.dbSize behavior.
func (p *pager) readerDbSizeBound(cache *pcache) uint32 {
	if cache != nil && cache.dbSize != 0 {
		return cache.dbSize
	}
	return p.dbSize.Load()
}

// readerMinFrame returns the WAL lookup floor for a read path: the snapshot
// minFrame carried on the reader cache (captured at BeginRead, like SQLite's
// pWal->minFrame), or the live checkpoint frontier when there is no reader
// cache (writer / uncached paths, where the write lock or exclusivity rules
// out a concurrent WAL restart). An unset cache value (0) degrades to the
// live frontier.
func (p *pager) readerMinFrame(cache *pcache) uint32 {
	if cache != nil && cache.minFrame != 0 {
		return cache.minFrame
	}
	return p.wal.index.liveMinFrame()
}

// refreshHeaderFromPage1 refreshes p.header and p.dbSize from the latest
// page 1 bytes, consulting WAL first (via SHM hash tables in multi-process
// mode) and falling back to the database file. Called from beginWrite when
// another process's commit has been detected.
//
// It returns an error only on a genuine DOUBLE I/O failure: a page-1 WAL frame
// existed but its read failed AND the DB-file fallback read also failed. In
// that case the cached header/dbSize cannot be reconciled with the on-disk
// image at all, so continuing would later let commit() serialize a stale
// header over a peer's page 1. SQLite propagates the page-1 read error out of
// the shared-lock/begin path rather than continuing with a stale header
// (pager.c sqlite3PagerSharedLock -> readDbPage error propagation), and
// beginWrite mirrors that by aborting on this error.
//
// All other outcomes return nil to preserve the prior best-effort behavior:
//   - page 1 read successfully from the WAL frame or the DB file;
//   - page 1 legitimately absent from both sources (e.g. a brand-new WAL with
//     no frames and no backing file, as in InMemory mode);
//   - no page-1 WAL frame existed and only the single DB-file read failed
//     (e.g. a truncated/corrupt file) — that corruption is still caught when
//     the writer first touches a page that must be read from disk, matching
//     the existing reader-path behavior.
func (p *pager) refreshHeaderFromPage1() error {
	// Determine effective max frame, same logic as readHeaderCounters.
	effectiveMaxFrame := p.wal.nFrame.Load()
	if p.inProcess {
		if mf := p.wal.index.mxCommitFrame.LoadLocal(); mf > effectiveMaxFrame {
			effectiveMaxFrame = mf
		}
	} else if hdr, valid := p.wal.index.readHeader(); valid && hdr.mxFrame > effectiveMaxFrame {
		effectiveMaxFrame = hdr.mxFrame
	}

	// Try WAL first. walErr records a WAL-frame read failure so that, if the
	// DB-file fallback also fails (or is unavailable), the original read error
	// can be propagated to the caller rather than swallowed.
	var walErr error
	if effectiveMaxFrame > 0 {
		frame, err := p.wal.index.get(1, effectiveMaxFrame, p.wal.index.liveMinFrame())
		if err != nil {
			// ErrCorrupt from a full/over-probed SHM hash chain is a hard error,
			// not a "WAL has no page 1" miss: do not fall back to the DB file
			// (which would mask the corruption with a stale header). Matches C's
			// walFindFrame SQLITE_CORRUPT_BKPT propagation (wal.c:3593).
			return err
		}
		if frame > 0 {
			var buf [dbHeaderSize]byte
			if err := p.readWalFrameData(frame, buf[:]); err == nil {
				_ = p.header.deserialize(buf[:])
				p.dbSize.Store(p.header.DatabaseSize)
				if debugTrace {
					trace("refreshHeaderFromPage1: source=wal frame=%d maxFrame=%d nBackfill=%d dbSize=%d firstFree=%d totalFree=%d",
						frame, effectiveMaxFrame, p.wal.index.nBackfill.Load(),
						p.header.DatabaseSize, p.header.FirstFreelistPg, p.header.TotalFreelistPgs)
				}
				return nil
			} else {
				walErr = err
			}
		}
	}

	// WAL didn't have page 1 (or the read failed) — fall back to the DB file.
	if p.file != nil {
		var buf [dbHeaderSize]byte
		if _, err := p.file.ReadAt(buf[:], 0); err == nil {
			_ = p.header.deserialize(buf[:])
			p.dbSize.Store(p.header.DatabaseSize)
			if debugTrace {
				trace("refreshHeaderFromPage1: source=db maxFrame=%d nBackfill=%d dbSize=%d firstFree=%d totalFree=%d",
					effectiveMaxFrame, p.wal.index.nBackfill.Load(),
					p.header.DatabaseSize, p.header.FirstFreelistPg, p.header.TotalFreelistPgs)
			}
			return nil
		} else if walErr != nil {
			// Double I/O failure: the WAL frame read failed AND the DB-file
			// read failed. Continuing would leave p.header / p.dbSize stale,
			// which commit() would later serialize back over a peer's page 1.
			// Propagate so beginWrite aborts (matching SQLite, which surfaces
			// the page-1 read error).
			return fmt.Errorf("refreshHeaderFromPage1: wal read failed (%w) and db read failed (%w)", walErr, err)
		}
		// No WAL page-1 frame existed; only the single DB-file read failed.
		// Preserve the prior best-effort behavior (return nil): the corruption
		// is caught when the writer first touches a page read from disk, as the
		// reader path already relies on.
		return nil
	}

	// No DB file to fall back to. If a WAL page-1 frame existed but its read
	// failed, there is no second source to reconcile the header against — that
	// is the genuine page-1 read failure, so report it. Otherwise page 1 was
	// simply absent from both sources (best-effort, no read attempted): nil.
	if walErr != nil {
		return fmt.Errorf("refreshHeaderFromPage1: wal page 1 read failed and no db file: %w", walErr)
	}
	return nil
}

// committedCounters returns the FileChangeCount and SchemaCookie from the
// in-memory header. It is only valid to call while holding the writer
// (BeginWrite holds db.writeMu and the pager is in pagerWriter state after
// beginWrite, so p.header is owned exclusively by this writer — no data race
// with concurrent readers). The values reflect the latest committed page 1:
// beginWrite refreshes p.header via refreshHeaderFromPage1 on the stateChanged
// edge that signals a peer commit/checkpoint, so this returns exactly what
// readHeaderCounters would read from page 1 — without the per-tx WAL hash
// lookup + frame read. Mirrors SQLite keeping the header in the in-memory
// page-1 PgHdr rather than re-reading it from the WAL per statement.
func (p *pager) committedCounters() (fileChangeCount, schemaCookie uint32) {
	return p.header.FileChangeCount, p.header.SchemaCookie
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
		if mf := p.wal.index.mxCommitFrame.LoadLocal(); mf > effectiveMaxFrame {
			effectiveMaxFrame = mf
		}
	} else if hdr, valid := p.wal.index.readHeader(); valid && hdr.mxFrame > effectiveMaxFrame {
		effectiveMaxFrame = hdr.mxFrame
	}

	// Look up page 1's latest frame.
	// walIndex.get() merges the local page map with SHM, preferring newer SHM
	// frames. After an external state change, beginWrite rebuilds the local page
	// map from the WAL so page-1 refreshes do not depend solely on SHM hashes.
	if effectiveMaxFrame > 0 {
		frame, gerr := p.wal.index.get(1, effectiveMaxFrame, p.wal.index.liveMinFrame())
		if gerr != nil {
			// ErrCorrupt from a full/over-probed SHM hash chain is a hard error,
			// not a "WAL has no page 1" miss: fail rather than reading stale
			// counters from the DB file (C walFindFrame SQLITE_CORRUPT_BKPT,
			// wal.c:3593).
			return 0, 0, gerr
		}
		if frame > 0 {
			var buf [dbHeaderSize]byte
			// Propagate readWalFrameData failures — the resolved frame holds
			// the only current page-1 header; falling back to the DB file
			// would serve stale FileChangeCount/SchemaCookie, silently
			// defeating cross-process staleness detection and backup restart
			// detection. Same contract as the page getters (drift-6
			// resolution): the caller holds a reader slot, so the frame
			// cannot have been legitimately reset away.
			if err := p.readWalFrameData(frame, buf[:]); err != nil {
				return 0, 0, fmt.Errorf("btree: read page-1 header counters (WAL frame %d): %w", frame, err)
			}
			return binary.BigEndian.Uint32(buf[24:28]), binary.BigEndian.Uint32(buf[40:44]), nil
		}
	}

	// No WAL frame for page 1; read from database file or header.
	if p.file == nil {
		// InMemory: read page 1 from masterStore which is protected by
		// sync.RWMutex — safe for concurrent access from reader goroutines.
		// We cannot read p.header directly because commit() mutates
		// FileChangeCount/SchemaCookie without synchronization, which would
		// be a data race with concurrent BeginRead callers.
		var buf [dbHeaderSize]byte
		if p.master != nil && p.master.readPageInto(1, buf[:]) {
			return binary.BigEndian.Uint32(buf[24:28]), binary.BigEndian.Uint32(buf[40:44]), nil
		}
		// masterStore has no page 1 yet (before first checkpoint); header
		// is safe to read because no concurrent writer exists at this point.
		return p.header.FileChangeCount, p.header.SchemaCookie, nil
	}
	var buf [dbHeaderSize]byte
	if _, err := p.file.ReadAt(buf[:], 0); err != nil {
		return 0, 0, err
	}
	return binary.BigEndian.Uint32(buf[24:28]), binary.BigEndian.Uint32(buf[40:44]), nil
}

// readWalFrameData reads the first `len(buf)` bytes of page data from a WAL
// frame. Unlike wal.readFrame, this does not check the in-process nFrame
// counter, making it safe for cross-process reads where another process wrote
// the frame. For InMemory mode (memFrames), it reads from the in-memory frames.
func (p *pager) readWalFrameData(frame uint32, buf []byte) error {
	// Test-only fault-injection hook, shared with readFrame/readFrameRaw; see
	// walReadFrameFaultHook.
	if hook := walReadFrameFaultHook.Load(); hook != nil {
		if err := (*hook)(frame); err != nil {
			return err
		}
	}
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

// pagerStress is the pcache stress callback invoked when the writer's cache
// is full and all clean pages are exhausted. It spills a single dirty page
// to the WAL without committing, making it clean and evictable.
// Only the writer's cache has xStress set; reader caches have no stress callback.
// Modeled after SQLite's pagerStress() (pager.c:4609-4681).
// DRIFT: pagerStress guards pgno==1; C relies on page-1 staying pinned (never a victim) See docs/btree/NOTES.md#old-drift-pagerstress-page1-exclusion
// DRIFT: pagerStress skips WAL write for dontWrite pages, just makeClean; C writes the frame anyway See docs/btree/NOTES.md#old-drift-pagerstress-dontwrite-skip-walwrite
func (p *pager) pagerStress(pg *page) error {
	// Defense-in-depth: do not spill in error state (SQLite pager.c:4632).
	// SQLite marks this path NEVER() — it should be unreachable because
	// pcache.create is not called in error state — but the guard prevents
	// writing to a potentially corrupt WAL if the invariant is violated.
	if pagerState(p.state.Load()) == pagerError {
		return nil
	}

	// Do not spill if OFF or ROLLBACK flags are set (SQLite pager.c:4636-4641).
	if p.doNotSpill&(spillFlagOff|spillFlagRollback) != 0 {
		return nil
	}

	// Page 1 contains the database header and must not be spilled. We guard
	// it explicitly because page 1 may become unpinned between b-tree
	// operations (see the page1-exclusion DRIFT on this func's doc comment).
	if pg.pgno == 1 {
		return nil
	}

	// Skip the WAL write for dontWrite pages and just mark them clean (see
	// the dontWrite-skip DRIFT on this func's doc comment). Safe because
	// dontWrite page data is never read back. We must still make them clean
	// so they become evictable — without this, the cache grows unbounded
	// when freed pages are the only dirty victims.
	if p.dontWritePages[pg.pgno] {
		p.writerCache.makeClean(pg)
		return nil
	}

	// subjournalPageIfRequired equivalent (SQLite pager.c:4647):
	// Save page data for savepoint rollback before spilling.
	if len(p.savepoints) > 0 {
		sp := &p.savepoints[len(p.savepoints)-1]
		if _, exists := sp.pages[pg.pgno]; !exists {
			dataCopy := allocPageBuffer(int(p.pageSize), false)
			copy(dataCopy, pg.data)
			sp.pages[pg.pgno] = dataCopy
		}
	}

	// Write the page to WAL without commit (SQLite pager.c:4649).
	if err := p.wal.writeFrames([]*page{pg}, false, 0); err != nil {
		// Transition to error state on WAL write failure, matching SQLite's
		// return pager_error(pPager, rc) at pager.c:4680. Without this the
		// error is silently dropped by pcache.create() (which has no error
		// return) and the transaction continues on a corrupt WAL.
		p.pagerError()
		return err
	}

	// Mark the page as clean so it becomes evictable (SQLite pager.c:4677).
	p.writerCache.makeClean(pg)

	return nil
}

// commit writes all dirty pages to WAL and commits the transaction.
// dataChanged/schemaChanged control whether FileChangeCount/SchemaCookie are
// incremented. Returns the WAL frame count and the new counter values.
// DRIFT: FileChangeCount bumped only on dataChanged=true, not every data-page commit See docs/btree/NOTES.md#drift-77-filechangecount-bumped-conditionally-not-unconditionally
// DRIFT: commit doesn't prune dirty pages pgno>dbSize before WAL write (no nTruncate prune) See docs/btree/NOTES.md#drift-78-commit-does-not-prune-dirty-pages-above-dbsize-before-wal-wr
func (p *pager) commit(dataChanged, schemaChanged bool) (nFrame, newFCC, newSC uint32, err error) {
	p.writerOpMu.Lock()
	defer p.writerOpMu.Unlock()
	if pagerState(p.state.Load()) != pagerWriter {
		return 0, 0, 0, p.writeStateErr()
	}

	if debugTrace {
		trace("commit: dbSize=%d savepoints=%d dirtyPages=%d dontWritePages=%d hasContent=%d",
			p.dbSize.Load(), len(p.savepoints), p.writerCache.nDirty, len(p.dontWritePages), len(p.hasContent))
	}

	// Update the in-memory header with current database size.
	p.header.DatabaseSize = p.dbSize.Load()

	// Filter out dontWrite pages before WAL write (fix 5.4). These are freed
	// leaf pages whose content is irrelevant. We clean them directly from the
	// cache by iterating the dontWritePages map (no dirty-list collect): a
	// dontWrite page is either dirty (makeClean removes it from the dirty
	// list and decrements nDirty), already clean from a prior spill
	// (makeClean is a no-op), or evicted (absent from the cache — nothing to
	// do). This is equivalent to the previous dirty-list walk but avoids a
	// redundant dirty-page collection. The authoritative dirty set is
	// collected exactly once below, after page 1 is dirtied — mirroring
	// SQLite's single sqlite3PcacheDirtyList in sqlite3PagerCommitPhaseOne
	// (pager.c:6502).
	if len(p.dontWritePages) > 0 {
		if debugTrace {
			trace("commit: filtering %d dontWrite pages (nDirty=%d) savepoints=%d",
				len(p.dontWritePages), p.writerCache.nDirty, len(p.savepoints))
		}
		for pgno := range p.dontWritePages {
			if pg := p.writerCache.hashFind(pgno); pg != nil {
				if debugTrace && pg.dirty {
					trace("commit: dontWrite filtering pg=%d (skipping WAL write)", pgno)
				}
				p.writerCache.makeClean(pg)
			}
		}
		clear(p.dontWritePages)
	}

	// Determine if there are real changes: dirty data pages, header
	// modifications (freelist, dbSize changes), or spilled pages. Counter
	// increments are deferred until we confirm there's something to commit.
	// nDirty (post dontWrite-filter) counts the pages that would be collected.
	// The nFrame check catches transactions where all dirty pages were spilled
	// (making nDirty zero) but a commit frame is still needed.
	hasRealChanges := p.writerCache.nDirty > 0 || p.header != p.savedHeader ||
		p.wal.nFrame.Load() > p.savedWalFrame.Load()

	if !hasRealChanges {
		// Empty transaction — counters not incremented.
		p.state.Store(int32(pagerOpen))
		p.freeSavepointPageBuffers(0, len(p.savepoints))
		p.savepoints = p.savepoints[:0]
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
	// Keep VersionValidFor in lockstep with FileChangeCount. pager.open
	// asserts they match; a writer that advances FileChangeCount must
	// also update VersionValidFor or the next opener will reject the
	// DB as corrupt.
	p.header.VersionValidFor = p.header.FileChangeCount

	// Ensure page 1 is always written with the updated header (fix 5.3).
	pg1, err := p.getWritablePage(1)
	if err != nil {
		p.pagerError()
		return 0, 0, 0, err
	}
	p.header.serialize(pg1.data[:dbHeaderSize])
	if debugTrace {
		trace("commit: page1 dbSize=%d firstFree=%d totalFree=%d fcc=%d sc=%d",
			p.header.DatabaseSize, p.header.FirstFreelistPg, p.header.TotalFreelistPgs,
			p.header.FileChangeCount, p.header.SchemaCookie)
	}
	p.releasePage(pg1)

	// Collect the authoritative dirty set once, now that page 1 has been
	// dirtied. dontWrite pages were already cleaned above so they are not on
	// the dirty list. Single collection mirrors SQLite (pager.c:6502).
	p.dirtyBuf = p.writerCache.appendDirtyPages(p.dirtyBuf[:0])

	if debugTrace {
		dirtyPgnos := make([]uint32, 0, len(p.dirtyBuf))
		for _, pg := range p.dirtyBuf {
			dirtyPgnos = append(dirtyPgnos, pg.pgno)
		}
		trace("commit: writing %d dirty pages to WAL %v", len(p.dirtyBuf), dirtyPgnos)
	}

	// Write all dirty pages to WAL
	if err := p.wal.writeFrames(p.dirtyBuf, true, p.dbSize.Load()); err != nil {
		p.pagerError()
		return 0, 0, 0, err
	}

	// Capture nFrame atomically for checkpoint threshold decision.
	nFrame = p.wal.nFrame.Load()

	// Notify online backups that these pages have new committed content.
	// ~ sqlite3BackupUpdate dispatch (backup.c:687). Must happen here,
	// AFTER the WAL frames are durable, so dst sees committed data only.
	p.dispatchBackupUpdate(p.dirtyBuf)

	// Mark all pages as clean
	for _, pg := range p.dirtyBuf {
		p.writerCache.makeClean(pg)
	}

	// Truncate the cache to the current database size. Pages beyond dbSize
	// are stale (freed during the transaction, e.g. from freelist growth or
	// page consolidation) and must not accumulate across transactions.
	// Matches SQLite pager_end_transaction (pager.c:2134):
	//   sqlite3PcacheTruncate(pPager->pPCache, pPager->dbSize);
	// which is called from sqlite3PagerCommitPhaseTwo (pager.c:6738).
	p.writerCache.truncate(p.dbSize.Load())

	p.freeSavepointPageBuffers(0, len(p.savepoints))
	p.savepoints = p.savepoints[:0]
	clear(p.dontWritePages)
	clear(p.hasContent)
	p.state.Store(int32(pagerOpen))
	p.wal.endWrite()

	return nFrame, p.header.FileChangeCount, p.header.SchemaCookie, nil
}

// rollback discards all changes in the current write transaction.
func (p *pager) rollback() error {
	p.writerOpMu.Lock()
	defer p.writerOpMu.Unlock()
	return p.rollbackLocked()
}

// rollbackLocked is the inner rollback implementation. Caller must hold writerOpMu.
// DRIFT: no sqlite3WalUndo; rollback resets wal.nFrame to savedWalFrame + rollbackToFrame trims pages See docs/btree/NOTES.md#old-drift-wal-undo-via-pager-rollback
func (p *pager) rollbackLocked() error {
	st := pagerState(p.state.Load())
	if st != pagerWriter && st != pagerError {
		return nil
	}

	// Suppress spills during rollback (SQLite pager.c:2457).
	p.doNotSpill |= spillFlagRollback

	// Discard all dirty pages from cache
	dirtyPages := p.writerCache.dirtyPages()
	for _, pg := range dirtyPages {
		p.writerCache.discard(pg.pgno)
	}

	// Discard all dirty pages and spilled (clean) pages from cache.
	// Their cached content is stale after rollback. clear() keeps buffers
	// in pFree for reuse.
	p.writerCache.clear()

	// Roll back spilled frames in the WAL index. Spilled frames in the WAL
	// file are harmless (no commit marker), but pageMap entries and maxFrame
	// must be restored to the pre-transaction state.
	p.wal.index.rollbackToFrame(p.savedWalFrame.Load())
	// Restore nFrame so the next transaction overwrites the dead spill frames
	// instead of writing past them. Also restore cumulative checksums so the
	// checksum chain is correct when frames are overwritten.
	p.wal.nFrame.Store(p.savedWalFrame.Load())
	p.wal.cksum1 = p.savedWalCksum1
	p.wal.cksum2 = p.savedWalCksum2

	// Truncate in-memory WAL frames to match restored nFrame. Without this,
	// writeFramesMem appends past the stale entries, and readFrame(N) reads
	// memFrames[N-1] which points to rolled-back data instead of new data.
	if p.wal.inMemory {
		p.wal.mu.Lock()
		p.wal.memFrames = p.wal.memFrames[:p.savedWalFrame.Load()]
		p.wal.mu.Unlock()
	}

	// Restore the database header from the snapshot saved at beginWrite (fix 5.2).
	// This ensures FirstFreelistPg, TotalFreelistPgs, and DatabaseSize are
	// reverted to their pre-transaction values after dirty pages are discarded.
	p.header = p.savedHeader
	p.dbSize.Store(p.header.DatabaseSize)

	p.doNotSpill &^= spillFlagRollback
	p.freeSavepointPageBuffers(0, len(p.savepoints))
	p.savepoints = p.savepoints[:0]
	clear(p.dontWritePages)
	clear(p.hasContent)
	p.state.Store(int32(pagerOpen))
	p.wal.endWrite()
	return nil
}

// rollbackForClose is a variant of rollbackLocked for use by DB.Close().
// It only performs WAL-level rollback and lock release — all writer-owned
// state (dontWritePages, hasContent, header, savepoints, WAL checksums)
// is deliberately left untouched because the writer goroutine may still be
// in a B-tree operation accessing those structures, and pcache has no mutex.
// Since the database is closing, that state will be GC'd.
// Caller must hold writerOpMu.
func (p *pager) rollbackForClose() {
	st := pagerState(p.state.Load())
	if st != pagerWriter && st != pagerError {
		return
	}

	// Mark the writer transaction as invalidated by Close BEFORE demoting
	// the pager state: the abandoned writer goroutine loads the state first
	// and then calls writeStateErr, so this ordering guarantees it observes
	// ErrClosed rather than ErrReadOnly.
	p.closeInvalidated.Store(true)

	// Roll back spilled frames in the WAL index.
	// index.rollbackToFrame and nFrame are safe (own locking / atomic).
	p.wal.index.rollbackToFrame(p.savedWalFrame.Load())
	p.wal.nFrame.Store(p.savedWalFrame.Load())

	if p.wal.inMemory {
		p.wal.mu.Lock()
		p.wal.memFrames = p.wal.memFrames[:p.savedWalFrame.Load()]
		p.wal.mu.Unlock()
	}

	// Skip dontWritePages, hasContent, header, savepoints, and WAL
	// checksum restoration — all are writer-owned, non-atomic state that
	// the writer goroutine may be reading concurrently. Touching them
	// here would be a data race. None of it matters after Close().

	p.state.Store(int32(pagerOpen))
	p.wal.endWrite()
}

// pagerError transitions the pager to the error state. It ensures the WAL
// write lock is released (fix 2.2) so that other connections are not blocked
// indefinitely. Modeled after SQLite's pager_error() + pager_unlock() which
// calls sqlite3WalEndWriteTransaction() when in PAGER_ERROR state.
//
// Recovery path: the cache is purged, the header is restored from the saved
// snapshot, and the state transitions back to pagerOpen before this function
// returns — callers must NOT rely on a persistent error state to fence out
// subsequent transactions (anystore's failed-commit DDL unwind gates itself
// for exactly this reason; see db.ddlUnwindGate).
// DRIFT: pagerError eager-cleans (purge/WAL-rollback/unlock); C pager_error only sets errCode, defers See docs/btree/NOTES.md#old-drift-pagererror-eager-cleanup
func (p *pager) pagerError() {
	p.state.Store(int32(pagerError))

	// Purge the cache — its contents cannot be trusted after an error.
	dirtyPages := p.writerCache.dirtyPages()
	for _, pg := range dirtyPages {
		p.writerCache.discard(pg.pgno)
	}
	p.writerCache.clear()

	// Roll back spilled frames in the WAL index and restore nFrame/checksums.
	// Without this, pageMap and nFrame remain inflated after error recovery,
	// causing checkpoint to copy uncommitted spill data to the database.
	p.wal.index.rollbackToFrame(p.savedWalFrame.Load())
	p.wal.nFrame.Store(p.savedWalFrame.Load())
	p.wal.cksum1 = p.savedWalCksum1
	p.wal.cksum2 = p.savedWalCksum2

	// Restore mxCommitFrame: the commit path advances mxCommitFrame before
	// writeHeader (wal.go), so a writeHeader failure leaves mxCommitFrame
	// ahead of the rolled-back WAL state. At beginWrite() time,
	// mxCommitFrame == nFrame == savedWalFrame, so restore to that value.
	p.wal.index.mxCommitFrame.Store(p.savedWalFrame.Load())

	// Truncate in-memory WAL frames to match restored nFrame.
	if p.wal.inMemory {
		p.wal.mu.Lock()
		p.wal.memFrames = p.wal.memFrames[:p.savedWalFrame.Load()]
		p.wal.mu.Unlock()
	}

	// Restore the database header to pre-transaction state.
	p.header = p.savedHeader
	p.dbSize.Store(p.header.DatabaseSize)

	p.freeSavepointPageBuffers(0, len(p.savepoints))
	p.savepoints = p.savepoints[:0]

	clear(p.dontWritePages)
	clear(p.hasContent)

	// Release the WAL write lock so other writers are not blocked (fix 2.2).
	p.wal.endWrite()

	// Transition back to open state now that we have cleaned up.
	// This mirrors SQLite's pager_unlock() which transitions from
	// PAGER_ERROR -> PAGER_OPEN after clearing the error.
	p.state.Store(int32(pagerOpen))
}

// freeSavepointPageBuffers returns page copy buffers from savepoints[from:to]
// to the page buffer pool. Must be called before truncating the savepoints slice.
func (p *pager) freeSavepointPageBuffers(from, to int) {
	for i := from; i < to; i++ {
		for _, data := range p.savepoints[i].pages {
			freePageBuffer(data, false)
		}
		clear(p.savepoints[i].pages)
	}
}

// savepoint creates a new savepoint and returns its ID.
// Page copies are saved lazily in getWritablePage when pages are actually modified.
func (p *pager) savepoint() (int, error) {
	if pagerState(p.state.Load()) != pagerWriter {
		return 0, p.writeStateErr()
	}

	id := len(p.savepoints)
	dbSz := p.dbSize.Load()
	walFr := p.wal.nFrame.Load()
	if debugTrace {
		trace("savepoint: creating id=%d dbSize=%d walFrame=%d dirtyPages=%d dontWritePages=%d hasContent=%d",
			id, dbSz, walFr, p.writerCache.nDirty, len(p.dontWritePages), len(p.hasContent))
	}
	p.savepoints = append(p.savepoints, savepointState{
		id:     id,
		dbSize: dbSz,
		pages:  make(map[uint32][]byte),
		walHdr: WalIndexHdr{
			isInit:      1,
			mxFrame:     walFr,
			aFrameCksum: [2]uint32{p.wal.cksum1, p.wal.cksum2},
		},
		header: p.header, // snapshot header for rollback (fix 9.3)
	})
	return id, nil
}

// rollbackToSavepoint rolls back to the given savepoint, restoring pages.
func (p *pager) rollbackToSavepoint(id int) error {
	if pagerState(p.state.Load()) != pagerWriter {
		return p.writeStateErr()
	}
	if id < 0 || id >= len(p.savepoints) {
		return ErrInvalidSavepoint
	}

	sp := &p.savepoints[id]

	if debugTrace {
		trace("rollbackToSavepoint: id=%d spDbSize=%d currentDbSize=%d numSavepoints=%d cachedPages=%d",
			id, sp.dbSize, p.dbSize.Load(), len(p.savepoints), p.writerCache.nPage)
	}

	// Suppress spills during savepoint rollback (SQLite pager.c:2457).
	p.doNotSpill |= spillFlagRollback

	// Discard pages allocated after the savepoint.
	p.writerCache.truncate(sp.dbSize)

	// Roll back spilled frames in the WAL index to the savepoint's WAL position.
	p.wal.index.rollbackToFrame(sp.walHdr.mxFrame)
	// Restore nFrame and cumulative checksums so the next write overwrites
	// the dead spill frames with a correct checksum chain.
	p.wal.nFrame.Store(sp.walHdr.mxFrame)
	p.wal.cksum1 = sp.walHdr.aFrameCksum[0]
	p.wal.cksum2 = sp.walHdr.aFrameCksum[1]

	// If an in-tx frame-reuse overwrote a frame past the savepoint's
	// position, drop the pending checksum rewrite — that frame is
	// now discarded. Mirrors SQLite wal.c:3832-3834.
	p.wal.resetIReCksumIfPast(sp.walHdr.mxFrame)

	// Truncate in-memory WAL frames to match restored nFrame.
	if p.wal.inMemory {
		p.wal.mu.Lock()
		p.wal.memFrames = p.wal.memFrames[:sp.walHdr.mxFrame]
		p.wal.mu.Unlock()
	}

	// Restore saved page copies from all savepoints being rolled back.
	// Iterate from NEWEST to OLDEST (fix 9.1): this ensures that when a page
	// has copies at multiple savepoint levels, the oldest (correct) copy is
	// written last and wins. This is analogous to SQLite's pDone bitvec that
	// skips pages already restored — our reverse iteration achieves the same
	// result by letting the oldest copy overwrite newer ones.
	//
	// Skip any saved copy whose page number is above the target savepoint's
	// original dbSize. This mirrors SQLite, which resets pPager->dbSize to
	// pSavepoint->nOrig (pager.c:3426) and then has pager_playback_one_page
	// skip every journaled page with pgno > pPager->dbSize (pager.c:2341).
	// The comparison is against the single restored dbSize (the target
	// savepoint's nOrig), not each inner savepoint's, so capture it once
	// before the loop. Without this guard, pages allocated after the target
	// savepoint would be resurrected as ghost dirty pages above dbSize.
	target := sp.dbSize
	for i := len(p.savepoints) - 1; i >= id; i-- {
		if debugTrace {
			trace("rollbackToSavepoint: restoring sp[%d] pages (%d entries)", i, len(p.savepoints[i].pages))
		}
		for pgno, data := range p.savepoints[i].pages {
			if pgno > target {
				continue
			}
			// Find the page in cache, or create a new one if evicted.
			pg := p.writerCache.fetch(pgno)
			if pg == nil {
				pg = p.writerCache.create(pgno, 2)
			}
			{
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
					p.writerCache.makeDirty(pg)
				}
				p.writerCache.release(pg)
			}
		}
	}

	// Restore database header from savepoint snapshot (fix 9.3).
	// This covers the case where page 1 was not in any savepoint's page map
	// but the header was modified in memory (e.g., freelist changes).
	p.header = sp.header
	p.dbSize.Store(sp.dbSize)

	p.doNotSpill &^= spillFlagRollback

	// Free page buffers from discarded savepoints (above target).
	// Savepoint[id] is kept active — its page copies remain for future rollback.
	p.freeSavepointPageBuffers(id+1, len(p.savepoints))

	// Remove savepoints above the target but keep the target itself.
	// This matches SQLite's behavior (pager.c:7025): ROLLBACK TO keeps the
	// savepoint active so it can be released or rolled back again later.
	p.savepoints = p.savepoints[:id+1]

	return nil
}

// releaseSavepoint releases a savepoint and all savepoints above it,
// merging their changes into the parent savepoint (or transaction).
// DRIFT: out-of-range savepoint release errors in Go vs C documented no-op (OK) See docs/btree/NOTES.md#drift-61-out-of-range-savepoint-release-errors-instead-of-no-op
func (p *pager) releaseSavepoint(id int) error {
	if pagerState(p.state.Load()) != pagerWriter {
		return p.writeStateErr()
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
					parent.pages[pgno] = data // transfer ownership to parent
				} else {
					freePageBuffer(data, false) // parent already has a copy
				}
			}
			clear(p.savepoints[i].pages)
		}
	} else {
		// No parent — free all page buffers from all savepoints.
		p.freeSavepointPageBuffers(0, len(p.savepoints))
	}

	p.savepoints = p.savepoints[:id]
	return nil
}

// checkpointWithMode runs a WAL checkpoint with the specified mode.
// Does NOT take pager.mu.Lock — readers can continue during checkpoint.
// The WAL's busy handler is used for FULL/RESTART/TRUNCATE modes to wait
// for readers that block progress, matching SQLite's behavior. For
// non-PASSIVE modes an incomplete backfill (or a write-lock downgrade)
// surfaces ErrBusy from wal.checkpointWithMode; we only dispatch a backup
// restart on success, so a BUSY result correctly suppresses it.
func (p *pager) checkpointWithMode(mode CheckpointMode) error {
	err := p.wal.checkpointWithMode(p.file, p.master, mode, p.wal.busyHandler)
	if err == nil && (mode == CheckpointRestart || mode == CheckpointTruncate) {
		// ~ sqlite3BackupRestart (backup.c:701-707). WAL frame numbering
		// has reset, so any in-flight backup must start over.
		p.dispatchBackupRestart()
	}
	return err
}

// tryCheckpoint attempts a passive checkpoint for auto-checkpoint.
// Uses PASSIVE mode to avoid blocking writers or readers, matching SQLite.
// DRIFT: auto-checkpoint escalates to WAL-RESTART; SQLite auto-checkpoint is PASSIVE-only See docs/btree/NOTES.md#drift-53-auto-checkpoint-escalates-to-wal-restart-beyond-passive
func (p *pager) tryCheckpoint() error {
	// First run a non-blocking backfill (PASSIVE), matching SQLite's
	// auto-checkpoint behavior.
	if err := p.wal.checkpointPassive(p.file, p.master); err != nil {
		return err
	}

	// If all frames are backfilled, try a best-effort RESTART to recycle WAL
	// frame numbers and prevent unbounded WAL growth. With xBusy=nil this does
	// not wait for readers; it simply skips reset when locks are busy.
	//
	// Authoritative read: in multi-process mode the process-local cursor can
	// trail a peer's recent commit; comparing nBackfill against a stale local
	// cursor can return true prematurely and schedule a RESTART that races
	// the peer's fresh frames. See docs/btree/NOTES.md §"Checkpoint mxFrame source fix".
	if p.wal.index.nBackfill.Load() >= p.wal.authoritativeMxFrame() {
		if err := p.wal.checkpointWithMode(p.file, p.master, CheckpointRestart, nil); err == nil {
			// ~ sqlite3BackupRestart (backup.c:701-707) also applies to
			// auto-checkpoint's best-effort restart.
			p.dispatchBackupRestart()
		}
	}
	return nil
}

// writeOverflowChain writes data to a chain of overflow pages and returns
// the first page number in the chain.
func (p *pager) writeOverflowChain(data []byte) (uint32, error) {
	return p.writeOverflowChainMulti(data)
}

// writeOverflowChainMulti writes multiple data segments to a chain of overflow
// pages without assembling them into a contiguous intermediate buffer.
// This matches SQLite's fillInCell streaming pattern (btree.c:7158-7239).
func (p *pager) writeOverflowChainMulti(segments ...[]byte) (uint32, error) {
	if pagerState(p.state.Load()) != pagerWriter {
		return 0, p.writeStateErr()
	}

	usable := overflowPageUsable(p.usableSize())
	var firstPgno uint32
	var prevPg *page
	var totalLen int

	// Current page write position
	var pg *page
	spaceLeft := 0

	for _, seg := range segments {
		totalLen += len(seg)
		for len(seg) > 0 {
			if spaceLeft == 0 {
				// Allocate a new overflow page. Pass the previous
				// overflow page's pgno as the locality hint so the
				// freelist picks a physically nearby leaf — matches
				// SQLite fillInCell (btree.c:7197 `allocateBtreePage(
				// pBt, &pOvfl, &pgnoOvfl, pgnoOvfl, 0)`). For the
				// first overflow page in a chain prevPg is nil and we
				// pass 0 (no hint) — matches btree.c:7131
				// `pgnoOvfl = 0`.
				var nearby uint32
				if prevPg != nil {
					nearby = prevPg.pgno
				}
				newPg, err := p.allocatePageNear(nearby)
				if err != nil {
					// Release the currently-held overflow page before
					// returning, mirroring C fillInCell's allocation-failure
					// path (btree.c:7235-7238: releasePage(pToRelease);
					// return rc;). prevPg is nil on the first overflow page
					// (releasePage no-ops on nil), matching C's pToRelease==0.
					p.releasePage(prevPg)
					return 0, err
				}
				if firstPgno == 0 {
					firstPgno = newPg.pgno
				}
				if prevPg != nil {
					binary.BigEndian.PutUint32(prevPg.data[0:4], newPg.pgno)
					p.releasePage(prevPg)
				}
				binary.BigEndian.PutUint32(newPg.data[0:4], 0)
				pg = newPg
				prevPg = pg
				spaceLeft = usable
			}

			n := len(seg)
			if n > spaceLeft {
				n = spaceLeft
			}
			copy(pg.data[4+usable-spaceLeft:], seg[:n])
			seg = seg[n:]
			spaceLeft -= n
		}
	}
	if prevPg != nil {
		p.releasePage(prevPg)
	}
	if debugTrace {
		trace("writeOverflowChain: firstPg=%d totalDataLen=%d", firstPgno, totalLen)
	}
	return firstPgno, nil
}

// readOverflowChainAt reads data from a chain of overflow pages into buf,
// using the writer's page cache (getPage). Suitable for the writer path.
// DRIFT: no aOverflow[] cursor cache; overflow chains re-walked from start each read (perf, not corruption) See docs/btree/NOTES.md#old-drift-no-aoverflow-page-cache
func (p *pager) readOverflowChainAt(firstPgno uint32, buf []byte, walMaxFrame uint32) error {
	usable := overflowPageUsable(p.usableSize())
	pgno := firstPgno
	off := 0

	// Compute max iterations to prevent infinite loops on circular chains (fix 8.2).
	maxIter := len(buf)/usable + 2
	if maxIter < 10 {
		maxIter = 10
	}
	iter := 0
	dbSize := p.dbSize.Load()

	for pgno != 0 && off < len(buf) {
		if pgno < 2 || pgno > dbSize {
			return ErrCorrupt
		}
		iter++
		if iter > maxIter {
			return ErrCorrupt
		}

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
	// Post-loop completeness check: if the chain's next-page pointer became 0
	// before all requested bytes were transferred, the overflow chain ended
	// prematurely. Mirrors C accessPayload (btree.c:5327-5330
	// `if( rc==SQLITE_OK && amt>0 ) return SQLITE_CORRUPT_PAGE(pPage)`).
	if off < len(buf) {
		return ErrCorrupt
	}
	return nil
}

// readOverflowChainReader reads overflow chain data using the reader's private
// cache. Pages are cached across overflow reads within the same transaction.
// Falls back to the writer path (readOverflowChainAt) if no reader cache.
func (p *pager) readOverflowChainReader(firstPgno uint32, buf []byte, walMaxFrame uint32, cache *pcache) error {
	if cache == nil {
		return p.readOverflowChainAt(firstPgno, buf, walMaxFrame)
	}
	usable := overflowPageUsable(p.usableSize())
	pgno := firstPgno
	off := 0

	maxIter := len(buf)/usable + 2
	if maxIter < 10 {
		maxIter = 10
	}
	iter := 0
	// Reader snapshot bound: accept overflow pages a peer allocated after we
	// opened (cache is non-nil here). See pcache.dbSize / readerDbSizeBound.
	dbSize := p.readerDbSizeBound(cache)

	for pgno != 0 && off < len(buf) {
		if pgno < 2 || pgno > dbSize {
			return ErrCorrupt
		}
		iter++
		if iter > maxIter {
			return ErrCorrupt
		}
		pg, err := p.getPageReader(pgno, walMaxFrame, cache)
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
	// Post-loop completeness check: chain ended before all requested bytes were
	// transferred. Mirrors C accessPayload (btree.c:5327-5330).
	if off < len(buf) {
		return ErrCorrupt
	}
	return nil
}

// readOverflowAt reads amt bytes from an overflow chain starting at byte offset
// skip into the chain payload. Copies directly into dst. Matches SQLite's
// accessPayload() offset logic — skips overflow pages that fall before skip,
// then copies from the first relevant page onward.
func (p *pager) readOverflowAt(firstPgno uint32, skip, amt int, dst []byte, walMaxFrame uint32, cache *pcache) error {
	usable := overflowPageUsable(p.usableSize())
	pgno := firstPgno
	off := 0 // current byte offset in chain payload

	maxIter := (skip+amt)/usable + 2
	if maxIter < 10 {
		maxIter = 10
	}
	iter := 0
	// Reader snapshot bound (falls back to p.dbSize when cache is nil — the
	// writer/uncached path). See pcache.dbSize / readerDbSizeBound.
	dbSize := p.readerDbSizeBound(cache)
	written := 0

	for pgno != 0 && written < amt {
		if pgno < 2 || pgno > dbSize {
			return ErrCorrupt
		}
		iter++
		if iter > maxIter {
			return ErrCorrupt
		}

		end := off + usable // byte range [off, end) in this page
		if end <= skip {
			// Entire page falls before our region — just follow the pointer.
			// Read page only to get the next-page pointer.
			var pg *page
			var err error
			if cache != nil {
				pg, err = p.getPageReader(pgno, walMaxFrame, cache)
			} else {
				pg, err = p.getPage(pgno)
			}
			if err != nil {
				return err
			}
			pgno = binary.BigEndian.Uint32(pg.data[0:4])
			p.releasePage(pg)
			off = end
			continue
		}

		// This page overlaps our region.
		var pg *page
		var err error
		if cache != nil {
			pg, err = p.getPageReader(pgno, walMaxFrame, cache)
		} else {
			pg, err = p.getPage(pgno)
		}
		if err != nil {
			return err
		}

		// Compute which bytes of this page's data [0, usable) to copy.
		srcStart := 0
		if skip > off {
			srcStart = skip - off
		}
		srcEnd := usable
		if written+(srcEnd-srcStart) > amt {
			srcEnd = srcStart + (amt - written)
		}
		copy(dst[written:], pg.data[4+srcStart:4+srcEnd])
		written += srcEnd - srcStart

		pgno = binary.BigEndian.Uint32(pg.data[0:4])
		p.releasePage(pg)
		off = end
	}
	// Post-loop completeness check: chain ended before amt bytes were
	// transferred. Mirrors C accessPayload (btree.c:5327-5330).
	if written < amt {
		return ErrCorrupt
	}
	return nil
}

// freeOverflowChain frees all pages in an overflow chain.
// DRIFT: freeOverflowChain walks-to-terminator and omits refcount==1 / nOvfl fixed count See docs/btree/NOTES.md#drift-72-freeoverflowchain-omits-refcount-and-fixed-count-versus-term
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

// truncateTo shrinks the database to the given page count. Matches
// SQLite's sqlite3PagerTruncateImage (pager.c). Discards writerCache
// entries above the new size (via pcache.truncate) and updates the
// atomic dbSize so subsequent writes see the new bound. The on-disk file
// is physically shrunk to the committed page count at the next full-backfill
// checkpoint (wal.checkpointWithMode's post-backfill dbFile.Truncate, matching
// SQLite walCheckpoint wal.c:2320-2329).
// DRIFT: truncateTo eagerly drops dirty pages (savepoint hazard) + adds non-C guards See docs/btree/NOTES.md#drift-79-truncateto-eager-dirty-page-drop-and-extra-guards
func (p *pager) truncateTo(newDbSize uint32) error {
	if pagerState(p.state.Load()) != pagerWriter {
		return p.writeStateErr()
	}
	if newDbSize == 0 {
		return errors.New("btree: cannot truncate to zero pages")
	}
	cur := p.dbSize.Load()
	if newDbSize >= cur {
		return nil
	}
	p.writerCache.truncate(newDbSize)
	p.dbSize.Store(newDbSize)
	p.header.DatabaseSize = newDbSize
	return nil
}

// attachBackup registers a Backup object to receive update/restart
// callbacks. ~ attachBackupObject (backup.c:302–309).
func (p *pager) attachBackup(b *Backup) {
	p.backupsMu.Lock()
	p.backups = append(p.backups, b)
	p.backupsMu.Unlock()
}

// detachBackup removes a Backup from the list. ~ inverse of
// attachBackupObject; called by Backup.Finish (~ backup.c:589–597).
func (p *pager) detachBackup(b *Backup) {
	p.backupsMu.Lock()
	defer p.backupsMu.Unlock()
	for i, x := range p.backups {
		if x == b {
			p.backups = append(p.backups[:i], p.backups[i+1:]...)
			return
		}
	}
}

// dispatchBackupUpdate notifies every registered Backup that the given
// dirty pages have been committed to this pager's WAL. Called from
// pager.commit just after wal.writeFrames succeeds, so the page data
// we hand to each backup is the authoritative post-commit version.
// ~ sqlite3BackupUpdate dispatch (backup.c:687–688).
func (p *pager) dispatchBackupUpdate(dirty []*page) {
	p.backupsMu.Lock()
	if len(p.backups) == 0 {
		p.backupsMu.Unlock()
		return
	}
	// Copy the slice so callbacks can run outside the lock without
	// nesting b.mu under backupsMu.
	bs := make([]*Backup, len(p.backups))
	copy(bs, p.backups)
	p.backupsMu.Unlock()

	for _, pg := range dirty {
		for _, b := range bs {
			b.update(pg.pgno, pg.data)
		}
	}
}

// dispatchBackupRestart notifies every registered Backup to restart
// from page 1. ~ sqlite3BackupRestart (backup.c:701–707).
func (p *pager) dispatchBackupRestart() {
	p.backupsMu.Lock()
	if len(p.backups) == 0 {
		p.backupsMu.Unlock()
		return
	}
	bs := make([]*Backup, len(p.backups))
	copy(bs, p.backups)
	p.backupsMu.Unlock()
	for _, b := range bs {
		b.restart()
	}
}

// withWriteLock acquires WAL_WRITE_LOCK exclusive via the configured busy
// handler (in multi-process mode) and holds it through fn. In-process and
// in-memory modes skip the lock entirely and pass locked=false.
//
// The unlock is deferred in one place — callers cannot leak the lock on
// early-return or panic. SQLite's equivalent is the explicit unlock pair
// inside sqlite3WalClose (wal.c:2530-2545); we pick the defer idiom
// because our close body has multiple exits.
//
// Must be called with p.mu held (same contract as pager.close).
func (p *pager) withWriteLock(fn func(locked bool) error) error {
	locked := false
	if !p.inProcess && !p.inMemory {
		if err := walBusyLock(p.wal.index, p.wal.busyHandler, lockWrite, lockExclusive); err == nil {
			locked = true
			defer func() { _ = p.wal.index.unlock(lockWrite, lockExclusive) }()
		}
	}
	return fn(locked)
}

// databaseIsUnmoved reports whether the live DB file on disk is still the same
// physical file (same device+inode) that was opened, so that the close-time
// checkpoint is safe to run into p.file. Models SQLite's databaseIsUnmoved
// (pager.c:4142-4159) + fileHasMoved (os_unix.c:1623-1632): if the DB file was
// renamed, unlinked, or relinked out from under the open fd, checkpointing into
// the (now detached or unrelated) inode would silently lose the committed
// transaction or clobber an unrelated file, so the checkpoint must be skipped.
//
// Returns true (unmoved) for:
//   - in-memory DB (analog of C tempFile short-circuit, pager.c:4146);
//   - dbSize==0 (C dbSize==0 short-circuit, pager.c:4147 — a brand-new/empty DB);
//   - !openIdentOK (no real inode available, e.g. non-unix / VFS-injected file):
//     analog of C SQLITE_NOTFOUND "HAS_MOVED unimplemented → assume unmoved"
//     fallback (pager.c:4150-4154), preserving today's behavior there.
//
// Otherwise it stats p.path and returns false on any stat error (file moved or
// gone) or on a (device,inode) mismatch against the identity captured at open.
func (p *pager) databaseIsUnmoved() bool {
	if p.inMemory || p.dbSize.Load() == 0 || !p.openIdentOK {
		return true
	}
	return dbFileUnmoved(p.path, p.openDev, p.openIno)
}

// close closes the pager, WAL, and database file.
// Matches SQLite's sqlite3PagerClose() -> sqlite3WalClose(): checkpoint the WAL,
// then truncate the WAL file to zero bytes before closing.
// DRIFT: last-client close truncates -wal to zero but never unlinks it (SQLite deletes) See docs/btree/NOTES.md#drift-55-wal-file-truncated-but-never-unlinked-on-last-client-close
func (p *pager) close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.wal != nil {
		isLastClient := false
		if p.inMemory {
			// InMemory: no peers, always last.
			isLastClient = true
		} else {
			// Hold WAL_WRITE_LOCK across checkpoint+truncate so a peer
			// process cannot be mid-writeFrames. withWriteLock defers the
			// unlock, so any early-return / panic inside the closure cannot
			// leak the lock. Matches SQLite's sqlite3WalClose which guards
			// walLimitSize with an exclusive DB-file lock (wal.c:2509-2534).
			_ = p.withWriteLock(func(lockedWrite bool) error {
				// databaseIsUnmoved gate: matches SQLite's sqlite3PagerClose
				// (pager.c:4189-4191), which passes a non-NULL buffer to
				// sqlite3WalClose (enabling the close-time checkpoint) only when
				// databaseIsUnmoved(pPager)==SQLITE_OK. A NULL buffer (moved DB)
				// skips the WHOLE checkpoint+truncate arm in sqlite3WalClose
				// (the `if(zBuf!=0 ...)` gate at wal.c:2522). Skipping is the
				// safe behavior: if the DB file was renamed/unlinked/relinked
				// out from under this fd, checkpointing into p.file would write
				// committed WAL frames into a path that is no longer the real
				// database — silently losing the transaction on the visible
				// path or clobbering an unrelated file now living at p.path.
				// We therefore skip both the checkpoint and the dependent
				// truncate (truncate stays gated on cpErr==nil below, and cpErr
				// is left non-nil for the moved case).
				moved := !p.databaseIsUnmoved()
				if moved {
					if debugTrace {
						trace("close: skipping close-time checkpoint, DB file moved/unlinked since open (path=%s)", p.path)
					}
					return nil
				}
				if debugTrace {
					trace("close: starting passive checkpoint before WAL truncation, dbSize=%d lockedWrite=%v", p.dbSize.Load(), lockedWrite)
				}
				cpErr := p.wal.checkpointPassive(p.file, p.master)
				if cpErr != nil {
					if debugTrace {
						trace("close: checkpointPassive incomplete or failed: %v", cpErr)
					}
				}
				// Truncate gating: matches SQLite's sqlite3WalClose (wal.c:2487-2551),
				// which calls walLimitSize (wal.c:2534) only after obtaining an
				// exclusive DB-file lock (wal.c:2509). We use a DB-file flock
				// upgrade as the analog; failure leaves the WAL intact so peer
				// readers can still find frames in the file.
				if p.inProcess {
					isLastClient = true
				} else if p.file != nil {
					ok, err := tryUpgradeDBLockExclusive(p.file)
					if err != nil {
						if debugTrace {
							trace("close: DB-file exclusive upgrade error: %v", err)
						}
					} else {
						isLastClient = ok
					}
				}
				if cpErr == nil && isLastClient {
					if debugTrace {
						trace("close: TRUNCATING WAL (cpErr=nil isLastClient=true) nBackfill=%d mxFrame=%d",
							p.wal.index.nBackfill.Load(), p.wal.authoritativeMxFrame())
					}
					// Forensic aid: BTREE_KEEP_WAL_ON_CLOSE=1 keeps the exact WAL
					// bytes the close-time checkpoint consumed, so a post-mortem can
					// replay the backfill decision after truncation. Deliberately NOT
					// tied to debugtrace: trace logging serializes the hot paths
					// (logger mutex + write syscalls) and suppresses the very races
					// being hunted. One getenv + one file copy at close is inert.
					// O_EXCL: first close wins (the workload process), so a later
					// verifier open+close cannot clobber the evidence with its
					// already-truncated WAL.
					if os.Getenv("BTREE_KEEP_WAL_ON_CLOSE") == "1" {
						if walBytes, rerr := os.ReadFile(p.path + "-wal"); rerr == nil {
							if f, ferr := os.OpenFile(p.path+"-wal.pretruncate", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644); ferr == nil {
								_, _ = f.Write(walBytes)
								_ = f.Close()
							}
						}
					}
					p.wal.truncateFile()
				} else if debugTrace {
					trace("close: NOT truncating WAL (cpErr=%v isLastClient=%v)", cpErr, isLastClient)
				}
				return nil
			})
		}
		_ = p.wal.close(isLastClient)
	}

	p.writerCache.destroy()

	// Tear down the mmap reader before closing the file so the fd
	// remains valid during munmap. Matches SQLite's unixUnmapfile
	// ordering (os_unix.c:5562-5566, called before the fd close).
	if p.dbMmap != nil {
		_ = p.dbMmap.unmap()
		p.dbMmap = nil
	}

	if p.file != nil {
		err := p.file.Close()
		p.file = nil
		return err
	}
	return nil
}
