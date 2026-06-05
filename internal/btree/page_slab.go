package btree

// Page buffer allocation has two modes:
//
// 1. Default (no slab): Uses sync.Pool for page-sized []byte buffers, matching
//    SQLite's default malloc-based page cache allocation. Buffers are GC'd when
//    not in use. No memory pressure tracking.
//
// 2. Slab mode (opt-in via SlabPages option or ConfigPageCache): Pre-allocates
//    a fixed number of page-sized buffers. Modeled after SQLite's pcache1.c slab
//    allocator (pcache1_g struct, pcache1Alloc, pcache1Free,
//    pcache1UnderMemoryPressure). When the slab is exhausted, falls back to
//    make() (overflow). UnderPressure triggers admission control and immediate
//    eviction.
//
// All page buffer allocation should go through allocPageBuffer/freePageBuffer,
// which dispatch to the appropriate mode.

import (
	"sync"
	"sync/atomic"
)

// pageSlab manages a pool of reusable []byte page buffers.
type pageSlab struct {
	mu            sync.Mutex
	freeList      [][]byte
	nTotal        int         // total buffers ever allocated (slab + overflow)
	nSlab         int         // number of pre-allocated slab buffers
	nOverflow     int         // number of overflow (heap) allocations
	nReserve      int         // pressure threshold: len(freeList) < nReserve => under pressure
	underPressure atomic.Bool // true when free list is below reserve
	pageSize      int         // immutable after Init; safe to read after initialized.Load() == true
	initialized   atomic.Bool // set last in Init(); acts as release barrier for pageSize
}

// globalPageSlab is the process-global singleton. Only initialized when slab
// mode is explicitly enabled via SlabPages option or ConfigPageCache.
var globalPageSlab pageSlab

// pageBufferPool is a sync.Pool for page-sized []byte buffers. Used as the
// default allocator when the slab is not configured.
var pageBufferPool sync.Pool

// pageBufferPoolSize tracks the page size that pageBufferPool is initialized for.
// 0 means uninitialized. Set via initPageBufferPool on first db.Open.
var pageBufferPoolSize atomic.Uint32

// initPageBufferPool sets the page buffer pool's page size. Returns an error
// if the pool was already initialized with a different size. All databases in
// a process must use the same page size. Call resetPageBufferPool() first if
// you need to switch page sizes (e.g., between tests).
func initPageBufferPool(pageSize uint32) error {
	for {
		cur := pageBufferPoolSize.Load()
		if cur == pageSize {
			return nil
		}
		if cur != 0 {
			return ErrPageBufferPoolSizeMismatch
		}
		if pageBufferPoolSize.CompareAndSwap(0, pageSize) {
			return nil
		}
	}
}

// resetPageBufferPool clears the page buffer pool and its size tracking.
// This allows re-initialization with a different page size.
// Must only be called when no databases are open (used in tests).
func resetPageBufferPool() {
	pageBufferPool = sync.Pool{}
	pageBufferPoolSize.Store(0)
}

// allocPageBuffer returns a page-sized buffer. useSlab is a local bool
// resolved once at pcache/pager creation time — no global reads on hot path.
//
// The sync.Pool path validates the pooled buffer's capacity against the
// requested pageSize and discards any buffer that is too small. In production
// this is a no-op (initPageBufferPool enforces a single page size per process,
// so every pooled buffer is the right size), but it makes the allocator robust
// against a stale buffer of a different page size lingering in the
// process-global pool — e.g. across tests that open DBs at multiple page sizes
// and reach this path without first calling resetPageBufferPool (newPager-based
// pager tests, backup_test.go's bare-Open dst). Without the guard such a buffer
// is sliced to [:pageSize] by callers (wal.readFrame) and panics with
// "slice bounds out of range", or reads short and surfaces as "database is
// corrupt" — an order- and GC-timing-dependent flake under `go test -shuffle`.
func allocPageBuffer(pageSize int, useSlab bool) []byte {
	if useSlab {
		buf := globalPageSlab.Get()
		if cap(buf) >= pageSize {
			return buf[:pageSize]
		}
		// Slab overflow handed back an undersized buffer (only reachable on an
		// uninitialized slab, which production rejects at db.Open). Allocate a
		// correctly-sized buffer instead of returning a short one.
		return make([]byte, pageSize)
	}
	if buf, ok := pageBufferPool.Get().([]byte); ok && cap(buf) >= pageSize {
		return buf[:pageSize]
	}
	return make([]byte, pageSize)
}

// freePageBuffer returns a buffer to the slab or sync.Pool.
//
// BEGIN ENCRYPTION
// Note: pageBufferPool stores []byte directly, which causes a 24-byte eface
// box on every Put (SA6002). We accept this cost: the alternative (*[]byte)
// requires every caller to track a handle and changes the whole API.
// Hot paths that run codec encrypt/decrypt should use a caller-owned scratch
// (p.codecScratch, pcache.codecScratch, etc.) instead of this pool to bypass
// the box entirely.
// END ENCRYPTION
func freePageBuffer(buf []byte, useSlab bool) {
	if buf == nil {
		return
	}
	if useSlab {
		globalPageSlab.Put(buf)
		return
	}
	pageBufferPool.Put(buf)
}

// Init pre-allocates nPages buffers of the given pageSize.
// If already initialized, this is a no-op.
// Matches sqlite3PCacheBufferSetup (pcache1.c:271-291).
// DRIFT: pageSlab.Init/ConfigPageCache idempotent & no-disable vs C re-configurable setup See docs/btree/NOTES.md#drift-68-pageslab-and-configpagecache-idempotent-versus-reconfigurabl
func (s *pageSlab) Init(pageSize, nPages int) {
	if s.initialized.Load() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.initialized.Load() {
		return
	}
	s.pageSize = pageSize
	s.nSlab = nPages
	s.nTotal = nPages
	// Matches pcache1.c:279: pcache1.nReserve = n>90 ? 10 : (n/10 + 1)
	if nPages > 90 {
		s.nReserve = 10
	} else {
		s.nReserve = nPages/10 + 1
	}
	s.freeList = make([][]byte, nPages)
	for i := range nPages {
		s.freeList[i] = make([]byte, pageSize)
	}
	s.underPressure.Store(false)
	s.initialized.Store(true)
}

// Get returns a page buffer from the slab. If the free list is empty,
// allocates a new buffer from the heap (overflow).
// Matches pcache1Alloc (pcache1.c:341-374).
func (s *pageSlab) Get() []byte {
	s.mu.Lock()
	n := len(s.freeList)
	if n > 0 {
		buf := s.freeList[n-1]
		s.freeList = s.freeList[:n-1]
		// Update pressure: pcache1.c:350 — set if freeList drops below reserve
		if len(s.freeList) < s.nReserve {
			s.underPressure.Store(true)
		}
		s.mu.Unlock()
		return buf
	}
	// Overflow: try sync.Pool first, then heap. sync.Pool lets the GC
	// reclaim overflow buffers in batches and reuse them across cycles,
	// reducing allocation pressure during sustained slab exhaustion.
	s.nOverflow++
	s.nTotal++
	pageSize := s.pageSize
	s.mu.Unlock()
	if buf, ok := pageBufferPool.Get().([]byte); ok {
		return buf
	}
	return make([]byte, pageSize)
}

// Put returns a buffer to the slab's free list.
// Matches pcache1Free (pcache1.c:379-406).
//
// Like SQLite's SQLITE_WITHIN check (pcache1.c:381), only slab-origin buffers
// are retained. Overflow buffers (heap-allocated when slab is exhausted) are
// discarded so the GC can collect them. This prevents unbounded free list
// growth and keeps underPressure semantics accurate: the flag only clears
// when actual slab buffers are returned, not when overflow buffers inflate
// the list.
func (s *pageSlab) Put(buf []byte) {
	if buf == nil {
		return
	}
	s.mu.Lock()
	// Cap the free list at the original slab size. Buffers beyond nSlab are
	// overflow allocations — route them to sync.Pool for reuse instead of
	// dropping them for GC. This reduces allocation pressure when the slab
	// is persistently exhausted (many DBs, heavy writes).
	if len(s.freeList) < s.nSlab {
		s.freeList = append(s.freeList, buf)
	} else {
		pageBufferPool.Put(buf)
	}
	// Update pressure: pcache1.c:389 — clear if freeList refills above reserve
	if len(s.freeList) >= s.nReserve {
		s.underPressure.Store(false)
	}
	s.mu.Unlock()
}

// UnderPressure returns true when the free list is below the reserve threshold.
// Matches pcache1UnderMemoryPressure (pcache1.c:518-524).
// DRIFT: UnderPressure drops C's sqlite3HeapNearlyFull() no-slab fallback branch See docs/btree/NOTES.md#drift-69-underpressure-drops-heap-nearly-full-fallback
func (s *pageSlab) UnderPressure() bool {
	return s.underPressure.Load()
}

// Initialized returns true if the slab has been initialized with the given
// page size. If pageSize is 0, it only checks whether the slab is initialized
// at all. This is used by pcache.initBulk() and create() to avoid pulling
// buffers of the wrong size from a slab initialized for a different page size.
// DRIFT: Initialized() reads initialized (atomic.Bool) + immutable pageSize lock-free, matching SQL See docs/btree/NOTES.md#old-drift-initialized-lock-free-atomic
func (s *pageSlab) Initialized(pageSize int) bool {
	if !s.initialized.Load() {
		return false
	}
	// pageSize is immutable after Init; safe to read after initialized.Load()
	// returns true (atomic acquire guarantees visibility).
	return pageSize == 0 || s.pageSize == pageSize
}

// Reset clears the slab state. Used only in tests.
func (s *pageSlab) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Store initialized false FIRST (under lock) to prevent concurrent
	// Initialized() from reading stale pageSize during reset.
	s.initialized.Store(false)
	s.freeList = nil
	s.nTotal = 0
	s.nSlab = 0
	s.nOverflow = 0
	s.nReserve = 0
	s.underPressure.Store(false)
	s.pageSize = 0
}

// ConfigPageCache initializes the global page slab with the given page size
// and number of pages. This mirrors sqlite3_config(SQLITE_CONFIG_PAGECACHE).
// Must be called before opening any databases.
//
// By default (without calling this or setting SlabPages), page buffers are
// allocated via sync.Pool (like SQLite's default malloc mode). Calling this
// enables slab mode: a soft cap on total page cache memory across all open
// databases. When the slab is exhausted, overflow allocations use make() but
// the UnderPressure flag triggers admission control and immediate eviction.
//
// Example: ConfigPageCache(4096, 5000) pre-allocates ~20MB of page buffers.
// DRIFT: pageSlab.Init/ConfigPageCache idempotent & no-disable vs C re-configurable setup See docs/btree/NOTES.md#drift-68-pageslab-and-configpagecache-idempotent-versus-reconfigurabl
func ConfigPageCache(pageSize, nPages int) {
	globalPageSlab.Init(pageSize, nPages)
}
