package btree

// Page buffer allocation has two modes:
//
// 1. Default (no slab): Uses sync.Pool for page-sized []byte buffers, matching
//    SQLite's default malloc-based page cache allocation. Buffers are GC'd when
//    not in use. No memory pressure tracking.
//
// 2. Slab mode (opt-in per DB via UsePageSlab, after ConfigPageCache): Pre-allocates
//    one buffer of nPages pages and hands out page-sized slices of it. Modeled
//    after SQLite's pcache1.c slab allocator (pcache1_g struct, pcache1Alloc,
//    pcache1Free, pcache1UnderMemoryPressure). When the slab is exhausted,
//    overflow buffers come from the sync.Pool, then make(). UnderPressure
//    triggers admission control and immediate eviction.
//
// All page buffer allocation should go through allocPageBuffer/freePageBuffer,
// which dispatch to the appropriate mode.

import (
	"math"
	"sync"
	"sync/atomic"
	"unsafe"
)

// pageSlab manages a pool of reusable []byte page buffers.
type pageSlab struct {
	mu            sync.Mutex
	freeList      [][]byte
	nTotal        int         // total buffers ever allocated (slab + overflow)
	nSlab         int         // number of pre-allocated slab buffers
	nOverflow     int         // number of overflow (heap) allocations
	pageSize      int         // immutable after Init; safe to read after initialized.Load() == true
	nReserve      int         // pressure threshold: len(freeList) < nReserve => under pressure
	underPressure atomic.Bool // true when free list is below reserve
	initialized   atomic.Bool // set last in Init(); acts as release barrier for pageSize
	slab          []byte      // backing array of the slab buffers; Put retains only slices of it
}

// globalPageSlab is the process-global singleton. Only initialized when slab
// mode is explicitly enabled via ConfigPageCache and opted into per DB with UsePageSlab.
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
// resolved once at open (btree.Open): true only when the database opted in
// and the slab serves its page size.
//
// The slab serves a request only when the request is for the slab's own slot
// size; any other size takes the default allocator, at the requested size.
// This is the per-request guard of pcache1Alloc (pcache1.c:344,
// `if( nByte<=pcache1.szSlot )`, else sqlite3Malloc(nByte) at 361): a slab
// configured for another page size, or with no slots, is never the source of a
// wrong-sized page. Go asks for equality where C asks for "fits": C compares
// the whole allocation, page plus header and extra bytes (szAlloc,
// pcache1.c:787), with a slot it addresses by pointer, so any slot at least
// that large serves; a Go page buffer is a slice whose length is the page
// size, and a larger slot would have to be cut down on Get and restored on Put.
// DRIFT: the slab serves a request of exactly its slot size, C one that fits See docs/btree/NOTES.md#drift-2026-06-25-22-slab-get-has-no-per-request-size-guard-and-its-initialized-pagesize-va
func allocPageBuffer(pageSize int, useSlab bool) []byte {
	if useSlab && globalPageSlab.serves(pageSize) {
		return globalPageSlab.Get()
	}
	if buf, ok := pageBufferPool.Get().([]byte); ok {
		return buf
	}
	return make([]byte, pageSize)
}

// freePageBuffer returns a buffer to the slab or sync.Pool. In slab mode a
// buffer that did not come out of the slab (an overflow buffer, or every
// buffer when the slab does not serve this page size) goes to the sync.Pool:
// pageSlab.Put keeps only slices of its own backing array.
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

// serves reports whether the slab hands out buffers of pageSize bytes. False
// for an empty slab (pageSize 0). s.pageSize is immutable once the slab is
// initialized, which btree.Open checks before it resolves useSlab.
func (s *pageSlab) serves(pageSize int) bool {
	return s.pageSize != 0 && pageSize == s.pageSize
}

// slabUsable reports whether Init allocates slots for these arguments: a slot
// of at least MinPageSize bytes, at least one of them, and a slab whose byte
// size an int can hold. The first two are SQLite's rule, sqlite3MallocInit's
// `szPage<512 || nPage<=0` (malloc.c:165-168), which clears the configured
// buffer; the third is Go's, since Go allocates the slab where C is handed
// one. A slot is larger than a slice header, so the free list's size fits
// whenever the slab's does.
func slabUsable(pageSize, nPages int) bool {
	return pageSize >= MinPageSize && nPages > 0 && nPages <= math.MaxInt/pageSize
}

// Init allocates one nPages*pageSize buffer and fills the free list with its
// page slices. If already initialized, this is a no-op: the first call's
// arguments stay, whatever a later call passes.
// Matches sqlite3PCacheBufferSetup (pcache1.c:271-291). Arguments slabUsable
// rejects leave an initialized slab with no slots and slot size 0, as SQLite
// leaves a configuration sqlite3MallocInit cleared (pPage = 0, szPage = 0):
// sqlite3PCacheBufferSetup then sets no slots (pcache1.c:274-275), and every
// allocation takes the default allocator.
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
	if !slabUsable(pageSize, nPages) {
		pageSize, nPages = 0, 0
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
	// One allocation carved into page slices, as pcache1.c:283-288 carves
	// pBuf by szSlot. A make() per page costs an allocator refill every two
	// pages (4 KiB is a two-object size class) and leaves nPages heap objects
	// for every GC cycle to mark; one object is allocated and marked once.
	// The three-index slice caps each buffer at pageSize so an append can
	// never reach its neighbour.
	s.slab = make([]byte, nPages*pageSize)
	s.freeList = make([][]byte, nPages)
	for i := range nPages {
		s.freeList[i] = s.slab[i*pageSize : (i+1)*pageSize : (i+1)*pageSize]
	}
	s.underPressure.Store(false)
	s.initialized.Store(true)
}

// Get returns a page buffer from the slab. If the free list is empty, takes
// one from the sync.Pool or the heap (overflow).
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
// are retained. Overflow buffers (heap-allocated when the slab is exhausted)
// recycle through sync.Pool instead. An overflow buffer admitted to the free
// list would displace a slab page: the slab is one backing array, so a slice
// the GC drops from the pool stays resident but unusable for the life of the
// process. The origin check also keeps underPressure accurate: the flag only
// clears when actual slab buffers are returned, as pcache1Free does.
func (s *pageSlab) Put(buf []byte) {
	if buf == nil {
		return
	}
	s.mu.Lock()
	switch {
	case !s.within(buf):
		pageBufferPool.Put(buf)
	case len(s.freeList) < s.nSlab:
		s.freeList = append(s.freeList, buf)
	default:
		// A slab buffer arriving at a full list is a double Put (C asserts
		// nFreeSlot <= nSlot). Dropping it keeps the page with its one owner;
		// pooling it would hand a live page to a second one. A double Put at
		// a non-full list is not detectable here.
	}
	// Update pressure: pcache1.c:389 — clear if freeList refills above reserve
	if len(s.freeList) >= s.nReserve {
		s.underPressure.Store(false)
	}
	s.mu.Unlock()
}

// within reports whether buf is a slice of the slab's backing array.
// Matches SQLITE_WITHIN(p, pcache1.pStart, pcache1.pEnd) (pcache1.c:381).
// Addresses are compared as integers; heap objects do not move, and s.slab
// keeps the array alive, so the range stays valid while the slab is initialized.
func (s *pageSlab) within(buf []byte) bool {
	if len(buf) == 0 || len(s.slab) == 0 {
		return false
	}
	p := uintptr(unsafe.Pointer(&buf[0]))
	start := uintptr(unsafe.Pointer(&s.slab[0]))
	return p >= start && p < start+uintptr(len(s.slab))
}

// UnderPressure returns true when the free list is below the reserve threshold.
// Matches pcache1UnderMemoryPressure (pcache1.c:518-524).
// DRIFT: UnderPressure drops C's sqlite3HeapNearlyFull() no-slab fallback branch See docs/btree/NOTES.md#drift-69-underpressure-drops-heap-nearly-full-fallback
func (s *pageSlab) UnderPressure() bool {
	return s.underPressure.Load()
}

// Reset clears the slab state. Used only in tests.
func (s *pageSlab) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Store initialized false FIRST (under lock) to prevent concurrent
	// Initialized() from reading stale pageSize during reset.
	s.initialized.Store(false)
	s.freeList = nil
	s.slab = nil
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
// pageSize must be the page size of the databases that opt in. A database of
// another page size opens and runs as if it had not opted in: Open resolves
// its useSlab to false, and allocPageBuffer would refuse it the slab anyway
// (pcache1Alloc's fallback). So does every database when the arguments leave
// no slots: a pageSize below MinPageSize or an nPages below 1, as
// sqlite3MallocInit clears such a configuration (malloc.c:165-168), or a slab
// too large for an int to describe. The first call configures the slab for
// the life of the process; later calls are no-ops.
//
// By default (without calling this and setting UsePageSlab), page buffers are
// allocated via sync.Pool (like SQLite's default malloc mode). Calling this
// enables slab mode: a soft cap on total page cache memory across all open
// databases. When the slab is exhausted, overflow buffers come from the
// sync.Pool, then make(), and the UnderPressure flag triggers admission
// control and immediate eviction.
//
// Example: ConfigPageCache(4096, 5000) pre-allocates ~20MB of page buffers.
// DRIFT: pageSlab.Init/ConfigPageCache idempotent & no-disable vs C re-configurable setup See docs/btree/NOTES.md#drift-68-pageslab-and-configpagecache-idempotent-versus-reconfigurabl
func ConfigPageCache(pageSize, nPages int) {
	globalPageSlab.Init(pageSize, nPages)
}
