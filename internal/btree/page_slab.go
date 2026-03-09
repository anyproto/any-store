package btree

// pageSlab is a process-global pre-allocated pool for []byte page buffers.
// Modeled after SQLite's pcache1.c slab allocator (pcache1_g struct,
// pcache1Alloc, pcache1Free, pcache1UnderMemoryPressure).
//
// The slab pre-allocates a fixed number of page-sized buffers at Init time.
// Get() pops from the free list; when exhausted, it falls back to make()
// (overflow allocation). Put() returns buffers to the free list.
//
// underPressure is set when the free list drops below nReserve (10% + 1
// of the initial slab size), matching pcache1.c:350,389 bUnderPressure.
//
// Drift from SQLite:
//   - Uses [][]byte slice, not contiguous void* buffer (drift #7)
//   - Accepts all buffers back, no SQLITE_WITHIN range check (drift #8)
//   - Lazy init, not library init (drift #9)

import (
	"sync"
	"sync/atomic"
)

// defaultSlabPages is the default number of page buffers to pre-allocate
// when the slab is lazily initialized on first Open() call.
const defaultSlabPages = 2000

// pageSlab manages a pool of reusable []byte page buffers.
type pageSlab struct {
	mu            sync.Mutex
	freeList      [][]byte
	nTotal        int         // total buffers ever allocated (slab + overflow)
	nSlab         int         // number of pre-allocated slab buffers
	nOverflow     int         // number of overflow (heap) allocations
	nReserve      int         // pressure threshold: len(freeList) < nReserve => under pressure
	underPressure atomic.Bool // true when free list is below reserve
	pageSize      int
	initialized   bool
}

// globalPageSlab is the process-global singleton.
var globalPageSlab pageSlab

// Init pre-allocates nPages buffers of the given pageSize.
// If already initialized, this is a no-op.
// Matches sqlite3PCacheBufferSetup (pcache1.c:271-291).
func (s *pageSlab) Init(pageSize, nPages int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.initialized {
		return
	}
	s.pageSize = pageSize
	s.nSlab = nPages
	s.nTotal = nPages
	s.nReserve = nPages/10 + 1 // matches pcache1.c:279
	s.freeList = make([][]byte, nPages)
	for i := range nPages {
		s.freeList[i] = make([]byte, pageSize)
	}
	s.underPressure.Store(false)
	s.initialized = true
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
	// Overflow: allocate from heap
	s.nOverflow++
	s.nTotal++
	pageSize := s.pageSize
	s.mu.Unlock()
	return make([]byte, pageSize)
}

// Put returns a buffer to the slab's free list.
// Matches pcache1Free (pcache1.c:379-406).
func (s *pageSlab) Put(buf []byte) {
	s.mu.Lock()
	s.freeList = append(s.freeList, buf)
	// Update pressure: pcache1.c:389 — clear if freeList refills above reserve
	if len(s.freeList) >= s.nReserve {
		s.underPressure.Store(false)
	}
	s.mu.Unlock()
}

// UnderPressure returns true when the free list is below the reserve threshold.
// Matches pcache1UnderMemoryPressure (pcache1.c:518-524).
func (s *pageSlab) UnderPressure() bool {
	return s.underPressure.Load()
}

// Reset clears the slab state. Used only in tests.
func (s *pageSlab) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.freeList = nil
	s.nTotal = 0
	s.nSlab = 0
	s.nOverflow = 0
	s.nReserve = 0
	s.underPressure.Store(false)
	s.pageSize = 0
	s.initialized = false
}

// ConfigPageCache initializes the global page slab with the given page size
// and number of pages. This mirrors sqlite3_config(SQLITE_CONFIG_PAGECACHE).
// Must be called before opening any databases, or the slab will be lazily
// initialized with defaults on the first Open() call.
func ConfigPageCache(pageSize, nPages int) {
	globalPageSlab.Init(pageSize, nPages)
}
