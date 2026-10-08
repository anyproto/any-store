package btree

import (
	"sync"
	"testing"
)

func TestPageSlab_InitAndGet(t *testing.T) {
	var s pageSlab
	defer s.Reset()
	s.Init(4096, 10)

	// Get all 10 pre-allocated buffers
	bufs := make([][]byte, 10)
	for i := range bufs {
		bufs[i] = s.Get()
		if len(bufs[i]) != 4096 {
			t.Fatalf("buffer %d: got len %d, want 4096", i, len(bufs[i]))
		}
	}

	// Free list should be empty now
	s.mu.Lock()
	freeCount := len(s.freeList)
	s.mu.Unlock()
	if freeCount != 0 {
		t.Fatalf("freeList should be empty after getting all buffers, got %d", freeCount)
	}

	// Verify nTotal and nSlab
	s.mu.Lock()
	if s.nSlab != 10 {
		t.Fatalf("nSlab: got %d, want 10", s.nSlab)
	}
	if s.nTotal != 10 {
		t.Fatalf("nTotal: got %d, want 10", s.nTotal)
	}
	s.mu.Unlock()
}

func TestPageSlab_OverflowAndPressure(t *testing.T) {
	var s pageSlab
	defer s.Reset()
	s.Init(4096, 10)

	// nReserve should be 10/10 + 1 = 2
	s.mu.Lock()
	if s.nReserve != 2 {
		t.Fatalf("nReserve: got %d, want 2", s.nReserve)
	}
	s.mu.Unlock()

	// Get all slab buffers
	for range 10 {
		s.Get()
	}

	// Now should be under pressure (freeList empty < nReserve=2)
	if !s.UnderPressure() {
		t.Fatal("should be under pressure with empty free list")
	}

	// Get one more — should overflow
	overflow := s.Get()
	if len(overflow) != 4096 {
		t.Fatalf("overflow buffer: got len %d, want 4096", len(overflow))
	}

	s.mu.Lock()
	if s.nOverflow != 1 {
		t.Fatalf("nOverflow: got %d, want 1", s.nOverflow)
	}
	if s.nTotal != 11 {
		t.Fatalf("nTotal after overflow: got %d, want 11", s.nTotal)
	}
	s.mu.Unlock()

	if !s.UnderPressure() {
		t.Fatal("should still be under pressure after overflow")
	}
}

func TestPageSlab_PutClearsPressure(t *testing.T) {
	var s pageSlab
	defer s.Reset()
	s.Init(4096, 10)
	// nReserve = 2

	// Get all buffers
	bufs := make([][]byte, 10)
	for i := range bufs {
		bufs[i] = s.Get()
	}

	if !s.UnderPressure() {
		t.Fatal("should be under pressure with empty free list")
	}

	// Put back 1 buffer — still under pressure (1 < nReserve=2)
	s.Put(bufs[0])
	if !s.UnderPressure() {
		t.Fatal("should still be under pressure with 1 buffer (need 2)")
	}

	// Put back another — now at nReserve, pressure should clear
	s.Put(bufs[1])
	if s.UnderPressure() {
		t.Fatal("pressure should be cleared with 2 buffers (nReserve=2)")
	}

	// Put back the rest
	for i := 2; i < len(bufs); i++ {
		s.Put(bufs[i])
	}

	s.mu.Lock()
	freeCount := len(s.freeList)
	s.mu.Unlock()
	if freeCount != 10 {
		t.Fatalf("freeList should have 10 after putting all back, got %d", freeCount)
	}

	if s.UnderPressure() {
		t.Fatal("should not be under pressure with full free list")
	}
}

func TestPageSlab_ConcurrentGetPut(t *testing.T) {
	var s pageSlab
	defer s.Reset()
	s.Init(4096, 100)

	const goroutines = 8
	const opsPerGoroutine = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := range goroutines {
		go func(id int) {
			defer wg.Done()
			var held [][]byte
			for i := range opsPerGoroutine {
				if i%3 == 0 && len(held) > 0 {
					// Return a buffer
					s.Put(held[len(held)-1])
					held = held[:len(held)-1]
				} else {
					// Get a buffer
					buf := s.Get()
					if len(buf) != 4096 {
						t.Errorf("goroutine %d op %d: got len %d", id, i, len(buf))
						return
					}
					held = append(held, buf)
				}
			}
			// Return all held buffers
			for _, buf := range held {
				s.Put(buf)
			}
		}(g)
	}

	wg.Wait()
}

func TestPageSlab_InitIdempotent(t *testing.T) {
	var s pageSlab
	defer s.Reset()
	s.Init(4096, 10)
	s.Init(8192, 20) // second call should be no-op

	s.mu.Lock()
	if s.pageSize != 4096 {
		t.Fatalf("pageSize should remain 4096, got %d", s.pageSize)
	}
	if s.nSlab != 10 {
		t.Fatalf("nSlab should remain 10, got %d", s.nSlab)
	}
	s.mu.Unlock()
}

func TestPageSlab_OverflowBuffersGoToPool(t *testing.T) {
	var s pageSlab
	defer s.Reset()
	s.Init(4096, 5) // nSlab=5, nReserve=5/10+1=1

	// Drain all slab buffers
	slabBufs := make([][]byte, 5)
	for i := range slabBufs {
		slabBufs[i] = s.Get()
	}

	// Get 3 overflow buffers
	overflowBufs := make([][]byte, 3)
	for i := range overflowBufs {
		overflowBufs[i] = s.Get()
	}

	s.mu.Lock()
	if s.nOverflow != 3 {
		t.Fatalf("nOverflow: got %d, want 3", s.nOverflow)
	}
	s.mu.Unlock()

	// Return all 8 buffers (5 slab + 3 overflow)
	for _, b := range slabBufs {
		s.Put(b)
	}
	for _, b := range overflowBufs {
		s.Put(b)
	}

	// Only the 5 slab buffers are retained; the overflow buffers went to the pool
	s.mu.Lock()
	freeCount := len(s.freeList)
	s.mu.Unlock()
	if freeCount != 5 {
		t.Fatalf("freeList should hold the 5 slab buffers, got %d", freeCount)
	}

	// Pressure should be cleared (5 >= nReserve=1)
	if s.UnderPressure() {
		t.Fatal("should not be under pressure after returning slab buffers")
	}
}

func TestPageSlab_PutNilIsNoOp(t *testing.T) {
	var s pageSlab
	defer s.Reset()
	s.Init(4096, 5)

	s.mu.Lock()
	before := len(s.freeList)
	s.mu.Unlock()

	s.Put(nil) // should be a no-op

	s.mu.Lock()
	after := len(s.freeList)
	s.mu.Unlock()

	if before != after {
		t.Fatalf("Put(nil) changed freeList length: before=%d, after=%d", before, after)
	}
}

func TestPageSlab_PressureEdgeCases(t *testing.T) {
	// Test with nPages=1 => nReserve = 1/10 + 1 = 1
	var s pageSlab
	defer s.Reset()
	s.Init(4096, 1)

	s.mu.Lock()
	if s.nReserve != 1 {
		t.Fatalf("nReserve: got %d, want 1", s.nReserve)
	}
	s.mu.Unlock()

	// Initially not under pressure (freeList=1 >= nReserve=1)
	if s.UnderPressure() {
		t.Fatal("should not be under pressure initially")
	}

	// Get the one buffer
	buf := s.Get()
	if !s.UnderPressure() {
		t.Fatal("should be under pressure with empty free list")
	}

	// Put it back
	s.Put(buf)
	if s.UnderPressure() {
		t.Fatal("should not be under pressure after putting buffer back")
	}
}

// Init makes one allocation for the page memory and one for the free list,
// whatever nPages is: the slab is a single buffer carved into page slices.
func TestPageSlab_InitAllocatesOnce(t *testing.T) {
	const nPages = 1024
	s := new(pageSlab) // allocated outside the measured closure
	allocs := testing.AllocsPerRun(10, func() {
		s.Reset()
		s.Init(4096, nPages)
	})
	if allocs != 2 {
		t.Fatalf("Init(4096, %d): %v allocations, want 2 (slab + free list)", nPages, allocs)
	}
}

// Slab buffers are disjoint pages of one backing array, each capped at
// pageSize, so writing or appending to one never touches its neighbour.
func TestPageSlab_BuffersAreDisjointPages(t *testing.T) {
	var s pageSlab
	defer s.Reset()
	const pageSize, nPages = 512, 8
	s.Init(pageSize, nPages)

	bufs := make([][]byte, nPages)
	for i := range bufs {
		bufs[i] = s.Get()
		if len(bufs[i]) != pageSize || cap(bufs[i]) != pageSize {
			t.Fatalf("buffer %d: len %d cap %d, want %d/%d", i, len(bufs[i]), cap(bufs[i]), pageSize, pageSize)
		}
		for j := range bufs[i] {
			bufs[i][j] = byte(i + 1)
		}
	}
	// Appending past the cap must reallocate, not spill into the next page.
	// Page 0 (popped last) is the one a two-index slice would give the whole
	// array as capacity; the last page has cap == pageSize either way.
	first := bufs[len(bufs)-1]
	grown := append(first, 0xFF)
	if &grown[0] == &first[0] {
		t.Fatal("append grew in place: buffer cap exceeds pageSize")
	}
	for i := range bufs {
		for j, b := range bufs[i] {
			if b != byte(i+1) {
				t.Fatalf("buffer %d byte %d = %#x, want %#x: pages overlap", i, j, b, byte(i+1))
			}
		}
	}
	for _, buf := range bufs {
		s.Put(buf)
	}
}

func BenchmarkPageSlabInit(b *testing.B) {
	const nPages = (128 << 20) / 4096
	b.ReportAllocs()
	for b.Loop() {
		var s pageSlab
		s.Init(4096, nPages)
	}
}

// Overflow buffers never enter the free list, even when they are returned
// before the slab's own buffers: an admitted overflow buffer would displace a
// slab page that nothing can reclaim.
func TestPageSlab_PutKeepsOnlySlabBuffers(t *testing.T) {
	var s pageSlab
	defer s.Reset()
	const nPages = 4
	s.Init(4096, nPages)

	slabBufs := make([][]byte, nPages)
	for i := range slabBufs {
		slabBufs[i] = s.Get()
	}
	overflowBufs := make([][]byte, 2)
	for i := range overflowBufs {
		overflowBufs[i] = s.Get()
		if s.within(overflowBufs[i]) {
			t.Fatalf("overflow buffer %d lies inside the slab", i)
		}
	}

	for _, b := range overflowBufs {
		s.Put(b)
	}
	s.mu.Lock()
	n := len(s.freeList)
	s.mu.Unlock()
	if n != 0 {
		t.Fatalf("free list holds %d overflow buffers, want 0", n)
	}
	if !s.UnderPressure() {
		t.Fatal("returning overflow buffers must not clear pressure")
	}

	for _, b := range slabBufs {
		s.Put(b)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.freeList) != nPages {
		t.Fatalf("free list holds %d buffers, want %d", len(s.freeList), nPages)
	}
	for i, b := range s.freeList {
		if !s.within(b) {
			t.Fatalf("free list entry %d is not a slab buffer", i)
		}
	}
}

// A slab buffer returned when the free list is already full is a double Put.
// It is dropped, not pooled: a pooled copy would hand the page to a second
// owner through an overflow Get or a non-slab DB.
func TestPageSlab_DoublePutAtFullListIsDropped(t *testing.T) {
	var s pageSlab
	defer s.Reset()
	resetPageBufferPool()
	s.Init(4096, 2)

	a, b := s.Get(), s.Get()
	s.Put(a)
	s.Put(b)
	s.Put(a) // double Put at a full list

	s.mu.Lock()
	n := len(s.freeList)
	s.mu.Unlock()
	if n != 2 {
		t.Fatalf("free list holds %d buffers, want 2", n)
	}
	if got, _ := pageBufferPool.Get().([]byte); got != nil && s.within(got) {
		t.Fatal("double Put placed a slab buffer in the pool")
	}
}
