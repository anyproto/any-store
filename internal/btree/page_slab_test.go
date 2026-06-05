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

func TestPageSlab_OverflowBuffersCapped(t *testing.T) {
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

	// Free list should be capped at nSlab=5 (overflow buffers discarded)
	s.mu.Lock()
	freeCount := len(s.freeList)
	s.mu.Unlock()
	if freeCount != 5 {
		t.Fatalf("freeList should be capped at nSlab=5, got %d", freeCount)
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

// TestAllocPageBuffer_RejectsUndersizedPooledBuffer is a deterministic
// regression for the cross-test page-buffer-pool pollution flake: a test that
// opens a DB at a small page size (e.g. 1024) leaves 1024-cap buffers in the
// process-global pageBufferPool; a later 4096-page test that reaches
// allocPageBuffer without first calling resetPageBufferPool (newPager-based
// pager tests, backup_test.go's bare-Open dst) then draws the undersized buffer
// and either panics in wal.readFrame on buf[:4096] (slice bounds out of range
// with capacity 1024) or reads short and reports "database is corrupt". Under
// `go test -shuffle=on` this is order- AND GC/sync.Pool-timing-dependent.
func TestAllocPageBuffer_RejectsUndersizedPooledBuffer(t *testing.T) {
	// Isolate from any pool state leaked by other tests, and restore on exit.
	resetPageBufferPool()
	t.Cleanup(resetPageBufferPool)

	// Simulate a smaller-page-size predecessor leaving an undersized buffer in
	// the shared pool.
	pageBufferPool.Put(make([]byte, 1024))

	const pageSize = 4096
	for i := 0; i < 8; i++ {
		buf := allocPageBuffer(pageSize, false)
		if len(buf) != pageSize {
			t.Fatalf("allocPageBuffer returned len=%d, want %d (undersized buffer leaked from pool)", len(buf), pageSize)
		}
		// The caller slices buf[:pageSize]; with a too-small buffer this panics.
		_ = buf[:pageSize]
	}

	// An oversized pooled buffer (e.g. from a 65536-page predecessor) must be
	// usable too, sliced down to the requested page size.
	resetPageBufferPool()
	pageBufferPool.Put(make([]byte, 65536))
	buf := allocPageBuffer(pageSize, false)
	if len(buf) != pageSize {
		t.Fatalf("oversized pooled buffer: got len=%d, want %d", len(buf), pageSize)
	}
}

// TestAllocPageBuffer_SlabOverflowUndersized covers the useSlab=true overflow
// path on an uninitialized global slab (reachable only via tests that call
// newPcache(pageSize, n, true) directly; production rejects UsePageSlab on an
// uninitialized slab at db.Open). Get() on an uninit slab falls to
// pageBufferPool, which may hold an undersized buffer; allocPageBuffer must
// still return a correctly sized buffer rather than a zero/short one.
func TestAllocPageBuffer_SlabOverflowUndersized(t *testing.T) {
	globalPageSlab.Reset() // uninitialized: pageSize==0, freeList==nil
	t.Cleanup(globalPageSlab.Reset)
	resetPageBufferPool()
	t.Cleanup(resetPageBufferPool)

	pageBufferPool.Put(make([]byte, 1024))

	const pageSize = 4096
	buf := allocPageBuffer(pageSize, true)
	if len(buf) != pageSize {
		t.Fatalf("slab-overflow allocPageBuffer returned len=%d, want %d", len(buf), pageSize)
	}
	_ = buf[:pageSize]
}
