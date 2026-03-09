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
