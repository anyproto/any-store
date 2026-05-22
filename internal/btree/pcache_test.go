package btree

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPcacheCreateFetch(t *testing.T) {
	pc := newPcache(4096, 100, true)

	pg := pc.create(1, 2)
	require.NotNil(t, pg)
	assert.Equal(t, uint32(1), pg.pgno)
	assert.Equal(t, 1, pg.pinCount)
	assert.Len(t, pg.data, 4096)

	// Fetch the same page should return same object and increment pinCount
	pg2 := pc.fetch(1)
	require.NotNil(t, pg2)
	assert.Equal(t, uint32(1), pg2.pgno)
	assert.Equal(t, 2, pg.pinCount) // create(1) + fetch(1)

	// Fetch non-existent page
	pg3 := pc.fetch(999)
	assert.Nil(t, pg3)
}

func TestPcacheCreateExisting(t *testing.T) {
	pc := newPcache(4096, 100, true)

	pg1 := pc.create(5, 2)
	pg1.data[0] = 42
	pc.release(pg1)

	// Create the same page again should return existing
	pg2 := pc.create(5, 2)
	assert.Equal(t, uint8(42), pg2.data[0])
	pc.release(pg2)
}

func TestPcacheRelease(t *testing.T) {
	pc := newPcache(4096, 100, true)

	pg := pc.create(1, 2)
	assert.Equal(t, 1, pg.pinCount)
	pc.release(pg)
	assert.Equal(t, 0, pg.pinCount)

	// After release, page should be in LRU (clean)
	assert.Equal(t, 1, pc.nRecyclable)
}

func TestPcacheDirtyPages(t *testing.T) {
	pc := newPcache(4096, 100, true)

	pg1 := pc.create(1, 2)
	pg2 := pc.create(2, 2)
	pg3 := pc.create(3, 2)

	// No dirty pages initially
	assert.Empty(t, pc.dirtyPages())

	pc.makeDirty(pg1)
	pc.makeDirty(pg3)

	dirty := pc.dirtyPages()
	assert.Len(t, dirty, 2)
	assert.Equal(t, 2, pc.nDirty)

	// Making already-dirty page dirty again should be no-op
	pc.makeDirty(pg1)
	assert.Len(t, pc.dirtyPages(), 2)

	// Clean a page
	pc.makeClean(pg1)
	dirty = pc.dirtyPages()
	assert.Len(t, dirty, 1)
	assert.Equal(t, uint32(3), dirty[0].pgno)

	pc.release(pg1)
	pc.release(pg2)
	pc.release(pg3)
}

func TestPcacheMakeCleanPinned(t *testing.T) {
	pc := newPcache(4096, 100, true)

	pg := pc.create(1, 2)
	pc.makeDirty(pg)
	assert.True(t, pg.dirty)
	assert.Equal(t, 1, pc.nDirty)

	// makeClean while pinned: should remove from dirty list but NOT add to LRU
	pc.makeClean(pg)
	assert.False(t, pg.dirty)
	assert.Equal(t, 0, pc.nDirty)

	// Now release -> should go to LRU
	pc.release(pg)
	assert.Equal(t, pg.pinCount, 0)
}

func TestPcacheLRUEviction(t *testing.T) {
	pc := newPcache(4096, 3, true) // max 3 pages

	// Create and release 3 clean pages
	for i := uint32(1); i <= 3; i++ {
		pg := pc.create(i, 2)
		pc.release(pg)
	}
	assert.Equal(t, 3, pc.nPage)
	assert.Equal(t, 3, pc.nRecyclable)

	// Creating a 4th page should evict the oldest clean page (page 1)
	pg4 := pc.create(4, 2)
	assert.NotNil(t, pg4)
	assert.Nil(t, pc.hashFind(1)) // page 1 should be evicted
	assert.NotNil(t, pc.hashFind(2))
	assert.NotNil(t, pc.hashFind(3))
	assert.NotNil(t, pc.hashFind(4))
	pc.release(pg4)
}

func TestPcacheDirtyPagesNotEvicted(t *testing.T) {
	pc := newPcache(4096, 2, true) // max 2 pages

	pg1 := pc.create(1, 2)
	pc.makeDirty(pg1)
	pc.release(pg1)

	pg2 := pc.create(2, 2)
	pc.makeDirty(pg2)
	pc.release(pg2)

	// Both are dirty, cache is "full" but dirty pages won't be evicted
	pg3 := pc.create(3, 2)
	assert.NotNil(t, pg3)
	// All 3 should still be present (dirty pages can't be evicted)
	assert.NotNil(t, pc.hashFind(1))
	assert.NotNil(t, pc.hashFind(2))
	pc.release(pg3)
}

func TestPcacheDiscard(t *testing.T) {
	pc := newPcache(4096, 100, true)

	pg := pc.create(1, 2)
	pc.release(pg)

	pc.discard(1)
	assert.Nil(t, pc.hashFind(1))
	assert.Nil(t, pc.fetch(1))

	// Discard non-existent page should not panic
	pc.discard(999)
}

func TestPcacheDiscardDirty(t *testing.T) {
	pc := newPcache(4096, 100, true)

	pg := pc.create(1, 2)
	pc.makeDirty(pg)
	pc.release(pg)

	assert.Equal(t, 1, pc.nDirty)
	pc.discard(1)
	assert.Equal(t, 0, pc.nDirty)
	assert.Nil(t, pc.hashFind(1))
}

func TestPcacheClear(t *testing.T) {
	pc := newPcache(4096, 100, true)

	for i := uint32(1); i <= 10; i++ {
		pg := pc.create(i, 2)
		if i%2 == 0 {
			pc.makeDirty(pg)
		}
		pc.release(pg)
	}

	pc.clear()
	assert.Zero(t, pc.nPage)
	assert.Equal(t, 0, pc.nRecyclable)
	assert.Equal(t, 0, pc.nDirty)
	assert.Nil(t, pc.lruHead)
	assert.Nil(t, pc.lruTail)
	assert.Nil(t, pc.dirtyHead)
}

func TestPcacheTruncate(t *testing.T) {
	pc := newPcache(4096, 100, true)

	for i := uint32(1); i <= 10; i++ {
		pg := pc.create(i, 2)
		pc.release(pg)
	}

	pc.truncate(5)
	assert.Equal(t, 5, pc.nPage)
	for i := uint32(1); i <= 5; i++ {
		assert.NotNil(t, pc.hashFind(i))
	}
	for i := uint32(6); i <= 10; i++ {
		assert.Nil(t, pc.hashFind(i))
	}
}

func TestPcacheTruncateDirty(t *testing.T) {
	pc := newPcache(4096, 100, true)

	for i := uint32(1); i <= 5; i++ {
		pg := pc.create(i, 2)
		pc.makeDirty(pg)
		pc.release(pg)
	}

	pc.truncate(2)
	assert.Equal(t, 2, pc.nDirty)
	assert.NotNil(t, pc.hashFind(1))
	assert.NotNil(t, pc.hashFind(2))
	assert.Nil(t, pc.hashFind(3))
}

func TestPcacheFetchMovesFromLRU(t *testing.T) {
	pc := newPcache(4096, 100, true)

	pg := pc.create(1, 2)
	pc.release(pg)
	assert.Equal(t, 1, pc.nRecyclable)

	// Fetching should remove from LRU
	pg2 := pc.fetch(1)
	assert.NotNil(t, pg2)
	assert.Equal(t, 0, pc.nRecyclable)

	pc.release(pg2)
	assert.Equal(t, 1, pc.nRecyclable)
}

func TestPcacheDefaultCacheSize(t *testing.T) {
	pc := newPcache(4096, 0, true)
	assert.Equal(t, defaultCacheSize, pc.maxPages)

	pc2 := newPcache(4096, -1, true)
	assert.Equal(t, defaultCacheSize, pc2.maxPages)
}

func TestPcacheStressCallbackInvoked(t *testing.T) {
	pc := newPcache(4096, 3, true) // max 3 pages

	var stressCalled int
	var stressVictim uint32
	pc.xStress = func(p *page) error {
		stressCalled++
		stressVictim = p.pgno
		// Simulate what pagerStress does: write to WAL then makeClean
		pc.makeClean(p)
		return nil
	}

	// Fill cache with dirty pages and release them (unpinned)
	for i := uint32(1); i <= 3; i++ {
		pg := pc.create(i, 2)
		pc.makeDirty(pg)
		pc.release(pg)
	}
	assert.Equal(t, 3, pc.nDirty)
	assert.Equal(t, 0, pc.nRecyclable)

	// Creating a 4th page should trigger stress callback since no clean
	// pages are available for eviction
	pg4 := pc.create(4, 2)
	require.NotNil(t, pg4)
	assert.Equal(t, 1, stressCalled)
	assert.NotZero(t, stressVictim)
	pc.release(pg4)
}

func TestPcacheStressOnlyUnreferenced(t *testing.T) {
	pc := newPcache(4096, 3, true) // max 3 pages

	var stressCalled int
	pc.xStress = func(p *page) error {
		stressCalled++
		pc.makeClean(p)
		return nil
	}

	// Fill cache with dirty pages, but keep them ALL pinned
	pgs := make([]*page, 3)
	for i := uint32(1); i <= 3; i++ {
		pg := pc.create(i, 2)
		pc.makeDirty(pg)
		// Do NOT release — keep pinned
		pgs[i-1] = pg
	}

	// All dirty pages are pinned, stress callback should not find a victim
	pg4 := pc.create(4, 2)
	require.NotNil(t, pg4)
	assert.Equal(t, 0, stressCalled) // no victim found, no stress call
	assert.Equal(t, 4, pc.nPage)     // cache grows beyond maxPages

	// Cleanup
	pc.release(pg4)
	for _, pg := range pgs {
		pc.release(pg)
	}
}

func TestPcacheNoStressWhenCleanPagesAvailable(t *testing.T) {
	pc := newPcache(4096, 3, true) // max 3 pages

	var stressCalled int
	pc.xStress = func(p *page) error {
		stressCalled++
		pc.makeClean(p)
		return nil
	}

	// Fill cache with clean pages (released, not dirty)
	for i := uint32(1); i <= 3; i++ {
		pg := pc.create(i, 2)
		pc.release(pg)
	}
	assert.Equal(t, 3, pc.nRecyclable)

	// Creating a 4th page should evict a clean page, NOT trigger stress
	pg4 := pc.create(4, 2)
	require.NotNil(t, pg4)
	assert.Equal(t, 0, stressCalled) // clean eviction worked, no stress needed
	assert.Equal(t, 3, pc.nPage)     // one was evicted
	pc.release(pg4)
}

func TestPcacheLRUEvictionOrder(t *testing.T) {
	// Pin pages 1-5, release in order 5,4,3,2,1.
	// LRU list should be: HEAD(MRU) -> 1 -> 2 -> 3 -> 4 -> 5 -> TAIL(LRU)
	// Eviction should pop from TAIL: 5, 4, 3, 2, 1.
	pc := newPcache(4096, 5, true)

	pgs := make([]*page, 5)
	for i := uint32(1); i <= 5; i++ {
		pgs[i-1] = pc.create(i, 2)
	}
	// Release in reverse order: 5, 4, 3, 2, 1
	for i := 4; i >= 0; i-- {
		pc.release(pgs[i])
	}
	assert.Equal(t, 5, pc.nRecyclable)

	// Eviction order should be 5, 4, 3, 2, 1 (least recently released first = TAIL)
	expectedEviction := []uint32{5, 4, 3, 2, 1}
	for _, expectedPgno := range expectedEviction {
		evicted := pc.evictOne()
		require.NotNil(t, evicted, "expected to evict page %d", expectedPgno)
		assert.Equal(t, expectedPgno, evicted.pgno)
	}
	assert.Equal(t, 0, pc.nRecyclable)
	assert.Nil(t, pc.lruHead)
	assert.Nil(t, pc.lruTail)
}

func TestPcacheLRURefetchMovesMRU(t *testing.T) {
	// Release page A, fetch it again, release it — A should be at MRU (HEAD),
	// not evicted first.
	pc := newPcache(4096, 5, true)

	pgA := pc.create(1, 2)
	pgB := pc.create(2, 2)
	pgC := pc.create(3, 2)

	// Release A, B, C in order
	pc.release(pgA) // LRU: HEAD -> A -> TAIL
	pc.release(pgB) // LRU: HEAD -> B -> A -> TAIL
	pc.release(pgC) // LRU: HEAD -> C -> B -> A -> TAIL

	// Re-fetch A (removes from LRU), then release again (goes to HEAD/MRU)
	pgA = pc.fetch(1)
	require.NotNil(t, pgA)
	assert.Equal(t, 2, pc.nRecyclable) // B and C still in LRU
	pc.release(pgA)                    // LRU: HEAD -> A -> C -> B -> TAIL
	assert.Equal(t, 3, pc.nRecyclable)

	// Eviction order should be B (tail/LRU), C, A (head/MRU)
	evicted := pc.evictOne()
	require.NotNil(t, evicted)
	assert.Equal(t, uint32(2), evicted.pgno, "B should be evicted first (LRU)")

	evicted = pc.evictOne()
	require.NotNil(t, evicted)
	assert.Equal(t, uint32(3), evicted.pgno, "C should be evicted second")

	evicted = pc.evictOne()
	require.NotNil(t, evicted)
	assert.Equal(t, uint32(1), evicted.pgno, "A should be evicted last (MRU)")
}

func TestPcacheStressDisabledForInMemory(t *testing.T) {
	pc := newPcache(4096, 3, false) // non-purgeable (InMemory)

	var stressCalled int
	pc.xStress = func(p *page) error {
		stressCalled++
		pc.makeClean(p)
		return nil
	}

	// Fill cache with dirty pages
	for i := uint32(1); i <= 3; i++ {
		pg := pc.create(i, 2)
		pc.makeDirty(pg)
		pc.release(pg)
	}

	// Creating a 4th page should NOT trigger stress for non-purgeable caches
	pg4 := pc.create(4, 2)
	require.NotNil(t, pg4)
	assert.Equal(t, 0, stressCalled) // non-purgeable skips stress entirely
	assert.Equal(t, 4, pc.nPage)     // cache grows beyond maxPages
	pc.release(pg4)
}

func TestPcacheDirtyMoveToFrontOnRelease(t *testing.T) {
	// Dirty pages A(1), B(2), C(3). Release C then A.
	// After makeDirty order (most recent at head): C → B → A
	// Release C → move to front (already there): C → B → A
	// Release A → move to front: A → C → B
	pc := newPcache(4096, 100, true)

	pgA := pc.create(1, 2)
	pgB := pc.create(2, 2)
	pgC := pc.create(3, 2)

	pc.makeDirty(pgA)
	pc.makeDirty(pgB)
	pc.makeDirty(pgC)

	// Dirty list after makeDirty: C(head) → B → A
	assert.Equal(t, uint32(3), pc.dirtyHead.pgno)

	// Release C (already at front, no change), then A (moves to front)
	pc.release(pgC)
	pc.release(pgA)

	// Expected dirty list: A → C → B (most recently released at front)
	assert.Equal(t, uint32(1), pc.dirtyHead.pgno, "A should be at front")
	assert.Equal(t, uint32(3), pc.dirtyHead.next.pgno, "C should be second")
	assert.Equal(t, uint32(2), pc.dirtyHead.next.next.pgno, "B should be last")
	assert.Nil(t, pc.dirtyHead.next.next.next, "list should end after B")

	pc.release(pgB)
}

func TestPcacheFindSpillVictimOldestFirst(t *testing.T) {
	// After dirty move-to-front, findSpillVictim should return the oldest
	// dirty page (at the back of the list), not the most recently released.
	pc := newPcache(4096, 100, true)

	pgA := pc.create(1, 2)
	pgB := pc.create(2, 2)
	pgC := pc.create(3, 2)

	pc.makeDirty(pgA)
	pc.makeDirty(pgB)
	pc.makeDirty(pgC)

	// Release C then A — dirty list becomes: A → C → B
	pc.release(pgC)
	pc.release(pgA)
	// B is still pinned (pinCount=1), release it last
	pc.release(pgB)
	// Before B release: A → C → B. Release B → move to front: B → A → C
	// All have pinCount 0.

	// findSpillVictim should return C (oldest, at back)
	victim := pc.findSpillVictim()
	require.NotNil(t, victim)
	assert.Equal(t, uint32(3), victim.pgno, "C should be the spill victim (oldest at back)")
}

func TestPcacheBulkAlloc_InitBulkOnFirstCreate(t *testing.T) {
	// In non-slab mode, initBulk pre-allocates pages from sync.Pool/heap.
	// In slab mode, initBulk is a no-op (SQLite: nInitPage=0 when pPage set).
	globalPageSlab.Reset()
	defer globalPageSlab.Reset()

	t.Run("non-slab", func(t *testing.T) {
		pc := newPcache(4096, 50, true)
		// useSlab=false (default) — initBulk uses sync.Pool/heap

		assert.False(t, pc.bulkInit)
		assert.Empty(t, pc.pFree)

		// First create triggers initBulk (nBulk = min(50, 20) = 20)
		pg1 := pc.create(1, 2)
		require.NotNil(t, pg1)
		assert.True(t, pc.bulkInit)
		assert.Len(t, pc.pFree, 19, "pFree should have 19 (20 bulk - 1 used)")
		assert.True(t, pg1.isBulkLocal, "page from initBulk should be isBulkLocal")

		// Second create uses pFree
		pg2 := pc.create(2, 2)
		require.NotNil(t, pg2)
		assert.Len(t, pc.pFree, 18)
		assert.True(t, pg2.isBulkLocal)

		pc.release(pg1)
		pc.release(pg2)
		pc.destroy()
	})

	t.Run("slab", func(t *testing.T) {
		globalPageSlab.Reset()
		globalPageSlab.Init(4096, 500)
		defer globalPageSlab.Reset()

		pc := newPcache(4096, 50, true)
		pc.useSlab = true

		// initBulk is a no-op in slab mode (SQLite: nInitPage=0 when pPage set)
		pg := pc.create(1, 2)
		require.NotNil(t, pg)
		assert.True(t, pc.bulkInit, "bulkInit flag set even though it's a no-op")
		assert.Empty(t, pc.pFree, "pFree empty — initBulk skipped in slab mode")
		assert.False(t, pg.isBulkLocal, "slab pages are not bulk-local")

		// Slab was consumed (page allocated from slab, not heap)
		globalPageSlab.mu.Lock()
		freeCount := len(globalPageSlab.freeList)
		globalPageSlab.mu.Unlock()
		assert.Less(t, freeCount, 500, "slab should be consumed")

		pc.release(pg)
		pc.destroy()
	})
}

func TestPcacheBulkAlloc_FallbackToSlabAfterPFreeExhausted(t *testing.T) {
	globalPageSlab.Reset()
	globalPageSlab.Init(4096, 500)
	defer globalPageSlab.Reset()

	// In slab mode, initBulk is a no-op — all pages come from slab directly
	pc := newPcache(4096, 5, true)
	pc.useSlab = true

	// Create 5 pages — all from slab (initBulk skipped in slab mode)
	pgs := make([]*page, 5)
	for i := uint32(1); i <= 5; i++ {
		pgs[i-1] = pc.create(i, 2)
	}
	assert.True(t, pc.bulkInit)
	assert.Empty(t, pc.pFree, "pFree should be empty — initBulk skipped in slab mode")

	// Release all so cache isn't full
	for _, pg := range pgs {
		pc.release(pg)
	}

	// Now evict some to make room
	for i := uint32(1); i <= 3; i++ {
		pc.evictOne()
	}

	// Record slab free count before next create
	globalPageSlab.mu.Lock()
	freeCountBefore := len(globalPageSlab.freeList)
	globalPageSlab.mu.Unlock()

	// Create a new page — should allocate directly from slab (pFree is empty,
	// bulkInit already done)
	pg6 := pc.create(6, 2)
	require.NotNil(t, pg6)
	assert.Len(t, pg6.data, 4096)

	// Slab should have one fewer buffer
	globalPageSlab.mu.Lock()
	freeCountAfter := len(globalPageSlab.freeList)
	globalPageSlab.mu.Unlock()
	assert.Equal(t, freeCountBefore-1, freeCountAfter,
		"slab should have one fewer buffer after direct allocation")

	pc.release(pg6)
}

func TestPcacheBulkAlloc_MaxBulk20(t *testing.T) {
	// Verify initBulk caps at 20 (SQLITE_DEFAULT_PCACHE_INITSZ) even for large
	// maxPages. Uses non-slab mode since initBulk is a no-op with slab.
	globalPageSlab.Reset()
	defer globalPageSlab.Reset()

	pc := newPcache(4096, 5000, true)
	// useSlab=false — initBulk fires

	pg := pc.create(1, 2)
	require.NotNil(t, pg)
	// nBulk = min(5000, 20) = 20; 1 used for pg, 19 remain
	assert.Len(t, pc.pFree, 19, "pFree should have 19 (20 bulk - 1 used)")
	assert.True(t, pg.isBulkLocal)

	pc.release(pg)
	pc.destroy()
}

func TestPcacheBufferRecycling_EvictionFromPFreeOrSlab(t *testing.T) {
	// Fill cache to maxPages, create one more page. For writer caches (xStress
	// set), evicted page buffers are NOT returned to slab because writerCache may
	// alias the evicted page. The new page is allocated from pFree or slab.
	globalPageSlab.Reset()
	globalPageSlab.Init(4096, 500)
	defer globalPageSlab.Reset()

	pc := newPcache(4096, 5, true)
	pc.useSlab = true
	// Set xStress to simulate writer cache (writerCache aliasing concern)
	pc.xStress = func(p *page) error { return nil }

	// Create and release 5 pages (fills cache)
	pgs := make([]*page, 5)
	for i := uint32(1); i <= 5; i++ {
		pgs[i-1] = pc.create(i, 2)
	}
	for _, pg := range pgs {
		pc.release(pg)
	}
	assert.Equal(t, 5, pc.nRecyclable)
	assert.Equal(t, 5, pc.nPage)

	// Create page 6 — should evict page 1 (LRU tail) and allocate from pFree/slab
	pg6 := pc.create(6, 2)
	require.NotNil(t, pg6)

	// Verify the eviction happened correctly
	assert.Nil(t, pc.hashFind(1), "page 1 should have been evicted")
	assert.NotNil(t, pc.hashFind(6), "page 6 should exist")
	assert.Equal(t, 5, pc.nPage, "cache should still have 5 pages after eviction+create")

	pc.release(pg6)
}

func TestPcacheBufferRecycling_ReaderEvictionRecyclesDirect(t *testing.T) {
	// Reader caches (xStress == nil) should directly reuse the evicted page's
	// buffer for the new page, bypassing the Put→Get slab round-trip.
	// Matches SQLite pcache1.c:900 (step 4 reuses victim's buffer directly).
	globalPageSlab.Reset()
	globalPageSlab.Init(4096, 500)
	defer globalPageSlab.Reset()

	pc := newPcache(4096, 5, true) // no xStress = reader cache
	pc.useSlab = true

	// Create and release 5 pages (fills cache)
	for i := uint32(1); i <= 5; i++ {
		pg := pc.create(i, 2)
		pc.release(pg)
	}
	assert.Equal(t, 5, pc.nRecyclable)
	assert.Equal(t, 5, pc.nPage)

	// Record slab free count before eviction
	globalPageSlab.mu.Lock()
	freeCountBefore := len(globalPageSlab.freeList)
	globalPageSlab.mu.Unlock()

	// Create page 6 — should evict page 1 and reuse its buffer directly
	pg6 := pc.create(6, 2)
	require.NotNil(t, pg6)

	assert.Nil(t, pc.hashFind(1), "page 1 should have been evicted")
	assert.Equal(t, 5, pc.nPage)

	// Slab free count should be unchanged — no Put (evicted buffer reused
	// directly) and no Get (recycled page used instead of slab allocation).
	globalPageSlab.mu.Lock()
	freeCountAfter := len(globalPageSlab.freeList)
	globalPageSlab.mu.Unlock()

	assert.Equal(t, freeCountBefore, freeCountAfter,
		"direct recycling should not touch slab at all")

	pc.release(pg6)
}

func TestPcacheBufferRecycling_ClearReturnsSlab(t *testing.T) {
	// In slab mode: initBulk is disabled, pages come from slab. clear() moves
	// them to pFree for reuse (no pressure). destroy() frees all to slab.
	globalPageSlab.Reset()
	globalPageSlab.Init(4096, 500)
	defer globalPageSlab.Reset()

	pc := newPcache(4096, 50, true)
	pc.useSlab = true

	// Create 10 pages — all from slab (initBulk is a no-op in slab mode)
	for i := uint32(1); i <= 10; i++ {
		pg := pc.create(i, 2)
		pc.release(pg)
	}

	// pFree is empty — no bulk init in slab mode
	assert.Empty(t, pc.pFree)
	assert.Equal(t, 10, pc.nPage)

	// clear() moves pages to pFree (no pressure)
	pc.clear()

	assert.Zero(t, pc.nPage)
	assert.Equal(t, 0, pc.nRecyclable)
	assert.Equal(t, 0, pc.nDirty)
	// All 10 pages moved to pFree
	assert.Len(t, pc.pFree, 10,
		"clear() should move all 10 pages to pFree")

	// destroy() should free all buffers back to slab
	pc.destroy()
	assert.Empty(t, pc.pFree)
	assert.False(t, pc.bulkInit, "bulkInit should be reset after destroy")
}

func TestPcacheBufferRecycling_DiscardFreesBuffer(t *testing.T) {
	// discard() should free the evicted page's buffer via freePageBuffer.
	// Since initBulk uses heap (matching SQLite's pcache1InitBulk which
	// calls sqlite3Malloc), the buffer is heap-allocated. freePageBuffer
	// with useSlab=true routes through globalPageSlab.Put.
	globalPageSlab.Reset()
	globalPageSlab.Init(4096, 500)
	defer globalPageSlab.Reset()

	pc := newPcache(4096, 100, true)
	pc.useSlab = true

	pg := pc.create(1, 2)
	pc.release(pg)

	// discard should remove the page and nil out its data
	pc.discard(1)
	assert.Nil(t, pc.hashFind(1), "page should be removed after discard")
}

func TestPcacheBufferRecycling_TruncateFreesBuffers(t *testing.T) {
	// truncate() should free evicted page buffers and remove them from cache.
	globalPageSlab.Reset()
	globalPageSlab.Init(4096, 500)
	defer globalPageSlab.Reset()

	pc := newPcache(4096, 100, true)
	pc.useSlab = true

	for i := uint32(1); i <= 10; i++ {
		pg := pc.create(i, 2)
		pc.release(pg)
	}
	assert.Equal(t, 10, pc.nPage)

	pc.truncate(5) // remove pages 6-10

	assert.Equal(t, 5, pc.nPage, "truncate should remove pages > maxPage")
	for i := uint32(6); i <= 10; i++ {
		assert.Nil(t, pc.hashFind(i), "page %d should be removed", i)
	}
	for i := uint32(1); i <= 5; i++ {
		assert.NotNil(t, pc.hashFind(i), "page %d should be retained", i)
	}
}

func TestPcacheBulkAlloc_NoSlabFallsBackToSyncPool(t *testing.T) {
	// When slab is not initialized, create() should still work via sync.Pool/make.
	// initBulk pre-allocates page structs with buffers from sync.Pool (heap).
	globalPageSlab.Reset()
	defer globalPageSlab.Reset()

	pc := newPcache(4096, 100, true)
	pg := pc.create(1, 2)
	require.NotNil(t, pg)
	assert.Len(t, pg.data, 4096)
	assert.True(t, pc.bulkInit, "bulkInit should be set even without slab")
	assert.True(t, pg.isBulkLocal, "page from initBulk should be isBulkLocal")
	// pFree is populated by initBulk (pre-allocated from sync.Pool/make),
	// minus the one page consumed by create(). initBulk allocates min(maxPages, 20) = 20.
	assert.Len(t, pc.pFree, 19, "pFree should have 19 pages (20 bulk - 1 consumed by create)")

	pc.release(pg)
	pc.destroy()
}

func TestPcacheAdmissionControl_SoftCreateRefusedAt90Percent(t *testing.T) {
	// Pin 95% of maxPages, soft create (createFlag=1) should return nil.
	// Hard create (createFlag=2) should still succeed.
	pc := newPcache(4096, 100, true)

	// Pin 95 pages (95% of 100) — all pinned, none recyclable
	pinned := make([]*page, 95)
	for i := 0; i < 95; i++ {
		pinned[i] = pc.create(uint32(i+1), 2)
	}
	// nPinned = len(pages) - nRecyclable = 95 - 0 = 95
	// threshold = maxPages * 9 / 10 = 90
	// 95 >= 90 → soft create should be refused
	assert.Equal(t, 0, pc.nRecyclable)
	assert.Equal(t, 95, pc.nPage)

	// Soft create should return nil
	softPg := pc.create(200, 1)
	assert.Nil(t, softPg, "soft create should be refused when 95% pages are pinned")

	// Hard create should succeed
	hardPg := pc.create(200, 2)
	assert.NotNil(t, hardPg, "hard create should always succeed")

	// Cleanup
	pc.release(hardPg)
	for _, pg := range pinned {
		pc.release(pg)
	}
}

func TestPcacheAdmissionControl_SoftCreateAllowedBelow90Percent(t *testing.T) {
	// With fewer than 90% pinned, soft create should succeed.
	pc := newPcache(4096, 100, true)

	// Pin 80 pages, release all so nRecyclable=80
	for i := 0; i < 80; i++ {
		pg := pc.create(uint32(i+1), 2)
		pc.release(pg) // all go to LRU
	}
	// nPinned = 80 - 80 = 0 (all released); far below 90%
	softPg := pc.create(200, 1)
	assert.NotNil(t, softPg, "soft create should succeed when pinned count is below 90%")
	pc.release(softPg)
}

func TestPcacheAdmissionControl_SlabPressureLowRecyclable(t *testing.T) {
	// Slab under pressure + low recyclable ratio → soft create should be
	// refused. Matches SQLite Mode 1 (separateCache) pcache1FetchStage2
	// step 3 (pcache1.c:889): pcache1UnderMemoryPressure && nRecyclable < nPinned.
	// This prevents caches from growing under pressure, bounding total memory.
	globalPageSlab.Reset()
	globalPageSlab.Init(4096, 10) // small slab to easily create pressure
	defer globalPageSlab.Reset()

	pc := newPcache(4096, 100, true)
	pc.useSlab = true

	// Drain the slab to create pressure
	drained := make([][]byte, 0)
	for !globalPageSlab.UnderPressure() {
		drained = append(drained, globalPageSlab.Get())
	}
	assert.True(t, globalPageSlab.UnderPressure())

	// Create 10 pinned pages — nRecyclable=0, nPinned=10
	pinned := make([]*page, 10)
	for i := 0; i < 10; i++ {
		pinned[i] = pc.create(uint32(i+1), 2) // hard create always works
	}
	// nPinned=10, nRecyclable=0 — under slab pressure, soft create should
	// be refused (nRecyclable=0 < nPinned=10). Readers fall back to
	// readTempPage for uncached reads.
	softPg := pc.create(200, 1)
	assert.Nil(t, softPg, "soft create should be refused under slab pressure with low recyclable")

	// But if we release some pages (making them recyclable), soft create
	// should succeed because step 4 can recycle an LRU page.
	for i := 0; i < 5; i++ {
		pc.release(pinned[i])
	}
	// nPinned=5, nRecyclable=5 — nRecyclable >= nPinned, so step 3 passes.
	// Step 4 recycles an LRU page under pressure.
	softPg2 := pc.create(200, 1)
	assert.NotNil(t, softPg2, "soft create should succeed when recyclable >= pinned")
	pc.release(softPg2)

	// Return drained buffers to slab
	for _, buf := range drained {
		globalPageSlab.Put(buf)
	}

	// Cleanup
	for i := 5; i < 10; i++ {
		pc.release(pinned[i])
	}
}

func TestPcacheAdmissionControl_NonPurgeableIgnoresCreateFlag(t *testing.T) {
	// Non-purgeable (InMemory) caches should ignore admission control.
	pc := newPcache(4096, 10, false) // purgeable=false

	// Pin all 10 pages
	pinned := make([]*page, 10)
	for i := 0; i < 10; i++ {
		pinned[i] = pc.create(uint32(i+1), 2)
	}

	// Soft create should succeed for non-purgeable cache (no admission control)
	softPg := pc.create(200, 1)
	assert.NotNil(t, softPg, "soft create should succeed for non-purgeable cache")
	pc.release(softPg)

	for _, pg := range pinned {
		pc.release(pg)
	}
}

func TestPcacheUnpin_OverfullDiscardsImmediately(t *testing.T) {
	// When cache is overfull (len(pages) > maxPages), releasing a clean page
	// discards it immediately instead of adding to LRU. Matches SQLite
	// pcache1Unpin (pcache1.c:1094-1095):
	//   if( reuseUnlikely || pGroup->nPurgeable>pGroup->nMaxPage ){
	//       pcache1RemoveFromHash(pPage, 1);
	//   }
	// In Mode 1 (separateCache), pGroup is per-cache, so this is a per-cache
	// overfull check. This prevents pages from accumulating in the LRU after
	// transactions that grew the cache beyond maxPages via hard creates.
	pc := newPcache(4096, 5, true)

	// Create 6 pages with hard create (cache grows beyond maxPages=5)
	pgs := make([]*page, 6)
	for i := uint32(1); i <= 6; i++ {
		pgs[i-1] = pc.create(i, 2)
	}
	assert.Equal(t, 6, pc.nPage) // overfull

	// Release page 6 (clean) — should be discarded immediately (overfull)
	pc.release(pgs[5])
	assert.Nil(t, pc.hashFind(6), "page 6 should be discarded when cache is overfull")
	assert.Equal(t, 0, pc.nRecyclable, "page should NOT be in LRU")
	assert.Equal(t, 5, pc.nPage, "cache should shrink back to maxPages")

	// Release page 5 — cache is now at maxPages, should go to LRU normally
	pc.release(pgs[4])
	assert.NotNil(t, pc.hashFind(5), "page 5 should remain in cache (at maxPages)")
	assert.Equal(t, 1, pc.nRecyclable, "page should be in LRU")

	// Cleanup
	for i := 0; i < 4; i++ {
		pc.release(pgs[i])
	}
}
