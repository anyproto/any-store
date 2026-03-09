package btree

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPcacheCreateFetch(t *testing.T) {
	pc := newPcache(4096, 100, true)

	pg := pc.create(1)
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

	pg1 := pc.create(5)
	pg1.data[0] = 42
	pc.release(pg1)

	// Create the same page again should return existing
	pg2 := pc.create(5)
	assert.Equal(t, uint8(42), pg2.data[0])
	pc.release(pg2)
}

func TestPcacheRelease(t *testing.T) {
	pc := newPcache(4096, 100, true)

	pg := pc.create(1)
	assert.Equal(t, 1, pg.pinCount)
	pc.release(pg)
	assert.Equal(t, 0, pg.pinCount)

	// After release, page should be in LRU (clean)
	assert.Equal(t, 1, pc.nClean)
}

func TestPcacheDirtyPages(t *testing.T) {
	pc := newPcache(4096, 100, true)

	pg1 := pc.create(1)
	pg2 := pc.create(2)
	pg3 := pc.create(3)

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

	pg := pc.create(1)
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
		pg := pc.create(i)
		pc.release(pg)
	}
	assert.Len(t, pc.pages, 3)
	assert.Equal(t, 3, pc.nClean)

	// Creating a 4th page should evict the oldest clean page (page 1)
	pg4 := pc.create(4)
	assert.NotNil(t, pg4)
	assert.Nil(t, pc.pages[1]) // page 1 should be evicted
	assert.NotNil(t, pc.pages[2])
	assert.NotNil(t, pc.pages[3])
	assert.NotNil(t, pc.pages[4])
	pc.release(pg4)
}

func TestPcacheDirtyPagesNotEvicted(t *testing.T) {
	pc := newPcache(4096, 2, true) // max 2 pages

	pg1 := pc.create(1)
	pc.makeDirty(pg1)
	pc.release(pg1)

	pg2 := pc.create(2)
	pc.makeDirty(pg2)
	pc.release(pg2)

	// Both are dirty, cache is "full" but dirty pages won't be evicted
	pg3 := pc.create(3)
	assert.NotNil(t, pg3)
	// All 3 should still be present (dirty pages can't be evicted)
	assert.NotNil(t, pc.pages[1])
	assert.NotNil(t, pc.pages[2])
	pc.release(pg3)
}

func TestPcacheDiscard(t *testing.T) {
	pc := newPcache(4096, 100, true)

	pg := pc.create(1)
	pc.release(pg)

	pc.discard(1)
	assert.Nil(t, pc.pages[1])
	assert.Nil(t, pc.fetch(1))

	// Discard non-existent page should not panic
	pc.discard(999)
}

func TestPcacheDiscardDirty(t *testing.T) {
	pc := newPcache(4096, 100, true)

	pg := pc.create(1)
	pc.makeDirty(pg)
	pc.release(pg)

	assert.Equal(t, 1, pc.nDirty)
	pc.discard(1)
	assert.Equal(t, 0, pc.nDirty)
	assert.Nil(t, pc.pages[1])
}

func TestPcacheClear(t *testing.T) {
	pc := newPcache(4096, 100, true)

	for i := uint32(1); i <= 10; i++ {
		pg := pc.create(i)
		if i%2 == 0 {
			pc.makeDirty(pg)
		}
		pc.release(pg)
	}

	pc.clear()
	assert.Empty(t, pc.pages)
	assert.Equal(t, 0, pc.nClean)
	assert.Equal(t, 0, pc.nDirty)
	assert.Nil(t, pc.lruHead)
	assert.Nil(t, pc.lruTail)
	assert.Nil(t, pc.dirtyHead)
}

func TestPcacheTruncate(t *testing.T) {
	pc := newPcache(4096, 100, true)

	for i := uint32(1); i <= 10; i++ {
		pg := pc.create(i)
		pc.release(pg)
	}

	pc.truncate(5)
	assert.Len(t, pc.pages, 5)
	for i := uint32(1); i <= 5; i++ {
		assert.NotNil(t, pc.pages[i])
	}
	for i := uint32(6); i <= 10; i++ {
		assert.Nil(t, pc.pages[i])
	}
}

func TestPcacheTruncateDirty(t *testing.T) {
	pc := newPcache(4096, 100, true)

	for i := uint32(1); i <= 5; i++ {
		pg := pc.create(i)
		pc.makeDirty(pg)
		pc.release(pg)
	}

	pc.truncate(2)
	assert.Equal(t, 2, pc.nDirty)
	assert.NotNil(t, pc.pages[1])
	assert.NotNil(t, pc.pages[2])
	assert.Nil(t, pc.pages[3])
}

func TestPcacheFetchMovesFromLRU(t *testing.T) {
	pc := newPcache(4096, 100, true)

	pg := pc.create(1)
	pc.release(pg)
	assert.Equal(t, 1, pc.nClean)

	// Fetching should remove from LRU
	pg2 := pc.fetch(1)
	assert.NotNil(t, pg2)
	assert.Equal(t, 0, pc.nClean)

	pc.release(pg2)
	assert.Equal(t, 1, pc.nClean)
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
		pg := pc.create(i)
		pc.makeDirty(pg)
		pc.release(pg)
	}
	assert.Equal(t, 3, pc.nDirty)
	assert.Equal(t, 0, pc.nClean)

	// Creating a 4th page should trigger stress callback since no clean
	// pages are available for eviction
	pg4 := pc.create(4)
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
		pg := pc.create(i)
		pc.makeDirty(pg)
		// Do NOT release — keep pinned
		pgs[i-1] = pg
	}

	// All dirty pages are pinned, stress callback should not find a victim
	pg4 := pc.create(4)
	require.NotNil(t, pg4)
	assert.Equal(t, 0, stressCalled) // no victim found, no stress call
	assert.Len(t, pc.pages, 4)       // cache grows beyond maxPages

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
		pg := pc.create(i)
		pc.release(pg)
	}
	assert.Equal(t, 3, pc.nClean)

	// Creating a 4th page should evict a clean page, NOT trigger stress
	pg4 := pc.create(4)
	require.NotNil(t, pg4)
	assert.Equal(t, 0, stressCalled) // clean eviction worked, no stress needed
	assert.Len(t, pc.pages, 3)       // one was evicted
	pc.release(pg4)
}

func TestPcacheLRUEvictionOrder(t *testing.T) {
	// Pin pages 1-5, release in order 5,4,3,2,1.
	// LRU list should be: HEAD(MRU) -> 1 -> 2 -> 3 -> 4 -> 5 -> TAIL(LRU)
	// Eviction should pop from TAIL: 5, 4, 3, 2, 1.
	pc := newPcache(4096, 5, true)

	pgs := make([]*page, 5)
	for i := uint32(1); i <= 5; i++ {
		pgs[i-1] = pc.create(i)
	}
	// Release in reverse order: 5, 4, 3, 2, 1
	for i := 4; i >= 0; i-- {
		pc.release(pgs[i])
	}
	assert.Equal(t, 5, pc.nClean)

	// Eviction order should be 5, 4, 3, 2, 1 (least recently released first = TAIL)
	expectedEviction := []uint32{5, 4, 3, 2, 1}
	for _, expectedPgno := range expectedEviction {
		evicted := pc.evictOne()
		require.NotNil(t, evicted, "expected to evict page %d", expectedPgno)
		assert.Equal(t, expectedPgno, evicted.pgno)
	}
	assert.Equal(t, 0, pc.nClean)
	assert.Nil(t, pc.lruHead)
	assert.Nil(t, pc.lruTail)
}

func TestPcacheLRURefetchMovesMRU(t *testing.T) {
	// Release page A, fetch it again, release it — A should be at MRU (HEAD),
	// not evicted first.
	pc := newPcache(4096, 5, true)

	pgA := pc.create(1)
	pgB := pc.create(2)
	pgC := pc.create(3)

	// Release A, B, C in order
	pc.release(pgA) // LRU: HEAD -> A -> TAIL
	pc.release(pgB) // LRU: HEAD -> B -> A -> TAIL
	pc.release(pgC) // LRU: HEAD -> C -> B -> A -> TAIL

	// Re-fetch A (removes from LRU), then release again (goes to HEAD/MRU)
	pgA = pc.fetch(1)
	require.NotNil(t, pgA)
	assert.Equal(t, 2, pc.nClean) // B and C still in LRU
	pc.release(pgA)               // LRU: HEAD -> A -> C -> B -> TAIL
	assert.Equal(t, 3, pc.nClean)

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
		pg := pc.create(i)
		pc.makeDirty(pg)
		pc.release(pg)
	}

	// Creating a 4th page should NOT trigger stress for non-purgeable caches
	pg4 := pc.create(4)
	require.NotNil(t, pg4)
	assert.Equal(t, 0, stressCalled) // non-purgeable skips stress entirely
	assert.Len(t, pc.pages, 4)       // cache grows beyond maxPages
	pc.release(pg4)
}

func TestPcacheDirtyMoveToFrontOnRelease(t *testing.T) {
	// Dirty pages A(1), B(2), C(3). Release C then A.
	// After makeDirty order (most recent at head): C → B → A
	// Release C → move to front (already there): C → B → A
	// Release A → move to front: A → C → B
	pc := newPcache(4096, 100, true)

	pgA := pc.create(1)
	pgB := pc.create(2)
	pgC := pc.create(3)

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

	pgA := pc.create(1)
	pgB := pc.create(2)
	pgC := pc.create(3)

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
	// Initialize a local slab for this test to avoid polluting globalPageSlab.
	globalPageSlab.Reset()
	globalPageSlab.Init(4096, 500)
	defer globalPageSlab.Reset()

	pc := newPcache(4096, 50, true)

	// Before any create(), bulkInit should be false and pFree empty
	assert.False(t, pc.bulkInit)
	assert.Empty(t, pc.pFree)

	// First create() should trigger initBulk (nBulk = min(50, 100) = 50)
	pg1 := pc.create(1)
	require.NotNil(t, pg1)
	assert.True(t, pc.bulkInit, "bulkInit should be true after first create")
	// pFree should have 49 remaining (50 allocated, 1 used for pg1)
	assert.Len(t, pc.pFree, 49)
	assert.Len(t, pg1.data, 4096)

	// Second create() should use pFree without calling initBulk again
	pg2 := pc.create(2)
	require.NotNil(t, pg2)
	assert.Len(t, pc.pFree, 48, "pFree should have 48 after second create")
	assert.Len(t, pg2.data, 4096)

	// Verify slab was consumed: 50 buffers taken from slab during initBulk
	globalPageSlab.mu.Lock()
	freeCount := len(globalPageSlab.freeList)
	globalPageSlab.mu.Unlock()
	assert.Equal(t, 450, freeCount, "slab should have 500-50=450 free buffers")

	pc.release(pg1)
	pc.release(pg2)
}

func TestPcacheBulkAlloc_FallbackToSlabAfterPFreeExhausted(t *testing.T) {
	globalPageSlab.Reset()
	globalPageSlab.Init(4096, 500)
	defer globalPageSlab.Reset()

	// Use maxPages=5 so initBulk allocates only 5 pages
	pc := newPcache(4096, 5, true)

	// Create 5 pages — first triggers initBulk (nBulk = min(5, 100) = 5),
	// uses all 5 from pFree
	pgs := make([]*page, 5)
	for i := uint32(1); i <= 5; i++ {
		pgs[i-1] = pc.create(i)
	}
	assert.True(t, pc.bulkInit)
	assert.Empty(t, pc.pFree, "pFree should be exhausted after 5 creates")

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
	pg6 := pc.create(6)
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

func TestPcacheBulkAlloc_MaxBulk100(t *testing.T) {
	// Verify initBulk caps at 100 even for large maxPages
	globalPageSlab.Reset()
	globalPageSlab.Init(4096, 500)
	defer globalPageSlab.Reset()

	pc := newPcache(4096, 5000, true)

	// Trigger initBulk via first create
	pg := pc.create(1)
	require.NotNil(t, pg)
	// nBulk = min(5000, 100) = 100; 1 used for pg, 99 remain
	assert.Len(t, pc.pFree, 99, "pFree should have 99 (100 bulk - 1 used)")

	pc.release(pg)
}

func TestPcacheBulkAlloc_NoSlabFallsBackToMake(t *testing.T) {
	// When slab is not initialized, create() should still work via make()
	globalPageSlab.Reset()
	defer globalPageSlab.Reset()

	pc := newPcache(4096, 100, true)
	pg := pc.create(1)
	require.NotNil(t, pg)
	assert.Len(t, pg.data, 4096)
	assert.True(t, pc.bulkInit, "bulkInit should be set even without slab")
	assert.Empty(t, pc.pFree, "pFree should be empty when slab not initialized")

	pc.release(pg)
}
