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
