package btree

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPcacheGhostPage_ReleaseAfterRemovalWhilePinned is the invariant the whole
// apHash port hinges on: a page removed from the cache (via discard/truncate/
// stress-evict) while a caller still holds it pinned must NOT be re-added to the
// LRU when that caller finally releases it. apHash carries membership on the page
// via page.inCache (set by hashInsert, cleared by hashRemove); release() gates the
// LRU insert on inCache instead of re-probing a Go map.
//
// Concretely we assert, for each removal path, that after the late release:
//   - the page is not findable (hashRemove unlinked it),
//   - p.inCache is false and the page is NOT on the LRU (nRecyclable unchanged),
//   - nPage accounting is exact,
//   - the page's buffer was returned exactly once (no double-free / no leak),
//   - evictOne over the remaining real pages terminates and drains to zero (a
//     ghost on the LRU would make evictOne spin without shrinking nPage).
//
// SQLite never hits this case (its invariants forbid removing a pinned page from
// apHash), so pcache1Unpin adds to the LRU unconditionally; the inCache gate is
// the v2-specific guard. See docs/btree/plans/2026-05-22-pcache-apHash-port.md.
func TestPcacheGhostPage_ReleaseAfterRemovalWhilePinned(t *testing.T) {
	t.Run("discard while pinned", func(t *testing.T) {
		pc := newPcache(4096, 100, true)

		// Pin two pages so we can verify accounting on the survivor too.
		keep := pc.create(1, 2)
		pc.release(keep) // page 1 is clean+unpinned → on LRU
		require.Equal(t, 1, pc.nRecyclable)

		pinned := pc.create(2, 2) // page 2 stays pinned
		require.Equal(t, 1, pinned.pinCount)
		require.True(t, pinned.inCache)
		require.Equal(t, 2, pc.nPage)

		// Force removal of the still-pinned page.
		pc.discard(2)
		assert.Nil(t, pc.hashFind(2), "discarded page must be unlinked from apHash")
		assert.False(t, pinned.inCache, "discard must clear inCache (ghost guard)")
		assert.Equal(t, 1, pc.nPage, "nPage must drop on discard")
		assert.Equal(t, 1, pc.nRecyclable, "discarding a pinned page must not touch the LRU")
		nRecBefore := pc.nRecyclable

		// Late release of the ghost: must be a no-op for the LRU and accounting.
		pc.release(pinned)
		assert.Equal(t, 0, pinned.pinCount)
		assert.False(t, pinned.inCache, "ghost must not be re-marked in-cache by release")
		assert.Nil(t, pc.hashFind(2), "ghost must not reappear in apHash after release")
		assert.Equal(t, nRecBefore, pc.nRecyclable, "ghost must NOT be added to the LRU on release")
		assert.Equal(t, 1, pc.nPage, "nPage must be unchanged by releasing a ghost")

		// evictOne must drain the one real page and then stop — no infinite loop.
		ev := pc.evictOne()
		require.NotNil(t, ev)
		assert.Equal(t, uint32(1), ev.pgno)
		assert.Equal(t, 0, pc.nPage)
		assert.Equal(t, 0, pc.nRecyclable)
		assert.Nil(t, pc.evictOne(), "evictOne on an empty LRU returns nil")
	})

	t.Run("truncate while pinned", func(t *testing.T) {
		pc := newPcache(4096, 100, true)

		// Pages 1..3 unpinned (on LRU), page 4 pinned.
		for i := uint32(1); i <= 3; i++ {
			pc.release(pc.create(i, 2))
		}
		pinned := pc.create(4, 2)
		require.Equal(t, 4, pc.nPage)
		require.Equal(t, 3, pc.nRecyclable)

		// truncate removes pages with pgno > 2 — including the pinned page 4.
		pc.truncate(2)
		assert.Nil(t, pc.hashFind(4), "truncated pinned page must be unlinked")
		assert.Nil(t, pc.hashFind(3), "truncated unpinned page must be unlinked")
		assert.False(t, pinned.inCache, "truncate must clear inCache on the pinned page")
		assert.Equal(t, 2, pc.nPage, "nPage drops by the two truncated pages")
		assert.Equal(t, 2, pc.nRecyclable, "only the unpinned truncated page leaves the LRU")

		// Late release of the truncated, still-pinned page 4: no LRU re-insert.
		pc.release(pinned)
		assert.Equal(t, 0, pinned.pinCount)
		assert.False(t, pinned.inCache)
		assert.Equal(t, 2, pc.nRecyclable, "ghost release must not grow the LRU")
		assert.Equal(t, 2, pc.nPage, "ghost release must not change nPage")

		// Drain the survivors deterministically; evictOne must terminate.
		seen := map[uint32]bool{}
		for i := 0; i < 2; i++ {
			p := pc.evictOne()
			require.NotNil(t, p)
			seen[p.pgno] = true
		}
		assert.Equal(t, map[uint32]bool{1: true, 2: true}, seen)
		assert.Equal(t, 0, pc.nPage)
		assert.Equal(t, 0, pc.nRecyclable)
		assert.Nil(t, pc.evictOne())
	})

	t.Run("discard of a pinned bulk-local page does not double-return its buffer", func(t *testing.T) {
		// returnPageBuffer routes isBulkLocal pages to pFree. The ghost guard must
		// ensure the buffer is returned exactly once (by discard), not again by the
		// late release — otherwise the same page struct lands in pFree twice and a
		// future create() would hand out an aliased buffer.
		globalPageSlab.Reset()
		defer globalPageSlab.Reset()

		pc := newPcache(4096, 100, true) // non-slab → initBulk populates pFree with isBulkLocal pages

		pinned := pc.create(1, 2)
		require.True(t, pinned.isBulkLocal, "non-slab create draws from the bulk-local pool")

		pc.discard(1) // returns pinned to pFree (bulk-local)
		require.False(t, pinned.inCache)
		pFreeAfterDiscard := len(pc.pFree)

		pc.release(pinned) // ghost release: must NOT append to pFree again
		assert.Equal(t, pFreeAfterDiscard, len(pc.pFree),
			"releasing a discarded bulk-local page must not re-append it to pFree")

		// pFree must not contain the same struct twice (would alias buffers).
		count := 0
		for _, fp := range pc.pFree {
			if fp == pinned {
				count++
			}
		}
		assert.Equal(t, 1, count, "discarded page must appear in pFree exactly once")
	})

	t.Run("stress-evict ghost: pinned dirty page spilled then evicted, released late", func(t *testing.T) {
		// Reproduces the original ghost case from the cache core: a writer-style
		// cache (xStress set) at capacity, with a dirty page held pinned. A create
		// triggers the stress callback (makeClean) on the oldest dirty page, then
		// evictOne removes it from apHash even though it is still pinned. The later
		// release must not put it back on the LRU.
		pc := newPcache(4096, 3, true)

		var spilled *page
		pc.xStress = func(p *page) error {
			// Simulate pagerStress: write to WAL then mark clean so it can evict.
			pc.makeClean(p)
			spilled = p
			return nil
		}

		// Three dirty pages; release them so they sit unpinned-but-dirty, oldest
		// at the dirty tail. Keep one (page 1) pinned and dirty.
		p1 := pc.create(1, 2)
		pc.makeDirty(p1) // page 1 dirty, stays pinned

		p2 := pc.create(2, 2)
		pc.makeDirty(p2)
		pc.release(p2) // dirty, unpinned

		p3 := pc.create(3, 2)
		pc.makeDirty(p3)
		pc.release(p3) // dirty, unpinned
		require.Equal(t, 3, pc.nPage)
		require.Equal(t, 3, pc.nDirty)

		// Cache is at capacity with all-dirty pages and zero recyclable. Creating a
		// 4th page must invoke xStress (spill the oldest unpinned dirty page) and
		// then evict it. The pinned dirty page 1 must be left alone.
		p4 := pc.create(4, 2)
		require.NotNil(t, p4)
		require.NotNil(t, spilled, "stress callback should have fired")
		assert.NotEqual(t, uint32(1), spilled.pgno, "the pinned page must not be chosen as spill victim")
		assert.True(t, p1.inCache, "the pinned dirty page must remain in the cache")

		// Now force the *pinned* page out of the cache while it is still pinned, to
		// manufacture the ghost (discard models a rollback/free of an in-use page).
		require.Equal(t, 1, p1.pinCount)
		pc.discard(1)
		assert.False(t, p1.inCache)
		nRecBefore := pc.nRecyclable
		nPageBefore := pc.nPage

		// Late release of the ghost dirty page: the dirty branch of release() must
		// also gate on inCache (dirtyMoveToFront must be skipped).
		pc.release(p1)
		assert.Equal(t, 0, p1.pinCount)
		assert.Equal(t, nRecBefore, pc.nRecyclable, "ghost dirty release must not add to LRU")
		assert.Equal(t, nPageBefore, pc.nPage, "ghost dirty release must not change nPage")
		assert.Nil(t, pc.hashFind(1), "ghost must stay out of apHash")

		// Drain everything; evictOne must terminate (no ghost spinning the loop).
		// Release the remaining pinned pages first so they can be evicted.
		pc.release(p4)
		// Spill remaining dirty pages to clean via the stress path is not needed:
		// makeClean already ran on the victim; the others are still dirty+unpinned.
		// Evict only counts clean LRU pages; assert the loop bounds correctly.
		guard := 0
		for pc.evictOne() != nil {
			guard++
			require.Less(t, guard, 1000, "evictOne must not loop forever after a ghost")
		}
		assert.Equal(t, 0, pc.nRecyclable, "all recyclable pages drained")
	})
}
