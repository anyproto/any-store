package btree

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assertPcacheConsistent walks every internal structure of a pcache and asserts
// the cross-field invariants that create()/clear()/eviction must preserve. It is
// the test-only oracle for the re-entrant-clear regression below; a leaked,
// double-linked, or orphaned page makes one of these checks fail loudly.
func assertPcacheConsistent(t *testing.T, pc *pcache) {
	t.Helper()

	// 1. nPage must equal the number of pages reachable through apHash, and every
	//    reachable page must be flagged inCache. A page appearing in two buckets
	//    (a stale hashNext link left by a botched clear) is caught here because it
	//    would be counted twice and/or seen twice.
	seen := make(map[*page]int)
	hashCount := 0
	for bucket, head := range pc.apHash {
		for p := head; p != nil; p = p.hashNext {
			seen[p]++
			require.LessOrEqual(t, seen[p], 1,
				"page %d appears more than once in apHash (stale/duplicate hash link)", p.pgno)
			assert.True(t, p.inCache,
				"page %d reachable via apHash but inCache=false", p.pgno)
			assert.Equal(t, p.pgno&uint32(len(pc.apHash)-1), uint32(bucket),
				"page %d is in the wrong bucket", p.pgno)
			hashCount++
		}
	}
	assert.Equal(t, pc.nPage, hashCount,
		"nPage (%d) disagrees with pages actually reachable via apHash (%d)", pc.nPage, hashCount)

	// 2. The clean LRU list must have exactly nRecyclable entries, all clean,
	//    all inCache, and all reachable from apHash (no orphan served into a
	//    later transaction).
	lruCount := 0
	for p := pc.lruHead; p != nil; p = p.next {
		lruCount++
		assert.False(t, p.dirty, "dirty page %d found on the clean LRU list", p.pgno)
		assert.True(t, p.inCache, "LRU page %d not inCache", p.pgno)
		assert.Greater(t, seen[p], 0, "LRU page %d is not reachable via apHash (orphan)", p.pgno)
	}
	assert.Equal(t, pc.nRecyclable, lruCount,
		"nRecyclable (%d) disagrees with the LRU list length (%d)", pc.nRecyclable, lruCount)

	// 3. The dirty list must have exactly nDirty entries, all dirty, all inCache,
	//    all reachable from apHash.
	dirtyCount := 0
	for p := pc.dirtyHead; p != nil; p = p.next {
		dirtyCount++
		assert.True(t, p.dirty, "clean page %d found on the dirty list", p.pgno)
		assert.True(t, p.inCache, "dirty page %d not inCache", p.pgno)
		assert.Greater(t, seen[p], 0, "dirty page %d is not reachable via apHash (orphan)", p.pgno)
	}
	assert.Equal(t, pc.nDirty, dirtyCount,
		"nDirty (%d) disagrees with the dirty list length (%d)", pc.nDirty, dirtyCount)

	// 4. No buffer double-ownership: the data slice backing any cached/free page
	//    struct must be unique. A double-free would alias one buffer across two
	//    page structs.
	buffers := make(map[*byte]uint32)
	record := func(p *page, label string, id uint32) {
		if len(p.data) == 0 {
			return
		}
		key := &p.data[0]
		if prev, dup := buffers[key]; dup {
			t.Fatalf("page buffer aliased between %s (pgno %d) and a prior holder (pgno %d): double-free/leak",
				label, id, prev)
		}
		buffers[key] = id
	}
	for p := range seen {
		record(p, "cached", p.pgno)
	}
	for _, p := range pc.pFree {
		record(p, "pFree", p.pgno)
	}
}

// TestXStressReentrantClear_CacheStaysConsistentAndNoOrphan is a regression PIN
// for the by-design drift documented at
// docs/btree/NOTES.md#old-drift-pcache-create-drops-xstress-error.
//
// THE DRIFT: pcache.create() (pcache.go:create) has no error return. When the
// cache is full of dirty pages it calls pc.xStress(victim) (pcache.go:352) to
// spill one to the WAL, and it DISCARDS xStress's error. SQLite's
// sqlite3PcacheFetchStress (pcache.c:481-485) instead propagates a non-BUSY
// xStress error, and getPageNormal (pager.c:5552-5556) aborts the page
// acquisition via pager_acquire_err — dropping the half-built page. Go keeps the
// page and the in-flight b-tree op keeps mutating it.
//
// In production the xStress callback is pager.pagerStress. On a WAL-write failure
// mid-transaction (disk full / I/O error), pagerStress calls pager.pagerError()
// (pager.go:2084) BEFORE returning the (dropped) error. pagerError() runs, among
// other cleanup, p.writerCache.clear() (pager.go:2352) — i.e. the cache is
// cleared RE-ENTRANTLY, from inside the pc.xStress(victim) call, while create()
// is still mid-flight. create() then resumes its retry-eviction loop, allocates
// a page struct, resetPage()s it, and hashInsert()s it into the just-cleared
// cache before returning.
//
// THE RELIED-UPON INVARIANT (what this test pins): that re-entrant clear() during
// xStress must leave the cache internally consistent and must NOT leak an orphan
// page into a future transaction. Specifically:
//   - clear() resets nPage to 0, so create()'s post-stress retry-eviction loop
//     (pcache.go:354, `for pc.nPage >= pc.maxPages`) becomes a no-op — it must not
//     touch the now-empty LRU/dirty lists or a recycled victim that clear() already
//     routed to pFree (no double-free).
//   - the single page create() returns afterward is the ONLY page in the cache,
//     correctly hash-linked (inCache, right bucket), with nPage/nRecyclable/nDirty
//     all agreeing with the actual lists.
//
// If a future refactor breaks this (e.g. clear() forgets to zero nPage, or
// create() retains a recycled victim across the clear and double-frees its
// buffer, or the retry loop runs against the cleared lists and orphans a page),
// assertPcacheConsistent fails. That, combined with the separate guarantee that
// commit() is state-gated (pager.go:2102) and a second spill is a no-op in error
// state (pager.go:2041), is what makes the dropped xStress error harmless.
//
// This is a pure-pcache unit test: it needs no production fault-injection hook
// because xStress is an assignable field on the pcache. It simulates pagerError's
// re-entrant clear() directly inside the callback, which is the exact ordering
// the production drop relies on.
func TestXStressReentrantClear_CacheStaysConsistentAndNoOrphan(t *testing.T) {
	const maxPages = 3
	pc := newPcache(4096, maxPages, true)

	// errSpill stands in for the WAL-write failure (disk full / I/O error) that
	// pagerStress hits mid-transaction. The production code drops this exact
	// return value.
	errSpill := errors.New("simulated WAL write failure during spill")

	var stressCalls int
	var victimPgno uint32
	var clearedDuringStress bool
	pc.xStress = func(victim *page) error {
		stressCalls++
		victimPgno = victim.pgno

		// Reproduce pagerStress's failure path: pagerError() purges the writer
		// cache BEFORE the (dropped) error is returned. The observable cache
		// effect of pager.pagerError() (pager.go:2348-2352) is a full clear()
		// that tears the cache down to empty. We invoke clear() directly so that
		// its own counter/list reset (nPage=0, nRecyclable=0, dirty/LRU heads
		// nil) is the sole thing keeping create()'s post-stress retry-eviction
		// loop (pcache.go:354) a safe no-op — exactly the invariant the dropped
		// error relies on.
		pc.clear()
		clearedDuringStress = true

		// The error is what production drops. Returning it documents that the
		// scenario is the error path; create() discards it by design.
		return errSpill
	}

	// Fill the cache to maxPages with dirty, unpinned pages so the next create()
	// finds no clean eviction candidate and must invoke xStress to spill.
	for i := uint32(1); i <= maxPages; i++ {
		pg := pc.create(i, 2)
		require.NotNil(t, pg)
		pc.makeDirty(pg)
		pc.release(pg)
	}
	require.Equal(t, maxPages, pc.nDirty)
	require.Equal(t, 0, pc.nRecyclable)
	assertPcacheConsistent(t, pc)

	// Hard-create page 4 (createFlag=2, the writer path). The cache is full of
	// dirty pages, so create() invokes xStress, which re-entrantly clears the
	// cache and returns the dropped error. create() must still return a usable,
	// hash-linked page.
	pg4 := pc.create(4, 2)

	require.Equal(t, 1, stressCalls, "xStress must have been invoked exactly once")
	require.NotZero(t, victimPgno, "a dirty spill victim must have been chosen")
	require.True(t, clearedDuringStress, "the re-entrant clear() must have run")

	require.NotNil(t, pg4, "create() must still return a page after the dropped xStress error")
	require.Equal(t, uint32(4), pg4.pgno)
	require.Equal(t, 1, pg4.pinCount, "returned page must be pinned exactly once")
	require.Len(t, pg4.data, 4096)
	require.True(t, pg4.inCache, "the returned page must be linked into the cleared cache's hash")
	require.False(t, pg4.dirty, "freshly created page must start clean")

	// The cleared cache must now hold EXACTLY the one page create() returned —
	// no orphan from the wiped dirty set, no stale hash link, no buffer leak.
	require.Equal(t, 1, pc.nPage, "cache must hold exactly the one page create() returned")
	require.Same(t, pg4, pc.hashFind(4), "page 4 must be findable in the hash")
	for i := uint32(1); i <= maxPages; i++ {
		require.Nil(t, pc.hashFind(i),
			"page %d was wiped by the re-entrant clear() and must not be served from cache", i)
	}
	assertPcacheConsistent(t, pc)

	// The page must round-trip a normal release back onto the LRU, proving it is
	// a fully-initialized cache citizen (not a half-built orphan).
	pc.release(pg4)
	require.Equal(t, 0, pg4.pinCount)
	require.Equal(t, 1, pc.nRecyclable, "released clean page must enter the LRU")
	assertPcacheConsistent(t, pc)
}

// TestXStressNonReentrant_BaselineConsistent is the control: the SAME fill/create
// sequence with a normal (non-clearing) xStress callback that just makeClean()s
// the victim. It proves the consistency oracle does not spuriously fire on the
// ordinary spill path and isolates the re-entrant-clear behavior as the thing
// under test above.
func TestXStressNonReentrant_BaselineConsistent(t *testing.T) {
	const maxPages = 3
	pc := newPcache(4096, maxPages, true)

	var stressCalls int
	pc.xStress = func(victim *page) error {
		stressCalls++
		pc.makeClean(victim) // ordinary spill: WAL write succeeded
		return nil
	}

	for i := uint32(1); i <= maxPages; i++ {
		pg := pc.create(i, 2)
		require.NotNil(t, pg)
		pc.makeDirty(pg)
		pc.release(pg)
	}
	assertPcacheConsistent(t, pc)

	pg4 := pc.create(4, 2)
	require.NotNil(t, pg4)
	require.Equal(t, 1, stressCalls)
	// Spill made the victim clean+evictable, so the cache stays at maxPages and
	// keeps the surviving dirty pages.
	require.Equal(t, maxPages, pc.nPage)
	assertPcacheConsistent(t, pc)

	pc.release(pg4)
	assertPcacheConsistent(t, pc)
}
