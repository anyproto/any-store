package btree

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPagerStress_Page1NeverSpilled_RegressionPin pins a by-design drift between
// the Go port and upstream SQLite.
//
// DRIFT (docs/btree/NOTES.md#old-drift-pagerstress-page1-exclusion): Go's
// pagerStress explicitly guards pg.pgno==1 (pager.go:2053-2055); SQLite's
// pagerStress (sqlitec/src/pager.c:4609-4681) has NO such check. SQLite is safe
// without it because its btree layer keeps BtShared.pPage1 referenced
// (nRef>=1) for the entire open transaction (pager.c:5775-5778), so the pcache
// stress victim search (pcache.c:464,469 — skips any pPg with nRef) can never
// select page 1. any-store does NOT hold page 1 pinned across b-tree
// operations, so page 1 CAN sit unpinned (pinCount==0, which mirrors C nRef==0)
// at the dirty tail and be selected by findSpillVictim (pcache.go:808-815).
// The explicit pgno==1 guard substitutes for C's missing structural pin.
//
// WHY THE GUARD MATTERS (the invariants a future refactor must not break):
//   - A spilled page-1 frame is an UNCOMMITTED (commit=false) WAL frame carrying
//     mid-transaction header bytes. Concurrent readers / other processes must
//     not observe it (defended elsewhere by mxCommitFrame bounding at
//     pager.go:1952-1955). Not spilling page 1 at all keeps that surface small.
//   - The authoritative dbHeader is re-serialized over page 1 at commit
//     (pager.go:2173-2179); spilling-then-evicting page 1 forces a fragile
//     re-read + re-encode round-trip.
//
// This test reproduces the EXACT scenario the guard defends: page 1 dirtied by a
// master/namespace-catalog btree write (CreateNamespace -> btree{rootPage:1}.Put,
// db.go:1031-1032), released to pinCount==0, sitting at the dirty tail as the
// oldest unpinned dirty page, AND confirmed (via the real findSpillVictim) to be
// the page the writer cache would spill. It then asserts the guard holds:
// pagerStress(page1) is a no-op — no WAL frame is appended and page 1 stays
// dirty (never made clean / evictable). If a refactor deletes the pgno==1 guard,
// the WAL-frame and stays-dirty assertions fail loudly.
//
// Production logic is unchanged; the test only drives existing internal methods.
func TestPagerStress_Page1NeverSpilled_RegressionPin(t *testing.T) {
	db := tempDB(t)
	p := db.pager

	// Establish page 1 (the master/namespace catalog btree root) with a first
	// namespace, then commit so the on-disk header is authoritative.
	tx0, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx0.CreateNamespace("seed")
	require.NoError(t, err)
	require.NoError(t, tx0.Commit())

	// New write transaction. A master-catalog btree write (CreateNamespace ->
	// bt{rootPage:1}.Put at db.go:1031-1032) dirties page 1.
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.CreateNamespace("ns")
	require.NoError(t, err)

	// Grab page 1 from the writer cache. The catalog write left it cached.
	pg1 := p.writerCache.hashFind(1)
	require.NotNil(t, pg1, "page 1 must be in the writer cache after a catalog write")

	// Reproduce the precise pre-spill state the C pin would otherwise prevent:
	// page 1 dirty and fully released to pinCount==0 (mirrors C nRef==0). A page
	// with pinCount>0 is structurally unreachable as a victim; we drop it to 0.
	if !pg1.dirty {
		p.writerCache.makeDirty(pg1)
	}
	for pg1.pinCount > 0 {
		p.writerCache.release(pg1)
	}
	require.True(t, pg1.dirty, "page 1 must be dirty for the scenario to be real")
	require.Equal(t, 0, pg1.pinCount, "page 1 must be unpinned (mirrors C nRef==0)")

	// Park page 1 at the dirty TAIL so it is the OLDEST unpinned dirty page —
	// exactly the page findSpillVictim walks back to first (pcache.go:809). Any
	// other dirty pages created by the catalog write are moved ahead of it by
	// touching them through getWritablePage (dirtyMoveToFront), guaranteeing
	// page 1 is the genuine spill victim.
	moveAhead := func(pgno uint32) {
		if pgno == 1 {
			return
		}
		if other := p.writerCache.hashFind(pgno); other != nil && other.dirty {
			wp, gerr := p.getWritablePage(pgno)
			require.NoError(t, gerr)
			p.releasePage(wp) // released unpinned -> dirtyMoveToFront ahead of page 1
		}
	}
	for _, d := range p.writerCache.appendDirtyPages(nil) {
		moveAhead(d.pgno)
	}

	// Precondition (the load-bearing one): the writer cache's REAL victim search
	// selects page 1. This proves the scenario is genuinely reachable — without
	// the pgno==1 guard, page 1 would be spilled here.
	victim := p.writerCache.findSpillVictim()
	require.NotNil(t, victim, "there must be an unpinned dirty victim")
	require.Equal(t, uint32(1), victim.pgno,
		"page 1 must be the oldest unpinned dirty page (the spill victim)")

	// Capture observable state, then drive the REAL pagerStress on page 1.
	walFramesBefore := p.wal.index.maxFrame.Load()
	dirtyBefore := p.writerCache.nDirty

	require.NoError(t, p.pagerStress(pg1),
		"pagerStress(page1) must return nil (the pgno==1 guard short-circuits)")

	// INVARIANT 1: no WAL frame was appended. A spilled page-1 frame would
	// advance walIndex.maxFrame; the guard prevents the writeFrames call.
	assert.Equal(t, walFramesBefore, p.wal.index.maxFrame.Load(),
		"pagerStress(page1) must NOT append a WAL frame (page-1 spill is forbidden)")

	// INVARIANT 2: page 1 is still dirty and still pinned-out of the LRU as a
	// non-victim. makeClean was NOT called, so it cannot be evicted out from
	// under the at-commit getWritablePage(1) re-serialization of the header.
	assert.True(t, pg1.dirty,
		"pagerStress(page1) must NOT makeClean page 1 (it must stay dirty)")
	assert.Equal(t, dirtyBefore, p.writerCache.nDirty,
		"pagerStress(page1) must not change the dirty count")
	assert.Same(t, pg1, p.writerCache.findSpillVictim(),
		"page 1 must STILL be sitting at the dirty tail (not spilled away)")
}

// TestPagerStress_NonPage1Spilled_Control is the control for the regression pin
// above: it proves the test harness is not vacuous. A NON-page-1 dirty page in
// the identical state (dirty, unpinned, the findSpillVictim victim) IS spilled by
// the real pagerStress — a WAL frame is appended and the page is made clean. This
// guards against pagerStress silently becoming a no-op for ALL pages (which would
// make the page-1 assertions above pass for the wrong reason).
func TestPagerStress_NonPage1Spilled_Control(t *testing.T) {
	db := tempDB(t)
	p := db.pager

	// Seed a namespace and a few rows so the tree has data pages beyond page 1.
	tx0, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx0.CreateNamespace("ns")
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		require.NoError(t, tx0.Put(ns, []byte{byte(i)}, []byte("v")))
	}
	require.NoError(t, tx0.Commit())

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	ns2, err := db.getNamespaceLocked("ns")
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		require.NoError(t, tx.Put(ns2, []byte{byte(i)}, []byte("w")))
	}

	// Find a dirty, non-page-1 page; release it to pinCount==0 and make it the
	// spill victim by parking it at the dirty tail (move everything else ahead).
	var target *page
	for _, d := range p.writerCache.appendDirtyPages(nil) {
		if d.pgno != 1 {
			target = d
			break
		}
	}
	require.NotNil(t, target, "expected at least one dirty non-page-1 page")

	for target.pinCount > 0 {
		p.writerCache.release(target)
	}
	require.True(t, target.dirty)
	require.Equal(t, 0, target.pinCount)

	for _, d := range p.writerCache.appendDirtyPages(nil) {
		if d.pgno == target.pgno {
			continue
		}
		if d.dirty {
			wp, gerr := p.getWritablePage(d.pgno)
			require.NoError(t, gerr)
			p.releasePage(wp)
		}
	}

	victim := p.writerCache.findSpillVictim()
	require.NotNil(t, victim)
	require.Equal(t, target.pgno, victim.pgno, "target must be the spill victim")

	walFramesBefore := p.wal.index.maxFrame.Load()

	require.NoError(t, p.pagerStress(target))

	// A non-page-1 victim IS spilled: a WAL frame is appended and it is cleaned.
	assert.Greater(t, p.wal.index.maxFrame.Load(), walFramesBefore,
		"a non-page-1 spill victim must append a WAL frame (proves pagerStress is not a global no-op)")
	assert.False(t, target.dirty,
		"a non-page-1 spill victim must be made clean (evictable)")
}
