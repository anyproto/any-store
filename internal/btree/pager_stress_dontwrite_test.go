package btree

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPagerStress_DontWriteSkipsWALWrite_RegressionPin pins a by-design drift
// between the Go port and upstream SQLite.
//
// DRIFT (docs/btree/NOTES.md#old-drift-pagerstress-dontwrite-skip-walwrite):
// Go's pagerStress short-circuits a PGHDR_DONT_WRITE page (pager.go:2062-2065):
// it calls makeClean(pg) and returns WITHOUT writing a WAL frame. SQLite's
// pagerStress (sqlitec/src/pager.c:4645-4650) instead calls
// pagerWalFrames(pPager,pPg,0,0) in WAL mode, and pagerWalFrames
// (pager.c:3179-3236) writes the frame UNCONDITIONALLY — it never checks
// PGHDR_DONT_WRITE. The DONT_WRITE skip in C lives only in the rollback-journal
// path pager_write_pagelist (pager.c:4471), gated by assert(!pagerUseWal)
// (pager.c:4432). So in WAL mode C writes a (garbage) frame for a spilled
// dontWrite page; Go does not. This is an intentional I/O-avoidance optimization.
//
// THE INVARIANT a future refactor must not break (the relied-upon behavior that
// makes skipping the write safe): a freed freelist-leaf page is marked dontWrite,
// and pagerStress must still be able to make it CLEAN (evictable) WITHOUT
// emitting a WAL frame — so freed pages whose content is irrelevant never bloat
// the WAL, yet the cache can still drain them under memory pressure. The
// stale/garbage cached content is never read back: getWritablePage clears
// dontWrite and re-dirties on same-tx re-allocation (pager.go:1268), and
// allocateFromFreelist uses getPageNoContent in later txs (hasContent is cleared
// at commit), so no consumer ever interprets the spilled-but-not-rewritten bytes.
//
// This test builds the EXACT scenario the drift relies on: a real page that was
// allocated, then freed via freePage as a freelist LEAF (which calls
// dontWrite(pgno) at pager.go:1446 because the page is cached). The page is left
// dirty + cached, made the genuine spill victim via the real findSpillVictim,
// then handed to the real pagerStress. We assert the two halves of the
// invariant: NO WAL frame is appended, and the page IS made clean. If a refactor
// deletes the dontWrite branch (so it falls through to wal.writeFrames), the
// "no WAL frame" assertion fails loudly.
//
// Production logic is unchanged; the test only drives existing internal methods.
func TestPagerStress_DontWriteSkipsWALWrite_RegressionPin(t *testing.T) {
	db := tempDB(t)
	p := db.pager

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	// Allocate two fresh data pages (pgno>1, within dbSize, dirty + cached).
	// Both are released to pinCount==0 but stay dirty in the writer cache.
	pgA, err := p.allocatePage()
	require.NoError(t, err)
	pgnoA := pgA.pgno
	p.releasePage(pgA)

	pgB, err := p.allocatePage()
	require.NoError(t, err)
	pgnoB := pgB.pgno
	p.releasePage(pgB)

	require.Greater(t, pgnoA, uint32(1))
	require.Greater(t, pgnoB, uint32(1))

	// Free pgA first: with no freelist yet, it becomes the new trunk page (its
	// content is meaningful, so it is NOT marked dontWrite).
	require.NoError(t, p.freePage(pgnoA))

	// Free pgB: the trunk (pgA) now has room, so pgB is appended as a freelist
	// LEAF. Because pgB is still cached, freePage marks it dontWrite
	// (pager.go:1445-1447) — exactly the spill-then-free state the drift targets.
	require.NoError(t, p.freePage(pgnoB))

	require.True(t, p.dontWritePages[pgnoB],
		"freeing a cached page as a freelist leaf must mark it dontWrite")

	// pgB must still be a dirty, cached, unpinned page — the precondition for a
	// pagerStress spill victim.
	pgBcached := p.writerCache.hashFind(pgnoB)
	require.NotNil(t, pgBcached, "freed dontWrite leaf must still be cached")
	require.True(t, pgBcached.dirty, "freed dontWrite leaf must still be dirty")
	require.Equal(t, 0, pgBcached.pinCount, "freed dontWrite leaf must be unpinned")

	// Park every OTHER dirty page ahead of pgB so pgB is the oldest unpinned
	// dirty page — the page findSpillVictim walks back to first. We must NOT
	// touch pgB via getWritablePage: that would clear its dontWrite flag
	// (pager.go:1268) and defeat the scenario.
	for _, d := range p.writerCache.appendDirtyPages(nil) {
		if d.pgno == pgnoB {
			continue
		}
		if d.dirty {
			wp, gerr := p.getWritablePage(d.pgno)
			require.NoError(t, gerr)
			p.releasePage(wp)
		}
	}

	// Load-bearing precondition: the REAL victim search selects the dontWrite
	// page. This proves the dontWrite branch in pagerStress is genuinely
	// reachable for this page under memory pressure.
	victim := p.writerCache.findSpillVictim()
	require.NotNil(t, victim, "there must be an unpinned dirty victim")
	require.Equal(t, pgnoB, victim.pgno,
		"the dontWrite leaf must be the oldest unpinned dirty page (spill victim)")
	require.True(t, p.dontWritePages[pgnoB],
		"sanity: dontWrite flag must survive the parking step")

	walFramesBefore := p.wal.index.maxFrame.Load()
	dirtyBefore := p.writerCache.nDirty

	// Drive the REAL pagerStress on the dontWrite victim.
	require.NoError(t, p.pagerStress(pgBcached),
		"pagerStress on a dontWrite page must return nil")

	// INVARIANT 1 (the drift): NO WAL frame was appended. SQLite would append a
	// (garbage) frame here; Go must not. A wal.writeFrames call would advance
	// walIndex.maxFrame.
	assert.Equal(t, walFramesBefore, p.wal.index.maxFrame.Load(),
		"pagerStress on a dontWrite page must NOT append a WAL frame")

	// INVARIANT 2: the page WAS made clean (evictable) and the dirty count
	// dropped by exactly one. This is what lets the cache drain freed pages
	// under memory pressure even though no WAL write happened.
	assert.False(t, pgBcached.dirty,
		"pagerStress on a dontWrite page must make it clean (evictable)")
	assert.Equal(t, dirtyBefore-1, p.writerCache.nDirty,
		"pagerStress on a dontWrite page must drop the dirty count by exactly one")
}

// TestPagerStress_NonDontWriteWritesWALFrame_Control is the control proving the
// regression pin above is not vacuous. An otherwise identical dirty/unpinned
// spill victim that is NOT marked dontWrite IS written to the WAL by the real
// pagerStress (maxFrame advances) and then made clean. This guards against
// pagerStress silently becoming a no-op for all pages, which would make the
// "no WAL frame" assertion above pass for the wrong reason.
func TestPagerStress_NonDontWriteWritesWALFrame_Control(t *testing.T) {
	db := tempDB(t)
	p := db.pager

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	// A plain dirty data page that is NOT freed / NOT marked dontWrite.
	pg, err := p.allocatePage()
	require.NoError(t, err)
	pgno := pg.pgno
	require.Greater(t, pgno, uint32(1))
	p.releasePage(pg)

	require.False(t, p.dontWritePages[pgno],
		"control page must NOT be marked dontWrite")

	pgCached := p.writerCache.hashFind(pgno)
	require.NotNil(t, pgCached)
	require.True(t, pgCached.dirty)
	require.Equal(t, 0, pgCached.pinCount)

	// Park other dirty pages ahead so this page is the spill victim.
	for _, d := range p.writerCache.appendDirtyPages(nil) {
		if d.pgno == pgno {
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
	require.Equal(t, pgno, victim.pgno)

	walFramesBefore := p.wal.index.maxFrame.Load()

	require.NoError(t, p.pagerStress(pgCached))

	// A non-dontWrite victim IS spilled: a WAL frame is appended and it is cleaned.
	assert.Greater(t, p.wal.index.maxFrame.Load(), walFramesBefore,
		"a non-dontWrite spill victim must append a WAL frame (proves pagerStress is not a global no-op)")
	assert.False(t, pgCached.dirty,
		"a non-dontWrite spill victim must be made clean (evictable)")
}
