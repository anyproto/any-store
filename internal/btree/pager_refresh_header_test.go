package btree

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Regression tests for the beginWrite page-1 re-read error-propagation
// hardening (docs/btree/NOTES.md#drift-76-beginwrite-re-reads-page-1-header-on-state-change).
//
// On the stateChanged edge (a peer committed/checkpointed), beginWrite clears
// the writerCache and re-reads page 1 via refreshHeaderFromPage1. If BOTH the
// WAL-frame read AND the DB-file read fail (double I/O failure),
// refreshHeaderFromPage1 must now return that error and beginWrite must
// PROPAGATE it (aborting the write) instead of silently continuing with a
// stale header/dbSize that commit() would later serialize over the peer's
// page 1. This mirrors SQLite, which propagates the page-1 read error out of
// the begin/shared-lock path.

// TestRefreshHeaderFromPage1_DoubleIOFailure exercises the core unit:
// refreshHeaderFromPage1 returns an error when page 1 exists (a WAL frame is
// recorded for it) but neither the WAL frame nor the DB file can be read.
func TestRefreshHeaderFromPage1_DoubleIOFailure(t *testing.T) {
	dir := t.TempDir()
	p := newPager(filepath.Join(dir, "test.db"), 4096, 100, true)
	p.inProcess = true
	require.NoError(t, p.open())
	defer p.close()

	// Commit a write so page 1 is recorded as a WAL frame (effectiveMaxFrame>0
	// and wal.index.get(1, ...) returns frame>0).
	mf, slot, err := p.beginRead()
	require.NoError(t, err)
	p.walMaxFrame.Store(mf)
	require.NoError(t, p.beginWrite(WalIndexHdr{}))
	pg, err := p.getWritablePage(1)
	require.NoError(t, err)
	p.releasePage(pg)
	_, _, _, err = p.commit(true, false)
	require.NoError(t, err)
	p.endRead(slot)

	// Sanity: page 1 is reachable as a WAL frame, so refreshHeaderFromPage1
	// will attempt the WAL-frame read (not the no-read best-effort path).
	effectiveMaxFrame := p.wal.nFrame.Load()
	if mf := p.wal.index.mxCommitFrame.LoadLocal(); mf > effectiveMaxFrame {
		effectiveMaxFrame = mf
	}
	require.Greater(t, effectiveMaxFrame, uint32(0))
	require.Greater(t, p.wal.index.get(1, effectiveMaxFrame), uint32(0))

	// Force the double I/O failure: WAL-frame read fails (file-backed WAL with
	// nil file => ErrWALCorrupt) AND the DB-file fallback is unavailable.
	walFile := p.wal.file
	require.NotNil(t, walFile)
	_ = walFile.Close()
	p.wal.file = nil
	dbFile := p.file
	p.file = nil
	// Restore the handles for clean teardown so p.close() does not touch nil.
	defer func() {
		p.wal.file = walFile
		p.file = dbFile
	}()

	err = p.refreshHeaderFromPage1()
	require.Error(t, err, "refreshHeaderFromPage1 must propagate the double I/O read failure")
	assert.ErrorIs(t, err, ErrWALCorrupt)
}

// TestBeginWriteHeaderRefreshError_DoubleIOFailure exercises the end-to-end
// propagation: a real multi-process setup (two DB handles on one file) drives
// the stateChanged edge in beginWrite, and a double page-1 read failure on
// that edge must cause beginWrite to return the error and leave the pager in a
// non-writer state.
func TestBeginWriteHeaderRefreshError_DoubleIOFailure(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db1, err := testOpen(t, dbPath, Options{PageSize: 4096})
	require.NoError(t, err)
	defer db1.Close()

	allowDoubleOpen(dbPath)
	db2, err := testOpen(t, dbPath, Options{PageSize: 4096})
	require.NoError(t, err)
	defer db2.Close()

	// Prime db2's writerHdr baseline with a no-op write so a subsequent peer
	// commit (db1) is what flips stateChanged — not the first-write
	// writerHdr.isInit==0 case.
	wtx, err := db2.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("seed")
	require.NoError(t, err)
	require.NoError(t, wtx.Put(ns, []byte("k"), []byte("v")))
	wtx.MarkDataChanged()
	require.NoError(t, wtx.Commit())

	// db1 (the "other process") commits, advancing the shared SHM header past
	// db2's writerHdr snapshot. db2's next beginWrite will see stateChanged.
	wtx, err = db1.BeginWrite()
	require.NoError(t, err)
	ns1, err := wtx.GetNamespace("seed")
	require.NoError(t, err)
	require.NoError(t, wtx.Put(ns1, []byte("k2"), []byte("v2")))
	wtx.MarkDataChanged()
	require.NoError(t, wtx.Commit())

	// Drive db2's beginWrite manually so we can inject the read failure on the
	// stateChanged edge. Mirror DB.BeginWrite's beginReadHdr -> beginWrite flow.
	p := db2.pager
	readSnap, maxFrame, slot, err := p.beginReadHdr()
	require.NoError(t, err)
	p.writerWalSlot = slot

	// Inject the double I/O failure: both the WAL-frame read and the DB-file
	// read fail when beginWrite re-reads page 1 on the stateChanged edge.
	walFile := p.wal.file
	require.NotNil(t, walFile)
	_ = walFile.Close()
	p.wal.file = nil
	dbFile := p.file
	p.file = nil
	defer func() {
		p.wal.file = walFile
		p.file = dbFile
		p.endRead(slot)
	}()

	err = p.beginWrite(readSnap)
	require.Error(t, err, "beginWrite must propagate the page-1 re-read failure on the stateChanged edge")
	assert.ErrorIs(t, err, ErrWALCorrupt)
	// The pager must NOT have transitioned to a writer state on the aborted
	// begin (no half-open write transaction left behind).
	assert.NotEqual(t, int32(pagerWriter), p.state.Load(),
		"pager must not be in writer state after an aborted beginWrite")
	_ = maxFrame
}
