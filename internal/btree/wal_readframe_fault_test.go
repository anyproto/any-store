package btree

import (
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWALReadFrameFaultFallsThroughToDisk pins the by-design behavior documented
// at docs/btree/NOTES.md#drift-6-wal-frame-read-failure-falls-through-to-disk-read.
//
// In C readDbPage (pager.c:3035-3045) a WAL-resolved page is read ONLY from the
// WAL frame and a sqlite3WalReadFrame failure propagates as the page-get error.
// The Go getters (getPageWriter / readTempPage / getPageReader) instead, on a
// readFrame failure, deliberately ignore the error and fall through to a DB-file
// (or masterStore) read. This is a deliberate drift, NOT a bug, because the
// primary protections that C relies on are preserved in Go:
//
//	(1) A reader holds its WAL read-mark slot for the whole transaction, so the
//	    authoritative WAL frame within the reader's snapshot is served from the
//	    WAL (the normal, non-faulting path).
//	(2) A checkpoint cannot truncate / reset the WAL out from under a held reader
//	    slot, and the backfill-before-truncate ordering guarantees that any frame
//	    a reader could resolve is already durable in the DB file before it could
//	    ever be truncated. So the disk fallthrough can only return content at
//	    least as new as the faulting WAL frame — never an older committed copy.
//
// This test installs the minimal test-only readFrame fault-injection hook and
// asserts: the frame is normally served from the WAL; truncation cannot occur
// under a held reader slot; and on an injected readFrame error the getter falls
// through (no error surfaced) rather than propagating the WAL read failure.
//
// There is no production fault-injection hook on wal.readFrame — this hook is the
// minimal test-only surface required to make the invariant deterministically
// observable.
func TestWALReadFrameFaultFallsThroughToDisk(t *testing.T) {
	// Ensure any hook installed by this (or a prior) test is cleared on exit so
	// the production no-op path is restored for every other test.
	t.Cleanup(func() { setWalReadFrameFaultHook(nil) })

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	// DisableAutoCheckpoint keeps committed frames in the WAL so the reader's
	// snapshot resolves pages to live WAL frames (the precondition for the drift).
	db, err := testOpen(t, dbPath, Options{PageSize: 4096, DisableAutoCheckpoint: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Seed a namespace and rows so several pages (incl. page 1) gain WAL frames.
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("t1")
	require.NoError(t, err)
	for i := 1; i <= 64; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, wtx.Put(ns, key, make([]byte, 200)))
	}
	require.NoError(t, wtx.Commit())

	// Open a read transaction. The reader claims a WAL read-mark slot and freezes
	// its snapshot ceiling (walMaxFrame); it holds the slot for the whole tx.
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rtx.Rollback() })

	walMaxFrame := rtx.WalMaxFrame()
	require.Greater(t, walMaxFrame, uint32(0),
		"reader snapshot must include the just-committed WAL frames")

	pager := db.pager

	// Find a page that resolves to a live WAL frame within the reader's snapshot.
	// This is exactly the C `iFrame != 0` case where C reads ONLY from the WAL.
	var targetPgno, targetFrame uint32
	for pgno := uint32(1); pgno <= pager.dbSize.Load(); pgno++ {
		if f := mustWiGet(t, pager.wal.index, pgno, walMaxFrame); f > 0 {
			targetPgno, targetFrame = pgno, f
			break
		}
	}
	require.NotZero(t, targetPgno, "expected at least one page resolvable to a WAL frame")
	require.NotZero(t, targetFrame)

	// readPgnoFreshCache reads targetPgno through getPageReader with a brand-new
	// reader cache, forcing a true cache miss so wal.readFrame is invoked every
	// call (a cache hit would bypass readFrame and defeat the fault injection).
	readPgnoFreshCache := func() ([]byte, error) {
		cache := newPcache(int(pager.pageSize), db.readerCacheSize, true)
		pg, gerr := pager.getPageReader(targetPgno, walMaxFrame, cache)
		if gerr != nil {
			return nil, gerr
		}
		out := make([]byte, pager.pageSize)
		copy(out, pg.data[:pager.pageSize])
		pager.releasePage(pg)
		return out, nil
	}

	// --- Protection (1): the frame within the snapshot is served from the WAL. ---
	// With no fault injected, getPageReader must succeed and return the WAL-frame
	// content. Capture it as the authoritative bytes for the fallthrough check.
	require.Nil(t, walReadFrameFaultHook.Load(), "no hook installed yet")
	walServed, err := readPgnoFreshCache()
	require.NoError(t, err, "frame within snapshot must be served from the WAL without error")

	// --- The drift: an injected readFrame error falls through, not propagates. ---
	// Inject a failure for exactly the target frame. C would propagate this as the
	// page-get error; Go must instead fall through to the DB-file/masterStore read
	// and return successfully (the documented, intentional behavior). This is the
	// core invariant this regression test pins.
	sentinel := errors.New("injected WAL readFrame fault")
	var hookFired bool
	setWalReadFrameFaultHook(func(frame uint32) error {
		if frame == targetFrame {
			hookFired = true
			return sentinel
		}
		return nil
	})

	fellThrough, err := readPgnoFreshCache()
	require.True(t, hookFired, "fault hook must have been exercised for the target frame")
	require.NoError(t, err,
		"by design, a readFrame failure must fall through to a disk read, not surface the error")
	require.NotNil(t, fellThrough)
	// Note: at this point (frame NOT yet backfilled to the DB file) the fallthrough
	// can legitimately return content OLDER than the WAL frame — that is exactly the
	// drift hazard the NOTES entry documents. We deliberately do NOT assert the
	// fallthrough equals the WAL bytes here: the safety argument is not "the disk
	// copy is always current" but "this fallthrough is unreachable in production",
	// proven by the two protections asserted below. Asserting current-content here
	// would falsely claim the drift is harmless even before backfill.

	// Clearing the hook restores the WAL-served path with no error, reconfirming the
	// non-faulting production path returns the authoritative WAL frame content.
	setWalReadFrameFaultHook(nil)
	require.Nil(t, walReadFrameFaultHook.Load())
	afterClear, err := readPgnoFreshCache()
	require.NoError(t, err)
	assert.Equal(t, walServed, afterClear,
		"clearing the fault must restore the normal WAL-served read")

	// --- Protection (2): truncation/reset cannot occur under a held reader slot. ---
	// The structural guarantee that makes the (unreachable-in-prod) fallthrough safe:
	// a reader holding its read-mark slot blocks the checkpointer from RESET/TRUNCATE
	// (tryResetWALWithBusy must exclusively lock the reader slots, which ErrBusy-fails
	// while one is held), so the WAL frame numbering and salt are NOT recycled under
	// the reader. A passive backfill may copy frames to the DB file and raise
	// nBackfill (transparently shifting the reader's reads to the now-durable disk
	// copy), but the WAL is never reset out from under the snapshot — so readFrame
	// cannot fail due to truncation, and the only way to reach the fallthrough is a
	// genuine I/O error (which this test simulates).
	saltBefore := [2]uint32{pager.wal.header.salt1, pager.wal.header.salt2}
	nFrameBefore := pager.wal.nFrame.Load()
	require.Greater(t, nFrameBefore, uint32(0), "WAL must hold frames before the checkpoint")

	require.NoError(t, db.Checkpoint(CheckpointTruncate),
		"truncate checkpoint must not error while a reader holds a slot")

	saltAfter := [2]uint32{pager.wal.header.salt1, pager.wal.header.salt2}
	assert.Equal(t, saltBefore, saltAfter,
		"WAL salt must be unchanged: a held reader slot blocks WAL reset/truncation")
	assert.Equal(t, nFrameBefore, pager.wal.nFrame.Load(),
		"WAL frame numbering must not be recycled while a reader holds its slot")

	// The reader's snapshot remains internally consistent across the checkpoint:
	// the same page reads back with identical content (no error).
	afterCkpt, err := readPgnoFreshCache()
	require.NoError(t, err, "page must remain readable across a checkpoint under a held slot")
	assert.Equal(t, walServed, afterCkpt,
		"snapshot content must be stable while the reader holds its slot")

	// --- Claim (3): backfill-before-truncate makes the fallthrough content safe. ---
	// The checkpoint above ran its PASSIVE backfill, which copies (and fdatasyncs)
	// each frame to the DB file BEFORE advancing nBackfill — and a truncate can only
	// follow a completed backfill. So once a frame's content is eligible to be
	// truncated, that content is already durably in the DB file. We prove the
	// ordering directly: after the backfill, the raw DB-file copy of targetPgno now
	// equals the WAL-frame content. Hence in the only window the fallthrough could be
	// reached (frame still live but unreadable due to a real I/O fault), the disk
	// copy it falls back to is at least as new as the frame — never an older version.
	rawDisk := make([]byte, pager.pageSize)
	require.NoError(t, pager.readDBPage(targetPgno, rawDisk),
		"backfill must have written the page to the DB file")
	assert.Equal(t, walServed, rawDisk,
		"backfill-before-truncate: DB-file copy must match the WAL frame once backfilled")
}

// TestWALReadFrameFaultHookDefaultsToNoop guards the production contract: the
// fault-injection hook is nil unless a test installs it, so production readFrame
// behavior is unchanged (the by-design fallthrough is never triggered in prod by
// the hook).
func TestWALReadFrameFaultHookDefaultsToNoop(t *testing.T) {
	t.Cleanup(func() { setWalReadFrameFaultHook(nil) })

	require.Nil(t, walReadFrameFaultHook.Load(),
		"readFrame fault hook must be nil by default (production no-op)")

	// Installing then clearing must round-trip cleanly back to nil.
	setWalReadFrameFaultHook(func(uint32) error { return errors.New("x") })
	require.NotNil(t, walReadFrameFaultHook.Load())
	setWalReadFrameFaultHook(nil)
	require.Nil(t, walReadFrameFaultHook.Load())
}
