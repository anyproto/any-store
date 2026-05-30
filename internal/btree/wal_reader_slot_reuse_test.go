package btree

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hookShm wraps a real shm and runs onLock(slot, lockType) just before each
// lock acquisition is granted. It lets a test deterministically inject a
// concurrent-checkpointer/writer state change into the precise window between
// tryBeginReadInProcessHdr's pre-lock metadata scan and its shared-lock
// acquisition on the reused reader slot — reproducing the cross-goroutine race
// (internal auto-checkpoint runs without pager.mu) without sleeps or timing.
type hookShm struct {
	shm
	onLock func(slot, lockType int)
}

func (h *hookShm) lock(slot, lockType int) error {
	if h.onLock != nil {
		h.onLock(slot, lockType)
	}
	return h.shm.lock(slot, lockType)
}

// TestWALReaderSlotReuseRevalidatesOnStaleMark reproduces the read-safety
// violation that arises when the in-process reader-slot REUSE branch of
// tryBeginReadInProcessHdr skips post-lock re-validation.
//
// Scenario:
//
//  1. A reader claimed slot 1 at mxFrame=10, then ended. endRead never resets a
//     slot's mark, so slot 1 keeps the stale mark 10 while nBackfill stays < 10.
//  2. A reusing reader runs the lock-free scan and picks slot 1 (mark 10 <=
//     mxFrame 10). BUT between that scan and acquiring the shared lock, an
//     internal concurrent checkpoint advances nBackfill past the snapshot the
//     reusing reader is about to adopt (and a commit advances mxCommitFrame).
//  3. If the reuse branch returns without re-validating, the reader proceeds
//     with maxFrame=10 while nBackfill has moved to 10 — the checkpointer is
//     now free to backfill/overwrite frames the reader believes it can read,
//     corrupting the reader's snapshot.
//
// With the fix the reuse branch re-loads mxCommitFrame/nBackfill after taking
// the shared lock; because they changed it drops the lock and returns
// errWALRetry. On retry the nBackfill==mxFrame slot-0 fast path is safe.
func TestWALReaderSlotReuseRevalidatesOnStaleMark(t *testing.T) {
	dir := t.TempDir()
	w := newWal(filepath.Join(dir, "test.wal"), 4096)
	w.inProcess = true
	require.NoError(t, w.open())
	t.Cleanup(func() { _ = w.close(false) })

	// Establish the precondition directly on the WAL index (the established
	// pattern for WAL unit tests): committed up to frame 10, only frames <= 3
	// backfilled, and slot 1 holding a stale mark of 10 from a departed reader.
	w.index.mxCommitFrame.Store(10)
	w.index.maxFrame.Store(10)
	w.index.nBackfill.Store(3)
	w.index.aReadMark[1].Store(10) // stale mark left by an ended reader

	// Inject the concurrent checkpoint + writer that runs in the race window:
	// when the reusing reader takes the SHARED lock on reused slot 1
	// (lockRead0+1), advance nBackfill up to (and past) the stale mark and bump
	// the commit frame, exactly as an internal auto-checkpoint + commit would.
	var injected bool
	hooked := &hookShm{shm: w.index.shm}
	hooked.onLock = func(slot, lockType int) {
		if slot == lockRead0+1 && lockType == lockShared && !injected {
			injected = true
			// Checkpointer backfilled the WAL up to frame 10 and a writer
			// committed new frames, raising the commit ceiling to 20.
			w.index.nBackfill.Store(10)
			w.index.mxCommitFrame.Store(20)
			w.index.maxFrame.Store(20)
		}
	}
	w.index.shm = hooked

	// The reuse path must detect the changed state and signal retry rather than
	// silently adopting the stale (now-unsafe) snapshot.
	_, _, _, err := w.tryBeginReadInProcessHdr()
	require.True(t, injected, "test must exercise the slot-reuse shared-lock path")
	require.ErrorIs(t, err, errWALRetry,
		"reuse branch must re-validate and retry when the checkpointer advanced "+
			"nBackfill/mxCommitFrame past the reused slot's stale mark")

	// The retry attempt (state now stable: nBackfill=10, mxCommitFrame=20) must
	// adopt the LIVE commit ceiling (20), never the stale snapshot (10). A
	// snapshot that floored at the stale mark while nBackfill had already
	// advanced to 10 is exactly the corruption window the re-validation closes.
	hdr, maxFrame, slot, err := w.tryBeginReadInProcessHdr()
	require.NoError(t, err)
	assert.Equal(t, uint32(20), maxFrame, "retry must adopt the live commit ceiling, not the stale 10")
	assert.Equal(t, uint32(20), hdr.mxFrame)
	// The reader's snapshot ceiling (20) must stay strictly above the
	// checkpointer's backfill floor (nBackfill==10): walIndex.get() floors reads
	// at nBackfill+1==11, so no frame the checkpointer already backfilled past is
	// readable through this snapshot. A held reused slot's mark may lag the
	// snapshot — that is conservative-safe because the checkpointer cannot
	// exclusively lock a still-held slot and so keeps frames above the mark live.
	assert.Greater(t, maxFrame, w.index.nBackfill.Load(),
		"reader snapshot must stay above the checkpointer's backfill floor")
	w.endRead(slot)
}

// TestWALReaderSlotReuseFastPathSafeAfterBackfill verifies the companion
// guarantee referenced by the fix: once the checkpointer has backfilled the
// whole WAL (nBackfill == mxFrame), a reusing reader takes the slot-0 fast path
// and adopts a correct, fully-backfilled snapshot — the fix must not break it.
func TestWALReaderSlotReuseFastPathSafeAfterBackfill(t *testing.T) {
	dir := t.TempDir()
	w := newWal(filepath.Join(dir, "test.wal"), 4096)
	w.inProcess = true
	require.NoError(t, w.open())
	t.Cleanup(func() { _ = w.close(false) })

	// mxFrame==nBackfill: WAL fully checkpointed. A stale slot-1 mark of 10 must
	// be ignored in favor of the slot-0 fast path.
	w.index.mxCommitFrame.Store(10)
	w.index.maxFrame.Store(10)
	w.index.nBackfill.Store(10)
	w.index.aReadMark[1].Store(10)

	hdr, maxFrame, slot, err := w.tryBeginReadInProcessHdr()
	require.NoError(t, err)
	assert.Equal(t, 0, slot, "fully-backfilled WAL must use the slot-0 fast path")
	assert.Equal(t, uint32(10), maxFrame)
	assert.Equal(t, uint32(10), hdr.mxFrame)
	w.endRead(slot)
}

// TestWALReaderSlotReuseSucceedsWhenStateStable confirms the fix does not
// over-retry: when no checkpoint/writer advances state during slot reuse, a
// reusing reader on a valid slot proceeds normally on the reused slot.
func TestWALReaderSlotReuseSucceedsWhenStateStable(t *testing.T) {
	dir := t.TempDir()
	w := newWal(filepath.Join(dir, "test.wal"), 4096)
	w.inProcess = true
	require.NoError(t, w.open())
	t.Cleanup(func() { _ = w.close(false) })

	// Committed to 10, backfilled to 3, slot 1 holds mark 10 == mxFrame, and
	// no concurrent state change happens during acquisition.
	w.index.mxCommitFrame.Store(10)
	w.index.maxFrame.Store(10)
	w.index.nBackfill.Store(3)
	w.index.aReadMark[1].Store(10)

	hdr, maxFrame, slot, err := w.tryBeginReadInProcessHdr()
	require.NoError(t, err)
	require.False(t, errors.Is(err, errWALRetry))
	assert.Equal(t, 1, slot, "stable state must reuse the existing slot 1")
	assert.Equal(t, uint32(10), maxFrame)
	assert.Equal(t, uint32(10), hdr.mxFrame)
	w.endRead(slot)
}
