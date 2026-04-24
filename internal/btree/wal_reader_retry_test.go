package btree

import (
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTryBeginReadMultiProcessHdr_AllSlotsBusyReturnsRetry asserts that when
// all four reader slots (1..4) are held shared by peer readers AND mxFrame > 0
// (so slot-0 fast path is unavailable because nBackfill < mxFrame), a new
// reader's tryBeginReadMultiProcessHdr returns errWALRetry, not ErrBusy.
//
// This mirrors SQLite walTryBeginRead's conversion at wal.c:3186-3188:
//
//	if( mxI==0 ){
//	  assert( rc==SQLITE_BUSY || (pWal->readOnly & WAL_SHM_RDONLY)!=0 );
//	  return rc==SQLITE_BUSY ? WAL_RETRY : SQLITE_READONLY_CANTINIT;
//	}
//
// SQLite never surfaces SQLITE_BUSY to its caller; the WAL_RETRY sentinel is
// consumed by the do/while loop in walBeginReadTransaction (wal.c:3391-3393).
func TestTryBeginReadMultiProcessHdr_AllSlotsBusyReturnsRetry(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{PageSize: 4096})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Commit some frames so mxFrame > 0 and nBackfill == 0 — this forces
	// tryBeginReadMultiProcessHdr down the slot-1..4 selection path.
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 100)))
	}
	require.NoError(t, tx.Commit())

	w := db.pager.wal
	require.Greater(t, w.index.maxFrame.Load(), uint32(0), "mxFrame must be > 0 to force slot-1..4 path")

	// Hold slots 1..4 shared from this goroutine to simulate peer readers.
	// Before taking the shared lock, set each readmark to a value > mxFrame
	// so step 4's candidate search rejects all of them (bestSlot == -1),
	// forcing step 5 to try exclusive-claim — which must fail because we
	// hold each slot shared ourselves.
	mxFrame := w.index.maxFrame.Load()
	for i := 1; i <= 4; i++ {
		w.index.shmWriteReadMark(i, mxFrame+1000) // out of range
		require.NoError(t, w.index.lock(lockRead0+i, lockShared), "peer reader shared lock slot %d", i)
	}
	defer func() {
		for i := 1; i <= 4; i++ {
			_ = w.index.unlock(lockRead0+i, lockShared)
		}
	}()

	_, _, _, err = w.tryBeginReadMultiProcessHdr()
	if !errors.Is(err, errWALRetry) {
		t.Fatalf("got err=%v (type %T), want errWALRetry — SQLite wal.c:3188 converts SQLITE_BUSY to WAL_RETRY here", err, err)
	}
}
