//go:build (linux || darwin) && (amd64 || arm64)

package btree

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIntegrityCheckSlot0IgnoresNewWALGeneration: an integrity check that
// begins on a fully checkpointed WAL holds read slot 0, and a slot-0 reader
// reads nothing from the WAL (walFindFrame, wal.c:3524-3527). A WAL restart
// needs only slots 1-4, so it can run under the check, and the next writer's
// frames are numbered from 1 again, below the check's snapshot mxFrame. The
// check must keep reading its own snapshot, the database file, and report no
// corruption.
func TestIntegrityCheckSlot0IgnoresNewWALGeneration(t *testing.T) {
	db, err := testOpen(t, t.TempDir()+"/test.db", restartNPageOpts)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Many small commits: the WAL generation the check starts on has far
	// more frames than the commit that follows the restart.
	const rows = 200
	for i := 0; i < rows; i += 5 {
		dmsPutRange(t, db, "t1", i, i+5)
	}
	require.NoError(t, db.Checkpoint(CheckpointFull))
	hdr, valid := db.pager.wal.index.readHeader()
	require.True(t, valid)
	require.Equal(t, hdr.mxFrame, db.pager.wal.index.shmNBackfill(), "setup: the WAL is fully backfilled")

	inside := make(chan struct{})
	resume := make(chan struct{})
	testIntegritySnapshotHook = func() {
		testIntegritySnapshotHook = nil
		close(inside)
		<-resume
	}
	defer func() { testIntegritySnapshotHook = nil }()

	done := make(chan error, 1)
	go func() { done <- db.IntegrityCheck() }()
	<-inside

	// Under the check: restart the WAL, then a commit that empties the tree.
	// Its frames (1..k, k far below the check's mxFrame) free every page but
	// the root.
	require.NoError(t, db.Checkpoint(CheckpointRestart))
	after, valid := db.pager.wal.index.readHeader()
	require.True(t, valid)
	require.Equal(t, uint32(0), after.mxFrame, "setup: the restart ran under the check")
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.GetNamespace("t1")
	require.NoError(t, err)
	for i := 0; i < rows; i++ {
		require.NoError(t, wtx.Delete(ns, binary.BigEndian.AppendUint32(nil, uint32(i))))
	}
	require.NoError(t, wtx.Commit())
	after, valid = db.pager.wal.index.readHeader()
	require.True(t, valid)
	require.Less(t, after.mxFrame, hdr.mxFrame, "setup: the new frames are numbered below the check's snapshot")

	close(resume)
	require.NoError(t, <-done, "the check must read only its snapshot")
	require.NoError(t, db.IntegrityCheck())
}
