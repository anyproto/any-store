//go:build (linux || darwin) && (amd64 || arm64)

package btree

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIntegrityCheckSlot0IgnoresNewWALGeneration: an integrity check that
// begins on a fully checkpointed WAL holds read slot 0, and a slot-0 reader
// reads nothing from the WAL (walFindFrame, wal.c:3538-3541). A WAL restart
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

	// Under the check: a commit that empties the tree. The log is fully
	// backfilled and no reader is in it (the check is on slot 0), so the
	// writer starts the log over: its frames (1..k, k far below the check's
	// mxFrame) free every page but the root.
	require.NoError(t, db.Checkpoint(CheckpointRestart))
	after, valid := db.pager.wal.index.readHeader()
	require.True(t, valid)
	require.Equal(t, hdr, after, "a RESTART checkpoint leaves the header as it is")
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

// TestIntegrityCheckIgnoresOpenWriterTruncation: a reader's page count is its
// snapshot's (pagerPagecount takes sqlite3WalDbsize, pWal->hdr.nPage of the
// read snapshot, pager.c:3279-3306, wal.c:3669-3674), not the count of a
// write transaction still open in the same process. A writer that has
// truncated the database and not committed (a backup into this database from
// a smaller source does) must not make a concurrent integrity check, or a
// read transaction begun meanwhile, treat the committed pages above the
// truncation as out of range.
func TestIntegrityCheckIgnoresOpenWriterTruncation(t *testing.T) {
	const rows = 400
	cases := []struct {
		name   string
		opts   Options
		reopen bool // close and reopen first: the WAL then holds no commit
		// restart: the writer starts the log over before it truncates. The
		// restart moves the commit cursor to 0 and must keep the committed
		// size with it, as walRestartHdr keeps hdr.nPage (wal.c:2146-2160).
		restart bool
	}{
		{name: "in-process", opts: Options{PageSize: 4096, InProcess: true, DisableAutoCheckpoint: true}},
		{name: "in-process reopened", opts: Options{PageSize: 4096, InProcess: true, DisableAutoCheckpoint: true}, reopen: true},
		{name: "in-process restarted log", opts: Options{PageSize: 4096, InProcess: true, DisableAutoCheckpoint: true}, restart: true},
		{name: "in-memory", opts: Options{PageSize: 4096, InMemory: true}},
		{name: "in-memory restarted log", opts: Options{PageSize: 4096, InMemory: true, DisableAutoCheckpoint: true}, restart: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := t.TempDir() + "/test.db"
			db, err := testOpen(t, path, tc.opts)
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			dmsPutRange(t, db, "t1", 0, rows)
			if tc.reopen {
				require.NoError(t, db.Close())
				db, err = testOpen(t, path, tc.opts)
				require.NoError(t, err)
				require.Equal(t, uint32(0), db.pager.wal.index.mxCommitFrame.LoadLocal(), "setup: no commit in the WAL")
			}
			if tc.restart {
				// The whole log in the database file: the next writer's
				// first frames start it over (restartLogLocked).
				require.NoError(t, db.Checkpoint(CheckpointPassive))
			}
			committed := db.pager.dbSize.Load()
			require.Greater(t, committed, uint32(16), "setup: a database of many pages")

			wtx, err := db.BeginWrite()
			require.NoError(t, err)
			if tc.restart {
				restarts := db.pager.wal.index.restarts.Load()
				restartPutTx(t, wtx, 0, 1, 2) // one row, same size
				restartSpillAll(t, db)
				require.NotEqual(t, restarts, db.pager.wal.index.restarts.Load(), "setup: the writer started the log over")
				require.Zero(t, db.pager.wal.index.mxCommitFrame.LoadLocal(), "setup: no commit in the new log")
			}
			require.NoError(t, db.pager.truncateTo(committed/4))
			require.Equal(t, committed/4, db.pager.dbSize.Load(), "setup: the writer's count is the truncated one")

			// From another goroutine, as a concurrent reader would run.
			errs := make(chan error, 1)
			go func() {
				if err := db.IntegrityCheck(); err != nil {
					errs <- fmt.Errorf("integrity check: %w", err)
					return
				}
				rtx, err := db.BeginRead()
				if err != nil {
					errs <- err
					return
				}
				defer func() { _ = rtx.Rollback() }()
				if n := rtx.DatabaseSize(); n != committed {
					errs <- fmt.Errorf("reader's database size %d, committed %d", n, committed)
					return
				}
				ns, err := rtx.GetNamespace("t1")
				if err != nil {
					errs <- err
					return
				}
				for i := 0; i < rows; i++ {
					v, err := rtx.Get(ns, binary.BigEndian.AppendUint32(nil, uint32(i)))
					if err != nil || len(v) != 300 {
						errs <- fmt.Errorf("row %d: %d bytes, %v", i, len(v), err)
						return
					}
				}
				errs <- nil
			}()
			require.NoError(t, <-errs, "a reader of the committed snapshot must not be bounded by the open writer's page count")

			require.NoError(t, wtx.Rollback())
			require.Equal(t, committed, db.pager.dbSize.Load())
			require.NoError(t, db.IntegrityCheck())
		})
	}
}
