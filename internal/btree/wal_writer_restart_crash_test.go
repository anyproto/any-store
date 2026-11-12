//go:build (linux || darwin) && (amd64 || arm64)

package btree

// The writer-side log restart (wal_writer_restart_test.go) at its edges: a
// log that is only partly in the database file, a checkpoint that finds the
// log started over under it, a crash at each step of a restart, a panic right
// after one, and a reader in another process.

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A log that is only partly in the database file is never started over, even
// once no reader uses it: the frames above nBackfill exist nowhere else.
// SQLite gets this from its read lock 0, which implies a full backfill
// (wal.c:3128-3136, asserted at wal.c:3872); here it is an explicit test.
func TestWalWriterRestart_NotAfterPartialBackfill(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		db, path := openRestartDB(t, m)
		restartPut(t, db, 0, 60, 1)
		w := db.pager.wal
		n1 := w.index.mxCommitFrame.LoadLocal()

		rtx, err := db.BeginRead() // pins n1
		require.NoError(t, err)
		done := false
		t.Cleanup(func() {
			if !done {
				_ = rtx.Rollback()
			}
		})
		restartPut(t, db, 60, 120, 2)
		n2 := w.index.mxCommitFrame.LoadLocal()
		require.NoError(t, db.Checkpoint(CheckpointPassive))
		require.Equal(t, n1, w.index.liveNBackfill(), "setup: the backfill stops at the reader")
		done = true
		require.NoError(t, rtx.Rollback())

		restartPut(t, db, 0, 2, 3)
		require.Greater(t, w.index.mxCommitFrame.LoadLocal(), n2, "frames are appended: part of the log is not backfilled")

		restartCheck(t, db, 0, 2, 3)
		restartCheck(t, db, 2, 60, 1)
		restartCheck(t, db, 60, 120, 2)
		db = restartReopen(t, db, path, m)
		restartCheck(t, db, 0, 2, 3)
		restartCheck(t, db, 2, 60, 1)
		restartCheck(t, db, 60, 120, 2)
		require.NoError(t, db.IntegrityCheck())
	})
}

// A checkpoint that finds, once it holds reader slot 0, that the log was
// started over since it read the header copies nothing (version-3.52.0
// wal.c:2256-2267). Every restart in this code base holds the checkpoint
// lock, which a checkpoint holds too, so the hook stands in for a writer
// that follows SQLite's protocol and does not take it.
func TestWalCheckpoint_SkipsBackfillAfterLogWrapped(t *testing.T) {
	m := restartModes[0] // shared memory: the salts are in the wal-index header
	db, _ := openRestartDB(t, m)
	restartPut(t, db, 0, 60, 1)
	w := db.pager.wal
	n := w.index.mxCommitFrame.LoadLocal()
	require.Zero(t, w.index.liveNBackfill())

	wrapped := false
	testCheckpointRead0Hook = func() {
		if wrapped {
			return
		}
		wrapped = true
		// What walRestartHdr publishes: no frames, new salts, nBackfill 0.
		// The checkpoint holds w.mu, as this function's callers do.
		hdr, valid := w.index.readHeader()
		require.True(t, valid)
		w.restartHeaderStateLocked(false, hdr.nPage, hdr.aSalt[0]+1)
	}
	defer func() { testCheckpointRead0Hook = nil }()

	require.NoError(t, db.Checkpoint(CheckpointPassive))
	require.True(t, wrapped, "setup: the hook ran")
	require.Zero(t, w.index.liveNBackfill(), "nothing was backfilled from the old header's %d frames", n)
	hdr, valid := w.index.readHeader()
	require.True(t, valid)
	require.Zero(t, hdr.mxFrame)
}

func restartCopyFiles(t *testing.T, path, name string) string {
	t.Helper()
	dir := filepath.Join(filepath.Dir(path), name)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	dst := filepath.Join(dir, "test.db")
	copyFileForTest(t, path, dst)
	copyFileForTest(t, path+"-wal", dst+"-wal") // no -shm: the copy recovers from the WAL
	return dst
}

// The files as a crash would leave them at each step of a restart must
// recover to the last commit: right after the restart is published (the WAL
// file still holds the old log, all of it backfilled), with the new log's
// header and uncommitted frames in the file, and after the commit.
func TestWalWriterRestart_CrashPoints(t *testing.T) {
	for _, m := range restartModes[:2] { // the modes with files
		t.Run(m.name, func(t *testing.T) {
			db, path := openRestartDB(t, m)
			restartPut(t, db, 0, 200, 1)
			require.NoError(t, db.Checkpoint(CheckpointPassive))

			var atRestart string
			testRestartLogHook = func(step string) {
				if step == "restarted" {
					atRestart = restartCopyFiles(t, path, "at-restart")
				}
			}
			defer func() { testRestartLogHook = nil }()

			wtx, err := db.BeginWrite()
			require.NoError(t, err)
			restartPutTx(t, wtx, 0, 100, 2)
			restartSpillAll(t, db)
			require.NotEmpty(t, atRestart, "setup: the log was started over")
			testRestartLogHook = nil
			afterSpill := restartCopyFiles(t, path, "after-spill")
			require.NoError(t, wtx.Commit())
			afterCommit := restartCopyFiles(t, path, "after-commit")

			for _, c := range []struct {
				path string
				fill byte
			}{{atRestart, 1}, {afterSpill, 1}, {afterCommit, 2}} {
				cdb, err := testOpen(t, c.path, m.opts)
				require.NoError(t, err, c.path)
				restartCheck(t, cdb, 0, 100, c.fill)
				restartCheck(t, cdb, 100, 200, 1)
				require.NoError(t, cdb.IntegrityCheck(), c.path)
				// The recovered log takes a new commit and recovers again.
				restartPut(t, cdb, 0, 3, 9)
				require.NoError(t, cdb.Close())
				cdb, err = testOpen(t, c.path, m.opts)
				require.NoError(t, err, c.path)
				restartCheck(t, cdb, 0, 3, 9)
				restartCheck(t, cdb, 100, 200, 1)
				require.NoError(t, cdb.Close())
			}
		})
	}
}

// A panic right after the log was started over, before any frame of the
// transaction is written, unwinds to the start of the new log, not to the
// frame number the transaction began at. The next commit must be found by a
// later open.
func TestWalWriterRestart_PanicAfterRestart(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		for _, at := range []string{"spill", "commit"} {
			db, path := openRestartDB(t, m)
			restartPut(t, db, 0, 200, 1)
			require.NoError(t, db.Checkpoint(CheckpointPassive))
			n := db.pager.wal.index.mxCommitFrame.LoadLocal()

			testRestartLogHook = func(step string) {
				if step == "restarted" {
					testRestartLogHook = nil
					panic("injected: after the log restart")
				}
			}
			wtx, err := db.BeginWrite()
			require.NoError(t, err)
			restartPutTx(t, wtx, 0, 2, 2)
			if at == "spill" {
				require.Panics(t, func() { restartSpillAll(t, db) })
				require.NoError(t, wtx.Rollback())
			} else {
				require.Panics(t, func() { _ = wtx.Commit() })
			}
			testRestartLogHook = nil
			require.Zero(t, db.pager.wal.nFrame.Load(), "%s: the unwind went to the start of the new log (old end %d)", at, n)
			restartCheck(t, db, 0, 200, 1)

			restartPut(t, db, 0, 3, 3)
			restartCheck(t, db, 0, 3, 3)
			restartCheck(t, db, 3, 200, 1)
			db = restartReopen(t, db, path, m)
			restartCheck(t, db, 0, 3, 3)
			restartCheck(t, db, 3, 200, 1)
			require.NoError(t, db.IntegrityCheck())
			require.NoError(t, db.Close())
		}
	})
}

const (
	restartPeerEnvPath = "TEST_WAL_RESTART_PEER_DB_PATH"
	restartPeerEnvMode = "TEST_WAL_RESTART_PEER_MODE"
)

// restartPeerChild is a peer process. Modes:
//
//	reader: begin a read transaction, create <path>.reading, wait for
//	        <path>.stop, exit
//	wrap:   commit rows [60, 200), checkpoint everything, then start a
//	        transaction that starts the log over, spill it, and exit without
//	        committing or closing
func restartPeerChild(t *testing.T) bool {
	path := os.Getenv(restartPeerEnvPath)
	if path == "" {
		return false
	}
	db, err := testOpen(t, path, restartModes[0].opts)
	require.NoError(t, err)
	switch mode := os.Getenv(restartPeerEnvMode); mode {
	case "reader":
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		require.NotZero(t, rtx.walSlot, "the peer's snapshot is in the log")
		require.NoError(t, os.WriteFile(path+".reading", nil, 0o644))
		for i := 0; i < 3000; i++ {
			if _, err := os.Stat(path + ".stop"); err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	case "wrap":
		restartPut(t, db, 60, 200, 1)
		require.NoError(t, db.Checkpoint(CheckpointPassive))
		n := db.pager.wal.index.mxCommitFrame.LoadLocal()
		wtx, err := db.BeginWrite()
		require.NoError(t, err)
		restartPutTx(t, wtx, 0, 30, 2)
		restartSpillAll(t, db)
		require.Less(t, db.pager.wal.nFrame.Load(), n, "the peer started the log over")
	default:
		t.Fatalf("unknown mode %q", mode)
	}
	if t.Failed() {
		return true
	}
	os.Exit(0) // no Close, no unlock: as a killed process leaves things
	return true
}

func startRestartPeer(t *testing.T, testName, path, mode string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^"+testName+"$", "-test.v", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), restartPeerEnvPath+"="+path, restartPeerEnvMode+"="+mode)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	require.NoError(t, cmd.Start())
	return cmd
}

// A reader in another process that still uses the log keeps this process's
// writer from starting it over; once the reader is gone the next writer does.
func TestWalWriterRestart_PeerReaderKeepsLog(t *testing.T) {
	if restartPeerChild(t) {
		return
	}
	db, path := openRestartDB(t, restartModes[0])
	restartPut(t, db, 0, 60, 1)
	w := db.pager.wal
	n := w.index.mxCommitFrame.LoadLocal()

	peer := startRestartPeer(t, "TestWalWriterRestart_PeerReaderKeepsLog", path, "reader")
	stopped := false
	stop := func() {
		if !stopped {
			stopped = true
			_ = os.WriteFile(path+".stop", nil, 0o644)
			_ = peer.Wait()
		}
	}
	defer stop()
	require.Eventually(t, func() bool {
		_, err := os.Stat(path + ".reading")
		return err == nil
	}, 30*time.Second, 10*time.Millisecond, "the peer never began its read")

	require.NoError(t, db.Checkpoint(CheckpointPassive))
	require.Equal(t, n, w.index.liveNBackfill(), "setup: the whole log is backfilled")
	restartPut(t, db, 0, 2, 2)
	n2 := w.index.mxCommitFrame.LoadLocal()
	require.Greater(t, n2, n, "frames are appended behind the peer's reader")

	stop()
	require.NoError(t, db.Checkpoint(CheckpointPassive))
	require.Equal(t, n2, w.index.liveNBackfill())
	restartPut(t, db, 0, 2, 3)
	require.Less(t, w.index.mxCommitFrame.LoadLocal(), n2, "with the reader gone the writer started the log over")
	restartCheck(t, db, 0, 2, 3)
	restartCheck(t, db, 2, 60, 1)
	require.NoError(t, db.IntegrityCheck())
}

// A peer grows the database, starts the log over and dies before it commits.
// The restart header it published has no frames and keeps the committed
// database size (walRestartHdr leaves hdr.nPage alone, wal.c:2146-2160), so a
// process whose local size is stale still reads every row, and the next
// writer takes the log over from that header.
func TestWalWriterRestart_PeerDiesAfterRestart(t *testing.T) {
	if restartPeerChild(t) {
		return
	}
	db, path := openRestartDB(t, restartModes[0])
	restartPut(t, db, 0, 60, 1)
	stale := db.pager.dbSize.Load()

	peer := startRestartPeer(t, "TestWalWriterRestart_PeerDiesAfterRestart", path, "wrap")
	require.NoError(t, peer.Wait())

	hdr, valid := db.pager.wal.index.readHeader()
	require.True(t, valid)
	require.Zero(t, hdr.mxFrame, "setup: the peer started the log over and committed nothing")
	require.Greater(t, hdr.nPage, stale, "the restart header keeps the committed database size")
	require.Equal(t, stale, db.pager.dbSize.Load(), "setup: the local size is stale")
	restartCheck(t, db, 0, 200, 1)
	require.NoError(t, db.IntegrityCheck())

	restartPut(t, db, 0, 3, 3)
	restartCheck(t, db, 0, 3, 3)
	restartCheck(t, db, 3, 200, 1)
	require.NoError(t, db.Close())
	db2, err := testOpen(t, path, restartModes[0].opts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()
	restartCheck(t, db2, 0, 3, 3)
	restartCheck(t, db2, 3, 200, 1)
	require.NoError(t, db2.IntegrityCheck())
}
