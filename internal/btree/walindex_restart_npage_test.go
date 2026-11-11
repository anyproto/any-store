//go:build (linux || darwin) && (amd64 || arm64)

package btree

// A process that has not written since a peer grew the database has a stale
// process-local size. Its readers bound page numbers by the snapshot's
// wal-index header instead (hdr.nPage, SQLite's sqlite3WalDbsize), so that
// header has to keep carrying the committed size after the WAL is reset:
// walRestartHdr zeroes mxFrame and leaves nPage alone (wal.c:2146-2160). And
// when the header has no size at all (no commit since the wal-index was
// built), the size is the database file's, as in pagerPagecount
// (pager.c:3292-3306), not the stale local one.
//
// The peers are real processes (re-exec, as in multiprocess_dms_test.go).

import (
	"encoding/binary"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

var restartNPageOpts = Options{
	PageSize:              4096,
	CacheSize:             100,
	InProcess:             false,
	DisableAutoCheckpoint: true,
}

const (
	restartNPageSeedRows = 20
	restartNPageRows     = 90
	restartNPageEnvPath  = "TEST_RESTART_NPAGE_DB_PATH"
	restartNPageEnvMode  = "TEST_RESTART_NPAGE_MODE"
)

// restartNPageChild is a peer process. Modes:
//
//	grow:          commit rows [seed, rows), exit without closing
//	grow-truncate: the same, then a TRUNCATE checkpoint
//	grow-restart:  the same, then a RESTART checkpoint
//	attach:        open and exit (a fresh attach rebuilds the wal-index)
func restartNPageChild(t *testing.T) bool {
	path := os.Getenv(restartNPageEnvPath)
	if path == "" {
		return false
	}
	db, err := testOpen(t, path, restartNPageOpts)
	require.NoError(t, err)
	switch mode := os.Getenv(restartNPageEnvMode); mode {
	case "grow", "grow-truncate", "grow-restart":
		dmsPutRange(t, db, "t1", restartNPageSeedRows, restartNPageRows)
		if mode == "grow-truncate" {
			require.NoError(t, db.Checkpoint(CheckpointTruncate))
		}
		if mode == "grow-restart" {
			require.NoError(t, db.Checkpoint(CheckpointRestart))
		}
	case "attach":
	default:
		t.Fatalf("unknown mode %q", mode)
	}
	if t.Failed() {
		return true
	}
	os.Exit(0) // no Close: no close-time checkpoint under the parent
	return true
}

func runRestartNPageChild(t *testing.T, testName, path, mode string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^"+testName+"$", "-test.v", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), restartNPageEnvPath+"="+path, restartNPageEnvMode+"="+mode)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "peer (%s) failed:\n%s", mode, out)
}

// openRestartNPageSurvivor opens the database, commits the seed rows and
// returns the handle. It starts no write transaction afterwards, so its
// process-local size stays at the seed's.
func openRestartNPageSurvivor(t *testing.T) (*DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := testOpen(t, path, restartNPageOpts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	dmsPutRange(t, db, "t1", 0, restartNPageSeedRows)
	return db, path
}

func requireRestartNPageRows(t *testing.T, db *DB) {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Rollback()) }()
	ns, err := rtx.GetNamespace("t1")
	require.NoError(t, err)
	for i := 0; i < restartNPageRows; i++ {
		val, err := rtx.Get(ns, binary.BigEndian.AppendUint32(nil, uint32(i)))
		require.NoError(t, err, "row %d", i)
		require.Len(t, val, 300, "row %d", i)
	}
}

// TestWalRestartKeepsNPage_PeerResets: a peer grows the database and resets
// the WAL (TRUNCATE, RESTART). A process that only reads must keep reading
// every row; it used to get ErrCorrupt for pages beyond its stale size.
func TestWalRestartKeepsNPage_PeerResets(t *testing.T) {
	if restartNPageChild(t) {
		return
	}
	for _, mode := range []string{"grow-truncate", "grow-restart"} {
		t.Run(mode, func(t *testing.T) {
			db, path := openRestartNPageSurvivor(t)
			stale := db.pager.dbSize.Load()
			runRestartNPageChild(t, "TestWalRestartKeepsNPage_PeerResets", path, mode)

			hdr, valid := db.pager.wal.index.readHeader()
			require.True(t, valid)
			require.Equal(t, uint32(0), hdr.mxFrame, "setup: the peer reset the WAL")
			require.Greater(t, hdr.nPage, stale, "the reset header must keep the committed database size")
			require.Equal(t, stale, db.pager.dbSize.Load(), "setup: the local size is stale")

			requireRestartNPageRows(t, db)
			require.NoError(t, db.IntegrityCheck())
		})
	}
}

// TestWalRestartKeepsNPage_OwnCheckpoint: the stale process resets the WAL
// itself, without having read the peer's commits first.
func TestWalRestartKeepsNPage_OwnCheckpoint(t *testing.T) {
	if restartNPageChild(t) {
		return
	}
	db, path := openRestartNPageSurvivor(t)
	stale := db.pager.dbSize.Load()
	runRestartNPageChild(t, "TestWalRestartKeepsNPage_OwnCheckpoint", path, "grow")

	require.NoError(t, db.Checkpoint(CheckpointTruncate))
	hdr, valid := db.pager.wal.index.readHeader()
	require.True(t, valid)
	require.Equal(t, uint32(0), hdr.mxFrame)
	require.Greater(t, hdr.nPage, stale, "the reset header must keep the committed database size")

	requireRestartNPageRows(t, db)
	require.NoError(t, db.IntegrityCheck())
}

// TestReaderSizeWithoutWalCommit: the wal-index header carries no size (a
// later process attached and rebuilt the index from the empty WAL). The
// reader's bound is then the database file's size, not this process's stale
// local size.
func TestReaderSizeWithoutWalCommit(t *testing.T) {
	if restartNPageChild(t) {
		return
	}
	db, path := openRestartNPageSurvivor(t)
	stale := db.pager.dbSize.Load()
	runRestartNPageChild(t, "TestReaderSizeWithoutWalCommit", path, "grow-truncate")
	runRestartNPageChild(t, "TestReaderSizeWithoutWalCommit", path, "attach")

	hdr, valid := db.pager.wal.index.readHeader()
	require.True(t, valid)
	require.Equal(t, uint32(0), hdr.mxFrame)
	require.Equal(t, uint32(0), hdr.nPage, "setup: a wal-index rebuilt from an empty WAL has no size")
	require.Equal(t, stale, db.pager.dbSize.Load(), "setup: the local size is stale")

	requireRestartNPageRows(t, db)
	require.NoError(t, db.IntegrityCheck())
}

// TestIntegrityCheckAfterPeerGrowth: the peer's new pages are still only in
// the WAL. IntegrityCheck in the process that has not written since must
// bound page numbers by its snapshot, like every other reader.
func TestIntegrityCheckAfterPeerGrowth(t *testing.T) {
	if restartNPageChild(t) {
		return
	}
	db, path := openRestartNPageSurvivor(t)
	runRestartNPageChild(t, "TestIntegrityCheckAfterPeerGrowth", path, "grow")
	requireRestartNPageRows(t, db)
	require.NoError(t, db.IntegrityCheck())
}
