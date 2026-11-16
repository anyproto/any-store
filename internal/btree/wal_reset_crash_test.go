//go:build (linux || darwin) && (amd64 || arm64)

package btree

// A TRUNCATE checkpoint that dies partway through its WAL reset, while
// another process keeps the database open. SQLite publishes the reset
// wal-index header before it truncates the WAL (walRestartHdr, then
// sqlite3OsTruncate, wal.c:2377-2378), so at every point a survivor either
// sees the old header over the old, intact WAL, or the reset header (no
// frames) over whatever is left of it. A survivor must never see the old
// header over a truncated WAL: it would append its next commit after frames
// that no longer exist, and recovery, which reads the WAL from its start,
// would stop at the hole and drop that acknowledged commit.
//
// The checkpointer is a real process (re-exec, as in multiprocess_dms_test.go)
// that exits at a reset step, as a SIGKILL there would leave things: the shm
// as written so far, the WAL as truncated so far, every lock released.

import (
	"encoding/binary"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	resetCrashSeedRows  = 20 // committed by the survivor before the peer attaches
	resetCrashPeerRows  = 60 // the peer commits rows [seed, peer) and then checkpoints
	resetCrashTotalRows = 90 // the survivor commits rows [peer, total) after the crash
	resetCrashEnvPath   = "TEST_RESET_CRASH_DB_PATH"
	resetCrashEnvMode   = "TEST_RESET_CRASH_MODE"
	resetCrashEnvArg    = "TEST_RESET_CRASH_ARG"
)

// resetCrashChild is the peer process. Modes:
//
//	crash:  commit rows [seed, peer), then a TRUNCATE checkpoint that exits
//	        when the WAL reset reaches step ARG
//	verify: open (a fresh attach rebuilds the wal-index from the WAL) and
//	        check rows [0, ARG)
func resetCrashChild(t *testing.T) bool {
	path := os.Getenv(resetCrashEnvPath)
	if path == "" {
		return false
	}
	db, err := testOpen(t, path, restartNPageOpts)
	require.NoError(t, err)
	arg := os.Getenv(resetCrashEnvArg)
	switch mode := os.Getenv(resetCrashEnvMode); mode {
	case "crash":
		dmsPutRange(t, db, "t1", resetCrashSeedRows, resetCrashPeerRows)
		testResetWALHook = func(step string) {
			if step == arg {
				os.Exit(0) // die here: no unlock, no Close
			}
		}
		err := db.Checkpoint(CheckpointTruncate)
		t.Fatalf("the reset never reached step %q (checkpoint returned %v)", arg, err)
	case "verify":
		n, err := strconv.Atoi(arg)
		require.NoError(t, err)
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		ns, err := rtx.GetNamespace("t1")
		require.NoError(t, err)
		for i := 0; i < n; i++ {
			val, err := rtx.Get(ns, binary.BigEndian.AppendUint32(nil, uint32(i)))
			require.NoError(t, err, "row %d", i)
			require.Len(t, val, 300, "row %d", i)
			require.Equal(t, byte(i), val[0], "row %d", i)
		}
		require.NoError(t, rtx.Rollback())
	default:
		t.Fatalf("unknown mode %q", mode)
	}
	if t.Failed() {
		return true
	}
	os.Exit(0) // no Close: no close-time checkpoint under the parent
	return true
}

func runResetCrashChild(t *testing.T, path, mode, arg string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestWalResetCrash_SurvivorCommitsKept$", "-test.v", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), resetCrashEnvPath+"="+path, resetCrashEnvMode+"="+mode, resetCrashEnvArg+"="+arg)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "peer (%s %s) failed:\n%s", mode, arg, out)
}

// TestWalResetCrash_SurvivorCommitsKept: the peer's TRUNCATE checkpoint dies
// at each step of its WAL reset. The survivor then commits; that commit has
// been acknowledged and must be in what a fresh attach recovers from the WAL.
func TestWalResetCrash_SurvivorCommitsKept(t *testing.T) {
	if resetCrashChild(t) {
		return
	}
	for _, step := range []string{"publish", "truncate", "truncated"} {
		t.Run(step, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "test.db")
			db, err := testOpen(t, path, restartNPageOpts)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			dmsPutRange(t, db, "t1", 0, resetCrashSeedRows)

			runResetCrashChild(t, path, "crash", step)

			dmsPutRange(t, db, "t1", resetCrashPeerRows, resetCrashTotalRows)
			runResetCrashChild(t, path, "verify", strconv.Itoa(resetCrashTotalRows))
		})
	}
}
