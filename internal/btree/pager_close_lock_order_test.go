//go:build (linux || darwin) && (amd64 || arm64)

package btree

// Close decides whether it is the last connection before it checkpoints, as
// sqlite3WalClose does (wal.c:2522-2551): the exclusive lock on the database
// file first, and only under it the PASSIVE checkpoint and the WAL truncate.
// A connection that is not the last leaves the WAL alone.

import (
	"encoding/binary"
	"os"
	"os/exec"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	closeOrderEnvPath = "ANYSTORE_CLOSE_ORDER_DB"
	closeOrderEnvMode = "ANYSTORE_CLOSE_ORDER_MODE"
)

func closeOrderWaitFile(t *testing.T, name string) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, err := os.Stat(name)
		return err == nil
	}, 30*time.Second, 5*time.Millisecond, "%s never appeared", name)
}

// closeOrderChild is the peer process. It never closes its handle: it exits
// as a crashed process does, without a checkpoint of its own.
func closeOrderChild(t *testing.T) bool {
	path := os.Getenv(closeOrderEnvPath)
	if path == "" {
		return false
	}
	db, err := testOpen(t, path, restartModes[0].opts)
	require.NoError(t, err)
	switch mode := os.Getenv(closeOrderEnvMode); mode {
	case "attached": // attached, idle; reports the checkpoint frontier at the end
		restartCheck(t, db, 0, 60, 1)
		require.NoError(t, os.WriteFile(path+".attached", nil, 0o644))
		closeOrderWaitFile(t, path+".stop")
		nBackfill := db.pager.wal.index.liveNBackfill()
		require.NoError(t, os.WriteFile(path+".frontier", binary.BigEndian.AppendUint32(nil, nBackfill), 0o644))
	case "commit-on-signal": // an open write transaction, committed when told
		wtx, err := db.BeginWrite()
		require.NoError(t, err)
		restartPutTx(t, wtx, 100, 110, 7)
		require.NoError(t, os.WriteFile(path+".holding", nil, 0o644))
		closeOrderWaitFile(t, path+".commit")
		require.NoError(t, wtx.Commit())
		require.NoError(t, os.WriteFile(path+".committed", nil, 0o644))
	default:
		t.Fatalf("unknown mode %q", mode)
	}
	if t.Failed() {
		return true
	}
	os.Exit(0)
	return true
}

func startCloseOrderPeer(t *testing.T, testName, path, mode string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^"+testName+"$", "-test.v", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), closeOrderEnvPath+"="+path, closeOrderEnvMode+"="+mode)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	require.NoError(t, cmd.Start())
	return cmd
}

// With another process attached, Close neither checkpoints nor truncates:
// without the exclusive lock sqlite3WalClose skips both (the `if` at
// wal.c:2522-2524).
func TestCloseNotLastClient_LeavesWAL(t *testing.T) {
	if closeOrderChild(t) {
		return
	}
	db, path := openRestartDB(t, restartModes[0])
	restartPut(t, db, 0, 60, 1)
	require.Zero(t, db.pager.wal.index.liveNBackfill(), "setup: nothing is backfilled")

	peer := startCloseOrderPeer(t, "TestCloseNotLastClient_LeavesWAL", path, "attached")
	stopped := false
	defer func() {
		if !stopped {
			_ = peer.Process.Kill()
			_ = peer.Wait()
		}
	}()
	closeOrderWaitFile(t, path+".attached")

	before, err := os.Stat(path + "-wal")
	require.NoError(t, err)
	require.NotZero(t, before.Size())
	ran := false
	checkpointed, err := db.CloseCheckpointed(func() { ran = true })
	require.NoError(t, err)
	require.False(t, checkpointed, "a close with another process attached reported a checkpoint")
	require.False(t, ran, "a close with another process attached ran the callback")
	after, err := os.Stat(path + "-wal")
	require.NoError(t, err)
	require.Equal(t, before.Size(), after.Size(), "the WAL is the peer's too")

	require.NoError(t, os.WriteFile(path+".stop", nil, 0o644))
	require.NoError(t, peer.Wait())
	stopped = true
	b, err := os.ReadFile(path + ".frontier")
	require.NoError(t, err)
	require.Len(t, b, 4)
	require.Zero(t, binary.BigEndian.Uint32(b), "the close ran a checkpoint without the exclusive lock")

	db2, err := testOpen(t, path, restartModes[0].opts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()
	restartCheck(t, db2, 0, 60, 1)
	require.NoError(t, db2.IntegrityCheck())
}

// A close that is not the last does not wait for another process's write
// transaction: sqlite3WalClose takes nothing before the exclusive lock on the
// database file (wal.c:2522-2523), and without that lock it neither
// checkpoints nor truncates, so it never asks for the write lock and never
// calls its busy handler.
func TestCloseNotLastClient_PeerWriterNoBusyWait(t *testing.T) {
	if closeOrderChild(t) {
		return
	}
	db, path := openRestartDB(t, restartModes[0])
	restartPut(t, db, 0, 60, 1)

	peer := startCloseOrderPeer(t, "TestCloseNotLastClient_PeerWriterNoBusyWait", path, "commit-on-signal")
	waited := false
	defer func() {
		if !waited {
			_ = peer.Process.Kill()
			_ = peer.Wait()
		}
	}()
	closeOrderWaitFile(t, path+".holding")

	var busyCalls atomic.Int32
	db.pager.wal.busyHandler = func(int) bool {
		busyCalls.Add(1)
		return false
	}
	before, err := os.Stat(path + "-wal")
	require.NoError(t, err)
	require.NoError(t, db.Close())
	require.Zero(t, busyCalls.Load(), "the close waited on the peer's write lock")
	after, err := os.Stat(path + "-wal")
	require.NoError(t, err)
	require.Equal(t, before.Size(), after.Size(), "the WAL is the peer's too")

	// The peer commits after the close and ends without one of its own.
	require.NoError(t, os.WriteFile(path+".commit", nil, 0o644))
	require.NoError(t, peer.Wait())
	waited = true
	_, err = os.Stat(path + ".committed")
	require.NoError(t, err, "setup: the peer committed")

	db2, err := testOpen(t, path, restartModes[0].opts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()
	restartCheck(t, db2, 0, 60, 1)
	restartCheck(t, db2, 100, 110, 7)
	require.NoError(t, db2.IntegrityCheck())
}

// The last close runs CloseCheckpointed's callback after its checkpoint and
// the truncate of the WAL, and before it lets go of the exclusive lock on
// the database file: a shared lock from another descriptor, which every
// process attaching takes (acquireSharedDBLock), is refused while it runs.
// The durability sentinel is removed there, so no other process can attach
// and commit between the checkpoint and the removal.
func TestCloseCheckpointed_CallbackRunsUnderTheFileLock(t *testing.T) {
	db, path := openRestartDB(t, restartModes[0])
	restartPut(t, db, 0, 60, 1)

	sharedLock := func() error {
		f, err := os.Open(path)
		require.NoError(t, err)
		defer func() { _ = f.Close() }()
		return syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB)
	}
	ran := false
	checkpointed, err := db.CloseCheckpointed(func() {
		ran = true
		wal, err := os.Stat(path + "-wal")
		require.NoError(t, err)
		require.Zero(t, wal.Size(), "the callback ran before the WAL was truncated")
		require.ErrorIs(t, sharedLock(), syscall.EWOULDBLOCK, "the callback ran without the exclusive lock on the database file")
	})
	require.NoError(t, err)
	require.True(t, checkpointed)
	require.True(t, ran)
	require.NoError(t, sharedLock(), "the close kept the database file locked")
}
