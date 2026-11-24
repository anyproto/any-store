//go:build vfs && (linux || darwin) && (amd64 || arm64)

package btree

import (
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// dbLockStallFile runs a hook once, when the database file's descriptor is
// asked for by the lock call (flockNB): the closer is stopped at its request
// for the exclusive lock.
type dbLockStallFile struct {
	File
	hook atomic.Pointer[func()]
}

func (f *dbLockStallFile) Fd() uintptr {
	if f.hook.Load() != nil && calledFrom("btree.flockNB") {
		if h := f.hook.Swap(nil); h != nil {
			(*h)()
		}
	}
	return f.File.Fd()
}

func calledFrom(fn string) bool {
	var pcs [16]uintptr
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs[:])])
	for {
		fr, more := frames.Next()
		if strings.HasSuffix(fr.Function, fn) {
			return true
		}
		if !more {
			return false
		}
	}
}

// A peer holds the write lock in an open transaction for longer than the
// closer waits for it. While the closer is at its request for the exclusive
// lock on the database file, the peer commits and its process ends without a
// Close. The closer is then the last connection, and what it checkpoints and
// truncates has to include that commit: whatever it learned about the log
// before the lock is out of date.
func TestCloseLastClient_KeepsCommitBeforeLock(t *testing.T) {
	if closeOrderChild(t) {
		return
	}
	db, path := openRestartDB(t, restartModes[0])
	restartPut(t, db, 0, 60, 1)

	peer := startCloseOrderPeer(t, "TestCloseLastClient_KeepsCommitBeforeLock", path, "commit-on-signal")
	waited := false
	defer func() {
		if !waited {
			_ = peer.Process.Kill()
			_ = peer.Wait()
		}
	}()
	closeOrderWaitFile(t, path+".holding")

	db.TestSeam().SetBusyTimeout(50 * time.Millisecond)
	stall := &dbLockStallFile{File: db.pager.file}
	db.pager.file = stall
	var peerErr error
	committed := false
	h := func() {
		if err := os.WriteFile(path+".commit", nil, 0o644); err != nil {
			peerErr = err
			return
		}
		peerErr = peer.Wait()
		waited = true
		_, err := os.Stat(path + ".committed")
		committed = err == nil
	}
	stall.hook.Store(&h)
	require.NoError(t, db.Close())
	require.Nil(t, stall.hook.Load(), "setup: the close asked for the database file lock")
	require.NoError(t, peerErr)
	require.True(t, committed, "setup: the peer committed while the closer was at the lock")

	info, err := os.Stat(path + "-wal")
	require.NoError(t, err)
	require.Zero(t, info.Size(), "the last connection's close checkpoints the log and empties the WAL")

	db2, err := testOpen(t, path, restartModes[0].opts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()
	restartCheck(t, db2, 0, 60, 1)
	restartCheck(t, db2, 100, 110, 7)
	require.NoError(t, db2.IntegrityCheck())
}
