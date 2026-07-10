//go:build (linux || darwin) && (amd64 || arm64)

package btree

// Tests for the DMS first-attacher election in newPlatformShm (docs/btree/
// NOTES.md drift-2026-06-25-25, RESOLVED): the first process to attach a
// <db>-wal-shm takes the DMS byte exclusive and unconditionally resets the
// file to the 3-byte stump, so a crash-stale shm image can never be adopted;
// live attachers join shared and never touch the file; an opener observing a
// peer mid-election (DMS held exclusive) backs off instead of joining
// (unixLockSharedMemory, os_unix.c:4860-4913, 3.52.0).
//
// Lock-conflict simulation stubs the package-level sysFcntl: POSIX record
// locks never conflict within one process, so real cross-process contention
// on the DMS byte cannot be produced in-process. The stubs are global state —
// this package's tests do not use t.Parallel.

import (
	"bytes"
	"encoding/binary"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// stubDMSFcntl installs a sysFcntl stub that calls fake for operations on the
// DMS byte and passes everything else to the real implementation. Restored on
// test cleanup.
func stubDMSFcntl(t *testing.T, fake func(real func(uintptr, int, *syscall.Flock_t) syscall.Errno, fd uintptr, cmd int, flock *syscall.Flock_t) (syscall.Errno, bool)) {
	t.Helper()
	real := sysFcntl
	sysFcntl = func(fd uintptr, cmd int, flock *syscall.Flock_t) syscall.Errno {
		if flock != nil && flock.Start == shmDMSOffset {
			if errno, handled := fake(real, fd, cmd, flock); handled {
				return errno
			}
		}
		return real(fd, cmd, flock)
	}
	t.Cleanup(func() { sysFcntl = real })
}

// writeGarbageShm writes a shmRegionSize file of 0xFF bytes — a stand-in for
// a crash-persisted stale shm image (any content works: the election resets
// the file without looking at it).
func writeGarbageShm(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, bytes.Repeat([]byte{0xFF}, shmRegionSize), 0666))
}

// TestDMSFirstAttacherTruncatesStaleShm is the core protocol regression: a
// pre-existing (crash-persisted) shm with Size() > 0 must be reset to the
// 3-byte stump by a first attacher. Pre-election code gated the truncate on
// Size() == 0 and adopted the stale file as-is.
func TestDMSFirstAttacherTruncatesStaleShm(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x-wal-shm")
	writeGarbageShm(t, path)

	s, err := newPlatformShm(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.close(false) })

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, int64(3), info.Size(),
		"first attacher must unconditionally reset a stale shm to the 3-byte stump")

	// close(isLastClient=false) must not unlink — last-client detection
	// lives at the DB-file flock, not the DMS.
	require.NoError(t, s.close(false))
	_, err = os.Stat(path)
	require.NoError(t, err, "shm file must survive a non-last-client close")
}

// TestDMSNonFirstAttacherDoesNotTruncate: when the F_GETLK probe reports live
// shared holders (F_RDLCK), the opener joins without touching the file — a
// live process is maintaining the shm, and truncating it would SIGBUS peers'
// mappings.
func TestDMSNonFirstAttacherDoesNotTruncate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x-wal-shm")
	writeGarbageShm(t, path)

	stubDMSFcntl(t, func(real func(uintptr, int, *syscall.Flock_t) syscall.Errno, fd uintptr, cmd int, flock *syscall.Flock_t) (syscall.Errno, bool) {
		if cmd == syscall.F_GETLK {
			flock.Type = syscall.F_RDLCK // fake live peers
			return 0, true
		}
		return 0, false // real F_SETLK (no in-process conflict → succeeds)
	})

	s, err := newPlatformShm(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.close(false) })

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, int64(shmRegionSize), info.Size(),
		"non-first attacher must not touch a live-maintained shm")
}

// TestDMSMidElectionBacksOff: when the probe reports a peer holding the DMS
// EXCLUSIVE (mid-election), the opener must back off and retry — never join
// shared. SQLite documents the race this prevents (os_unix.c:4864-4871): an
// earlier version joined shared here, and an exclusive holder dying before
// its truncate left the joiner attached to an untruncated, possibly
// crash-corrupted shm. Exhaustion surfaces ErrBusy (~1s of bounded retries).
func TestDMSMidElectionBacksOff(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x-wal-shm")
	writeGarbageShm(t, path)

	var setlkCalls int
	stubDMSFcntl(t, func(real func(uintptr, int, *syscall.Flock_t) syscall.Errno, fd uintptr, cmd int, flock *syscall.Flock_t) (syscall.Errno, bool) {
		if cmd == syscall.F_GETLK {
			flock.Type = syscall.F_WRLCK // fake a peer mid-election, forever
			return 0, true
		}
		setlkCalls++ // any F_SETLK on the DMS would be a protocol violation
		return 0, false
	})

	_, err := newPlatformShm(path)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrBusy, "retry exhaustion must wrap ErrBusy")
	require.Zero(t, setlkCalls,
		"observing a mid-election peer must never attempt any DMS lock (no shared join)")

	info, serr := os.Stat(path)
	require.NoError(t, serr)
	require.Equal(t, int64(shmRegionSize), info.Size(), "backed-off opener must not touch the file")
}

// TestDMSDowngradeFailureFailsOpen: the WRLCK→RDLCK conversion of a held lock
// cannot legitimately be denied; a denial is a protocol violation and must
// hard-fail the open (even when the errno maps to ErrBusy).
func TestDMSDowngradeFailureFailsOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x-wal-shm")
	writeGarbageShm(t, path)

	stubDMSFcntl(t, func(real func(uintptr, int, *syscall.Flock_t) syscall.Errno, fd uintptr, cmd int, flock *syscall.Flock_t) (syscall.Errno, bool) {
		if cmd == syscall.F_SETLK && flock.Type == syscall.F_RDLCK {
			return syscall.EAGAIN, true // deny only the downgrade
		}
		return 0, false // real probe (F_UNLCK) and real WRLCK
	})

	_, err := newPlatformShm(path)
	require.Error(t, err)
	require.Contains(t, err.Error(), "downgrade DMS lock",
		"a denied downgrade must fail the open as a protocol error, not retry")
}

// cpViaChild copies src to dst from a CHILD process. Required whenever the
// copying test still has the source shm attached: an in-process os.ReadFile
// would open+close an fd on the shm inode and drop this process's POSIX
// record locks on it (see the fcntl invariant on newPlatformShm).
func cpViaChild(t *testing.T, src, dst string) {
	t.Helper()
	out, err := exec.Command("cp", src, dst).CombinedOutput()
	require.NoError(t, err, "cp: %s", out)
}

func dmsTestOpen(t *testing.T, path string) *DB {
	t.Helper()
	db, err := testOpen(t, path, Options{PageSize: 4096, DisableAutoCheckpoint: true})
	require.NoError(t, err)
	return db
}

func dmsPutRange(t *testing.T, db *DB, ns string, from, to int) {
	t.Helper()
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	nsh, err := wtx.GetNamespace(ns)
	if err != nil {
		nsh, err = wtx.CreateNamespace(ns)
		require.NoError(t, err)
	}
	for i := from; i < to; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, wtx.Put(nsh, key, bytes.Repeat([]byte{byte(i)}, 300)))
	}
	require.NoError(t, wtx.Commit())
}

// TestStaleShmMxFrameBehind_CommittedTailVisible: power-loss scenario where
// the persisted shm predates the last committed WAL frames (mxFrame behind).
// Adopting it would hide the committed tail; the election forces recovery
// from the WAL file, so ALL committed rows must be visible after reopen.
func TestStaleShmMxFrameBehind_CommittedTailVisible(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	shmPath := dbPath + "-wal-shm"
	stale := filepath.Join(dir, "stale-shm")

	db := dmsTestOpen(t, dbPath)
	dmsPutRange(t, db, "t1", 0, 20)
	// Snapshot the live shm from a child process (fcntl invariant).
	cpViaChild(t, shmPath, stale)
	dmsPutRange(t, db, "t1", 20, 40)
	rawClose(db)

	// Simulate the power-loss outcome: the shm image on disk is the OLD one.
	data, err := os.ReadFile(stale)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(shmPath, data, 0666))

	db2 := dmsTestOpen(t, dbPath)
	defer func() { _ = db2.Close() }()
	require.Equal(t, 40, countKeys(t, db2, "t1"),
		"a stale (mxFrame-behind) shm must not hide the committed WAL tail")
}

// TestStaleShmMxFrameAhead_UncommittedNotResurrected: the persisted shm
// references frames BEYOND the surviving WAL tail (mxFrame ahead — e.g. the
// crash also lost the tail of the WAL file). Adopting it would resurrect
// frames that no longer exist; recovery from the actual WAL must yield
// exactly the surviving committed prefix, with a clean open.
func TestStaleShmMxFrameAhead_UncommittedNotResurrected(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	walPath := dbPath + "-wal"

	db := dmsTestOpen(t, dbPath)
	dmsPutRange(t, db, "t1", 0, 20)
	walInfo, err := os.Stat(walPath)
	require.NoError(t, err)
	walSizeAtK1 := walInfo.Size()

	dmsPutRange(t, db, "t1", 20, 40)
	rawClose(db)

	// The on-disk shm now claims frames up to the end of the second batch.
	// Roll the WAL back to the first-batch boundary: the shm is "ahead".
	require.NoError(t, os.Truncate(walPath, walSizeAtK1))

	db2 := dmsTestOpen(t, dbPath)
	defer func() { _ = db2.Close() }()
	require.Equal(t, 20, countKeys(t, db2, "t1"),
		"a stale (mxFrame-ahead) shm must not resurrect frames beyond the real WAL tail")
}

// TestStaleShmGarbageDiscardedOnReopen: a completely garbage shm image (torn
// power-loss write) must be physically discarded by the first attacher —
// reopen succeeds and data is intact.
func TestStaleShmGarbageDiscardedOnReopen(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	shmPath := dbPath + "-wal-shm"

	db := dmsTestOpen(t, dbPath)
	dmsPutRange(t, db, "t1", 0, 30)
	rawClose(db)

	writeGarbageShm(t, shmPath)

	db2 := dmsTestOpen(t, dbPath)
	defer func() { _ = db2.Close() }()
	require.Equal(t, 30, countKeys(t, db2, "t1"),
		"garbage shm must be discarded and the wal-index rebuilt from the WAL")
}

// TestShmStumpCrashBeforeRecoverySelfHeals: a first attacher that crashed
// after the truncate-to-3 but before recovery leaves the 3-byte stump behind
// (its locks died with it). The next opener must repeat the idempotent
// election and recover everything from the WAL — pinning the whole
// stump → region-growth zero-fill → readHeader-invalid → recoverLocked chain.
func TestShmStumpCrashBeforeRecoverySelfHeals(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	shmPath := dbPath + "-wal-shm"

	db := dmsTestOpen(t, dbPath)
	dmsPutRange(t, db, "t1", 0, 30)
	rawClose(db)

	require.NoError(t, os.Truncate(shmPath, 3))

	db2 := dmsTestOpen(t, dbPath)
	defer func() { _ = db2.Close() }()
	require.Equal(t, 30, countKeys(t, db2, "t1"),
		"a crash between the stump reset and recovery must self-heal on the next open")
}
