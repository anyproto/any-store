package btree

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// DB-file lock protocol
// =====================
//
// SQLite's WAL-mode close protocol (wal.c:2487-2551 sqlite3WalClose) acquires
// an EXCLUSIVE fcntl lock on the database file before unlinking the shm.
// New openers take SHARED on the DB file first, which blocks against the
// closer's EXCLUSIVE — serializing "last-client-unlink" vs. "new-opener-
// attach" at the DB-file lock level.
//
// any-store used to handle this via SHM dead-man-switch (DMS) alone, which
// left a narrow orphan-inode window (see NOTES.md §SHM open/close protocol
// drift). This file adopts SQLite's approach — adapted to use BSD flock
// (whole-file, per-file-description) instead of byte-range fcntl. flock
// semantics dodge POSIX fcntl's "close any fd releases all locks on inode"
// gotcha when two goroutines in the same process open the same DB path.
//
// Simplified state machine (vs. SQLite's 5-state NO/SHARED/RESERVED/PENDING/
// EXCLUSIVE): WAL mode only needs NO / SHARED / EXCLUSIVE. RESERVED and
// PENDING exist in SQLite to support rollback-journal mode and don't apply
// here.

// acquireSharedDBLock takes a non-blocking shared flock on fd. Returns
// ErrBusy if another process holds exclusive on the same file.
func acquireSharedDBLock(fd *os.File) error {
	return flockNB(fd, syscall.LOCK_SH)
}

// tryUpgradeDBLockExclusive attempts to upgrade the caller's shared flock
// to exclusive. Returns (true, nil) if the upgrade succeeded (caller is
// the only holder, safe to unlink shm / truncate WAL). Returns (false, nil)
// if another holder prevents the upgrade. Returns (false, err) on OS
// errors other than EWOULDBLOCK.
func tryUpgradeDBLockExclusive(fd *os.File) (bool, error) {
	err := flockNB(fd, syscall.LOCK_EX)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, ErrBusy):
		return false, nil
	default:
		return false, err
	}
}

// downgradeDBLockToShared converts an exclusive lock back to shared.
func downgradeDBLockToShared(fd *os.File) error {
	return flockNB(fd, syscall.LOCK_SH)
}

// flockNB is a thin wrapper that maps EWOULDBLOCK/EAGAIN to ErrBusy so
// callers share the same BUSY handling they use for shm fcntl locks.
func flockNB(fd *os.File, how int) error {
	if fd == nil {
		return fmt.Errorf("btree: dbfile lock: nil fd")
	}
	if err := syscall.Flock(int(fd.Fd()), how|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return ErrBusy
		}
		return fmt.Errorf("btree: flock(%d): %w", how, err)
	}
	return nil
}
