//go:build windows

package btree

import (
	"errors"
	"fmt"

	"golang.org/x/sys/windows"
)

// On Windows, mmap-backed WAL-index SHM is unavailable, so any-store forces
// InProcess=true for file-backed databases (db.go via hasMmapShm=false). The
// multi-process SHARED-lock protocol is therefore never exercised on Windows:
// acquireSharedDBLock / tryUpgradeDBLockExclusive / downgradeDBLockToShared are
// no-op stubs that must compile but never run.
//
// acquireExclusiveDBLock IS used: in-process mode is single-process only (heap
// SHM is process-local), so on open it takes a non-blocking, whole-file
// EXCLUSIVE lock via LockFileEx. A second process opening the same database is
// rejected (mapped to ErrBusy -> ErrInProcessLocked by the caller) instead of
// silently corrupting through a separate heap SHM. The lock is released when the
// file handle is closed (CloseHandle releases all of a handle's locks).

func acquireSharedDBLock(_ fileHandle) error               { return nil }
func tryUpgradeDBLockExclusive(_ fileHandle) (bool, error) { return true, nil }
func downgradeDBLockToShared(_ fileHandle) error           { return nil }

func acquireExclusiveDBLock(fd fileHandle) error {
	if fd == nil {
		return fmt.Errorf("btree: dbfile lock: nil fd")
	}
	h := windows.Handle(fd.Fd())
	// Lock the entire 64-bit file range, non-blocking + exclusive.
	err := windows.LockFileEx(
		h,
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0,
		0xFFFFFFFF, // bytesLow
		0xFFFFFFFF, // bytesHigh
		&windows.Overlapped{},
	)
	if err != nil {
		// Another handle (this or another process) holds an overlapping lock.
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) || errors.Is(err, windows.ERROR_IO_PENDING) {
			return ErrBusy
		}
		return fmt.Errorf("btree: LockFileEx: %w", err)
	}
	return nil
}
