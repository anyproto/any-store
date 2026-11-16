//go:build windows

package anystore

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

// lockFileExclusive takes an exclusive lock on the whole range of f without
// waiting. LockFileEx locks belong to the handle, so two calls in one
// process exclude each other as two processes do.
func lockFileExclusive(f *os.File) error {
	err := windows.LockFileEx(
		windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0,
		0xFFFFFFFF, // bytesLow
		0xFFFFFFFF, // bytesHigh
		&windows.Overlapped{},
	)
	if err != nil {
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) || errors.Is(err, windows.ERROR_IO_PENDING) {
			return ErrBusy
		}
		return &os.PathError{Op: "LockFileEx", Path: f.Name(), Err: err}
	}
	return nil
}

// releaseCopyLock closes the lock file, which drops the lock, and then
// removes it: Windows does not delete a file that a handle without
// FILE_SHARE_DELETE has open, and os.OpenFile opens without it. For the same
// reason the removal cannot take the file from under a caller that has
// opened it meanwhile: it fails, and the file stays for that caller, who
// holds or is about to take the lock.
func releaseCopyLock(f *os.File, path string) {
	_ = f.Close()
	_ = os.Remove(path)
}

// syncLockFile flushes the byte withContentStat wrote.
func syncLockFile(f *os.File) error { return f.Sync() }
