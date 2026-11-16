//go:build unix

package anystore

import (
	"errors"
	"os"
	"syscall"
)

// lockFileExclusive takes an exclusive flock on f without waiting. flock
// locks belong to the open file description, so two calls in one process,
// each with its own descriptor, exclude each other as two processes do.
func lockFileExclusive(f *os.File) error {
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, syscall.EINTR):
			continue
		case errors.Is(err, syscall.EWOULDBLOCK), errors.Is(err, syscall.EAGAIN):
			return ErrBusy
		default:
			return &os.PathError{Op: "flock", Path: f.Name(), Err: err}
		}
	}
}

// releaseCopyLock removes the lock file while the lock is still held, then
// closes it, which drops the lock: whoever opened the file before the
// removal fails to lock it, or finds after locking that the path names
// another file (acquireCopyLock).
func releaseCopyLock(f *os.File, path string) {
	_ = os.Remove(path)
	_ = f.Close()
}

// syncLockFile syncs the byte withContentStat wrote with fsync(2), as
// findInodeInfo does (os_unix.c:1569 at version-3.52.0), not with
// f.Sync, which is F_FULLFSYNC on macOS: the sync is there for the volume to
// give the file its identity, not to make the byte durable.
func syncLockFile(f *os.File) error {
	for {
		err := syscall.Fsync(int(f.Fd()))
		switch {
		case err == nil:
			return nil
		case errors.Is(err, syscall.EINTR):
			continue
		default:
			return &os.PathError{Op: "fsync", Path: f.Name(), Err: err}
		}
	}
}

// reservedOpenFlags: the open of a lock file or of the staging file does
// not follow a symbolic link (openReserved), and does not wait for a FIFO or
// a device put there after its Lstat.
const reservedOpenFlags = syscall.O_NOFOLLOW | syscall.O_NONBLOCK

// linkCount returns the number of links of the file fi describes.
func linkCount(fi os.FileInfo) (uint64, bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return uint64(st.Nlink), true
}
