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
