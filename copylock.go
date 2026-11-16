//go:build unix || windows

package anystore

import (
	"os"
)

// copyLockAttempts bounds how often acquireCopyLock opens the lock file
// again after it locked a file the path no longer names.
const copyLockAttempts = 16

// acquireCopyLock takes the lock of a copy (CopyToLatestFormat): an
// exclusive, non-blocking lock of the operating system on the file at path,
// which it creates if needed. The lock belongs to the open file, so the
// system drops it when the process dies, however it dies: a killed copy
// leaves no lock behind, only, at most, an unlocked file. The primitives are
// those of the database file lock (internal/btree/dbfile_lock_unix.go,
// dbfile_lock_windows.go): flock(LOCK_EX|LOCK_NB) on Unix, LockFileEx with
// LOCKFILE_EXCLUSIVE_LOCK|LOCKFILE_FAIL_IMMEDIATELY on Windows. A lock
// another holder has, in this process or another, returns ErrBusy at once.
//
// release removes the lock file and drops the lock, so that a finished copy
// leaves nothing beside its destination. Removing a lock file is safe only
// with the check made here after locking: a caller that opened the file
// before its holder removed it would otherwise hold a lock on a file that
// has no name any more, while the next caller creates and locks a new one.
// So once the lock is taken, the path must still name the locked file
// (os.SameFile of the open file and the path); if it does not, the file is
// closed and the path opened again. A lock file that cannot be removed is
// left: it is empty, it locks nothing by existing, and the next copy to the
// destination takes it over.
func acquireCopyLock(path string) (release func(), err error) {
	for range copyLockAttempts {
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o666)
		if err != nil {
			return nil, err
		}
		if err = lockFileExclusive(f); err != nil {
			_ = f.Close()
			return nil, err
		}
		held, herr := f.Stat()
		named, nerr := os.Stat(path)
		if herr == nil && nerr == nil && os.SameFile(held, named) {
			return func() { releaseCopyLock(f, path) }, nil
		}
		// The holder before us removed the file between our open and our
		// lock: this lock excludes nobody.
		_ = f.Close()
	}
	// Each attempt met a holder that was just leaving.
	return nil, ErrBusy
}
