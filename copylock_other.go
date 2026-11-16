//go:build !unix && !windows

package anystore

import "sync/atomic"

// The platforms that are neither Unix nor Windows (js/wasm, wasip1, plan9)
// have no file lock that another process would see, and no second process:
// the database file lock is a no-op there too
// (internal/btree/dbfile_lock_other.go). The lock of a copy is kept in the
// process, and no lock file is created.
//
// It is one lock for the whole process, whatever the destination: one copy
// runs at a time, and any second copy returns ErrBusy at once. A lock per
// destination would need the destination's identity, which its path is
// not: copy.db, ./copy.db and an absolute spelling name one file, and a
// lock per spelling would let a second copy to that file remove the
// staging files of the first. On Unix and Windows the identity is the lock
// file's own, so every spelling meets one lock there. The price here is
// that copies to different destinations do not run at the same time
// either.
var copyLockHeld atomic.Bool

// acquireCopyLock takes the process's lock of a copy; a lock already held,
// for this destination or another, returns ErrBusy at once.
func acquireCopyLock(string) (release func(), err error) {
	if !copyLockHeld.CompareAndSwap(false, true) {
		return nil, ErrBusy
	}
	return func() { copyLockHeld.Store(false) }, nil
}

// stagingOpenAsDatabase reports no database open at path: an open database
// holds no lock here that another handle could see. The source of a copy
// is refused by name and identity before it gets here
// (sourceOutsideReservedNames).
func stagingOpenAsDatabase(string) (bool, error) { return false, nil }
