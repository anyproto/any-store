//go:build !unix && !windows

package anystore

import "sync"

// The platforms that are neither Unix nor Windows (js/wasm, wasip1) have no
// file lock that another process would see, and no second process: the
// database file lock is a no-op there too
// (internal/btree/dbfile_lock_other.go). The lock of a copy is kept in the
// process, by path, and no lock file is created.
var copyLocks struct {
	sync.Mutex
	held map[string]struct{}
}

// acquireCopyLock takes the lock of a copy within this process; a path
// already locked returns ErrBusy at once.
func acquireCopyLock(path string) (release func(), err error) {
	copyLocks.Lock()
	defer copyLocks.Unlock()
	if _, ok := copyLocks.held[path]; ok {
		return nil, ErrBusy
	}
	if copyLocks.held == nil {
		copyLocks.held = make(map[string]struct{})
	}
	copyLocks.held[path] = struct{}{}
	return func() {
		copyLocks.Lock()
		delete(copyLocks.held, path)
		copyLocks.Unlock()
	}, nil
}
