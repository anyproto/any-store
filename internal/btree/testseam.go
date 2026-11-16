package btree

import (
	"fmt"
	"time"
)

// TestSeam gives the tests of the packages above this one the reach this
// package's own tests have by being in it: the WAL's locks, its wal-index
// header, and the hooks inside a header publication and a write begin. It is
// for tests only; nothing in production code calls it.
type TestSeam struct{ db *DB }

// TestSeam returns the test seam of db.
func (db *DB) TestSeam() TestSeam { return TestSeam{db} }

// Path is the database file's path.
func (s TestSeam) Path() string { return s.db.path }

// Lock takes one of the WAL's locks exclusively through this handle's lock
// table, which refuses the handle's own transactions as fcntl refuses
// another process's: "write", "recover" or "checkpoint". It returns the
// unlock.
func (s TestSeam) Lock(name string) (unlock func(), err error) {
	var slot int
	switch name {
	case "write":
		slot = lockWrite
	case "recover":
		slot = lockRecover
	case "checkpoint":
		slot = lockCheckpoint
	default:
		return nil, fmt.Errorf("btree: test seam: unknown lock %q", name)
	}
	idx := s.db.pager.wal.index
	if err := idx.lock(slot, lockExclusive); err != nil {
		return nil, err
	}
	return func() { _ = idx.unlock(slot, lockExclusive) }, nil
}

// TearHeader makes the two copies of the wal-index header disagree, as a
// writer between its two copies leaves them, and returns the function that
// puts the header back. No effect on a heap-shm database.
func (s TestSeam) TearHeader() (restore func()) {
	region, err := s.db.pager.wal.index.shm.region(0, true)
	if err != nil || s.db.pager.wal.chg.Load() == nil {
		return func() {}
	}
	region[16] ^= 0x01
	return func() { region[16] ^= 0x01 }
}

// PeerPublish writes a wal-index header that differs from the live one in
// its iChange alone, both copies: what a begin sees when another process
// committed since it took its snapshot.
func (s TestSeam) PeerPublish() {
	w := s.db.pager.wal
	hdr, valid := w.index.readHeader()
	if !valid {
		return
	}
	hdr.iChange++
	hdr.computeCksum()
	var buf [walIndexHdrSize]byte
	hdr.serialize(buf[:])
	region, err := w.index.shm.region(0, true)
	if err != nil {
		return
	}
	copy(region[walIndexHdrSize:walIndexHdrSize*2], buf[:])
	copy(region[0:walIndexHdrSize], buf[:])
}

// SetBusyTimeout replaces the handle's busy handler with one of the given
// timeout.
func (s TestSeam) SetBusyTimeout(d time.Duration) {
	s.db.pager.wal.busyHandler = DefaultBusyTimeout(d)
}

// SetTestPublishHeaderHook installs the hook that runs inside every header
// publication of this process, with the step just completed: "announced"
// (the header is pending, nothing written), "between" (copy 2 written),
// "stored" (both copies written, the change state not settled). Nil removes
// it.
func SetTestPublishHeaderHook(f func(step string)) { testPublishHeaderHook = f }

// SetTestBeforeBeginWriteHook installs the hook that runs in every write
// begin of this process once it has its read snapshot and before it takes
// the WAL's write lock. Nil removes it.
func SetTestBeforeBeginWriteHook(f func(s TestSeam)) {
	if f == nil {
		testBeforeBeginWriteHook = nil
		return
	}
	testBeforeBeginWriteHook = func(p *pager) {
		if db := p.testOwner; db != nil {
			f(TestSeam{db})
		}
	}
}

// SetTestWalRetryHook installs the hook that runs in the WAL protocol's
// retry loop of every read begin of this process each time an attempt has
// to be made again (the header is being written, or a lock is held for an
// instant), with the number of attempts so far. Nil removes it.
func SetTestWalRetryHook(f func(cnt int)) { testWalRetryHook = f }

// SetTestBeginWriteRetryHook installs the hook that runs in every write
// begin of this process after an attempt failed with a busy error and
// before the next one, with that error. Nil removes it.
func SetTestBeginWriteRetryHook(f func(err error)) { testBeginWriteRetryHook = f }
