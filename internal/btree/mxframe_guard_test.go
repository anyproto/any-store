package btree

import (
	"errors"
	"path/filepath"
	"testing"
)

// TestWithWriteLock_AlwaysReleases exercises every termination path of
// the closure passed to pager.withWriteLock (clean return, error return,
// panic) and asserts WAL_WRITE_LOCK is released in each case. A leak
// would starve the next lockWrite attempt.
func TestWithWriteLock_AlwaysReleases(t *testing.T) {
	cases := []struct {
		name string
		fn   func() error
	}{
		{"clean return", func() error { return nil }},
		{"error return", func() error { return errors.New("boom") }},
		{"panic", func() error { panic("boom") }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "t.db")
			db, err := Open(dbPath, Options{})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			t.Cleanup(func() { _ = db.Close() })

			// pager.withWriteLock's contract is "must be called with p.mu
			// held". Take it here to honor that contract.
			db.pager.mu.Lock()
			defer db.pager.mu.Unlock()

			call := func() {
				defer func() {
					if r := recover(); r != nil {
						// swallow the planned panic; the outer defer
						// release is what the test observes.
					}
				}()
				_ = db.pager.withWriteLock(func(locked bool) error {
					return tc.fn()
				})
			}
			call()

			// After the helper returns, we must be able to acquire
			// WAL_WRITE_LOCK exclusive (same process — uses in-process
			// counters). A leaked lock would surface as ErrBusy.
			if err := db.pager.wal.index.lock(lockWrite, lockExclusive); err != nil {
				t.Fatalf("lockWrite exclusive after withWriteLock: %v (lock leaked?)", err)
			}
			_ = db.pager.wal.index.unlock(lockWrite, lockExclusive)
		})
	}
}
