//go:build btreetesthooks

package btree

// ErrBusySnapshot fault-injection tests for the BeginWrite retry loop
// (db.go busySnapshotInnerRetries). Run with -tags btreetesthooks, which
// enables the forceBusySnapshotForTest hook in wal.beginWriteWithSnapshot.

import (
	"errors"
	"path/filepath"
	"testing"
)

// TestBeginWrite_SurfacesBusySnapshotAfterBoundedRetries ensures the
// internal retry budget is bounded and ErrBusySnapshot surfaces to the
// caller so the app can throttle or fail fast.
func TestBeginWrite_SurfacesBusySnapshotAfterBoundedRetries(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "t.db")

	db, err := Open(dbPath, Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// Disable the busy handler so BeginWrite does not retry via backoff.
	db.pager.wal.busyHandler = nil
	// Force every write-snapshot check to return ErrBusySnapshot.
	db.pager.wal.forceBusySnapshotForTest.Store(true)
	defer db.pager.wal.forceBusySnapshotForTest.Store(false)

	_, err = db.BeginWrite()
	if !errors.Is(err, ErrBusySnapshot) {
		t.Fatalf("expected ErrBusySnapshot, got %v", err)
	}
}

// TestBeginWrite_BusySnapshotRoutesThroughBusyHandler proves the inner
// retry budget is bounded (busySnapshotInnerRetries) and further retries
// flow through the configured BusyHandler — matching SQLite's
// sqlite3InvokeBusyHandler dispatch (main.c:1700-1715).
func TestBeginWrite_BusySnapshotRoutesThroughBusyHandler(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "t.db")

	db, err := Open(dbPath, Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	db.pager.wal.forceBusySnapshotForTest.Store(true)
	defer db.pager.wal.forceBusySnapshotForTest.Store(false)

	// Counting busy handler: allow 2 callbacks, then deny.
	const allow = 2
	var invocations int
	db.pager.wal.busyHandler = func(count int) bool {
		invocations++
		return invocations <= allow
	}

	_, err = db.BeginWrite()
	if !errors.Is(err, ErrBusySnapshot) {
		t.Fatalf("expected ErrBusySnapshot, got %v", err)
	}
	// Handler must have been invoked once the inner budget is exhausted.
	if invocations == 0 {
		t.Fatalf("busy handler never invoked — inner retry loop spinning instead of delegating")
	}
	if invocations > allow+1 {
		t.Fatalf("busy handler kept being called after returning false: invocations=%d", invocations)
	}
}
