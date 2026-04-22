package btree

import (
	"errors"
	"path/filepath"
	"testing"
)

// clearHeaderForTest zeroes the shm header so the next reader's
// readHeader() returns valid=false and ensureHeaderInitialized takes
// the slow path. Test-only helper.
func (wi *walIndex) clearHeaderForTest() error {
	region, err := wi.shm.region(0, true)
	if err != nil {
		return err
	}
	for i := 0; i < walIndexHdrSize*2; i++ {
		region[i] = 0
	}
	return nil
}

// TestEnsureHeaderInitialized_SurfacesBusyRecovery exercises the
// slow-path in ensureHeaderInitialized where the RECOVER lock is held
// exclusive (simulating "peer is mid-walIndexRecover"). The helper
// must return ErrBusyRecovery (not the generic errWALRetry) so upstream
// callers can back off via busyHandler instead of spinning.
//
// Uses a single wal instance: the in-process mmapShm.locks[] counters
// serialize exclusive-vs-exclusive on the same slot even within the
// same process, so preholding RECOVER exclusive is enough to trip the
// helper's third branch.
func TestEnsureHeaderInitialized_SurfacesBusyRecovery(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "t.db")

	w := newWal(dbPath, 4096)
	if err := w.open(); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = w.close(false) })

	// Force slow path by zeroing the shm header.
	if err := w.index.clearHeaderForTest(); err != nil {
		t.Fatalf("clear shm header: %v", err)
	}

	// Prehold RECOVER exclusive — representing "peer is mid-recovery".
	if err := w.index.lock(lockRecover, lockExclusive); err != nil {
		t.Fatalf("acquire RECOVER exclusive: %v", err)
	}
	defer func() { _ = w.index.unlock(lockRecover, lockExclusive) }()

	_, err := w.ensureHeaderInitialized()
	if !errors.Is(err, ErrBusyRecovery) {
		t.Fatalf("expected ErrBusyRecovery, got %v", err)
	}
}

// TestBeginWrite_SurfacesBusySnapshotAfterBoundedRetries ensures the
// internal retry budget is bounded (not 1000) and ErrBusySnapshot
// surfaces to the caller so the app can throttle or fail fast.
// Forces every beginWriteWithSnapshot to return ErrBusySnapshot via a
// test hook; disables the busy handler so the surface is immediate.
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

// TestBeginWrite_BusySnapshotRoutesThroughBusyHandler proves the
// inner retry budget is bounded (busySnapshotInnerRetries) and further
// retries flow through the configured BusyHandler — matching SQLite's
// sqlite3InvokeBusyHandler dispatch (main.c:1700-1715). Previously the
// 1000-attempt hidden loop never called the handler.
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
	// Handler must have been invoked at least once (inner budget < 1000).
	if invocations == 0 {
		t.Fatalf("busy handler never invoked — inner retry loop likely still spinning instead of delegating")
	}
	if invocations > allow+1 {
		t.Fatalf("busy handler kept being called after returning false: invocations=%d", invocations)
	}
}
