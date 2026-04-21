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
