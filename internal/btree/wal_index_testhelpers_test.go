package btree

import "testing"

// Test-only thin wrappers that assert the (now error-returning) walIndex
// lookup/insert methods succeed, so the many existing positive-path tests can
// keep their compact `assert.Equal(t, want, wi.getX(...))` form without each
// site re-deriving the (value, error) pair. A real ErrCorrupt still fails the
// test loudly via t.Fatal. Tests that specifically exercise the error path
// call the error-returning methods directly.

func mustShmHashGet(t *testing.T, wi *walIndex, pgno, maxFrame, minFrame uint32) uint32 {
	t.Helper()
	v, err := wi.shmHashGet(pgno, maxFrame, minFrame)
	if err != nil {
		t.Fatalf("shmHashGet(%d, %d, %d): unexpected error: %v", pgno, maxFrame, minFrame, err)
	}
	return v
}

func mustShmHashWrite(t *testing.T, wi *walIndex, pgno, frame uint32) {
	t.Helper()
	if err := wi.shmHashWrite(pgno, frame); err != nil {
		t.Fatalf("shmHashWrite(%d, %d): unexpected error: %v", pgno, frame, err)
	}
}

func mustWiGet(t *testing.T, wi *walIndex, pgno, maxFrame uint32) uint32 {
	t.Helper()
	v, err := wi.get(pgno, maxFrame)
	if err != nil {
		t.Fatalf("walIndex.get(%d, %d): unexpected error: %v", pgno, maxFrame, err)
	}
	return v
}

func mustWiGetLatest(t *testing.T, wi *walIndex, pgno uint32) uint32 {
	t.Helper()
	v, err := wi.getLatest(pgno)
	if err != nil {
		t.Fatalf("walIndex.getLatest(%d): unexpected error: %v", pgno, err)
	}
	return v
}

func mustWiSet(t *testing.T, wi *walIndex, pgno, frame uint32) {
	t.Helper()
	if err := wi.set(pgno, frame); err != nil {
		t.Fatalf("walIndex.set(%d, %d): unexpected error: %v", pgno, frame, err)
	}
}
