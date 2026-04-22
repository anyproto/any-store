package btree

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestP3_1_FreshShmMarkerTruncatesToThree verifies that a newly created
// shm file is ftruncate'd to exactly 3 bytes before any region is
// allocated. Matches SQLite's os_unix.c:4902.
func TestP3_1_FreshShmMarkerTruncatesToThree(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fresh.shm")

	s, err := newPlatformShm(path)
	if err != nil {
		t.Fatalf("newPlatformShm: %v", err)
	}
	defer s.close(false)

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() != 3 {
		t.Fatalf("fresh shm size = %d, want 3 (SQLite os_unix.c:4902 marker)", info.Size())
	}
}

// TestP3_1_ExistingShmNotTruncated verifies the 3-byte truncate does
// NOT fire when opening an already-populated shm file (second opener).
func TestP3_1_ExistingShmNotTruncated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "populated.shm")

	sA, err := newPlatformShm(path)
	if err != nil {
		t.Fatalf("open A: %v", err)
	}
	if _, err := sA.region(0, true); err != nil {
		t.Fatalf("region: %v", err)
	}
	grownInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat after grow: %v", err)
	}
	if grownInfo.Size() <= 3 {
		t.Fatalf("after region(0, true), size should exceed 3, got %d", grownInfo.Size())
	}

	sB, err := newPlatformShm(path)
	if err != nil {
		t.Fatalf("open B: %v", err)
	}
	defer sB.close(false)
	defer sA.close(false)
	sizeB, _ := os.Stat(path)
	if sizeB.Size() != grownInfo.Size() {
		t.Fatalf("opener B changed shm size %d → %d; should leave it alone", grownInfo.Size(), sizeB.Size())
	}
}

// TestP3_3_VersionValidForMismatchRejected verifies that pager.open
// refuses to open a DB where VersionValidFor != FileChangeCount AND
// VersionValidFor != 0. The zero case is an escape hatch for legacy /
// test fixtures that never wrote the field.
func TestP3_3_VersionValidForMismatchRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tampered.db")

	// Open, commit a write to advance FileChangeCount AND VersionValidFor
	// in lockstep, close.
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("ns")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Checkpoint(CheckpointFull))
	require.NoError(t, db.Close())

	// Tamper: set VersionValidFor (offset 92) to something that is
	// both non-zero AND != the on-disk FileChangeCount. The on-disk
	// FCC is small (1 after one commit); 0x42 is a safe "different" value.
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	binary.BigEndian.PutUint32(data[92:96], 0x42)
	require.NoError(t, os.WriteFile(path, data, 0644))

	// Reopen — must reject.
	_, err = testOpen(t, path, DefaultOptions())
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt on VersionValidFor mismatch, got %v", err)
	}
}

// TestP3_4_LastAutoCheckpointErrorIsNilOnFreshOpen verifies the
// accessor returns nil before any auto-checkpoint has run. Covers the
// API surface; a full error-injection test would need disk-full
// injection.
func TestP3_4_LastAutoCheckpointErrorIsNilOnFreshOpen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	if got := db.LastAutoCheckpointError(); got != nil {
		t.Fatalf("fresh open: LastAutoCheckpointError should be nil, got %v", got)
	}
}
