//go:build (linux || darwin) && (amd64 || arm64)

package btree

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDBMmap_DisabledUsesReadAt verifies that with MmapSize=0 the
// reader reports disabled. readDBPage falls through to ReadAt.
func TestDBMmap_DisabledUsesReadAt(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "t.db")
	db, err := Open(dbPath, Options{MmapSize: 0})
	require.NoError(t, err)
	defer db.Close()

	if db.pager.dbMmap.enabled() {
		t.Fatal("MmapSize=0 should leave dbMmap disabled")
	}
}

// TestDBMmap_EnabledSmallDB verifies the mapping is populated on demand
// after a read when MmapSize > DB size.
func TestDBMmap_EnabledSmallDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "t.db")
	db, err := Open(dbPath, Options{MmapSize: 16 << 20}) // 16 MiB
	require.NoError(t, err)
	defer db.Close()

	// Drive a write + checkpoint so there's something in the DB file.
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("ns")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Checkpoint(CheckpointFull))

	// Force a read that exercises readDBPage.
	tx2, err := db.BeginRead()
	require.NoError(t, err)
	require.NoError(t, tx2.Rollback())

	// The mapping should be populated after at least one readDBPage hit.
	db.pager.dbMmap.mu.RLock()
	regionLen := len(db.pager.dbMmap.region)
	db.pager.dbMmap.mu.RUnlock()
	if regionLen == 0 {
		t.Fatal("mapping should be populated after a readDBPage call")
	}
}

// TestDBMmap_GrowRemapsAndReadsCorrect writes enough pages to force a
// remap past the initial small cap, then reads back and verifies no
// error. Coverage: miss-then-remap path in readDBPage.
func TestDBMmap_GrowRemapsAndReadsCorrect(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "t.db")

	// 64 KiB cap — tiny. After a handful of page allocations the DB
	// file will exceed the initial mapping and force remap() calls.
	db, err := Open(dbPath, Options{MmapSize: 64 << 10})
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	// Create many namespaces — each allocates at least one page.
	for i := 0; i < 50; i++ {
		name := []byte{'n', byte('a' + (i % 26)), byte('0' + (i / 26))}
		_, _ = tx.CreateNamespace(string(name))
		// Ignore dup errors from name collisions.
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Checkpoint(CheckpointFull))

	// Read back — readDBPage's miss-then-remap should kick in for
	// pages past the 64 KiB initial cap.
	tx2, err := db.BeginRead()
	require.NoError(t, err)
	require.NoError(t, tx2.Rollback())
}

// TestDBMmap_CloseUnmapsCleanly verifies Close tears down the mapping
// without panic / leak.
func TestDBMmap_CloseUnmapsCleanly(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "t.db")
	db, err := Open(dbPath, Options{MmapSize: 1 << 20})
	require.NoError(t, err)

	tx, err := db.BeginRead()
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())

	require.NoError(t, db.Close())
}
