package btree

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOpen_DoubleOpenByInodeIdentity verifies that the process-global
// double-open guard keys on FILE IDENTITY (dev,ino), not the lexical path.
// The same physical file reached via a non-lexically-identical spelling — a
// symlink or a hardlink — must be recognised as already open and rejected with
// ErrDatabaseOpen. Otherwise two independent writers would each believe they
// hold the exclusive WAL write lock, corrupting the file (lost updates / torn
// pages). This mirrors SQLite's process-global unixInodeInfo keyed by (dev,ino)
// (os_unix.c:1282-1349).
func TestOpen_DoubleOpenByInodeIdentity(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink/hardlink identity guard is a unix concern; non-unix forces single-process")
	}

	dir := t.TempDir()
	realPath := filepath.Join(dir, "real.db")
	symPath := filepath.Join(dir, "link.db")    // symlink -> real.db
	hardPath := filepath.Join(dir, "hard.db")   // hardlink to the same inode
	otherPath := filepath.Join(dir, "other.db") // distinct file, must stay openable

	// Open the real file first. Use a real on-disk (non-InProcess) DB so the
	// double-open guard is exercised as the load-bearing protection.
	opts := Options{PageSize: 4096}
	db, err := testOpen(t, realPath, opts)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Create a symlink and a hardlink pointing at the same inode.
	require.NoError(t, os.Symlink(realPath, symPath))
	require.NoError(t, os.Link(realPath, hardPath))

	// Sanity: the OS agrees these spellings are the same physical file, but
	// their lexical absolute paths differ (so a path-keyed guard would miss them).
	realInfo, err := os.Stat(realPath)
	require.NoError(t, err)
	symInfo, err := os.Stat(symPath)
	require.NoError(t, err)
	hardInfo, err := os.Stat(hardPath)
	require.NoError(t, err)
	assert.True(t, os.SameFile(realInfo, symInfo), "symlink must resolve to the same inode")
	assert.True(t, os.SameFile(realInfo, hardInfo), "hardlink must share the same inode")

	symAbs, _ := filepath.Abs(symPath)
	hardAbs, _ := filepath.Abs(hardPath)
	realAbs, _ := filepath.Abs(realPath)
	require.NotEqual(t, realAbs, symAbs, "lexical paths must differ (guard cannot rely on them)")
	require.NotEqual(t, realAbs, hardAbs, "lexical paths must differ (guard cannot rely on them)")

	// Second open via the SYMLINK must be rejected as already open.
	db2, err := Open(symPath, opts)
	if db2 != nil {
		_ = db2.Close()
	}
	require.ErrorIs(t, err, ErrDatabaseOpen, "opening the same inode via symlink must be rejected")

	// Third open via the HARDLINK must also be rejected as already open.
	db3, err := Open(hardPath, opts)
	if db3 != nil {
		_ = db3.Close()
	}
	require.ErrorIs(t, err, ErrDatabaseOpen, "opening the same inode via hardlink must be rejected")

	// A genuinely different file must remain openable (the guard must not be a
	// blanket lock on the directory).
	dbOther, err := Open(otherPath, opts)
	require.NoError(t, err, "a distinct inode must still open")
	require.NoError(t, dbOther.Close())

	// After closing the original handle, the inode is free again: reopening via
	// any spelling must succeed (registry entry was removed under the same key).
	require.NoError(t, db.Close())
	dbReopen, err := testOpen(t, symPath, opts)
	require.NoError(t, err, "after Close the same inode must be reopenable via the symlink")
	require.NoError(t, dbReopen.Close())
}

// TestOpen_DoubleOpenSamePathStillRejected guards the pre-existing behaviour:
// opening the exact same path string (and lexically-equivalent spellings)
// twice in one process is still rejected. This must keep working alongside the
// inode-identity keying.
func TestOpen_DoubleOpenSamePathStillRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db, err := testOpen(t, path, Options{PageSize: 4096})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Exact same path.
	db2, err := Open(path, Options{PageSize: 4096})
	if db2 != nil {
		_ = db2.Close()
	}
	require.ErrorIs(t, err, ErrDatabaseOpen)

	// Lexically-different but equivalent spelling (./ and a/../ collapse).
	weird := filepath.Join(dir, ".", "x", "..", "test.db")
	db3, err := Open(weird, Options{PageSize: 4096})
	if db3 != nil {
		_ = db3.Close()
	}
	require.ErrorIs(t, err, ErrDatabaseOpen)
}
