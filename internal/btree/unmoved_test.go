//go:build unix

package btree

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unmovedPutN writes n key/value rows in n separate committed transactions so
// that the WAL accumulates uncheckpointed frames (auto-checkpoint disabled).
func unmovedPutN(t *testing.T, db *DB, nsName string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := tx.GetNamespace(nsName)
		require.NoError(t, err)
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 200)))
		require.NoError(t, tx.Commit())
	}
}

func unmovedCreateNS(t *testing.T, db *DB, nsName string) {
	t.Helper()
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace(nsName)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

// TestUnmoved_DatabaseIsUnmoved_Direct unit-tests the databaseIsUnmoved guard
// against the three filesystem states C's databaseIsUnmoved (pager.c:4142-4159)
// / fileHasMoved (os_unix.c:1623-1632) distinguish: in place (unmoved), renamed
// away (moved), and relinked with a different file at the path (moved).
func TestUnmoved_DatabaseIsUnmoved_Direct(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	opts := DefaultOptions()
	opts.DisableAutoCheckpoint = true
	opts.InProcess = true
	db, err := testOpen(t, path, opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	p := db.pager
	require.True(t, p.openIdentOK, "unix open must capture a real inode identity")

	// File in place -> unmoved.
	assert.True(t, p.databaseIsUnmoved(), "file in place must report unmoved")

	// Rename the DB file away -> nothing at path -> moved (stat error).
	moved := filepath.Join(dir, "test.db.bak")
	require.NoError(t, os.Rename(path, moved))
	assert.False(t, p.databaseIsUnmoved(), "renamed-away DB file must report moved")

	// Relink: put a DIFFERENT file back at the original path. Its inode
	// differs from the one captured at open -> still moved.
	require.NoError(t, os.WriteFile(path, []byte("unrelated"), 0o600))
	assert.False(t, p.databaseIsUnmoved(), "relinked (different inode) DB file must report moved")

	// Restore the original file so cleanup Close() runs the happy path.
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Rename(moved, path))
	assert.True(t, p.databaseIsUnmoved(), "restored original inode must report unmoved")
}

// TestUnmoved_CloseSkipsCheckpointWhenMoved is the end-to-end regression test:
// when the DB file is renamed out from under the open handle before Close, the
// close-time checkpoint must be SKIPPED (matching SQLite's databaseIsUnmoved
// gate in sqlite3PagerClose, pager.c:4189-4191). Skipping the checkpoint leaves
// the whole checkpoint+truncate arm unrun (wal.c:2522 `if(zBuf!=0 ...)`), so the
// WAL retains its committed frames instead of being truncated to zero.
//
// Without the fix, pager.close() unconditionally runs checkpointPassive into
// p.file (which still references the renamed-away inode) and, in InProcess mode,
// then truncates the WAL — committing frames into a path that is no longer the
// real database and discarding them from the WAL. The assertion below (WAL not
// truncated) fails in that case.
func TestUnmoved_CloseSkipsCheckpointWhenMoved(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	walPath := path + "-wal"
	opts := DefaultOptions()
	opts.DisableAutoCheckpoint = true
	opts.InProcess = true

	db, err := testOpen(t, path, opts)
	require.NoError(t, err)

	unmovedCreateNS(t, db, "data")
	unmovedPutN(t, db, "data", 50)

	// Sanity: the WAL holds uncheckpointed frames before close.
	require.Greater(t, db.pager.wal.index.maxFrame.Load(), uint32(0),
		"precondition: WAL must hold uncheckpointed frames")
	walBefore, err := os.Stat(walPath)
	require.NoError(t, err)
	require.Greater(t, walBefore.Size(), int64(0), "precondition: WAL must be non-empty")

	// Rename the DB file out from under the open handle, then put an
	// unrelated file back at the original path (the relink scenario the C
	// guard protects against — close must not clobber it via a stale-WAL
	// checkpoint, nor discard the WAL frames).
	movedPath := filepath.Join(dir, "test.db.moved")
	require.NoError(t, os.Rename(path, movedPath))
	require.NoError(t, os.WriteFile(path, []byte("a different unrelated file"), 0o600))

	require.NoError(t, db.Close())

	// With the fix: checkpoint skipped -> WAL NOT truncated -> still holds
	// the committed frames. Without the fix: checkpoint ran and (InProcess,
	// last client) truncated the WAL to zero.
	walAfter, err := os.Stat(walPath)
	require.NoError(t, err)
	assert.Greater(t, walAfter.Size(), int64(0),
		"DRIFT-54 REGRESSION: close-time checkpoint must be skipped when the DB file was moved, leaving the WAL un-truncated")

	// The unrelated file placed at the original path must be untouched by the
	// skipped checkpoint.
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "a different unrelated file", string(got),
		"close must not write checkpoint pages over the unrelated file now at the DB path")
}

// TestUnmoved_CloseCheckpointsWhenInPlace pins the happy path: when the DB file
// is never moved, databaseIsUnmoved()==true and Close runs the unchanged
// checkpoint+truncate path, so the WAL is truncated to zero on a last-client
// InProcess close exactly as before the guard was added.
func TestUnmoved_CloseCheckpointsWhenInPlace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	walPath := path + "-wal"
	opts := DefaultOptions()
	opts.DisableAutoCheckpoint = true
	opts.InProcess = true

	db, err := testOpen(t, path, opts)
	require.NoError(t, err)

	unmovedCreateNS(t, db, "data")
	unmovedPutN(t, db, "data", 50)

	require.Greater(t, db.pager.wal.index.maxFrame.Load(), uint32(0))

	require.NoError(t, db.Close())

	// File in place -> checkpoint ran -> WAL truncated to zero (last client).
	walAfter, err := os.Stat(walPath)
	require.NoError(t, err)
	assert.Equal(t, int64(0), walAfter.Size(),
		"in-place close must run the close-time checkpoint and truncate the WAL")

	// Reopen and verify all data survived the checkpoint into the DB file.
	db2, err := testOpen(t, path, opts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()
	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	ns, err := rtx.GetNamespace("data")
	require.NoError(t, err)
	n, err := rtx.Count(ns)
	require.NoError(t, err)
	assert.Equal(t, 50, n, "all 50 rows must survive the in-place close-time checkpoint")
}
