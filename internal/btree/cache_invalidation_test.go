package btree

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// allowDoubleOpen removes a path from the open registry so a second Open()
// can succeed. Used only in multi-process simulation tests.
func allowDoubleOpen(path string) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return
	}
	openDBs.Delete(abs)
}

// TestCacheInvalidation_SingleProcess_NotStaleAfterOwnWrite verifies that
// after a process writes and commits with MarkDataChanged, its next
// transaction does NOT report staleness (same-process avoidance).
func TestCacheInvalidation_SingleProcess_NotStaleAfterOwnWrite(t *testing.T) {
	db := tempDB(t)

	// Write some data with MarkDataChanged
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, wtx.Put(ns, []byte("key1"), []byte("val1")))
	wtx.MarkDataChanged()
	require.NoError(t, wtx.Commit())

	// Next read transaction should NOT be stale
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	assert.False(t, rtx.IsDataStale(), "should not be stale after own write")
	assert.False(t, rtx.IsSchemaStale(), "schema should not be stale")
	require.NoError(t, rtx.Rollback())
}

// TestCacheInvalidation_MultiProcess_DetectsExternalWrite verifies that
// when two DB handles share the same file, writes from one are detected
// as stale by the other.
func TestCacheInvalidation_MultiProcess_DetectsExternalWrite(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Open first DB handle
	db1, err := testOpen(t, dbPath, Options{PageSize: 4096})
	require.NoError(t, err)
	defer db1.Close()

	// Open second DB handle (simulates another process)
	allowDoubleOpen(dbPath)
	db2, err := testOpen(t, dbPath, Options{PageSize: 4096})
	require.NoError(t, err)
	defer db2.Close()

	// Both should start clean (not stale)
	rtx1, err := db1.BeginRead()
	require.NoError(t, err)
	assert.False(t, rtx1.IsDataStale())
	assert.False(t, rtx1.IsSchemaStale())
	require.NoError(t, rtx1.Rollback())

	rtx2, err := db2.BeginRead()
	require.NoError(t, err)
	assert.False(t, rtx2.IsDataStale())
	assert.False(t, rtx2.IsSchemaStale())
	require.NoError(t, rtx2.Rollback())

	// DB1 writes data
	wtx, err := db1.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, wtx.Put(ns, []byte("key1"), []byte("val1")))
	wtx.MarkDataChanged()
	require.NoError(t, wtx.Commit())

	// DB1 should NOT be stale (own write)
	rtx1, err = db1.BeginRead()
	require.NoError(t, err)
	assert.False(t, rtx1.IsDataStale(), "db1 should not be stale after own write")
	require.NoError(t, rtx1.Rollback())

	// DB2 should detect staleness (external write)
	rtx2, err = db2.BeginRead()
	require.NoError(t, err)
	assert.True(t, rtx2.IsDataStale(), "db2 should be stale after db1's write")
	require.NoError(t, rtx2.Rollback())
}

// TestCacheInvalidation_SchemaCookie verifies that MarkSchemaChanged
// increments the schema cookie and is detected by other handles.
func TestCacheInvalidation_SchemaCookie(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db1, err := testOpen(t, dbPath, Options{PageSize: 4096})
	require.NoError(t, err)
	defer db1.Close()

	allowDoubleOpen(dbPath)
	db2, err := testOpen(t, dbPath, Options{PageSize: 4096})
	require.NoError(t, err)
	defer db2.Close()

	// DB1 makes a schema change
	wtx, err := db1.BeginWrite()
	require.NoError(t, err)
	_, err = wtx.CreateNamespace("newcollection")
	require.NoError(t, err)
	wtx.MarkSchemaChanged()
	wtx.MarkDataChanged()
	require.NoError(t, wtx.Commit())

	// DB2 should detect schema staleness
	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	assert.True(t, rtx.IsSchemaStale(), "db2 should detect schema change")
	assert.True(t, rtx.IsDataStale(), "db2 should detect data change")
	require.NoError(t, rtx.Rollback())

	// DB1 should NOT be stale
	rtx, err = db1.BeginRead()
	require.NoError(t, err)
	assert.False(t, rtx.IsSchemaStale(), "db1 should not be stale after own schema change")
	assert.False(t, rtx.IsDataStale(), "db1 should not be stale after own data change")
	require.NoError(t, rtx.Rollback())
}

// TestCacheInvalidation_NoMarks_CountersUnchanged verifies that committing
// without calling MarkDataChanged/MarkSchemaChanged does NOT increment counters.
func TestCacheInvalidation_NoMarks_CountersUnchanged(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db1, err := testOpen(t, dbPath, Options{PageSize: 4096})
	require.NoError(t, err)
	defer db1.Close()

	allowDoubleOpen(dbPath)
	db2, err := testOpen(t, dbPath, Options{PageSize: 4096})
	require.NoError(t, err)
	defer db2.Close()

	// Read initial counter values via db2
	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	initialFCC := rtx.DiskFileChangeCounter()
	initialSC := rtx.DiskSchemaCookie()
	require.NoError(t, rtx.Rollback())

	// DB1 writes data but does NOT call MarkDataChanged
	wtx, err := db1.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, wtx.Put(ns, []byte("key1"), []byte("val1")))
	// Intentionally NOT calling MarkDataChanged() or MarkSchemaChanged()
	require.NoError(t, wtx.Commit())

	// DB2 should see unchanged counters
	rtx, err = db2.BeginRead()
	require.NoError(t, err)
	assert.Equal(t, initialFCC, rtx.DiskFileChangeCounter(), "FileChangeCount should not have changed")
	assert.Equal(t, initialSC, rtx.DiskSchemaCookie(), "SchemaCookie should not have changed")
	assert.False(t, rtx.IsDataStale(), "should not be stale when counters unchanged")
	assert.False(t, rtx.IsSchemaStale(), "schema should not be stale when counters unchanged")
	require.NoError(t, rtx.Rollback())
}

// TestCacheInvalidation_UpdateLocalCounters verifies that calling
// UpdateLocalCounters clears staleness detection.
func TestCacheInvalidation_UpdateLocalCounters(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db1, err := testOpen(t, dbPath, Options{PageSize: 4096})
	require.NoError(t, err)
	defer db1.Close()

	allowDoubleOpen(dbPath)
	db2, err := testOpen(t, dbPath, Options{PageSize: 4096})
	require.NoError(t, err)
	defer db2.Close()

	// DB1 writes data
	wtx, err := db1.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, wtx.Put(ns, []byte("key1"), []byte("val1")))
	wtx.MarkDataChanged()
	wtx.MarkSchemaChanged()
	require.NoError(t, wtx.Commit())

	// DB2 detects staleness
	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	assert.True(t, rtx.IsDataStale())
	assert.True(t, rtx.IsSchemaStale())
	// Get the current disk values to sync
	fcc := rtx.DiskFileChangeCounter()
	sc := rtx.DiskSchemaCookie()
	require.NoError(t, rtx.Rollback())

	// Update local counters to sync
	db2.UpdateLocalCounters(fcc, sc)

	// DB2 should no longer be stale
	rtx, err = db2.BeginRead()
	require.NoError(t, err)
	assert.False(t, rtx.IsDataStale(), "should not be stale after UpdateLocalCounters")
	assert.False(t, rtx.IsSchemaStale(), "schema should not be stale after UpdateLocalCounters")
	require.NoError(t, rtx.Rollback())
}

// TestCacheInvalidation_Rollback_NoCounterChange verifies that rolling
// back a write transaction does NOT change counters, even if marks were set.
func TestCacheInvalidation_Rollback_NoCounterChange(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db1, err := testOpen(t, dbPath, Options{PageSize: 4096})
	require.NoError(t, err)
	defer db1.Close()

	// Get initial counter
	rtx, err := db1.BeginRead()
	require.NoError(t, err)
	initialFCC := rtx.DiskFileChangeCounter()
	require.NoError(t, rtx.Rollback())

	// Write with marks, then rollback
	wtx, err := db1.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, wtx.Put(ns, []byte("key1"), []byte("val1")))
	wtx.MarkDataChanged()
	wtx.MarkSchemaChanged()
	require.NoError(t, wtx.Rollback())

	// Counter should be unchanged
	rtx, err = db1.BeginRead()
	require.NoError(t, err)
	assert.Equal(t, initialFCC, rtx.DiskFileChangeCounter(), "counter should not change after rollback")
	assert.False(t, rtx.IsDataStale(), "should not be stale after rollback")
	require.NoError(t, rtx.Rollback())
}

// TestCacheInvalidation_MultipleWrites_CounterIncrements verifies that
// each committed write with MarkDataChanged increments the counter.
func TestCacheInvalidation_MultipleWrites_CounterIncrements(t *testing.T) {
	db := tempDB(t)

	var counters []uint32
	for i := 0; i < 3; i++ {
		wtx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := wtx.GetNamespace("test")
		if err != nil {
			ns, err = wtx.CreateNamespace("test")
			require.NoError(t, err)
		}
		require.NoError(t, wtx.Put(ns, []byte("key"), []byte("val")))
		wtx.MarkDataChanged()
		require.NoError(t, wtx.Commit())

		rtx, err := db.BeginRead()
		require.NoError(t, err)
		counters = append(counters, rtx.DiskFileChangeCounter())
		require.NoError(t, rtx.Rollback())
	}

	// Each counter should be strictly greater than the previous
	for i := 1; i < len(counters); i++ {
		assert.Greater(t, counters[i], counters[i-1],
			"counter should increment: counters[%d]=%d should be > counters[%d]=%d",
			i, counters[i], i-1, counters[i-1])
	}
}

// TestCacheInvalidation_WriteTx_StaleCheck verifies that IsDataStale/IsSchemaStale
// work on WriteTx too (inherited from ReadTx).
func TestCacheInvalidation_WriteTx_StaleCheck(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db1, err := testOpen(t, dbPath, Options{PageSize: 4096})
	require.NoError(t, err)
	defer db1.Close()

	allowDoubleOpen(dbPath)
	db2, err := testOpen(t, dbPath, Options{PageSize: 4096})
	require.NoError(t, err)
	defer db2.Close()

	// DB1 writes data
	wtx, err := db1.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, wtx.Put(ns, []byte("key1"), []byte("val1")))
	wtx.MarkDataChanged()
	require.NoError(t, wtx.Commit())

	// DB2 write transaction should detect staleness
	wtx2, err := db2.BeginWrite()
	require.NoError(t, err)
	assert.True(t, wtx2.IsDataStale(), "write tx on db2 should detect data staleness")
	require.NoError(t, wtx2.Rollback())
}

// TestCacheInvalidation_EmptyWrite_NoCounterChange verifies that committing
// an empty write transaction (no Put/Delete) does not change counters
// even with marks set.
func TestCacheInvalidation_EmptyWrite_NoCounterChange(t *testing.T) {
	db := tempDB(t)

	// Get initial counter
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	initialFCC := rtx.DiskFileChangeCounter()
	require.NoError(t, rtx.Rollback())

	// Empty write with marks
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	wtx.MarkDataChanged()
	require.NoError(t, wtx.Commit())

	// Counter should be unchanged (no dirty pages)
	rtx, err = db.BeginRead()
	require.NoError(t, err)
	assert.Equal(t, initialFCC, rtx.DiskFileChangeCounter(),
		"counter should not change for empty transaction")
	require.NoError(t, rtx.Rollback())
}

// TestCacheInvalidation_DataOnly_SchemaUnchanged verifies that
// MarkDataChanged does NOT affect SchemaCookie.
func TestCacheInvalidation_DataOnly_SchemaUnchanged(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db1, err := testOpen(t, dbPath, Options{PageSize: 4096})
	require.NoError(t, err)
	defer db1.Close()

	allowDoubleOpen(dbPath)
	db2, err := testOpen(t, dbPath, Options{PageSize: 4096})
	require.NoError(t, err)
	defer db2.Close()

	// DB1 writes data only (no schema change)
	wtx, err := db1.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, wtx.Put(ns, []byte("key1"), []byte("val1")))
	wtx.MarkDataChanged()
	// NOT calling MarkSchemaChanged
	require.NoError(t, wtx.Commit())

	// DB2: data stale, schema NOT stale
	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	assert.True(t, rtx.IsDataStale(), "data should be stale")
	assert.False(t, rtx.IsSchemaStale(), "schema should NOT be stale")
	require.NoError(t, rtx.Rollback())
}

// TestCacheInvalidation_FileReopened verifies counters survive close/reopen.
func TestCacheInvalidation_FileReopened(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Write some data and bump counters
	db1, err := testOpen(t, dbPath, Options{PageSize: 4096})
	require.NoError(t, err)

	wtx, err := db1.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, wtx.Put(ns, []byte("key1"), []byte("val1")))
	wtx.MarkDataChanged()
	wtx.MarkSchemaChanged()
	require.NoError(t, wtx.Commit())

	// Checkpoint to ensure data is in the main file
	require.NoError(t, db1.Checkpoint(CheckpointFull))

	// Get the counter values
	rtx, err := db1.BeginRead()
	require.NoError(t, err)
	fcc := rtx.DiskFileChangeCounter()
	sc := rtx.DiskSchemaCookie()
	require.NoError(t, rtx.Rollback())
	require.NoError(t, db1.Close())

	// Reopen and verify counters are preserved
	db2, err := testOpen(t, dbPath, Options{PageSize: 4096})
	require.NoError(t, err)
	defer db2.Close()

	rtx, err = db2.BeginRead()
	require.NoError(t, err)
	assert.Equal(t, fcc, rtx.DiskFileChangeCounter(), "FileChangeCounter should survive reopen")
	assert.Equal(t, sc, rtx.DiskSchemaCookie(), "SchemaCookie should survive reopen")
	assert.False(t, rtx.IsDataStale(), "should not be stale on fresh open")
	assert.False(t, rtx.IsSchemaStale(), "schema should not be stale on fresh open")
	require.NoError(t, rtx.Rollback())
}

// TestCacheInvalidation_Checkpoint checks that Checkpoint() is available on DB.
func TestCacheInvalidation_Checkpoint(t *testing.T) {
	db := tempDB(t)

	// Write data
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, wtx.Put(ns, []byte("key1"), []byte("val1")))
	wtx.MarkDataChanged()
	require.NoError(t, wtx.Commit())

	// Verify data survives checkpoint
	require.NoError(t, db.Checkpoint(CheckpointFull))

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns, err = rtx.GetNamespace("test")
	require.NoError(t, err)
	val, err := rtx.Get(ns, []byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("val1"), val)
	assert.False(t, rtx.IsDataStale())
	require.NoError(t, rtx.Rollback())
}

// TestCacheInvalidation_InProcessMode verifies the mechanism works
// with InProcess mode too.
func TestCacheInvalidation_InProcessMode(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := testOpen(t, dbPath, Options{PageSize: 4096, InProcess: true})
	require.NoError(t, err)
	defer db.Close()

	// Initial state: not stale
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	assert.False(t, rtx.IsDataStale())
	require.NoError(t, rtx.Rollback())

	// Write and mark
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, wtx.Put(ns, []byte("k"), []byte("v")))
	wtx.MarkDataChanged()
	require.NoError(t, wtx.Commit())

	// Not stale after own write
	rtx, err = db.BeginRead()
	require.NoError(t, err)
	assert.False(t, rtx.IsDataStale())
	require.NoError(t, rtx.Rollback())
}

// checkpointHelper forces a checkpoint. tempDB databases may not have
// a Checkpoint method exposed, so we call it via the pager if needed.
func init() {
	// Ensure os and filepath are used
	_ = os.Remove
	_ = filepath.Join
}
