package btree

// What a database handle is told about commits, and by which signal.
//
// Another process's commit is found by the wal-index header: the change
// epoch moves at the first begin after it (ReadTx.ChangeEpoch). The handle's
// own commits move nothing. The schema cookie goes up at each commit marked
// as a schema change. In a schema format 5 file FileChangeCount goes up at
// each commit marked as a data change, which is what a v2.4.x process
// attached to the same file reads; in a schema format 6 file, which no such
// process opens, it moves only when a commit writes page 1, to page 1's
// counter as last read plus one (pager_write_changecounter).
//
// The "other process" here is a second handle on the same file in this
// process, with a change state of its own.

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// allowDoubleOpen removes a path from the open registry so a second Open()
// can succeed. Used only in multi-process simulation tests. It must use the
// same key derivation as Open (file identity, falling back to lexical path).
func allowDoubleOpen(path string) {
	key, err := openRegistryKey(path)
	if err != nil {
		return
	}
	openDBs.Delete(key)
}

// cacheInvState begins and ends a read transaction on db and returns what it
// saw: the change epoch, whether its snapshot is exact for it, and the
// page-1 counters of its snapshot.
func cacheInvState(t *testing.T, db *DB) (epoch uint64, exact bool, fcc, sc uint32) {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	epoch, exact = rtx.ChangeEpoch()
	fcc, sc, err = rtx.SnapshotHeaderCounters()
	require.NoError(t, err)
	require.Equal(t, sc, rtx.DiskSchemaCookie())
	require.NoError(t, rtx.Rollback())
	return epoch, exact, fcc, sc
}

// cacheInvWrite commits one Put, marked as the flags say.
func cacheInvWrite(t *testing.T, db *DB, data, schema bool) {
	t.Helper()
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.GetNamespace("test")
	if err != nil {
		ns, err = wtx.CreateNamespace("test")
		require.NoError(t, err)
	}
	require.NoError(t, wtx.Put(ns, []byte("key1"), []byte("val1")))
	if data {
		wtx.MarkDataChanged()
	}
	if schema {
		wtx.MarkSchemaChanged()
	}
	require.NoError(t, wtx.Commit())
}

func cacheInvPair(t *testing.T) (db1, db2 *DB) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db1, err := testOpen(t, dbPath, Options{PageSize: 4096})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db1.Close() })
	allowDoubleOpen(dbPath)
	db2, err = testOpen(t, dbPath, Options{PageSize: 4096})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })
	return db1, db2
}

// A handle's own commit is not a change to it.
func TestCacheInvalidation_SingleProcess_NotStaleAfterOwnWrite(t *testing.T) {
	db := tempDB(t)
	e0, _, _, _ := cacheInvState(t, db)
	cacheInvWrite(t, db, true, false)
	e, exact, _, _ := cacheInvState(t, db)
	assert.Equal(t, e0, e, "an own commit does not move the change epoch")
	assert.True(t, exact)
}

// Another handle's commit is a change, counted once; the writing handle sees
// none.
func TestCacheInvalidation_MultiProcess_DetectsExternalWrite(t *testing.T) {
	db1, db2 := cacheInvPair(t)
	e1, _, _, _ := cacheInvState(t, db1)
	e2, _, _, _ := cacheInvState(t, db2)

	cacheInvWrite(t, db1, true, false)

	e, exact, _, _ := cacheInvState(t, db1)
	assert.Equal(t, e1, e, "db1: its own write is no change")
	assert.True(t, exact)
	e, exact, _, _ = cacheInvState(t, db2)
	assert.Equal(t, e2+1, e, "db2: db1's write is a change")
	assert.True(t, exact)
	e, _, _, _ = cacheInvState(t, db2)
	assert.Equal(t, e2+1, e, "and it is counted once")
}

// A commit marked as a schema change raises the schema cookie, which the
// other handle reads in its snapshot; the writing handle's own cookie is the
// new one too, and its change epoch does not move.
func TestCacheInvalidation_SchemaCookie(t *testing.T) {
	db1, db2 := cacheInvPair(t)
	e1, _, _, sc0 := cacheInvState(t, db1)
	e2, _, _, _ := cacheInvState(t, db2)

	cacheInvWrite(t, db1, true, true)

	e, _, _, sc := cacheInvState(t, db2)
	assert.Equal(t, sc0+1, sc, "db2 reads the new schema cookie")
	assert.Equal(t, e2+1, e, "and counts the commit")

	e, _, _, sc = cacheInvState(t, db1)
	assert.Equal(t, sc0+1, sc)
	assert.Equal(t, sc0+1, db1.LocalSchemaCookie(), "db1 recorded the cookie it committed")
	assert.Equal(t, e1, e, "db1: its own schema change is no change to it")
}

// A commit that marks nothing leaves both page-1 counters of a schema format 5
// file as they were. The other handle still finds it: the wal-index header
// changed.
func TestCacheInvalidation_NoMarks_CountersUnchanged(t *testing.T) {
	useLegacyFormat(t)
	db1, db2 := cacheInvPair(t)
	e0, _, fcc0, sc0 := cacheInvState(t, db2)

	cacheInvWrite(t, db1, false, false)

	e, exact, fcc, sc := cacheInvState(t, db2)
	assert.Equal(t, fcc0, fcc, "FileChangeCount should not have changed")
	assert.Equal(t, sc0, sc, "SchemaCookie should not have changed")
	assert.Equal(t, e0+1, e, "the commit is found without the counters")
	assert.True(t, exact)
}

// A change is consumed by the begin that finds it: transactions after it see
// the same epoch, each with an exact snapshot, and the next change moves it
// again. (This replaces the manual acknowledgement through
// UpdateLocalCounters.)
func TestCacheInvalidation_ChangeIsConsumedByTheBeginThatFindsIt(t *testing.T) {
	db1, db2 := cacheInvPair(t)
	e0, _, _, _ := cacheInvState(t, db2)

	cacheInvWrite(t, db1, true, true)
	for i := 0; i < 3; i++ {
		e, exact, _, _ := cacheInvState(t, db2)
		assert.Equal(t, e0+1, e, "begin %d", i)
		assert.True(t, exact)
	}
	cacheInvWrite(t, db1, true, false)
	e, exact, _, _ := cacheInvState(t, db2)
	assert.Equal(t, e0+2, e)
	assert.True(t, exact)
}

// A transaction whose snapshot is older than the newest change the handle
// has counted is not exact for the epoch: what it loads must not be taken
// for current. (This replaces the ordering of page-1 counters by
// AdvanceLocalCounters and LocalCaughtUp.)
func TestCacheInvalidation_OlderSnapshotIsNotExact(t *testing.T) {
	db1, db2 := cacheInvPair(t)
	cacheInvWrite(t, db1, true, false)

	old, err := db2.BeginRead()
	require.NoError(t, err)
	e0, exact := old.ChangeEpoch()
	require.True(t, exact)

	cacheInvWrite(t, db1, true, false)
	e, exact, _, _ := cacheInvState(t, db2) // another transaction counts it
	require.Equal(t, e0+1, e)
	require.True(t, exact)

	e, exact = old.ChangeEpoch()
	assert.Equal(t, e0+1, e, "the epoch is the handle's, not the transaction's")
	assert.False(t, exact, "the older snapshot does not hold the change the epoch counts")
	require.NoError(t, old.Rollback())
}

// A rolled-back transaction changes no committed counter and is no change to
// anybody, whatever it marked.
func TestCacheInvalidation_Rollback_NoCounterChange(t *testing.T) {
	db1, db2 := cacheInvPair(t)
	e1, _, fcc0, sc0 := cacheInvState(t, db1)
	e2, _, _, _ := cacheInvState(t, db2)

	wtx, err := db1.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, wtx.Put(ns, []byte("key1"), []byte("val1")))
	wtx.MarkDataChanged()
	wtx.MarkSchemaChanged()
	require.NoError(t, wtx.Rollback())

	e, _, fcc, sc := cacheInvState(t, db1)
	assert.Equal(t, fcc0, fcc, "counter should not change after rollback")
	assert.Equal(t, sc0, sc)
	assert.Equal(t, e1, e)
	e, _, _, _ = cacheInvState(t, db2)
	assert.Equal(t, e2, e, "a rollback is no change to the other handle")
}

// Each commit marked as a data change raises a schema format 5 file's
// FileChangeCount by one: the signal a v2.4.x process attached to the file
// watches.
func TestCacheInvalidation_MultipleWrites_CounterIncrements(t *testing.T) {
	useLegacyFormat(t)
	db := tempDB(t)
	_, _, prev, _ := cacheInvState(t, db)
	for i := 0; i < 3; i++ {
		cacheInvWrite(t, db, true, false)
		_, _, fcc, _ := cacheInvState(t, db)
		assert.Equal(t, prev+1, fcc, "commit %d", i)
		prev = fcc
	}
}

// A write begin can be the first transaction to find another handle's
// commit.
func TestCacheInvalidation_WriteTx_StaleCheck(t *testing.T) {
	db1, db2 := cacheInvPair(t)
	e0, _, _, _ := cacheInvState(t, db2)

	cacheInvWrite(t, db1, true, false)

	wtx2, err := db2.BeginWrite()
	require.NoError(t, err)
	e, exact := wtx2.ChangeEpoch()
	assert.Equal(t, e0+1, e, "the write begin on db2 counted db1's commit")
	assert.True(t, exact)
	require.NoError(t, wtx2.Rollback())
	e, _, _, _ = cacheInvState(t, db2)
	assert.Equal(t, e0+1, e, "once")
}

// A write transaction that wrote nothing commits nothing, whatever it
// marked: no counter moves and no other handle is told.
func TestCacheInvalidation_EmptyWrite_NoCounterChange(t *testing.T) {
	db1, db2 := cacheInvPair(t)
	_, _, fcc0, _ := cacheInvState(t, db1)
	e0, _, _, _ := cacheInvState(t, db2)

	wtx, err := db1.BeginWrite()
	require.NoError(t, err)
	wtx.MarkDataChanged()
	require.NoError(t, wtx.Commit())

	_, _, fcc, _ := cacheInvState(t, db1)
	assert.Equal(t, fcc0, fcc, "counter should not change for empty transaction")
	e, _, _, _ := cacheInvState(t, db2)
	assert.Equal(t, e0, e, "an empty commit publishes nothing")
}

// A data change does not touch the schema cookie.
func TestCacheInvalidation_DataOnly_SchemaUnchanged(t *testing.T) {
	db1, db2 := cacheInvPair(t)
	e0, _, fcc0, sc0 := cacheInvState(t, db2)

	cacheInvWrite(t, db1, true, false)

	e, _, fcc, sc := cacheInvState(t, db2)
	assert.Equal(t, fcc0+1, fcc, "the data change is counted on page 1")
	assert.Equal(t, sc0, sc, "the schema cookie is as it was")
	assert.Equal(t, e0+1, e)
}

// The page-1 counters survive a close and reopen, and a freshly opened
// handle starts with nothing to find.
func TestCacheInvalidation_FileReopened(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db1, err := testOpen(t, dbPath, Options{PageSize: 4096})
	require.NoError(t, err)
	cacheInvWrite(t, db1, true, true)
	require.NoError(t, db1.Checkpoint(CheckpointFull))
	_, _, fcc0, sc0 := cacheInvState(t, db1)
	require.NoError(t, db1.Close())

	db2, err := testOpen(t, dbPath, Options{PageSize: 4096})
	require.NoError(t, err)
	defer db2.Close()
	e, exact, fcc, sc := cacheInvState(t, db2)
	assert.Equal(t, fcc0, fcc, "FileChangeCounter should survive reopen")
	assert.Equal(t, sc0, sc, "SchemaCookie should survive reopen")
	assert.Equal(t, sc0, db2.LocalSchemaCookie())
	assert.Zero(t, e, "a fresh open has nothing to find")
	assert.True(t, exact)
}

// A FULL checkpoint keeps the data and is no change to the handle that ran
// it, nor, changing no wal-index header, to another.
func TestCacheInvalidation_Checkpoint(t *testing.T) {
	db1, db2 := cacheInvPair(t)
	cacheInvWrite(t, db1, true, false)
	e1, _, _, _ := cacheInvState(t, db1)
	e2, _, _, _ := cacheInvState(t, db2)

	require.NoError(t, db1.Checkpoint(CheckpointFull))

	rtx, err := db1.BeginRead()
	require.NoError(t, err)
	ns, err := rtx.GetNamespace("test")
	require.NoError(t, err)
	val, err := rtx.Get(ns, []byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("val1"), val)
	e, _ := rtx.ChangeEpoch()
	require.NoError(t, rtx.Rollback())
	assert.Equal(t, e1, e)
	e, _, _, _ = cacheInvState(t, db2)
	assert.Equal(t, e2, e, "a checkpoint that resets nothing is no change")
}

// An in-process database has no other process: the epoch stays 0, every
// snapshot is exact, and own writes are visible to the next transaction.
func TestCacheInvalidation_InProcessMode(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := testOpen(t, dbPath, Options{PageSize: 4096, InProcess: true})
	require.NoError(t, err)
	defer db.Close()

	e, exact, fcc0, _ := cacheInvState(t, db)
	assert.Zero(t, e)
	assert.True(t, exact)

	cacheInvWrite(t, db, true, false)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns, err := rtx.GetNamespace("test")
	require.NoError(t, err)
	val, err := rtx.Get(ns, []byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("val1"), val, "the own write is visible")
	e, exact = rtx.ChangeEpoch()
	fcc, _, err := rtx.SnapshotHeaderCounters()
	require.NoError(t, err)
	require.NoError(t, rtx.Rollback())
	assert.Zero(t, e)
	assert.True(t, exact)
	assert.Equal(t, fcc0+1, fcc)
}

// In a schema format 6 file marks do not move FileChangeCount: a commit that
// leaves page 1 out leaves it as it is, and one that writes page 1 sets it to
// page 1's counter as last read plus one, marked or not. The other handle
// finds every commit all the same.
func TestCacheInvalidation_Format6_CounterMovesWithPage1(t *testing.T) {
	skipInLegacyFormatSuite(t)
	db1, db2 := cacheInvPair(t)
	cacheInvWrite(t, db1, false, false) // creates the namespace: page 1
	e0, _, fcc0, _ := cacheInvState(t, db2)

	cacheInvWrite(t, db1, true, false) // in place, marked: no page 1
	e, exact, fcc, _ := cacheInvState(t, db2)
	assert.Equal(t, fcc0, fcc, "no page 1, no new counter")
	assert.Equal(t, e0+1, e, "the commit is found without the counter")
	assert.True(t, exact)

	wtx, err := db1.BeginWrite()
	require.NoError(t, err)
	_, err = wtx.CreateNamespace("more") // page 1, unmarked
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())
	_, _, fcc, _ = cacheInvState(t, db2)
	assert.Equal(t, fcc0, fcc, "db1 read page 1 last at its own commit's counter minus one, and writes the same value again")
}
