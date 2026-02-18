/*
Ported from SQLite: savepoint.test
Source: /home/dev/work/sqlitec/test/savepoint.test

Test scenario:
Tests the SAVEPOINT, RELEASE, and ROLLBACK TO commands. Covers basic
savepoint creation, release, and rollback (groups 1-2); oddly named
savepoints mapped to integer IDs (group 8); and insert + savepoint +
rollback + full transaction rollback (group 17). The original file also
covers lock status in rollback mode, schema DDL inside savepoints,
incremental blob operations, auto_vacuum, authorization callbacks,
ATTACH databases, ON CONFLICT ROLLBACK, journal_mode=off, and
multi-client locking — all of which are out of scope for our WAL-only
key-value btree.

Deviations from original:
- savepoint-1.*: SQLite uses named savepoints outside explicit transactions
  (implicit auto-commit transaction). Our API requires savepoints within a
  WriteTx, so each test is wrapped in BeginWrite/Commit.
- savepoint-1.3: SQLite tests SAVEPOINT + db close without RELEASE.
  Adapted as BeginWrite + Savepoint + db.Close() without Commit.
- savepoint-1.6: Skipped — tests COMMIT of implicit auto-commit tx (SQL-only).
- savepoint-1.7: Skipped — wal_check_journal_mode (always WAL).
- savepoint-2.12: Skipped — wal_check_journal_mode.
- savepoint-3.*: Skipped — lock_status in rollback mode only (WAL-only btree).
- savepoint-4.*: Skipped — DDL (CREATE/DROP TABLE) inside savepoints.
- savepoint-5.*: Skipped — incrblob, multi-handle locking.
- savepoint-6.*: Skipped — incremental vacuum.
- savepoint-7.*: Skipped — incremental vacuum + DDL.
- savepoint-8-1, 8-2: SQLite tests quoted/whitespace savepoint names.
  Our API uses integer IDs; adapted as basic savepoint create + release.
- savepoint-9.*: Skipped — authorization callbacks.
- savepoint-10.*: Skipped — ATTACH databases.
- savepoint-11.*: Skipped — auto_vacuum + DDL.
- savepoint-12.*: Skipped — ON CONFLICT ROLLBACK with UNIQUE constraints.
- savepoint-13.*: Skipped — journal_mode=off (rollback mode only).
- savepoint-14.*, 15.*, 16.*: Skipped — multi-client (multi-process) tests.
- savepoint-17.1: CREATE TABLE inside BEGIN is out of scope. Adapted by
  creating namespace before the main tx, testing data insert + savepoint +
  rollback + full tx rollback.
- savepoint-17.2: Skipped — tests schema cache cleanup after ROLLBACK (SQL-only).
*/
package btree

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Port of savepoint-1.* (lines 26-96 in savepoint.test)
// Original: Tests that SAVEPOINT, RELEASE and ROLLBACK TO are correctly
// parsed, and that the auto-commit flag is correctly set and unset.
func TestSqlite_Savepoint_1(t *testing.T) {
	// --- savepoint-1.1 (lines 26-32) ---
	// Original: SAVEPOINT sp1; RELEASE sp1; — basic create and release.
	// DEVIATION: Wrapped in BeginWrite/Commit since our API requires explicit tx.
	t.Run("1.1", func(t *testing.T) {
		db := tempDB(t)
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		sp, err := tx.Savepoint()
		require.NoError(t, err)
		require.NoError(t, tx.ReleaseSavepoint(sp))
		require.NoError(t, tx.Commit())
	})

	// --- savepoint-1.2 (lines 33-38) ---
	// Original: SAVEPOINT sp1; ROLLBACK TO sp1; — basic create and rollback to.
	// DEVIATION: Wrapped in BeginWrite/Commit.
	t.Run("1.2", func(t *testing.T) {
		db := tempDB(t)
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		sp, err := tx.Savepoint()
		require.NoError(t, err)
		require.NoError(t, tx.RollbackToSavepoint(sp))
		require.NoError(t, tx.Commit())
	})

	// --- savepoint-1.3 (lines 39-43) ---
	// Original: SAVEPOINT sp1; db close — close without release should be safe.
	// DEVIATION: In SQLite, db close implicitly rolls back any active transaction.
	// In our API, we must explicitly rollback the tx before closing. We verify
	// that rollback succeeds after creating a savepoint without releasing it.
	t.Run("1.3", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.db")
		db, err := Open(path, DefaultOptions())
		require.NoError(t, err)
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		_, err = tx.Savepoint()
		require.NoError(t, err)
		// Rollback tx (implicit in SQLite's db close), then close
		require.NoError(t, tx.Rollback())
		require.NoError(t, db.Close())
	})

	// --- savepoint-1.4.1 (lines 44-51) ---
	// Original: SAVEPOINT sp1; SAVEPOINT sp2; RELEASE sp1; — releasing outer
	// releases both. sqlite3_get_autocommit returns 1 (back to autocommit).
	// DEVIATION: Our API uses integer IDs. Releasing the outer savepoint
	// releases all nested ones.
	t.Run("1.4.1", func(t *testing.T) {
		db := tempDB(t)
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		sp1, err := tx.Savepoint()
		require.NoError(t, err)
		_, err = tx.Savepoint()
		require.NoError(t, err)
		require.NoError(t, tx.ReleaseSavepoint(sp1)) // releases both sp1 and sp2
		require.NoError(t, tx.Commit())
	})

	// --- savepoint-1.4.2 + 1.4.3 (lines 52-63) ---
	// Original: SAVEPOINT sp1; SAVEPOINT sp2; RELEASE sp2; (autocommit=0)
	// then RELEASE sp1; (autocommit=1).
	// DEVIATION: Releasing inner savepoint sp2 leaves sp1 active;
	// releasing sp1 completes the savepoint stack.
	t.Run("1.4.2_and_1.4.3", func(t *testing.T) {
		db := tempDB(t)
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		sp1, err := tx.Savepoint()
		require.NoError(t, err)
		sp2, err := tx.Savepoint()
		require.NoError(t, err)
		require.NoError(t, tx.ReleaseSavepoint(sp2)) // release inner only
		require.NoError(t, tx.ReleaseSavepoint(sp1)) // release outer
		require.NoError(t, tx.Commit())
	})

	// --- savepoint-1.4.4 + 1.4.5 (lines 64-75) ---
	// Original: SAVEPOINT sp1; SAVEPOINT sp2; ROLLBACK TO sp1; (autocommit=0)
	// then RELEASE SAVEPOINT sp1; (autocommit=1).
	// Rolling back to sp1 discards sp2 implicitly.
	t.Run("1.4.4_and_1.4.5", func(t *testing.T) {
		db := tempDB(t)
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		sp1, err := tx.Savepoint()
		require.NoError(t, err)
		_, err = tx.Savepoint()
		require.NoError(t, err)
		require.NoError(t, tx.RollbackToSavepoint(sp1)) // rolls back to sp1, sp2 gone
		require.NoError(t, tx.ReleaseSavepoint(sp1))
		require.NoError(t, tx.Commit())
	})

	// --- savepoint-1.4.6 + 1.4.7 (lines 76-90) ---
	// Original: Triple-nested savepoints with sequential rollbacks.
	// SAVEPOINT sp1; SAVEPOINT sp2; SAVEPOINT sp3;
	// ROLLBACK TO sp3; ROLLBACK TO sp2; ROLLBACK TO sp1; (autocommit=0)
	// RELEASE SP1; (autocommit=1) — case-insensitive in SQLite.
	t.Run("1.4.6_and_1.4.7", func(t *testing.T) {
		db := tempDB(t)
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		sp1, err := tx.Savepoint()
		require.NoError(t, err)
		sp2, err := tx.Savepoint()
		require.NoError(t, err)
		sp3, err := tx.Savepoint()
		require.NoError(t, err)
		require.NoError(t, tx.RollbackToSavepoint(sp3))
		require.NoError(t, tx.RollbackToSavepoint(sp2))
		require.NoError(t, tx.RollbackToSavepoint(sp1))
		require.NoError(t, tx.ReleaseSavepoint(sp1))
		require.NoError(t, tx.Commit())
	})

	// --- savepoint-1.5 (lines 91-96) ---
	// Original: SAVEPOINT sp1; ROLLBACK TO sp1; — duplicate of 1.2 pattern.
	// DEVIATION: Wrapped in BeginWrite/Commit.
	t.Run("1.5", func(t *testing.T) {
		db := tempDB(t)
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		sp, err := tx.Savepoint()
		require.NoError(t, err)
		require.NoError(t, tx.RollbackToSavepoint(sp))
		require.NoError(t, tx.Commit())
	})

	// savepoint-1.6: SKIPPED — tests COMMIT of implicit auto-commit tx (SQL-only).
	// savepoint-1.7: SKIPPED — wal_check_journal_mode (always WAL).
}

// Port of savepoint-2.* (lines 107-183 in savepoint.test)
// Original: Tests rollbacks and releases of savepoints with a very simple data set.
// savepoint-2.1 through savepoint-2.11 form a single continuous transaction.
func TestSqlite_Savepoint_2(t *testing.T) {
	db, _ := tempDBWithNS(t, "t1")

	// The entire savepoint-2.* sequence is a single long transaction:
	//   CREATE TABLE t1(a, b, c);  -- done in setup
	//   BEGIN;
	//   INSERT INTO t1 VALUES(1, 2, 3);
	//   SAVEPOINT one;
	//   ... (various operations with rollback/release) ...
	//   ROLLBACK;

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)

	// --- savepoint-2.1 (lines 107-116) ---
	// Original: INSERT (1,2,3); SAVEPOINT one; UPDATE to (2,3,4); SELECT -> {2 3 4}.
	require.NoError(t, tx.Put(ns, []byte("k1"), []byte("1,2,3")))
	spOne, err := tx.Savepoint()
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("k1"), []byte("2,3,4")))
	val, err := tx.Get(ns, []byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("2,3,4"), val)

	// --- savepoint-2.2 (lines 117-122) ---
	// Original: ROLLBACK TO one; SELECT -> {1 2 3}.
	require.NoError(t, tx.RollbackToSavepoint(spOne))
	val, err = tx.Get(ns, []byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("1,2,3"), val)

	// --- savepoint-2.3 (lines 123-128) ---
	// Original: INSERT (4,5,6); SELECT -> {1 2 3 4 5 6}.
	require.NoError(t, tx.Put(ns, []byte("k2"), []byte("4,5,6")))
	val, err = tx.Get(ns, []byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("1,2,3"), val)
	val, err = tx.Get(ns, []byte("k2"))
	require.NoError(t, err)
	assert.Equal(t, []byte("4,5,6"), val)

	// --- savepoint-2.4 (lines 129-134) ---
	// Original: ROLLBACK TO one; SELECT -> {1 2 3} (k2 insert rolled back).
	require.NoError(t, tx.RollbackToSavepoint(spOne))
	val, err = tx.Get(ns, []byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("1,2,3"), val)
	_, err = tx.Get(ns, []byte("k2"))
	assert.ErrorIs(t, err, ErrKeyNotFound)

	// --- savepoint-2.5 (lines 137-144) ---
	// Original: INSERT (7,8,9); SAVEPOINT two; INSERT (10,11,12);
	// SELECT -> {1 2 3 7 8 9 10 11 12}.
	require.NoError(t, tx.Put(ns, []byte("k3"), []byte("7,8,9")))
	spTwo, err := tx.Savepoint()
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("k4"), []byte("10,11,12")))
	val, err = tx.Get(ns, []byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("1,2,3"), val)
	val, err = tx.Get(ns, []byte("k3"))
	require.NoError(t, err)
	assert.Equal(t, []byte("7,8,9"), val)
	val, err = tx.Get(ns, []byte("k4"))
	require.NoError(t, err)
	assert.Equal(t, []byte("10,11,12"), val)

	// --- savepoint-2.6 (lines 145-150) ---
	// Original: ROLLBACK TO two; SELECT -> {1 2 3 7 8 9} (k4 rolled back).
	require.NoError(t, tx.RollbackToSavepoint(spTwo))
	val, err = tx.Get(ns, []byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("1,2,3"), val)
	val, err = tx.Get(ns, []byte("k3"))
	require.NoError(t, err)
	assert.Equal(t, []byte("7,8,9"), val)
	_, err = tx.Get(ns, []byte("k4"))
	assert.ErrorIs(t, err, ErrKeyNotFound)

	// --- savepoint-2.7 (lines 151-156) ---
	// Original: INSERT (10,11,12) (re-insert); SELECT -> {1 2 3 7 8 9 10 11 12}.
	require.NoError(t, tx.Put(ns, []byte("k4"), []byte("10,11,12")))
	val, err = tx.Get(ns, []byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("1,2,3"), val)
	val, err = tx.Get(ns, []byte("k3"))
	require.NoError(t, err)
	assert.Equal(t, []byte("7,8,9"), val)
	val, err = tx.Get(ns, []byte("k4"))
	require.NoError(t, err)
	assert.Equal(t, []byte("10,11,12"), val)

	// --- savepoint-2.8 (lines 157-162) ---
	// Original: ROLLBACK TO one; SELECT -> {1 2 3} (k3 and k4 rolled back).
	require.NoError(t, tx.RollbackToSavepoint(spOne))
	val, err = tx.Get(ns, []byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("1,2,3"), val)
	_, err = tx.Get(ns, []byte("k3"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
	_, err = tx.Get(ns, []byte("k4"))
	assert.ErrorIs(t, err, ErrKeyNotFound)

	// --- savepoint-2.9 (lines 163-170) ---
	// Original: INSERT ('a','b','c'); SAVEPOINT two; INSERT ('d','e','f');
	// SELECT -> {1 2 3 a b c d e f}.
	require.NoError(t, tx.Put(ns, []byte("ka"), []byte("a,b,c")))
	spTwo, err = tx.Savepoint()
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("kd"), []byte("d,e,f")))
	val, err = tx.Get(ns, []byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("1,2,3"), val)
	val, err = tx.Get(ns, []byte("ka"))
	require.NoError(t, err)
	assert.Equal(t, []byte("a,b,c"), val)
	val, err = tx.Get(ns, []byte("kd"))
	require.NoError(t, err)
	assert.Equal(t, []byte("d,e,f"), val)

	// --- savepoint-2.10 (lines 171-176) ---
	// Original: RELEASE two; SELECT -> {1 2 3 a b c d e f} (still visible).
	require.NoError(t, tx.ReleaseSavepoint(spTwo))
	val, err = tx.Get(ns, []byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("1,2,3"), val)
	val, err = tx.Get(ns, []byte("ka"))
	require.NoError(t, err)
	assert.Equal(t, []byte("a,b,c"), val)
	val, err = tx.Get(ns, []byte("kd"))
	require.NoError(t, err)
	assert.Equal(t, []byte("d,e,f"), val)

	// --- savepoint-2.11 (lines 177-182) ---
	// Original: ROLLBACK; SELECT -> {} (entire transaction rolled back).
	require.NoError(t, tx.Rollback())
	count := countKeys(t, db, "t1")
	assert.Equal(t, 0, count, "namespace should be empty after full rollback")

	// savepoint-2.12: SKIPPED — wal_check_journal_mode.
}

// savepoint-3.*: SKIPPED
// Reason: Tests lock_status PRAGMA in rollback mode. Entire block is wrapped
// in `if {[wal_is_wal_mode]==0}`. Our btree is WAL-only.

// savepoint-4.*: SKIPPED
// Reason: Tests schema modifications (CREATE/DROP TABLE) inside savepoints.
// Our API does not support CreateNamespace/DeleteNamespace rollback via savepoints.

// savepoint-5.*: SKIPPED
// Reason: Tests incremental blob (incrblob) API, multi-handle locking.

// savepoint-6.*: SKIPPED
// Reason: Tests incremental vacuum inside nested savepoints.

// savepoint-7.*: SKIPPED
// Reason: Tests growing/shrinking database via incremental vacuum.

// Port of savepoint-8-1 and savepoint-8-2 (lines 553-560 in savepoint.test)
// Original: Tests oddly named and quoted savepoints.
// DEVIATION: Our API uses integer savepoint IDs, not names. The naming
// aspect is irrelevant; adapted as basic savepoint create + release tests.
func TestSqlite_Savepoint_8(t *testing.T) {
	// --- savepoint-8-1 (lines 553-556) ---
	// Original: SAVEPOINT "save1"; RELEASE save1;
	t.Run("8-1", func(t *testing.T) {
		db := tempDB(t)
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		sp, err := tx.Savepoint()
		require.NoError(t, err)
		require.NoError(t, tx.ReleaseSavepoint(sp))
		require.NoError(t, tx.Commit())
	})

	// --- savepoint-8-2 (lines 557-560) ---
	// Original: SAVEPOINT "Including whitespace "; RELEASE "including Whitespace ";
	t.Run("8-2", func(t *testing.T) {
		db := tempDB(t)
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		sp, err := tx.Savepoint()
		require.NoError(t, err)
		require.NoError(t, tx.ReleaseSavepoint(sp))
		require.NoError(t, tx.Commit())
	})
}

// savepoint-9.*: SKIPPED
// Reason: Tests SQLite authorization callback with savepoint operations.

// savepoint-10.*: SKIPPED
// Reason: Tests interaction of savepoints with ATTACH databases.

// savepoint-11.*: SKIPPED
// Reason: Tests auto_vacuum + DDL inside savepoints.

// savepoint-12.*: SKIPPED
// Reason: Tests ON CONFLICT ROLLBACK with UNIQUE constraints.

// savepoint-13.*: SKIPPED
// Reason: Tests journal_mode=off (rollback mode only).

// savepoint-14.*, 15.*, 16.*: SKIPPED
// Reason: Multi-client (multi-process) tests.

// Port of savepoint-17.1 (lines 1043-1052 in savepoint.test)
// Original: BEGIN; CREATE TABLE t6; INSERT (1,2); SAVEPOINT one;
// INSERT (3,4); ROLLBACK TO one; SELECT -> {1 2}; ROLLBACK;
// DEVIATION: Namespace "t6" is created before the main transaction since
// our API does not support rolling back namespace creation via savepoints.
// The core data insert + savepoint + rollback pattern is preserved.
func TestSqlite_Savepoint_17(t *testing.T) {
	// --- savepoint-17.1 (lines 1043-1052) ---
	t.Run("17.1", func(t *testing.T) {
		db, _ := tempDBWithNS(t, "t6")

		// BEGIN
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t6")
		require.NoError(t, err)

		// INSERT INTO t6 VALUES(1, 2)
		require.NoError(t, tx.Put(ns, []byte("k1"), []byte("1,2")))

		// SAVEPOINT one
		spOne, err := tx.Savepoint()
		require.NoError(t, err)

		// INSERT INTO t6 VALUES(3, 4)
		require.NoError(t, tx.Put(ns, []byte("k2"), []byte("3,4")))

		// ROLLBACK TO one
		require.NoError(t, tx.RollbackToSavepoint(spOne))

		// SELECT * FROM t6 -> {1 2}
		val, err := tx.Get(ns, []byte("k1"))
		require.NoError(t, err)
		assert.Equal(t, []byte("1,2"), val)
		_, err = tx.Get(ns, []byte("k2"))
		assert.ErrorIs(t, err, ErrKeyNotFound)

		// ROLLBACK (entire transaction)
		require.NoError(t, tx.Rollback())

		// Verify namespace is empty after full rollback
		count := countKeys(t, db, "t6")
		assert.Equal(t, 0, count, "namespace should be empty after full tx rollback")
	})

	// savepoint-17.2: SKIPPED — tests that CREATE TABLE t6 succeeds after
	// ROLLBACK (schema cache cleanup). SQL-only behavior.
}
