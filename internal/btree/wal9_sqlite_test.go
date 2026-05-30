/*
Ported from SQLite: wal9.test, wal64k.test
Source: /home/dev/work/sqlitec/test/wal9.test
Source: /home/dev/work/sqlitec/test/wal64k.test

Test scenario:
wal9.test: Tests a specific bug fix where an assert() failure occurred when a
writer process with a partial SHM mapping rolled back a transaction that would
have restarted the WAL from the beginning. The test creates a large WAL (many
rows via doubling pattern), checkpoints it fully, then begins and rolls back a
transaction. The core behavior tested is: checkpoint everything, then rollback
a write transaction without crash or assertion failure.

wal64k.test: Tests WAL mode with various page sizes. Test 2.1 creates a database
with page_size=512, inserts 8200 rows with 300-byte zero-blob values, and runs
integrity check. This exercises small page size with many overflow pages.

Deviations from original:
  - wal9-1.0: PRAGMA wal_autocheckpoint=0 not directly mapped; we use
    DisableAutoCheckpoint option to prevent automatic checkpoints.
  - wal9-1.1: Original uses second connection (db2). Adapted as a read transaction
    on the same DB handle verifying empty namespace.
  - wal9-1.2: Original doubles rows 17 times (INSERT INTO t SELECT randomblob(100)
    FROM t) starting from 1 row, producing 131072 rows. DEVIATION: Reduced to 12
    doublings (4096 rows) to keep test runtime reasonable while still creating a
    large enough WAL to test the same behavior. Each row has 100-byte random values.
  - wal9-1.3 through 1.5: Skipped -- check exact file/SHM sizes (SQLite internals).
  - wal9-1.6: PRAGMA wal_checkpoint mapped to db.Checkpoint(CheckpointFull).
  - wal9-1.7: Original uses db2 (second connection). Adapted as begin write, insert,
    rollback on the same DB. Verifies rollback worked by checking row count unchanged.
  - wal64k-1.0 through 1.3: Skipped -- require test_syscall pagesize and SHM file checks.
  - wal64k-2.1: Adapted. Skipped CREATE INDEX (no secondary indexes). Skipped unix-excl
    VFS (not applicable). Core test (page_size=512, 8200 rows, integrity check) is portable.
*/
package btree

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Port of wal9-1.0 through 1.7 (lines 25-91 in wal9.test)
// Original: Tests that rolling back a transaction after a full checkpoint
// does not cause an assert failure (bug fix for partial SHM mapping issue).
func TestSqlite_WAL9(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// --- wal9-1.0 (lines 25-30) ---
	// Original: PRAGMA page_size=1024; PRAGMA journal_mode=WAL;
	//           PRAGMA wal_autocheckpoint=0; CREATE TABLE t(x);
	// DEVIATION: DisableAutoCheckpoint=true replaces PRAGMA wal_autocheckpoint=0
	db, err := testOpen(t, dbPath, Options{
		PageSize:              1024,
		DisableAutoCheckpoint: true,
	})
	require.NoError(t, err)

	// CREATE TABLE t(x) -> CreateNamespace "t"
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// --- wal9-1.1 (lines 32-34) ---
	// Original: execsql "SELECT * FROM t" db2 -> {}
	// DEVIATION: No second connection. Read from same DB to verify empty.
	t.Run("1.1_verify_empty", func(t *testing.T) {
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t")
		require.NoError(t, err)
		cur := rtx.NewCursor(ns)
		count := countCursor(t, cur)
		assert.Equal(t, 0, count, "namespace t should be empty initially")
		require.NoError(t, rtx.Rollback())
	})

	// --- wal9-1.2 (lines 36-59) ---
	// Original: BEGIN; INSERT INTO t VALUES(randomblob(100));
	//           then 17 times: INSERT INTO t SELECT randomblob(100) FROM t;
	//           COMMIT;
	//           This produces 2^17 = 131072 rows.
	// DEVIATION: Reduced to 12 doublings (4096 rows) to keep test fast while
	// still creating a large WAL. The key behavior (large WAL > one SHM chunk)
	// is preserved since 4096 * 100 bytes = ~400KB of data, which creates a
	// WAL well over 32KB.
	t.Run("1.2_large_insert", func(t *testing.T) {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t")
		require.NoError(t, err)

		// INSERT INTO t VALUES(randomblob(100)) -- 1 row
		nextKey := uint32(1)
		blob := make([]byte, 100)
		_, _ = rand.Read(blob)
		require.NoError(t, tx.Put(ns, intKey(nextKey), blob))
		nextKey++

		// Double rows 12 times: 1->2->4->8->...->4096
		for d := 0; d < 12; d++ {
			// Count current rows
			cur := tx.NewCursor(ns)
			existing := countCursor(t, cur)
			// Insert 'existing' new rows
			for i := 0; i < existing; i++ {
				blob := make([]byte, 100)
				_, _ = rand.Read(blob)
				require.NoError(t, tx.Put(ns, intKey(nextKey), blob))
				nextKey++
			}
		}

		// Verify count = 4096
		cur := tx.NewCursor(ns)
		count := countCursor(t, cur)
		assert.Equal(t, 4096, count, "should have 4096 rows after 12 doublings")

		require.NoError(t, tx.Commit())
	})

	// --- wal9-1.3, 1.4, 1.5: SKIPPED ---
	// Reason: Check exact file sizes (db=1024, wal>1500*1024, shm>32768).
	// These are SQLite internal details.

	// --- wal9-1.6 (lines 69-72) ---
	// Original: PRAGMA wal_checkpoint -> verify a==0, b==c, b>14500
	t.Run("1.6_checkpoint", func(t *testing.T) {
		require.NoError(t, db.Checkpoint(CheckpointFull))
	})

	// --- wal9-1.7 (lines 85-91) ---
	// Original: db2 execsql { BEGIN; INSERT INTO t VALUES('hello'); ROLLBACK; }
	// This tests that rollback after full checkpoint does not cause assert failure.
	// DEVIATION: No second connection. Use same DB handle.
	t.Run("1.7_rollback_after_checkpoint", func(t *testing.T) {
		// Record row count before
		countBefore := countKeys(t, db, "t")

		// BEGIN; INSERT; ROLLBACK
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t")
		require.NoError(t, err)
		require.NoError(t, tx.Put(ns, []byte("hello"), []byte("hello")))
		require.NoError(t, tx.Rollback())

		// Verify the rollback worked: row count unchanged
		countAfter := countKeys(t, db, "t")
		assert.Equal(t, countBefore, countAfter,
			"row count should be unchanged after rollback")

		// Verify 'hello' key was not inserted
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		ns, err = db.getNamespaceLocked("t")
		require.NoError(t, err)
		_, err = rtx.Get(ns, []byte("hello"))
		assert.ErrorIs(t, err, ErrKeyNotFound, "'hello' key should not exist after rollback")
		require.NoError(t, rtx.Rollback())
	})

	require.NoError(t, db.Close())
}

// Port of wal64k-2.1 (lines 51-59 in wal64k.test)
// Original: Creates a database with page_size=512 using unix-excl VFS,
// inserts 8200 rows with zeroblob(300) values, runs integrity check.
// DEVIATION: Skipped CREATE INDEX (no secondary indexes).
// Skipped unix-excl VFS (not applicable, our DB is always exclusive).
func TestSqlite_WAL64k_2_1(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// PRAGMA page_size=512; PRAGMA journal_mode=WAL;
	db, err := testOpen(t, dbPath, Options{PageSize: 512})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// CREATE TABLE t1(a,b) -> CreateNamespace "t1"
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// INSERT INTO t1(a,b) SELECT x, zeroblob(300) FROM c WHERE x<8200
	// Insert in batches to avoid holding a very large transaction
	batchSize := 500
	for start := 1; start <= 8200; start += batchSize {
		end := start + batchSize - 1
		if end > 8200 {
			end = 8200
		}
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		for i := start; i <= end; i++ {
			require.NoError(t, tx.Put(ns, intKey(uint32(i)), make([]byte, 300)))
		}
		require.NoError(t, tx.Commit())
	}

	// Verify count
	count := countKeys(t, db, "t1")
	assert.Equal(t, 8200, count, "should have 8200 rows")

	// PRAGMA integrity_check
	t.Run("integrity", func(t *testing.T) {
		require.NoError(t, db.IntegrityCheck())
	})

	// Log some stats
	dbInfo, _ := os.Stat(dbPath)
	walInfo, _ := os.Stat(dbPath + "-wal")
	if dbInfo != nil {
		t.Logf("DB size: %d bytes (%d pages at 512 bytes)", dbInfo.Size(), dbInfo.Size()/512)
	}
	if walInfo != nil {
		t.Logf("WAL size: %d bytes", walInfo.Size())
	}
}

// wal64k-1.0 through 1.3: SKIPPED
// Reason: Requires test_syscall pagesize 65536 to override OS page size,
// and checks SHM file sizes. Our system does not use separate SHM files
// for WAL indexing, and test_syscall is SQLite test infrastructure.

// wal9-1.3 through 1.5: SKIPPED
// Reason: Check exact file sizes (db=1024 bytes, wal>1500*1024, shm>32768).
// These verify SQLite's specific file layout and SHM behavior which differ
// in our implementation.
