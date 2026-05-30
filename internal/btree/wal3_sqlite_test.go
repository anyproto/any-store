/*
Ported from SQLite: wal3.test
Source: /home/dev/work/sqlitec/test/wal3.test

Test scenario:
Tests WAL mode behavior when a rollback or savepoint rollback occurs,
verifying that WAL-index hash tables are correctly maintained. Populates
a large table (4018 rows with 800-byte values in a 1024-byte page-size
database) within a single transaction, then repeatedly updates single rows,
inserts 100 rows in a transaction that is rolled back, and verifies data
integrity. Also tests crash recovery by copying DB+WAL files and reopening.

Deviations from original:
  - wal3-1.0: SQLite uses doubling INSERT...SELECT FROM t1 to build 4018 rows.
    Adapted as a simple loop inserting 4018 rows sequentially with unique
    800-byte values. PRAGMA auto_vacuum=off, synchronous=normal,
    wal_autocheckpoint=0 have no direct equivalents; auto_vacuum is off by
    default, and we use DisableAutoCheckpoint to prevent checkpoint during test.
    WAL frame count check replaced with WAL file size check (approximate).
  - wal3-1.$i.2 through wal3-1.$i.4: "Second connection" simulated by closing
    and reopening the database (which triggers WAL recovery).
  - wal3-1.$i.5 through wal3-1.$i.7: File copy for crash recovery testing
    preserved as-is.
  - wal3-1.$i.3 and wal3-1.$i.6: SQLite reads the updated row via SQL
    "SELECT x FROM t1 WHERE rowid=$i". Our API uses Get with the integer key.
  - wal3-2.*: Skipped — do_multiclient_test with checkpoint coordination
    (aReadMark[] slot management). SQLite-internal WAL locking protocol.
  - wal3-3.*: Skipped — xSync counting via testvfs VFS shim.
  - wal3-5.*: Skipped — WAL recovery locking protocol via testvfs.
  - wal3-6.*: Skipped — aReadMark[] read-lock slot management via testvfs.
  - wal3-7.*: Skipped — reader snapshot interleaving via testvfs xShmLock.
  - wal3-9.*: Skipped — aReadMark[] slot exhaustion (50+ connections).
  - wal3-10.*: Skipped — do_multiclient_test + CREATE INDEX.
*/
package btree

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// aString generates a unique string of length n, mimicking SQLite's a_string(n).
// Each call increments a counter and repeats "${counter}." to fill n bytes.
func aString(counter int, n int) []byte {
	pattern := fmt.Sprintf("%d.", counter)
	buf := make([]byte, n)
	for i := 0; i < n; {
		i += copy(buf[i:], pattern)
	}
	return buf
}

// wal3CopyFile copies src to dst. If src doesn't exist, does nothing.
func wal3CopyFile(t *testing.T, src, dst string) {
	t.Helper()
	in, err := os.Open(src)
	if err != nil {
		return // file may not exist
	}
	defer in.Close()
	out, err := os.Create(dst)
	require.NoError(t, err)
	defer out.Close()
	_, err = io.Copy(out, in)
	require.NoError(t, err)
}

// Port of wal3-1.0 (lines 38-69 in wal3.test)
// Original: Creates a 1024-byte page-size database, inserts 4018 rows with
// 800-byte values via doubling INSERT...SELECT, then checks WAL frame count = 4056.
// DEVIATION: Rows inserted via sequential loop instead of doubling SELECT.
// WAL frame count check replaced with WAL file size bounds check.
// PRAGMA auto_vacuum=off, synchronous=normal, wal_autocheckpoint=0 adapted
// via DisableAutoCheckpoint option.
//
// Port of wal3-1.$i.* (lines 71-116 in wal3.test, loop i=1..49)
// Original: For each i in 1..49:
//   - Update row i with new 800-byte value, commit
//   - Begin tx, insert 100 rows, rollback
//   - Integrity check
//   - Open second connection, verify count=4018 and updated value, integrity check
//   - Copy DB+WAL, open copy, verify count=4018 and updated value, integrity check
func TestSqlite_WAL3_1(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	walPath := dbPath + "-wal"

	// Open with page_size=1024 and auto-checkpoint disabled
	db, err := testOpen(t, dbPath, Options{
		PageSize:              1024,
		DisableAutoCheckpoint: true,
	})
	require.NoError(t, err)

	// --- wal3-1.0 (lines 38-69) ---
	// CREATE TABLE t1(x); then insert 4018 rows with 800-byte values.
	// Original uses doubling INSERT...SELECT to build rows exponentially:
	//   1 -> 2 -> 4 -> 8 -> ... -> 2048, then LIMIT 1970 to reach 4018.
	// DEVIATION: We insert all 4018 rows in a single transaction loop.
	t.Run("1.0", func(t *testing.T) {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		_, err = tx.CreateNamespace("t1")
		require.NoError(t, err)

		ns, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		for i := 1; i <= 4018; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			val := aString(i, 800)
			require.NoError(t, tx.Put(ns, key, val))
		}
		require.NoError(t, tx.Commit())

		// Verify WAL file exists and has substantial size.
		// Original expects exactly 4056 frames.
		// Each frame = 1024 (page) + 24 (frame header) = 1048 bytes.
		// WAL header = 32 bytes.
		// Minimum expected: thousands of frames for 4018 rows * 800 bytes each.
		info, err := os.Stat(walPath)
		require.NoError(t, err)
		minExpectedSize := int64(32 + 3000*(1024+24)) // at least 3000 frames
		assert.Greater(t, info.Size(), minExpectedSize,
			"WAL file should contain thousands of frames after inserting 4018 rows")
	})

	// --- wal3-1.$i.* (lines 71-116) ---
	// Loop i=1..49, testing update + rollback + integrity + reopen + file copy.
	for i := 1; i < 50; i++ {
		i := i // capture for subtest
		t.Run(fmt.Sprintf("1.%d", i), func(t *testing.T) {
			keyI := binary.BigEndian.AppendUint32(nil, uint32(i))
			newValue := aString(4018+i, 800) // unique value per iteration

			// --- wal3-1.$i.1 (lines 73-83) ---
			// Original: UPDATE t1 SET x=$str WHERE rowid=$i; then BEGIN;
			// INSERT 100 rows; ROLLBACK; PRAGMA integrity_check -> ok.
			tx, err := db.BeginWrite()
			require.NoError(t, err)
			ns, err := db.getNamespaceLocked("t1")
			require.NoError(t, err)
			require.NoError(t, tx.Put(ns, keyI, newValue))
			require.NoError(t, tx.Commit())

			// BEGIN; INSERT 100 rows; ROLLBACK
			tx, err = db.BeginWrite()
			require.NoError(t, err)
			ns, err = db.getNamespaceLocked("t1")
			require.NoError(t, err)
			for j := 4019; j <= 4118; j++ {
				key := binary.BigEndian.AppendUint32(nil, uint32(j))
				require.NoError(t, tx.Put(ns, key, aString(j, 800)))
			}
			require.NoError(t, tx.Rollback())

			// PRAGMA integrity_check
			require.NoError(t, db.IntegrityCheck())

			// --- wal3-1.$i.2 through wal3-1.$i.4 (lines 88-98) ---
			// Original: Open second connection (db2), verify count=4018,
			// verify updated value of row $i, integrity check on db2.
			// DEVIATION: Simulate second connection by closing and reopening.
			require.NoError(t, db.Close())
			db2, err := testOpen(t, dbPath, Options{
				PageSize:              1024,
				DisableAutoCheckpoint: true,
			})
			require.NoError(t, err)

			// wal3-1.$i.2: SELECT count(*) FROM t1 -> 4018
			count := countKeys(t, db2, "t1")
			assert.Equal(t, 4018, count, "second connection should see 4018 rows")

			// wal3-1.$i.3: SELECT x FROM t1 WHERE rowid=$i -> $str
			rtx, err := db2.BeginRead()
			require.NoError(t, err)
			ns2, err := db2.getNamespaceLocked("t1")
			require.NoError(t, err)
			gotVal, err := rtx.Get(ns2, keyI)
			require.NoError(t, err)
			assert.Equal(t, newValue, gotVal, "row %d value should be the updated string", i)
			require.NoError(t, rtx.Rollback())

			// wal3-1.$i.4: PRAGMA integrity_check -> ok
			require.NoError(t, db2.IntegrityCheck())

			require.NoError(t, db2.Close())

			// --- wal3-1.$i.5 through wal3-1.$i.7 (lines 100-115) ---
			// Original: Copy DB+WAL to new location, open copy, verify count=4018,
			// verify updated row value, integrity check.
			copyDir := filepath.Join(dir, fmt.Sprintf("copy_%d", i))
			require.NoError(t, os.MkdirAll(copyDir, 0755))
			copyDBPath := filepath.Join(copyDir, "test.db")
			wal3CopyFile(t, dbPath, copyDBPath)
			wal3CopyFile(t, walPath, copyDBPath+"-wal")
			// Also copy WAL index if present
			wal3CopyFile(t, dbPath+"-wal-index", copyDBPath+"-wal-index")

			db3, err := testOpen(t, copyDBPath, Options{
				PageSize:              1024,
				DisableAutoCheckpoint: true,
			})
			require.NoError(t, err)

			// wal3-1.$i.5: SELECT count(*) FROM t1 -> 4018
			count = countKeys(t, db3, "t1")
			assert.Equal(t, 4018, count, "copied db should see 4018 rows")

			// wal3-1.$i.6: SELECT x FROM t1 WHERE rowid=$i -> $str
			rtx, err = db3.BeginRead()
			require.NoError(t, err)
			ns3, err := db3.getNamespaceLocked("t1")
			require.NoError(t, err)
			gotVal, err = rtx.Get(ns3, keyI)
			require.NoError(t, err)
			assert.Equal(t, newValue, gotVal, "copied db row %d should have updated value", i)
			require.NoError(t, rtx.Rollback())

			// wal3-1.$i.7: PRAGMA integrity_check -> ok
			require.NoError(t, db3.IntegrityCheck())

			require.NoError(t, db3.Close())

			// Reopen original for next iteration
			db, err = testOpen(t, dbPath, Options{
				PageSize:              1024,
				DisableAutoCheckpoint: true,
			})
			require.NoError(t, err)
		})
	}

	// Final close
	_ = db.Close()
}

// wal3-2.*: SKIPPED
// Reason: do_multiclient_test with checkpoint coordination using byte_is_zero
// and $AUTOVACUUM. Tests aReadMark[] slot behavior during checkpoint with
// concurrent readers. SQLite-internal WAL locking protocol.

// wal3-3.*: SKIPPED
// Reason: Tests xSync call counts for different PRAGMA synchronous levels
// using testvfs VFS shim. No equivalent in our system.

// wal3-5.*: SKIPPED
// Reason: Tests WAL recovery locking protocol using testvfs to intercept
// xShmBarrier and xShmLock during recovery. SQLite-internal mechanism.

// wal3-6.1.*, wal3-6.2.*: SKIPPED
// Reason: Tests aReadMark[] read-lock slot management with testvfs lock
// callback interception. SQLite-internal WAL index locking protocol.

// wal3-7.*: SKIPPED
// Reason: Tests reader snapshot interleaving when writer appends between
// header read and lock acquisition. Uses testvfs xShmLock callbacks.

// wal3-9.*: SKIPPED
// Reason: Tests aReadMark[] slot exhaustion with 50+ concurrent connections.
// Our API uses a single DB handle with BeginRead/BeginWrite.

// wal3-10.*: SKIPPED
// Reason: do_multiclient_test + CREATE INDEX (out of scope).
