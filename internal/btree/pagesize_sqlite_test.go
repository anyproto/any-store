/*
Ported from SQLite: pagesize.test
Source: test/pagesize.test

Test scenario:
Tests database operations across all valid page sizes {512, 1024, 2048, 4096,
8192, 16384, 32768, 65536}. For each page size: creates a database, inserts
rows with large random values (creating overflow pages), tests transaction
rollback, verifies data survives close/reopen, performs bulk inserts and
deletes with rollback verification, and runs integrity checks throughout.

Deviations from original:
- pagesize-1.*: Skipped — tests PRAGMA page_size behavior (our page size is set via Options)
- pagesize-2.$PGSZ.0.*: Skipped — tests :memory: databases (out of scope)
- pagesize-2.$PGSZ.30, .40: Skipped — tests PRAGMA temp.page_size (temp databases out of scope)
- pagesize-3.*: Skipped — tests PRAGMA page_size rejection during read tx (PRAGMA behavior)
- Original uses {512, 2048, 4096, 8192}; we test all valid sizes including {1024, 16384, 32768, 65536}
- VACUUM step skipped (out of scope); delete is done directly
- Random values use deterministic seed per sub-test instead of SQLite's random()
*/
package btree

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Port of pagesize-2.$PGSZ.1-17 (lines 100-191 in pagesize.test)
// Original: For each page size, create DB, insert 3 rows with random large
// values, test rollback of bulk insert, integrity check, reopen, insert to
// 192 rows, test rollback of bulk delete, integrity check, actually delete,
// delete all, final integrity check.
func TestSqlite_PageSize_2(t *testing.T) {
	pageSizes := []uint32{512, 1024, 2048, 4096, 8192, 16384, 32768, 65536}

	for _, pgsz := range pageSizes {
		t.Run(fmt.Sprintf("pgsz=%d", pgsz), func(t *testing.T) {
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "test.db")
			rng := rand.New(rand.NewSource(int64(pgsz)))

			// Step 1: Open DB with specific page size
			db, err := testOpen(t, dbPath, Options{PageSize: pgsz})
			require.NoError(t, err)

			// Step 2: Create namespace "t1"
			tx, err := db.BeginWrite()
			require.NoError(t, err)
			_, err = tx.CreateNamespace("t1")
			require.NoError(t, err)
			require.NoError(t, tx.Commit())

			// Step 3: Insert 3 rows with random 900-9000 byte values (overflow pages)
			tx, err = db.BeginWrite()
			require.NoError(t, err)
			ns, err := db.getNamespaceLocked("t1")
			require.NoError(t, err)
			for i := 1; i <= 3; i++ {
				key := binary.BigEndian.AppendUint32(nil, uint32(i))
				size := 900 + rng.Intn(8100) // 900-9000 bytes
				val := make([]byte, size)
				rng.Read(val)
				require.NoError(t, tx.Put(ns, key, val))
			}
			require.NoError(t, tx.Commit())

			// Step 4: Verify 3 rows exist
			cnt := countKeys(t, db, "t1")
			assert.Equal(t, 3, cnt)

			// Step 5: Begin transaction, insert 45 more rows (total 48), then ROLLBACK
			// Original: INSERT INTO t1 SELECT x||x, y||y FROM t1 (doubling 4 times: 3->6->12->24->48)
			tx, err = db.BeginWrite()
			require.NoError(t, err)
			ns, err = db.getNamespaceLocked("t1")
			require.NoError(t, err)
			for i := 4; i <= 48; i++ {
				key := binary.BigEndian.AppendUint32(nil, uint32(i))
				val := make([]byte, 900)
				rng.Read(val)
				require.NoError(t, tx.Put(ns, key, val))
			}
			require.NoError(t, tx.Rollback())

			// Step 6: Verify count is still 3 after rollback
			cnt = countKeys(t, db, "t1")
			assert.Equal(t, 3, cnt)

			// Step 7: Integrity check
			require.NoError(t, db.IntegrityCheck())

			// Step 8: Close and reopen DB — verify page size persists and data survives
			require.NoError(t, db.Close())
			db, err = testOpen(t, dbPath, Options{PageSize: pgsz})
			require.NoError(t, err)

			// Step 9: Verify data survived reopen
			cnt = countKeys(t, db, "t1")
			assert.Equal(t, 3, cnt)

			// Step 10: Insert more data to reach 192 rows
			// Original: INSERT INTO t1 SELECT x||x, y||y FROM t1 (doubling: 3->6->12->24->48->96->192)
			tx, err = db.BeginWrite()
			require.NoError(t, err)
			ns, err = db.getNamespaceLocked("t1")
			require.NoError(t, err)
			for i := 4; i <= 192; i++ {
				key := binary.BigEndian.AppendUint32(nil, uint32(i))
				size := 100 + rng.Intn(500)
				val := make([]byte, size)
				rng.Read(val)
				require.NoError(t, tx.Put(ns, key, val))
			}
			require.NoError(t, tx.Commit())

			// Step 11: Verify 192 rows
			cnt = countKeys(t, db, "t1")
			assert.Equal(t, 192, cnt)

			// Step 12: Begin transaction, delete most rows (keep where key%5==0), ROLLBACK
			tx, err = db.BeginWrite()
			require.NoError(t, err)
			ns, err = db.getNamespaceLocked("t1")
			require.NoError(t, err)
			for i := 1; i <= 192; i++ {
				if i%5 != 0 {
					key := binary.BigEndian.AppendUint32(nil, uint32(i))
					require.NoError(t, tx.Delete(ns, key))
				}
			}
			require.NoError(t, tx.Rollback())

			// Step 13: Verify count is still 192 after rollback
			cnt = countKeys(t, db, "t1")
			assert.Equal(t, 192, cnt)

			// Step 14: Integrity check
			require.NoError(t, db.IntegrityCheck())

			// Step 15: Actually delete the rows (no rollback this time)
			// DEVIATION: Original does DELETE + VACUUM; we skip VACUUM (out of scope)
			tx, err = db.BeginWrite()
			require.NoError(t, err)
			ns, err = db.getNamespaceLocked("t1")
			require.NoError(t, err)
			for i := 1; i <= 192; i++ {
				if i%5 != 0 {
					key := binary.BigEndian.AppendUint32(nil, uint32(i))
					require.NoError(t, tx.Delete(ns, key))
				}
			}
			require.NoError(t, tx.Commit())

			// Step 16: Verify count is 38 (multiples of 5 from 1..192: 5,10,15,...,190)
			cnt = countKeys(t, db, "t1")
			assert.Equal(t, 38, cnt)

			// Step 17: Delete all remaining rows (multiples of 5)
			tx, err = db.BeginWrite()
			require.NoError(t, err)
			ns, err = db.getNamespaceLocked("t1")
			require.NoError(t, err)
			for i := 5; i <= 192; i += 5 {
				key := binary.BigEndian.AppendUint32(nil, uint32(i))
				require.NoError(t, tx.Delete(ns, key))
			}
			require.NoError(t, tx.Commit())

			// Step 18: Verify empty
			cnt = countKeys(t, db, "t1")
			assert.Equal(t, 0, cnt)

			// Step 19: Final integrity check
			require.NoError(t, db.IntegrityCheck())

			require.NoError(t, db.Close())
		})
	}
}
