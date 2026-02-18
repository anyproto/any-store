/*
Ported from SQLite: btree01.test
Source: /home/dev/work/sqlitec/test/btree01.test

Test scenario:
Exercises the b-tree balance() routine. Inserts rows with large blobs into
a 65536-byte page-size database, then shrinks and selectively expands values
to trigger rebalancing. Verifies integrity after each permutation.

Deviations from original:
- btree01-1.4: Conditional UPDATE (WHERE a%3==N) translated to loop with if-check
- btree01-2.1, btree01-2.2: Skipped — require WITHOUT ROWID and SQL JOINs
*/
package btree

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// Port of btree01-1.1 (lines 23-32 in btree01.test)
// Original: Insert 30 rows with 6500-byte blobs at page_size=65536,
// shrink all to 3000, expand row 2 to 64000, integrity check.
func TestSqlite_Btree01_1_1(t *testing.T) {
	db := tempDBWithPageSize(t, 65536)

	// CREATE TABLE t1 -> CreateNamespace
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// INSERT 30 rows with zeroblob(6500)
	putN(t, db, "t1", 30, 6500)

	// UPDATE t1 SET b=zeroblob(3000)
	updateAll(t, db, "t1", 30, 3000)

	// UPDATE t1 SET b=zeroblob(64000) WHERE a=2
	updateOne(t, db, "t1", 2, 64000)

	// PRAGMA integrity_check -> {ok}
	require.NoError(t, db.IntegrityCheck())
}

// Port of btree01-1.2.* (lines 38-50 in btree01.test)
// Original: Same as 1.1 but loops target row 1..30 (expand each row in turn).
func TestSqlite_Btree01_1_2(t *testing.T) {
	for i := 1; i <= 30; i++ {
		t.Run(fmt.Sprintf("i=%d", i), func(t *testing.T) {
			db := tempDBWithPageSize(t, 65536)
			stressRebalance(t, db, 30, 6500, 3000, 64000, i)
		})
	}
}

// Port of btree01-1.3.* (lines 51-63 in btree01.test)
// Original: Insert 6500, shrink to 2000, expand to 64000 for each target.
func TestSqlite_Btree01_1_3(t *testing.T) {
	for i := 1; i <= 30; i++ {
		t.Run(fmt.Sprintf("i=%d", i), func(t *testing.T) {
			db := tempDBWithPageSize(t, 65536)
			stressRebalance(t, db, 30, 6500, 2000, 64000, i)
		})
	}
}

// Port of btree01-1.4.* (lines 64-78 in btree01.test)
// Original: Insert 6500, conditional shrink by (a%3) to 6499, expand to 64000.
// DEVIATION: Conditional UPDATE (WHERE a%3==N) translated to loop with if-check.
func TestSqlite_Btree01_1_4(t *testing.T) {
	for target := 1; target <= 30; target++ {
		t.Run(fmt.Sprintf("i=%d", target), func(t *testing.T) {
			db := tempDBWithPageSize(t, 65536)

			tx, err := db.BeginWrite()
			require.NoError(t, err)
			_, err = tx.CreateNamespace("t1")
			require.NoError(t, err)
			require.NoError(t, tx.Commit())

			// INSERT 30 rows with zeroblob(6500)
			putN(t, db, "t1", 30, 6500)

			// UPDATE t1 SET b=zeroblob(6499) WHERE (a%3)==0
			tx, err = db.BeginWrite()
			require.NoError(t, err)
			ns, err := db.getNamespaceLocked("t1")
			require.NoError(t, err)
			for i := 1; i <= 30; i++ {
				if i%3 == 0 {
					key := binary.BigEndian.AppendUint32(nil, uint32(i))
					require.NoError(t, tx.Put(ns, key, make([]byte, 6499)))
				}
			}
			require.NoError(t, tx.Commit())

			// UPDATE t1 SET b=zeroblob(6499) WHERE (a%3)==1
			tx, err = db.BeginWrite()
			require.NoError(t, err)
			ns, err = db.getNamespaceLocked("t1")
			require.NoError(t, err)
			for i := 1; i <= 30; i++ {
				if i%3 == 1 {
					key := binary.BigEndian.AppendUint32(nil, uint32(i))
					require.NoError(t, tx.Put(ns, key, make([]byte, 6499)))
				}
			}
			require.NoError(t, tx.Commit())

			// UPDATE t1 SET b=zeroblob(6499) WHERE (a%3)==2
			tx, err = db.BeginWrite()
			require.NoError(t, err)
			ns, err = db.getNamespaceLocked("t1")
			require.NoError(t, err)
			for i := 1; i <= 30; i++ {
				if i%3 == 2 {
					key := binary.BigEndian.AppendUint32(nil, uint32(i))
					require.NoError(t, tx.Put(ns, key, make([]byte, 6499)))
				}
			}
			require.NoError(t, tx.Commit())

			// UPDATE t1 SET b=zeroblob(64000) WHERE a=$target
			updateOne(t, db, "t1", target, 64000)

			// PRAGMA integrity_check
			require.NoError(t, db.IntegrityCheck())
		})
	}
}

// Port of btree01-1.5.* (lines 79-91 in btree01.test)
// Original: Insert 6542, shrink to 2331, expand to 65496 for each target.
func TestSqlite_Btree01_1_5(t *testing.T) {
	for i := 1; i <= 30; i++ {
		t.Run(fmt.Sprintf("i=%d", i), func(t *testing.T) {
			db := tempDBWithPageSize(t, 65536)
			stressRebalance(t, db, 30, 6542, 2331, 65496, i)
		})
	}
}

// Port of btree01-1.6.* (lines 92-104 in btree01.test)
// Original: Insert 6542, shrink to 2332, expand to 65496 for each target.
// Differs from 1.5 only in shrinkSize: 2332 vs 2331 (boundary test).
func TestSqlite_Btree01_1_6(t *testing.T) {
	for i := 1; i <= 30; i++ {
		t.Run(fmt.Sprintf("i=%d", i), func(t *testing.T) {
			db := tempDBWithPageSize(t, 65536)
			stressRebalance(t, db, 30, 6542, 2332, 65496, i)
		})
	}
}

// Port of btree01-1.7.* (lines 105-117 in btree01.test)
// Original: Insert 6500, extreme shrink to 1, expand to 65000 for each target.
func TestSqlite_Btree01_1_7(t *testing.T) {
	for i := 1; i <= 30; i++ {
		t.Run(fmt.Sprintf("i=%d", i), func(t *testing.T) {
			db := tempDBWithPageSize(t, 65536)
			stressRebalance(t, db, 30, 6500, 1, 65000, i)
		})
	}
}

// Port of btree01-1.8.* (lines 118-130 in btree01.test)
// Original: Insert 31 rows (not 30) with 6500, shrink to 4000, expand to 65000.
// Loop goes to 31 (not 30).
func TestSqlite_Btree01_1_8(t *testing.T) {
	for i := 1; i <= 31; i++ {
		t.Run(fmt.Sprintf("i=%d", i), func(t *testing.T) {
			db := tempDBWithPageSize(t, 65536)
			stressRebalance(t, db, 31, 6500, 4000, 65000, i)
		})
	}
}

// btree01-2.1, btree01-2.2: SKIPPED
// Reason: Requires WITHOUT ROWID tables and SQL JOINs (out of scope).
