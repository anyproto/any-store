/*
Ported from SQLite: insert.test, delete.test, delete3.test, delete4.test, zeroblob.test
Source: test/delete.test, delete3.test, delete4.test, zeroblob.test

Test scenario:
Exercises bulk insert, selective delete, large-value overflow pages, rollback of
deletes, sequential vs random key insertion order, and boundary conditions on
value sizes. Covers B-tree freelist reuse, rebalancing during large deletes,
overflow page management, and transaction rollback correctness.

Deviations from original:
- All SQL WHERE-clause filtering replaced with explicit key enumeration or cursor scans
- delete3.test row count scaled from 524288 to 8192 for practical test time
- zeroblob tests adapted: SQL round-trip replaced with Put/Get key-value operations
- insert.test, insert2-5.test: Entirely OUT_OF_SCOPE (SQL syntax, triggers, constraints)
- delete2.test: OUT_OF_SCOPE (C API cursor lifecycle)
- delete_db.test: OUT_OF_SCOPE (file management API)
*/
package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// =============================================================================
// Test Group 1: Bulk Insert and Delete All (from delete.test section 5)
// =============================================================================

// Port of delete-5.* (lines ~100-150 in delete.test)
// Original: Insert 200 rows, delete all, verify empty, re-insert selectively.
func TestSqlite_InsertDelete_BulkInsertDeleteAll(t *testing.T) {
	db, _ := tempDBWithNS(t, "t1")

	// Insert 200 rows (keys 1..200, value = 4-byte key squared as uint32)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= 200; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val := binary.BigEndian.AppendUint32(nil, uint32(i*i))
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	// Verify count = 200
	require.Equal(t, 200, countKeys(t, db, "t1"))

	// Delete ALL 200 keys
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= 200; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Delete(ns, key))
	}
	require.NoError(t, tx.Commit())

	// Verify count = 0
	require.Equal(t, 0, countKeys(t, db, "t1"))

	// IntegrityCheck
	require.NoError(t, db.IntegrityCheck())
}

// Port of delete-5.* selective delete patterns (delete.test)
// Original: Insert 200 rows, delete every 4th, then delete range >50, verify counts.
func TestSqlite_InsertDelete_BulkInsertSelectiveDelete(t *testing.T) {
	db, _ := tempDBWithNS(t, "t1")

	// Insert 200 rows
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= 200; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val := binary.BigEndian.AppendUint32(nil, uint32(i*i))
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	// Delete every 4th row (keys 1, 5, 9, 13, ... = keys where (i-1)%4 == 0)
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= 200; i += 4 {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Delete(ns, key))
	}
	require.NoError(t, tx.Commit())

	// Verify count = 150 (200 - 50 deleted)
	require.Equal(t, 150, countKeys(t, db, "t1"))

	// Delete all rows where key > 50
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 51; i <= 200; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		// Some of these were already deleted; ignore ErrKeyNotFound
		err := tx.Delete(ns, key)
		if err != nil && err != ErrKeyNotFound {
			require.NoError(t, err)
		}
	}
	require.NoError(t, tx.Commit())

	// Count remaining: keys 1..50 minus every-4th in that range
	// Every 4th in 1..50: 1,5,9,13,17,21,25,29,33,37,41,45,49 = 13 keys deleted
	// Remaining = 50 - 13 = 37
	require.Equal(t, 37, countKeys(t, db, "t1"))

	// IntegrityCheck
	require.NoError(t, db.IntegrityCheck())
}

// =============================================================================
// Test Group 2: Large-Scale Multi-Namespace (from delete.test section 6)
// =============================================================================

// Port of delete-6.* (lines ~160-210 in delete.test)
// Original: Insert 3000 rows into two tables, delete subsets, verify counts.
func TestSqlite_InsertDelete_LargeScaleMultiNamespace(t *testing.T) {
	db := tempDB(t)

	// Create namespaces "t1" and "t2"
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t2")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Insert 3000 rows into "t1" (keys 1..3000, value = 8 zero bytes)
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns1, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= 3000; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns1, key, make([]byte, 8)))
	}
	require.NoError(t, tx.Commit())

	// Insert 3000 rows into "t2"
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t2")
	require.NoError(t, err)
	for i := 1; i <= 3000; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns2, key, make([]byte, 8)))
	}
	require.NoError(t, tx.Commit())

	// Verify counts
	require.Equal(t, 3000, countKeys(t, db, "t1"))
	require.Equal(t, 3000, countKeys(t, db, "t2"))

	// Delete from "t1" all keys > 7
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns1, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 8; i <= 3000; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Delete(ns1, key))
	}
	require.NoError(t, tx.Commit())

	// Verify remaining in "t1": keys 1..7
	require.Equal(t, 7, countKeys(t, db, "t1"))

	// Delete from "t2" all keys > 7
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns2, err = db.getNamespaceLocked("t2")
	require.NoError(t, err)
	for i := 8; i <= 3000; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Delete(ns2, key))
	}
	require.NoError(t, tx.Commit())

	// Verify remaining in "t2": keys 1..7
	require.Equal(t, 7, countKeys(t, db, "t2"))

	// Delete ALL from "t1", verify empty
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns1, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= 7; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Delete(ns1, key))
	}
	require.NoError(t, tx.Commit())
	require.Equal(t, 0, countKeys(t, db, "t1"))

	// Insert key 2 into "t1", verify it exists
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns1, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	key2 := binary.BigEndian.AppendUint32(nil, uint32(2))
	require.NoError(t, tx.Put(ns1, key2, make([]byte, 8)))
	require.NoError(t, tx.Commit())
	require.Equal(t, 1, countKeys(t, db, "t1"))

	// Delete ALL from "t2", verify empty
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns2, err = db.getNamespaceLocked("t2")
	require.NoError(t, err)
	for i := 1; i <= 7; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Delete(ns2, key))
	}
	require.NoError(t, tx.Commit())
	require.Equal(t, 0, countKeys(t, db, "t2"))

	// Insert key 2 into "t2", verify it exists
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns2, err = db.getNamespaceLocked("t2")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns2, key2, make([]byte, 8)))
	require.NoError(t, tx.Commit())
	require.Equal(t, 1, countKeys(t, db, "t2"))

	// IntegrityCheck
	require.NoError(t, db.IntegrityCheck())
}

// =============================================================================
// Test Group 3: Massive Insert, Delete Half (from delete3.test)
// =============================================================================

// Port of delete3-1.1 (lines 1-40 in delete3.test)
// Original: Exponential doubling insert to 524288 rows, delete even keys, verify half remain.
// DEVIATION: Scaled from 524288 to 8192 rows for practical test time.
func TestSqlite_InsertDelete_MassiveInsertDeleteHalf(t *testing.T) {
	db, _ := tempDBWithNS(t, "t1")

	// Insert keys 1..8192
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= 8192; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	// Verify count = 8192
	require.Equal(t, 8192, countKeys(t, db, "t1"))

	// Delete all even-numbered keys
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 2; i <= 8192; i += 2 {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Delete(ns, key))
	}
	require.NoError(t, tx.Commit())

	// Verify count = 4096
	require.Equal(t, 4096, countKeys(t, db, "t1"))

	// IntegrityCheck
	require.NoError(t, db.IntegrityCheck())
}

// =============================================================================
// Test Group 4: Delete Alternating Small Values (from delete4.test section 1)
// =============================================================================

// Port of delete4-1.* (lines 1-30 in delete4.test)
// Original: Insert 8 rows with alternating flag values, delete flagged rows.
func TestSqlite_InsertDelete_DeleteAlternatingSmall(t *testing.T) {
	db, _ := tempDBWithNS(t, "t1")

	// Insert 8 rows: odd keys get val=[]byte{0}, even keys get val=[]byte{1}
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= 8; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		var val []byte
		if i%2 == 0 {
			val = []byte{1} // flag=1
		} else {
			val = []byte{0} // flag=0
		}
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	// Delete all keys with flag=1 (keys 2, 4, 6, 8)
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for _, i := range []int{2, 4, 6, 8} {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Delete(ns, key))
	}
	require.NoError(t, tx.Commit())

	// Verify remaining keys: 1, 3, 5, 7
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	cur := rtx.NewCursor(ns)
	var remaining []uint32
	for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
		k, err := cur.Key()
		require.NoError(t, err)
		remaining = append(remaining, binary.BigEndian.Uint32(k))
	}
	require.NoError(t, rtx.Rollback())

	require.Equal(t, []uint32{1, 3, 5, 7}, remaining)

	// IntegrityCheck
	require.NoError(t, db.IntegrityCheck())
}

// =============================================================================
// Test Group 5: Delete Alternating with Overflow Values (from delete4.test section 2)
// =============================================================================

// Port of delete4-2.* (lines 30-60 in delete4.test)
// Original: Same pattern as delete4-1 but with 200-byte randomblob values on 1024-page DB.
func TestSqlite_InsertDelete_DeleteAlternatingOverflow(t *testing.T) {
	db := tempDBWithPageSize(t, 1024)

	// Create namespace
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Insert 8 rows with 200-byte values; even keys have first byte=1
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= 8; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val := make([]byte, 200)
		if i%2 == 0 {
			val[0] = 1 // flag=1
		}
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	// Delete keys 2, 4, 6, 8 (the flag=1 rows)
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for _, i := range []int{2, 4, 6, 8} {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Delete(ns, key))
	}
	require.NoError(t, tx.Commit())

	// Verify remaining keys: 1, 3, 5, 7
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	cur := rtx.NewCursor(ns)
	var remaining []uint32
	for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
		k, err := cur.Key()
		require.NoError(t, err)
		remaining = append(remaining, binary.BigEndian.Uint32(k))
	}
	require.NoError(t, rtx.Rollback())

	require.Equal(t, []uint32{1, 3, 5, 7}, remaining)

	// IntegrityCheck
	require.NoError(t, db.IntegrityCheck())
}

// =============================================================================
// Test Group 6: Large Value Overflow Round-Trip (from zeroblob.test sections 1, 6)
// =============================================================================

// Port of zeroblob-1.* (lines 20-50 in zeroblob.test)
// Original: Insert 1MB zeroblob, 10K zeroblobs; verify length and content round-trip.
func TestSqlite_InsertDelete_LargeValueOverflow(t *testing.T) {
	t.Run("ZeroblobRoundTrip", func(t *testing.T) {
		db, _ := tempDBWithNS(t, "t1")

		// Insert key=1 with 1,000,000-byte zeroblob
		// Insert key=2 with 10,000-byte zeroblob
		// Insert key=3 with 10,000-byte zeroblob
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		require.NoError(t, tx.Put(ns, binary.BigEndian.AppendUint32(nil, 1), make([]byte, 1_000_000)))
		require.NoError(t, tx.Put(ns, binary.BigEndian.AppendUint32(nil, 2), make([]byte, 10_000)))
		require.NoError(t, tx.Put(ns, binary.BigEndian.AppendUint32(nil, 3), make([]byte, 10_000)))
		require.NoError(t, tx.Commit())

		// Verify via read tx
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		ns, err = db.getNamespaceLocked("t1")
		require.NoError(t, err)

		v1, err := rtx.Get(ns, binary.BigEndian.AppendUint32(nil, 1))
		require.NoError(t, err)
		require.Equal(t, 1_000_000, len(v1))
		// Verify all bytes are 0x00
		require.True(t, bytes.Equal(v1, make([]byte, 1_000_000)), "1MB zeroblob should be all zeros")

		v2, err := rtx.Get(ns, binary.BigEndian.AppendUint32(nil, 2))
		require.NoError(t, err)
		require.Equal(t, 10_000, len(v2))

		v3, err := rtx.Get(ns, binary.BigEndian.AppendUint32(nil, 3))
		require.NoError(t, err)
		require.Equal(t, 10_000, len(v3))

		require.NoError(t, rtx.Rollback())
		require.NoError(t, db.IntegrityCheck())
	})

	t.Run("MultipleOverflowSizes", func(t *testing.T) {
		db, _ := tempDBWithNS(t, "t1")

		// Insert values of different overflow sizes with deterministic random content
		rng := rand.New(rand.NewSource(42))
		sizes := []int{50_000, 100_000, 500_000}
		originals := make([][]byte, len(sizes))

		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		for idx, size := range sizes {
			blob := make([]byte, size)
			rng.Read(blob)
			originals[idx] = blob
			key := binary.BigEndian.AppendUint32(nil, uint32(idx+1))
			require.NoError(t, tx.Put(ns, key, blob))
		}
		require.NoError(t, tx.Commit())

		// Verify each value matches
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		ns, err = db.getNamespaceLocked("t1")
		require.NoError(t, err)
		for idx, size := range sizes {
			key := binary.BigEndian.AppendUint32(nil, uint32(idx+1))
			val, err := rtx.Get(ns, key)
			require.NoError(t, err)
			require.Equal(t, size, len(val))
			require.True(t, bytes.Equal(originals[idx], val),
				fmt.Sprintf("value mismatch for key %d (size %d)", idx+1, size))
		}
		require.NoError(t, rtx.Rollback())
		require.NoError(t, db.IntegrityCheck())
	})

	t.Run("OverflowValueUpdate", func(t *testing.T) {
		db, _ := tempDBWithNS(t, "t1")

		// Insert key=1 with 100,000-byte value (all 0xAA)
		val1 := bytes.Repeat([]byte{0xAA}, 100_000)
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		require.NoError(t, tx.Put(ns, binary.BigEndian.AppendUint32(nil, 1), val1))
		require.NoError(t, tx.Commit())

		// Update key=1 with 200,000-byte value (all 0xBB)
		val2 := bytes.Repeat([]byte{0xBB}, 200_000)
		tx, err = db.BeginWrite()
		require.NoError(t, err)
		ns, err = db.getNamespaceLocked("t1")
		require.NoError(t, err)
		require.NoError(t, tx.Put(ns, binary.BigEndian.AppendUint32(nil, 1), val2))
		require.NoError(t, tx.Commit())

		// Read and verify
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		ns, err = db.getNamespaceLocked("t1")
		require.NoError(t, err)
		got, err := rtx.Get(ns, binary.BigEndian.AppendUint32(nil, 1))
		require.NoError(t, err)
		require.Equal(t, 200_000, len(got))
		require.True(t, bytes.Equal(val2, got), "updated overflow value should be all 0xBB")
		require.NoError(t, rtx.Rollback())

		require.NoError(t, db.IntegrityCheck())
	})
}

// =============================================================================
// Test Group 7: Empty Namespace After Delete (from delete.test sections 5-6)
// =============================================================================

// Port of delete-5/6 empty-then-reinsert patterns (delete.test)
// Original: Delete all rows, re-insert, verify namespace is reusable.
func TestSqlite_InsertDelete_EmptyNamespaceAfterDelete(t *testing.T) {
	db, _ := tempDBWithNS(t, "t1")

	// Insert 100 rows
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= 100; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 8)))
	}
	require.NoError(t, tx.Commit())

	// Delete ALL rows
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= 100; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Delete(ns, key))
	}
	require.NoError(t, tx.Commit())
	require.Equal(t, 0, countKeys(t, db, "t1"))

	// Insert 50 new rows
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= 50; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 8)))
	}
	require.NoError(t, tx.Commit())
	require.Equal(t, 50, countKeys(t, db, "t1"))

	// Delete ALL rows again
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= 50; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Delete(ns, key))
	}
	require.NoError(t, tx.Commit())
	require.Equal(t, 0, countKeys(t, db, "t1"))

	// Re-insert keys 1..100 with different values to verify reusability
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= 100; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val := binary.BigEndian.AppendUint32(nil, uint32(i*3))
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	// Verify all 100 keys present with new values
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= 100; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val, err := rtx.Get(ns, key)
		require.NoError(t, err)
		expected := binary.BigEndian.AppendUint32(nil, uint32(i*3))
		require.Equal(t, expected, val, "key %d should have new value", i)
	}
	require.NoError(t, rtx.Rollback())

	require.NoError(t, db.IntegrityCheck())
}

// =============================================================================
// Test Group 8: Delete with Rollback (from delete.test section 5, insert2-3.5/3.7)
// =============================================================================

// Port of delete/insert rollback patterns
// Original: Delete within transaction, rollback, verify data is restored.
func TestSqlite_InsertDelete_DeleteWithRollback(t *testing.T) {
	t.Run("DeleteAllRollback", func(t *testing.T) {
		db, _ := tempDBWithNS(t, "t1")

		// Insert 100 rows
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		for i := 1; i <= 100; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			val := binary.BigEndian.AppendUint32(nil, uint32(i))
			require.NoError(t, tx.Put(ns, key, val))
		}
		require.NoError(t, tx.Commit())

		// Begin new write tx, delete all 100 rows
		tx, err = db.BeginWrite()
		require.NoError(t, err)
		ns, err = db.getNamespaceLocked("t1")
		require.NoError(t, err)
		for i := 1; i <= 100; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			require.NoError(t, tx.Delete(ns, key))
		}

		// Rollback instead of commit
		require.NoError(t, tx.Rollback())

		// Verify count = 100 (all rows restored)
		require.Equal(t, 100, countKeys(t, db, "t1"))

		require.NoError(t, db.IntegrityCheck())
	})

	t.Run("DeleteSubsetRollback", func(t *testing.T) {
		db, _ := tempDBWithNS(t, "t1")

		// Insert 100 rows
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		for i := 1; i <= 100; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			val := binary.BigEndian.AppendUint32(nil, uint32(i))
			require.NoError(t, tx.Put(ns, key, val))
		}
		require.NoError(t, tx.Commit())

		// Begin write tx, delete all rows except key=50
		tx, err = db.BeginWrite()
		require.NoError(t, err)
		ns, err = db.getNamespaceLocked("t1")
		require.NoError(t, err)
		for i := 1; i <= 100; i++ {
			if i == 50 {
				continue
			}
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			require.NoError(t, tx.Delete(ns, key))
		}

		// Rollback
		require.NoError(t, tx.Rollback())

		// Verify count = 100
		require.Equal(t, 100, countKeys(t, db, "t1"))

		require.NoError(t, db.IntegrityCheck())
	})
}

// =============================================================================
// Test Group 9: Sequential vs Random Key Insertion (general B-tree concept)
// =============================================================================

// Port of general B-tree insertion order concepts
// Original: Verify that sequential, reverse, and random key orderings produce correct results.
func TestSqlite_InsertDelete_SequentialVsRandomKey(t *testing.T) {
	t.Run("Sequential", func(t *testing.T) {
		db, _ := tempDBWithNS(t, "t1")

		// Insert keys 1..500 in order
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		for i := 1; i <= 500; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			val := binary.BigEndian.AppendUint32(nil, uint32(i))
			require.NoError(t, tx.Put(ns, key, val))
		}
		require.NoError(t, tx.Commit())

		// Cursor scan: verify all 500 keys present in sorted order
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		ns, err = db.getNamespaceLocked("t1")
		require.NoError(t, err)
		cur := rtx.NewCursor(ns)
		count := 0
		expected := uint32(1)
		for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
			k, err := cur.Key()
			require.NoError(t, err)
			require.Equal(t, expected, binary.BigEndian.Uint32(k))
			expected++
			count++
		}
		require.Equal(t, 500, count)
		require.NoError(t, rtx.Rollback())
		require.NoError(t, db.IntegrityCheck())
	})

	t.Run("Reverse", func(t *testing.T) {
		db, _ := tempDBWithNS(t, "t1")

		// Insert keys 500..1 in reverse order
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		for i := 500; i >= 1; i-- {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			val := binary.BigEndian.AppendUint32(nil, uint32(i))
			require.NoError(t, tx.Put(ns, key, val))
		}
		require.NoError(t, tx.Commit())

		// Cursor scan: verify all 500 keys present in sorted order (1..500)
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		ns, err = db.getNamespaceLocked("t1")
		require.NoError(t, err)
		cur := rtx.NewCursor(ns)
		count := 0
		expected := uint32(1)
		for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
			k, err := cur.Key()
			require.NoError(t, err)
			require.Equal(t, expected, binary.BigEndian.Uint32(k))
			expected++
			count++
		}
		require.Equal(t, 500, count)
		require.NoError(t, rtx.Rollback())
		require.NoError(t, db.IntegrityCheck())
	})

	t.Run("Random", func(t *testing.T) {
		db, _ := tempDBWithNS(t, "t1")

		// Generate keys 1..500, shuffle randomly with fixed seed
		rng := rand.New(rand.NewSource(12345))
		keys := make([]int, 500)
		for i := range keys {
			keys[i] = i + 1
		}
		rng.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

		// Insert in random order
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		for _, k := range keys {
			key := binary.BigEndian.AppendUint32(nil, uint32(k))
			val := binary.BigEndian.AppendUint32(nil, uint32(k))
			require.NoError(t, tx.Put(ns, key, val))
		}
		require.NoError(t, tx.Commit())

		// Cursor scan: verify all 500 keys present in sorted order
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		ns, err = db.getNamespaceLocked("t1")
		require.NoError(t, err)
		cur := rtx.NewCursor(ns)
		count := 0
		expected := uint32(1)
		for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
			k, err := cur.Key()
			require.NoError(t, err)
			require.Equal(t, expected, binary.BigEndian.Uint32(k))
			expected++
			count++
		}
		require.Equal(t, 500, count)
		require.NoError(t, rtx.Rollback())
		require.NoError(t, db.IntegrityCheck())
	})
}

// =============================================================================
// Test Group 10: Boundary Conditions (from zeroblob.test concepts)
// =============================================================================

// Port of zeroblob-6.* and general boundary concepts
// Original: Test maximum and minimum value sizes, empty values.
func TestSqlite_InsertDelete_BoundaryConditions(t *testing.T) {
	t.Run("EmptyValue", func(t *testing.T) {
		db, _ := tempDBWithNS(t, "t1")

		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		key := binary.BigEndian.AppendUint32(nil, 1)
		require.NoError(t, tx.Put(ns, key, []byte{}))
		require.NoError(t, tx.Commit())

		rtx, err := db.BeginRead()
		require.NoError(t, err)
		ns, err = db.getNamespaceLocked("t1")
		require.NoError(t, err)
		val, err := rtx.Get(ns, key)
		require.NoError(t, err)
		require.Equal(t, 0, len(val))
		require.NoError(t, rtx.Rollback())
		require.NoError(t, db.IntegrityCheck())
	})

	t.Run("SingleByteValue", func(t *testing.T) {
		db, _ := tempDBWithNS(t, "t1")

		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		key := binary.BigEndian.AppendUint32(nil, 1)
		require.NoError(t, tx.Put(ns, key, []byte{0x42}))
		require.NoError(t, tx.Commit())

		rtx, err := db.BeginRead()
		require.NoError(t, err)
		ns, err = db.getNamespaceLocked("t1")
		require.NoError(t, err)
		val, err := rtx.Get(ns, key)
		require.NoError(t, err)
		require.Equal(t, []byte{0x42}, val)
		require.NoError(t, rtx.Rollback())
		require.NoError(t, db.IntegrityCheck())
	})

	t.Run("MaxInlineValue", func(t *testing.T) {
		// With page size 4096, max inline is roughly ~3800 bytes
		db := tempDBWithPageSize(t, 4096)

		tx, err := db.BeginWrite()
		require.NoError(t, err)
		_, err = tx.CreateNamespace("t1")
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		tx, err = db.BeginWrite()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		key := binary.BigEndian.AppendUint32(nil, 1)
		val := make([]byte, 3800)
		for i := range val {
			val[i] = byte(i % 256)
		}
		require.NoError(t, tx.Put(ns, key, val))
		require.NoError(t, tx.Commit())

		rtx, err := db.BeginRead()
		require.NoError(t, err)
		ns, err = db.getNamespaceLocked("t1")
		require.NoError(t, err)
		got, err := rtx.Get(ns, key)
		require.NoError(t, err)
		require.True(t, bytes.Equal(val, got))
		require.NoError(t, rtx.Rollback())
		require.NoError(t, db.IntegrityCheck())
	})

	t.Run("JustOverflowValue", func(t *testing.T) {
		// With page size 1024, a 1000-byte value forces overflow
		dir := t.TempDir()
		db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{PageSize: 1024})
		require.NoError(t, err)
		defer db.Close()

		tx, err := db.BeginWrite()
		require.NoError(t, err)
		_, err = tx.CreateNamespace("t1")
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		tx, err = db.BeginWrite()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		key := binary.BigEndian.AppendUint32(nil, 1)
		val := make([]byte, 1000)
		for i := range val {
			val[i] = byte(i % 256)
		}
		require.NoError(t, tx.Put(ns, key, val))
		require.NoError(t, tx.Commit())

		rtx, err := db.BeginRead()
		require.NoError(t, err)
		ns, err = db.getNamespaceLocked("t1")
		require.NoError(t, err)
		got, err := rtx.Get(ns, key)
		require.NoError(t, err)
		require.True(t, bytes.Equal(val, got))
		require.NoError(t, rtx.Rollback())
		require.NoError(t, db.IntegrityCheck())
	})

	t.Run("VeryLargeValue", func(t *testing.T) {
		db, _ := tempDBWithNS(t, "t1")

		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		key := binary.BigEndian.AppendUint32(nil, 1)
		val := make([]byte, 1_000_000) // 1MB zeroblob
		require.NoError(t, tx.Put(ns, key, val))
		require.NoError(t, tx.Commit())

		rtx, err := db.BeginRead()
		require.NoError(t, err)
		ns, err = db.getNamespaceLocked("t1")
		require.NoError(t, err)
		got, err := rtx.Get(ns, key)
		require.NoError(t, err)
		require.Equal(t, 1_000_000, len(got))
		require.True(t, bytes.Equal(val, got), "1MB zeroblob should round-trip correctly")
		require.NoError(t, rtx.Rollback())
		require.NoError(t, db.IntegrityCheck())
	})
}
