/*
Ported from SQLite: trans.test, trans2.test, rollback2.test
Sources:

	test/trans.test
	test/trans2.test
	test/rollback2.test

Test scenario:
Tests transaction commit, rollback, and MVCC read isolation. Covers:
  - Snapshot isolation: a reader started before a write sees the old state
    even after the write commits (trans-3)
  - Rollback restores exact prior state after mass delete/insert (trans-7)
  - Repeated rollback stress with growing dataset (trans-9)
  - Rollback-then-commit cycle: rollback restores, re-commit persists (trans2)
  - MVCC: concurrent reader unaffected by writer rollback (rollback2)

Deviations from original:
  - trans-3: Uses a single DB handle instead of two separate sqlite3 connections.
    Namespaces replace tables. Our single-DB API shares dirty pages between
    concurrent read/write txs, so we test WAL-level MVCC (read tx sees old
    snapshot after writer commits) rather than cross-connection isolation
    (read tx during active writer).
  - trans-7: Uses full key-value snapshot comparison instead of md5sum().
    Skipped steps involving DDL (CREATE TABLE, DROP TABLE) rollback.
  - trans-9: Uses deterministic patterns (every Nth key) instead of random()%10.
    Uses Go math/rand for value generation instead of randstr().
  - trans2: Removed UNIQUE constraint and statement journal tests. Kept the
    core rollback/commit state verification pattern.
  - rollback2: Tests WAL-level MVCC (read tx started before write keeps old
    snapshot) and rollback preservation. Cannot test cross-connection isolation
    since our single-DB shares dirty pages (wal-3.2 pattern).
  - trans-1,2,4,5,6,8: Skipped — SQL syntax, DDL rollback, multi-process crash.
  - trans3 (all): Skipped — SQL statement abort semantics.
  - rollback (all): Skipped — ON CONFLICT ROLLBACK, journal recovery.
  - rollback2-2.2,3,4: Skipped — covered by savepoint tests + 2.1 adaptation.
*/
package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// kvPair holds a key-value pair for snapshot comparison.
type kvPair struct {
	key   []byte
	value []byte
}

// snapshotNamespace returns all key-value pairs in a namespace via a read tx.
func snapshotNamespace(t *testing.T, db *DB, nsName string) []kvPair {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Rollback()) }()
	ns, err := db.getNamespaceLocked(nsName)
	require.NoError(t, err)
	cur := rtx.NewCursor(ns)
	var pairs []kvPair
	for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
		k, kerr := cur.Key()
		require.NoError(t, kerr)
		v, verr := cur.Value()
		require.NoError(t, verr)
		pairs = append(pairs, kvPair{key: bytes.Clone(k), value: bytes.Clone(v)})
	}
	return pairs
}

// snapshotFromWriteTx returns all key-value pairs visible in the write tx.
func snapshotFromWriteTx(t *testing.T, tx *WriteTx, ns *Namespace) []kvPair {
	t.Helper()
	cur := tx.NewCursor(ns)
	var pairs []kvPair
	for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
		k, kerr := cur.Key()
		require.NoError(t, kerr)
		v, verr := cur.Value()
		require.NoError(t, verr)
		pairs = append(pairs, kvPair{key: bytes.Clone(k), value: bytes.Clone(v)})
	}
	return pairs
}

// requireSnapshotsEqual asserts two snapshots are exactly equal.
func requireSnapshotsEqual(t *testing.T, expected, actual []kvPair, msg string) {
	t.Helper()
	require.Equal(t, len(expected), len(actual), "%s: snapshot length mismatch", msg)
	for i := range expected {
		if !bytes.Equal(expected[i].key, actual[i].key) {
			t.Fatalf("%s: key mismatch at index %d: expected %x, got %x", msg, i, expected[i].key, actual[i].key)
		}
		if !bytes.Equal(expected[i].value, actual[i].value) {
			t.Fatalf("%s: value mismatch at index %d for key %x: expected len %d, got len %d",
				msg, i, expected[i].key, len(expected[i].value), len(actual[i].value))
		}
	}
}

// countKeysInReadTx counts keys in a namespace using an existing read tx.
func countKeysInReadTx(t *testing.T, rtx *ReadTx, ns *Namespace) int {
	t.Helper()
	cur := rtx.NewCursor(ns)
	count := 0
	for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
		count++
	}
	return count
}

// Port of trans-3.* (lines 106-197 in trans.test)
// Original: Tests MVCC read isolation. Opens two connections to the same database.
// While one has an active write transaction with uncommitted changes, the other
// still sees the old (pre-write) state. After commit, a new read sees updated data.
// DEVIATION: Original uses two separate sqlite3 connections for cross-connection
// MVCC isolation. Our single-DB API shares dirty pages, so a read tx started during
// an active write sees the writer's uncommitted changes. We test the WAL-level MVCC:
// a read tx started before a committed write still sees the pre-commit snapshot,
// while a new read tx after commit sees the updated data.
func TestSqlite_Trans_3_ReadIsolation(t *testing.T) {
	db := tempDB(t)

	// Setup: create namespaces "one" and "two"
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("one")
	require.NoError(t, err)
	_, err = tx.CreateNamespace("two")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Insert initial data: one={k1,k2,k3}, two={k01,k05,k10}
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	nsOne, err := db.getNamespaceLocked("one")
	require.NoError(t, err)
	nsTwo, err := db.getNamespaceLocked("two")
	require.NoError(t, err)
	require.NoError(t, tx.Put(nsOne, []byte("k1"), []byte("one")))
	require.NoError(t, tx.Put(nsOne, []byte("k2"), []byte("two")))
	require.NoError(t, tx.Put(nsOne, []byte("k3"), []byte("three")))
	require.NoError(t, tx.Put(nsTwo, []byte("k01"), []byte("I")))
	require.NoError(t, tx.Put(nsTwo, []byte("k05"), []byte("V")))
	require.NoError(t, tx.Put(nsTwo, []byte("k10"), []byte("X")))
	require.NoError(t, tx.Commit())

	// trans-3.3/3.4: Start a read tx -- snapshot sees initial state (3 keys each)
	rtx1, err := db.BeginRead()
	require.NoError(t, err)
	nsOneR, err := db.getNamespaceLocked("one")
	require.NoError(t, err)
	nsTwoR, err := db.getNamespaceLocked("two")
	require.NoError(t, err)

	count1 := countKeysInReadTx(t, rtx1, nsOneR)
	assert.Equal(t, 3, count1, "initial read: namespace 'one' should have 3 keys")
	count2 := countKeysInReadTx(t, rtx1, nsTwoR)
	assert.Equal(t, 3, count2, "initial read: namespace 'two' should have 3 keys")

	// trans-3.5 through 3.9: Write and commit new data
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	nsOne, err = db.getNamespaceLocked("one")
	require.NoError(t, err)
	nsTwo, err = db.getNamespaceLocked("two")
	require.NoError(t, err)
	require.NoError(t, tx.Put(nsOne, []byte("k4"), []byte("four")))
	require.NoError(t, tx.Put(nsTwo, []byte("k04"), []byte("IV")))
	require.NoError(t, tx.Commit())

	// trans-3.10/3.11: Old read tx STILL sees old data (WAL snapshot isolation)
	count1 = countKeysInReadTx(t, rtx1, nsOneR)
	assert.Equal(t, 3, count1, "after commit: old read tx should still see 3 keys in 'one'")
	count2 = countKeysInReadTx(t, rtx1, nsTwoR)
	assert.Equal(t, 3, count2, "after commit: old read tx should still see 3 keys in 'two'")

	// End old read tx
	require.NoError(t, rtx1.Rollback())

	// trans-3.12/3.13: New read tx sees updated data
	rtx2, err := db.BeginRead()
	require.NoError(t, err)
	nsOneR2, err := db.getNamespaceLocked("one")
	require.NoError(t, err)
	nsTwoR2, err := db.getNamespaceLocked("two")
	require.NoError(t, err)
	count1 = countKeysInReadTx(t, rtx2, nsOneR2)
	assert.Equal(t, 4, count1, "new read tx: namespace 'one' should have 4 keys")
	count2 = countKeysInReadTx(t, rtx2, nsTwoR2)
	assert.Equal(t, 4, count2, "new read tx: namespace 'two' should have 4 keys")
	require.NoError(t, rtx2.Rollback())

	// trans-3.14: integrity check
	require.NoError(t, db.IntegrityCheck())
}

// Port of trans-7.* (lines 712-821 in trans.test)
// Original: Tests that rollback restores database to exact prior state after large
// modifications. Inserts 1000 random rows, records checksum, then cycles through
// DELETE all + ROLLBACK and INSERT doubling + ROLLBACK, verifying checksum each time.
func TestSqlite_Trans_7_RollbackRestoresState(t *testing.T) {
	db := tempDB(t)

	// Setup: create namespace "t2"
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t2")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Insert 1000 keys with random values
	rng := rand.New(rand.NewSource(42))
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t2")
	require.NoError(t, err)
	for i := 0; i < 1000; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		size := 50 + rng.Intn(200)
		val := make([]byte, size)
		rng.Read(val)
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	// Record snapshot
	snapshot := snapshotNamespace(t, db, "t2")
	require.Equal(t, 1000, len(snapshot), "should have 1000 keys")

	// trans-7.3: DELETE all, ROLLBACK, verify snapshot matches
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t2")
	require.NoError(t, err)
	for i := 0; i < 1000; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Delete(ns, key))
	}
	require.NoError(t, tx.Rollback())
	snapshot2 := snapshotNamespace(t, db, "t2")
	requireSnapshotsEqual(t, snapshot, snapshot2, "trans-7.3: after DELETE+ROLLBACK")

	// trans-7.4: INSERT doubling, ROLLBACK, verify snapshot matches
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t2")
	require.NoError(t, err)
	for i := 1000; i < 2000; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val := make([]byte, 100)
		rng.Read(val)
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Rollback())
	snapshot3 := snapshotNamespace(t, db, "t2")
	requireSnapshotsEqual(t, snapshot, snapshot3, "trans-7.4: after INSERT+ROLLBACK")

	// trans-7.5: DELETE all again, ROLLBACK
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t2")
	require.NoError(t, err)
	for i := 0; i < 1000; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Delete(ns, key))
	}
	require.NoError(t, tx.Rollback())
	snapshot4 := snapshotNamespace(t, db, "t2")
	requireSnapshotsEqual(t, snapshot, snapshot4, "trans-7.5: after DELETE+ROLLBACK again")

	// trans-7.6: INSERT doubling again, ROLLBACK
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t2")
	require.NoError(t, err)
	for i := 1000; i < 2000; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val := make([]byte, 100)
		rng.Read(val)
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Rollback())
	snapshot5 := snapshotNamespace(t, db, "t2")
	requireSnapshotsEqual(t, snapshot, snapshot5, "trans-7.6: after INSERT+ROLLBACK again")

	// Integrity check
	require.NoError(t, db.IntegrityCheck())
}

// Port of trans-9.* (lines 878-992 in trans.test)
// Original: Repeated rollback stress test. Creates 1024 rows with random strings.
// In a loop (20 iterations): record signature, DELETE subset + INSERT new + ROLLBACK,
// verify signature. Then evolve dataset with ~10% new committed inserts.
func TestSqlite_Trans_9_RepeatedRollbackStress(t *testing.T) {
	db := tempDB(t)

	// Setup: insert 1024 keys with random-sized values
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t3")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	rng := rand.New(rand.NewSource(1))
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t3")
	require.NoError(t, err)
	for i := 0; i < 1024; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		size := 10 + rng.Intn(391)
		val := make([]byte, size)
		rng.Read(val)
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	nextKey := uint32(1024)

	for iter := 0; iter < 20; iter++ {
		t.Run(fmt.Sprintf("iter=%d", iter), func(t *testing.T) {
			// Record signature
			sig := snapshotNamespace(t, db, "t3")
			sigCount := len(sig)

			// trans-9.$i.1: DELETE every 3rd + INSERT 50 new + ROLLBACK
			tx, err := db.BeginWrite()
			require.NoError(t, err)
			ns, err := db.getNamespaceLocked("t3")
			require.NoError(t, err)

			// Collect keys to delete (every 3rd)
			cur := tx.NewCursor(ns)
			var toDel [][]byte
			idx := 0
			for cerr := cur.First(); cerr == nil && cur.Valid(); cerr = cur.Next() {
				if idx%3 == 0 {
					k, kerr := cur.Key()
					require.NoError(t, kerr)
					toDel = append(toDel, bytes.Clone(k))
				}
				idx++
			}
			for _, k := range toDel {
				require.NoError(t, tx.Delete(ns, k))
			}
			// Insert 50 new keys
			for j := 0; j < 50; j++ {
				key := binary.BigEndian.AppendUint32(nil, nextKey)
				nextKey++
				val := make([]byte, 100)
				rng.Read(val)
				require.NoError(t, tx.Put(ns, key, val))
			}
			require.NoError(t, tx.Rollback())

			// Verify snapshot matches
			sig2 := snapshotNamespace(t, db, "t3")
			requireSnapshotsEqual(t, sig, sig2, fmt.Sprintf("iter %d phase 1", iter))

			// trans-9.$i.2: More complex mutation + ROLLBACK
			tx, err = db.BeginWrite()
			require.NoError(t, err)
			ns, err = db.getNamespaceLocked("t3")
			require.NoError(t, err)

			// Delete every 5th key
			cur = tx.NewCursor(ns)
			toDel = nil
			idx = 0
			for cerr := cur.First(); cerr == nil && cur.Valid(); cerr = cur.Next() {
				if idx%5 == 0 {
					k, kerr := cur.Key()
					require.NoError(t, kerr)
					toDel = append(toDel, bytes.Clone(k))
				}
				idx++
			}
			for _, k := range toDel {
				require.NoError(t, tx.Delete(ns, k))
			}
			// Insert 30 new keys
			for j := 0; j < 30; j++ {
				key := binary.BigEndian.AppendUint32(nil, nextKey)
				nextKey++
				val := make([]byte, 80)
				rng.Read(val)
				require.NoError(t, tx.Put(ns, key, val))
			}
			// Delete every 7th of remaining keys
			cur = tx.NewCursor(ns)
			toDel = nil
			idx = 0
			for cerr := cur.First(); cerr == nil && cur.Valid(); cerr = cur.Next() {
				if idx%7 == 0 {
					k, kerr := cur.Key()
					require.NoError(t, kerr)
					toDel = append(toDel, bytes.Clone(k))
				}
				idx++
			}
			for _, k := range toDel {
				require.NoError(t, tx.Delete(ns, k))
			}
			require.NoError(t, tx.Rollback())

			sig3 := snapshotNamespace(t, db, "t3")
			requireSnapshotsEqual(t, sig, sig3, fmt.Sprintf("iter %d phase 2", iter))

			// Evolve dataset: insert ~10% new keys (committed)
			if iter < 19 {
				tx, err = db.BeginWrite()
				require.NoError(t, err)
				ns, err = db.getNamespaceLocked("t3")
				require.NoError(t, err)
				newCount := sigCount / 10
				if newCount < 5 {
					newCount = 5
				}
				for j := 0; j < newCount; j++ {
					key := binary.BigEndian.AppendUint32(nil, nextKey)
					nextKey++
					size := 10 + rng.Intn(391)
					val := make([]byte, size)
					rng.Read(val)
					require.NoError(t, tx.Put(ns, key, val))
				}
				require.NoError(t, tx.Commit())
			}

			// Integrity check each iteration
			require.NoError(t, db.IntegrityCheck())
		})
	}
}

// Port of trans2-$i.* loop (lines 98-228 in trans2.test)
// Original: 30-iteration stress test with 400-row table. Each iteration:
// deletes 10%, records state, begins tx, inserts new rows, rolls back,
// verifies state restored, then re-does changes and commits.
// DEVIATION: Removed UNIQUE constraint tests (statement journal). Kept the
// core rollback/commit state verification pattern. Reduced to 10 iterations.
func TestSqlite_Trans2_RollbackThenCommit(t *testing.T) {
	db := tempDB(t)

	// Setup: create namespace "t1"
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Insert 400 keys with random-sized values (1000-6000 bytes)
	rng := rand.New(rand.NewSource(1))
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)

	// Track all live keys and their values for re-insertion
	type keyVal struct {
		id  uint32
		val []byte
	}
	var allKeys []uint32
	for i := uint32(0); i < 400; i++ {
		key := binary.BigEndian.AppendUint32(nil, i)
		size := 1000 + rng.Intn(5001)
		val := make([]byte, size)
		rng.Read(val)
		require.NoError(t, tx.Put(ns, key, val))
		allKeys = append(allKeys, i)
	}
	require.NoError(t, tx.Commit())

	nextID := uint32(400)

	for iter := 0; iter < 10; iter++ {
		t.Run(fmt.Sprintf("iter=%d", iter), func(t *testing.T) {
			// Delete ~10% of keys (committed)
			delCount := len(allKeys) / 10
			if delCount < 1 {
				delCount = 1
			}
			// Pick deterministic subset to delete
			perm := rng.Perm(len(allKeys))
			toDel := make([]uint32, delCount)
			for i := 0; i < delCount; i++ {
				toDel[i] = allKeys[perm[i]]
			}

			tx, err := db.BeginWrite()
			require.NoError(t, err)
			ns, err := db.getNamespaceLocked("t1")
			require.NoError(t, err)
			for _, id := range toDel {
				key := binary.BigEndian.AppendUint32(nil, id)
				require.NoError(t, tx.Delete(ns, key))
			}
			require.NoError(t, tx.Commit())

			// Remove deleted keys from allKeys
			delSet := make(map[uint32]bool, len(toDel))
			for _, id := range toDel {
				delSet[id] = true
			}
			var remaining []uint32
			for _, id := range allKeys {
				if !delSet[id] {
					remaining = append(remaining, id)
				}
			}
			allKeys = remaining

			// Record original state
			origState := snapshotNamespace(t, db, "t1")

			// Begin tx, insert new keys (re-insert deleted + 50 extra)
			tx, err = db.BeginWrite()
			require.NoError(t, err)
			ns, err = db.getNamespaceLocked("t1")
			require.NoError(t, err)

			var newKeys []keyVal
			for _, id := range toDel {
				key := binary.BigEndian.AppendUint32(nil, id)
				size := 1000 + rng.Intn(5001)
				val := make([]byte, size)
				rng.Read(val)
				require.NoError(t, tx.Put(ns, key, val))
				newKeys = append(newKeys, keyVal{id: id, val: bytes.Clone(val)})
			}
			for j := 0; j < 50; j++ {
				key := binary.BigEndian.AppendUint32(nil, nextID)
				size := 1000 + rng.Intn(5001)
				val := make([]byte, size)
				rng.Read(val)
				require.NoError(t, tx.Put(ns, key, val))
				newKeys = append(newKeys, keyVal{id: nextID, val: bytes.Clone(val)})
				nextID++
			}

			// Record the new state (with uncommitted writes)
			newState := snapshotFromWriteTx(t, tx, ns)

			// ROLLBACK
			require.NoError(t, tx.Rollback())

			// Verify state matches original
			afterRollback := snapshotNamespace(t, db, "t1")
			requireSnapshotsEqual(t, origState, afterRollback,
				fmt.Sprintf("iter %d: state should match after rollback", iter))

			// Re-do the same modifications and COMMIT
			tx, err = db.BeginWrite()
			require.NoError(t, err)
			ns, err = db.getNamespaceLocked("t1")
			require.NoError(t, err)
			for _, kv := range newKeys {
				key := binary.BigEndian.AppendUint32(nil, kv.id)
				require.NoError(t, tx.Put(ns, key, kv.val))
			}
			require.NoError(t, tx.Commit())

			// Verify state matches the new state
			afterCommit := snapshotNamespace(t, db, "t1")
			requireSnapshotsEqual(t, newState, afterCommit,
				fmt.Sprintf("iter %d: state should match after re-commit", iter))

			// Update allKeys with newly inserted keys
			for _, kv := range newKeys {
				allKeys = append(allKeys, kv.id)
			}

			// Integrity check
			require.NoError(t, db.IntegrityCheck())
		})
	}
}

// Port of rollback2-2.1 (lines 77-84 in rollback2.test)
// Original: Tests that after DELETE inside a BEGIN and then issuing ROLLBACK,
// a concurrent SELECT still sees all original rows.
// DEVIATION: Original uses separate connection (db2) for cross-connection MVCC.
// Our single-DB API shares dirty pages, so a read tx during an active write
// sees the writer's uncommitted changes (wal-3.2 pattern). We test that:
// (1) a read tx started BEFORE the write sees the pre-write state via WAL MVCC,
// (2) after rollback, a new read tx sees all original data preserved.
func TestSqlite_Rollback2_MVCCIsolation(t *testing.T) {
	db := tempDB(t)

	// Setup: create namespace "t1" with 40 keys
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= 40; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val := []byte(fmt.Sprintf("%02X", i))
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	// Start read tx BEFORE the write -- WAL snapshot sees all 40 keys
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	nsR, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	count := countKeysInReadTx(t, rtx, nsR)
	assert.Equal(t, 40, count, "reader should see all 40 keys initially")

	// Start write tx and delete odd keys, then commit
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= 40; i += 2 {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Delete(ns, key))
	}

	// Rollback the write tx (deletes are undone)
	require.NoError(t, tx.Rollback())

	// Old read tx still sees its WAL snapshot (all 40 keys)
	count = countKeysInReadTx(t, rtx, nsR)
	assert.Equal(t, 40, count, "old reader should still see 40 keys after rollback")
	require.NoError(t, rtx.Rollback())

	// New read tx also sees 40 (rollback preserved all data)
	rtx2, err := db.BeginRead()
	require.NoError(t, err)
	nsR2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	count = countKeysInReadTx(t, rtx2, nsR2)
	assert.Equal(t, 40, count, "new reader should see 40 keys after rollback")
	require.NoError(t, rtx2.Rollback())

	// Integrity check
	require.NoError(t, db.IntegrityCheck())
}
