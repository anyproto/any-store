package btree

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	stderrors "errors"
	"fmt"
	mrand "math/rand"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestOverflowSavepointCorruption exercises savepoint rollback/release with
// overflow-sized values to reproduce Bug 9: zero-content overflow pages
// after a clean commit. This test:
// 1. Inserts/updates/deletes keys with values large enough to trigger overflow
// 2. Uses savepoints with rollback and release
// 3. Verifies ALL data integrity after every commit
// 4. Enables debugOverflowReadErrors to catch silent readOverflowChainAt failures
func TestOverflowSavepointCorruption(t *testing.T) {
	// Enable debug panic on overflow read errors in collectLeafCells
	debugOverflowReadErrors.Store(1)
	defer debugOverflowReadErrors.Store(0)

	seeds := []int64{42, 12345, 55443322, 88776655, 99887766, 31337, 77777, 111222}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			testOverflowSavepointWithSeed(t, seed)
		})
	}
}

// TestOverflowSavepointDeepNesting exercises deeply nested savepoints with
// overflow-sized values, mimicking the crash test pattern where each executeOp
// creates its own savepoint (up to 3 levels deep).
func TestOverflowSavepointDeepNesting(t *testing.T) {
	debugOverflowReadErrors.Store(1)
	defer debugOverflowReadErrors.Store(0)

	seeds := []int64{42, 12345, 55443322, 88776655, 99887766, 31337}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			testOverflowDeepNesting(t, seed)
		})
	}
}

// TestOverflowSavepointConcurrent adds concurrent readers and background
// checkpoints to the overflow savepoint test.
func TestOverflowSavepointConcurrent(t *testing.T) {
	debugOverflowReadErrors.Store(1)
	defer debugOverflowReadErrors.Store(0)

	seeds := []int64{42, 12345, 55443322, 88776655}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			testOverflowSavepointConcurrent(t, seed)
		})
	}
}

func testOverflowDeepNesting(t *testing.T, seed int64) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := Open(dbPath, Options{PageSize: 4096})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("docs")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	rng := mrand.New(mrand.NewSource(seed))
	expected := make(map[string][]byte)
	maxDocs := 30

	for txIdx := 0; txIdx < 100; txIdx++ {
		tx, err := db.BeginWrite()
		require.NoError(t, err)

		ns, err := db.getNamespaceLocked("docs")
		require.NoError(t, err)

		expectedSnap := snapshotMap(expected)

		// Level 0: outer operations
		doOps(t, tx, ns, rng, expected, maxDocs, 1+rng.Intn(3))

		// Level 1: savepoint
		sp0, err := tx.Savepoint()
		require.NoError(t, err)
		expectedAtSp0 := snapshotMap(expected)

		doOps(t, tx, ns, rng, expected, maxDocs, 1+rng.Intn(5))

		// Level 2: nested savepoint
		sp1, err := tx.Savepoint()
		require.NoError(t, err)
		expectedAtSp1 := snapshotMap(expected)

		doOps(t, tx, ns, rng, expected, maxDocs, 1+rng.Intn(5))

		// Level 3: deeply nested savepoint (mimics executeOp creating savepoint)
		sp2, err := tx.Savepoint()
		require.NoError(t, err)
		expectedAtSp2 := snapshotMap(expected)

		doOps(t, tx, ns, rng, expected, maxDocs, 1+rng.Intn(3))

		// Unwind from deepest to shallowest
		if rng.Float64() < 0.4 {
			require.NoError(t, tx.RollbackToSavepoint(sp2))
			expected = expectedAtSp2
		} else {
			require.NoError(t, tx.ReleaseSavepoint(sp2))
		}

		// More ops at level 2
		doOps(t, tx, ns, rng, expected, maxDocs, rng.Intn(3))

		if rng.Float64() < 0.3 {
			require.NoError(t, tx.RollbackToSavepoint(sp1))
			expected = expectedAtSp1
		} else {
			require.NoError(t, tx.ReleaseSavepoint(sp1))
		}

		// More ops at level 1
		doOps(t, tx, ns, rng, expected, maxDocs, rng.Intn(3))

		if rng.Float64() < 0.3 {
			require.NoError(t, tx.RollbackToSavepoint(sp0))
			expected = expectedAtSp0
		} else {
			require.NoError(t, tx.ReleaseSavepoint(sp0))
		}

		// Post-savepoint ops
		doOps(t, tx, ns, rng, expected, maxDocs, rng.Intn(3))

		if rng.Float64() < 0.1 {
			require.NoError(t, tx.Rollback())
			expected = expectedSnap
		} else {
			require.NoError(t, tx.Commit())
		}

		verifyOverflowData(t, db, expected, txIdx)

		if rng.Float64() < 0.3 {
			modes := []CheckpointMode{CheckpointPassive, CheckpointFull, CheckpointRestart}
			_ = db.Checkpoint(modes[rng.Intn(len(modes))])
		}
	}

	require.NoError(t, db.IntegrityCheck())
}

func testOverflowSavepointConcurrent(t *testing.T, seed int64) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := Open(dbPath, Options{PageSize: 4096})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	// Use short busy timeout so checkpoint doesn't block writer for 5s per attempt
	// when readers are continuously active.
	db.pager.wal.busyHandler = DefaultBusyTimeout(200 * time.Millisecond)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("docs")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	rng := mrand.New(mrand.NewSource(seed))
	expected := make(map[string][]byte)
	maxDocs := 30

	// Background readers
	var stop atomic.Bool
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			localRng := mrand.New(mrand.NewSource(int64(id) + seed))
			for !stop.Load() {
				rtx, err := db.BeginRead()
				if err != nil {
					continue
				}
				ns, err := rtx.GetNamespace("docs")
				if err != nil {
					_ = rtx.Rollback()
					continue
				}
				// Random point reads
				for j := 0; j < 3; j++ {
					key := fmt.Sprintf("doc-%06d", localRng.Intn(maxDocs))
					_, _ = rtx.Get(ns, []byte(key))
				}
				// Scan
				cursor := rtx.NewCursor(ns)
				if cursor.First() == nil {
					count := 0
					for cursor.Valid() && count < 100 {
						_, _ = cursor.Key()
						_, _ = cursor.Value()
						count++
						if cursor.Next() != nil {
							break
						}
					}
				}
				cursor.Close()
				_ = rtx.Rollback()
			}
		}(r)
	}

	// Background checkpoint
	wg.Add(1)
	go func() {
		defer wg.Done()
		modes := []CheckpointMode{CheckpointPassive, CheckpointFull, CheckpointRestart}
		localRng := mrand.New(mrand.NewSource(seed + 999))
		for !stop.Load() {
			_ = db.Checkpoint(modes[localRng.Intn(len(modes))])
			time.Sleep(time.Millisecond)
		}
	}()

	for txIdx := 0; txIdx < 100; txIdx++ {
		tx, err := db.BeginWrite()
		if stderrors.Is(err, ErrBusy) {
			// CheckpointFull/Restart holds the WAL write lock while
			// busy-waiting on reader slots — skip this round.
			continue
		}
		require.NoError(t, err)

		ns, err := db.getNamespaceLocked("docs")
		require.NoError(t, err)

		expectedSnap := snapshotMap(expected)

		doOps(t, tx, ns, rng, expected, maxDocs, 1+rng.Intn(3))

		sp0, err := tx.Savepoint()
		require.NoError(t, err)
		expectedAtSp0 := snapshotMap(expected)

		doOps(t, tx, ns, rng, expected, maxDocs, 1+rng.Intn(5))

		if rng.Float64() < 0.3 {
			sp1, err := tx.Savepoint()
			require.NoError(t, err)
			expectedAtSp1 := snapshotMap(expected)

			doOps(t, tx, ns, rng, expected, maxDocs, 1+rng.Intn(3))

			if rng.Float64() < 0.5 {
				require.NoError(t, tx.RollbackToSavepoint(sp1))
				expected = expectedAtSp1
			} else {
				require.NoError(t, tx.ReleaseSavepoint(sp1))
			}
		}

		if rng.Float64() < 0.3 {
			require.NoError(t, tx.RollbackToSavepoint(sp0))
			expected = expectedAtSp0
		} else {
			require.NoError(t, tx.ReleaseSavepoint(sp0))
		}

		doOps(t, tx, ns, rng, expected, maxDocs, rng.Intn(3))

		if rng.Float64() < 0.1 {
			require.NoError(t, tx.Rollback())
			expected = expectedSnap
		} else {
			require.NoError(t, tx.Commit())
		}

		verifyOverflowData(t, db, expected, txIdx)
	}

	stop.Store(true)
	wg.Wait()

	require.NoError(t, db.IntegrityCheck())
}

// doOps performs random insert/update/delete operations on the namespace.
func doOps(t *testing.T, tx *WriteTx, ns *Namespace, rng *mrand.Rand, expected map[string][]byte, maxDocs, count int) {
	t.Helper()
	for j := 0; j < count; j++ {
		key := fmt.Sprintf("doc-%06d", rng.Intn(maxDocs))
		if rng.Float64() < 0.25 {
			if err := tx.Delete(ns, []byte(key)); err == nil {
				delete(expected, key)
			}
		} else {
			valSize := 1200 + rng.Intn(4000)
			val := makeRandomValue(rng, valSize)
			if err := tx.Put(ns, []byte(key), val); err != nil {
				t.Fatalf("Put(%s) failed: %v", key, err)
			}
			expected[key] = val
		}
	}
}

func snapshotMap(m map[string][]byte) map[string][]byte {
	snap := make(map[string][]byte, len(m))
	for k, v := range m {
		snap[k] = v
	}
	return snap
}

func testOverflowSavepointWithSeed(t *testing.T, seed int64) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := Open(dbPath, Options{PageSize: 4096})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Create namespace
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("docs")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	rng := mrand.New(mrand.NewSource(seed))

	// Reference map: tracks what data SHOULD be in the DB
	expected := make(map[string][]byte) // key -> value

	maxDocs := 50
	numTx := 100

	for txIdx := 0; txIdx < numTx; txIdx++ {
		tx, err := db.BeginWrite()
		require.NoError(t, err)

		ns, err := db.getNamespaceLocked("docs")
		require.NoError(t, err)

		// Take a snapshot of expected state before this tx
		expectedSnap := make(map[string][]byte, len(expected))
		for k, v := range expected {
			expectedSnap[k] = v
		}

		// Outer operations (before savepoint)
		outerOps := 1 + rng.Intn(5)
		for j := 0; j < outerOps; j++ {
			key := fmt.Sprintf("doc-%06d", rng.Intn(maxDocs))
			if rng.Float64() < 0.2 && len(expected) > 0 {
				// Delete
				if err := tx.Delete(ns, []byte(key)); err == nil {
					delete(expected, key)
				}
			} else {
				// Insert/update with overflow-sized value
				valSize := 1200 + rng.Intn(4000) // always overflow for 4096 page
				val := makeRandomValue(rng, valSize)
				if err := tx.Put(ns, []byte(key), val); err != nil {
					t.Fatalf("tx %d: outer Put(%s) failed: %v", txIdx, key, err)
				}
				expected[key] = val
			}
		}

		// Savepoint
		spId, spErr := tx.Savepoint()
		require.NoError(t, spErr, "tx %d: Savepoint failed", txIdx)

		// Save expected state at savepoint
		expectedAtSavepoint := make(map[string][]byte, len(expected))
		for k, v := range expected {
			expectedAtSavepoint[k] = v
		}

		// Inner operations (within savepoint)
		innerOps := 1 + rng.Intn(8)
		for j := 0; j < innerOps; j++ {
			key := fmt.Sprintf("doc-%06d", rng.Intn(maxDocs))
			if rng.Float64() < 0.3 {
				// Delete
				if err := tx.Delete(ns, []byte(key)); err == nil {
					delete(expected, key)
				}
			} else {
				// Insert/update with overflow-sized value
				valSize := 1200 + rng.Intn(4000)
				val := makeRandomValue(rng, valSize)
				if err := tx.Put(ns, []byte(key), val); err != nil {
					t.Fatalf("tx %d: inner Put(%s) failed: %v", txIdx, key, err)
				}
				expected[key] = val
			}
		}

		// Nested savepoint (30% chance)
		if rng.Float64() < 0.3 {
			spId2, sp2Err := tx.Savepoint()
			if sp2Err == nil {
				expectedAtSp2 := make(map[string][]byte, len(expected))
				for k, v := range expected {
					expectedAtSp2[k] = v
				}

				for j := 0; j < 1+rng.Intn(3); j++ {
					key := fmt.Sprintf("doc-%06d", rng.Intn(maxDocs))
					valSize := 1200 + rng.Intn(4000)
					val := makeRandomValue(rng, valSize)
					if err := tx.Put(ns, []byte(key), val); err != nil {
						t.Fatalf("tx %d: sp2 Put(%s) failed: %v", txIdx, key, err)
					}
					expected[key] = val
				}

				if rng.Float64() < 0.5 {
					// Rollback nested savepoint
					require.NoError(t, tx.RollbackToSavepoint(spId2))
					expected = expectedAtSp2 // restore reference state
				} else {
					require.NoError(t, tx.ReleaseSavepoint(spId2))
				}
			}
		}

		// Rollback or commit the main savepoint
		if rng.Float64() < 0.3 {
			// Rollback
			require.NoError(t, tx.RollbackToSavepoint(spId))
			expected = expectedAtSavepoint
		} else {
			require.NoError(t, tx.ReleaseSavepoint(spId))
		}

		// Post-savepoint operations (20% chance)
		if rng.Float64() < 0.2 {
			for j := 0; j < rng.Intn(5); j++ {
				key := fmt.Sprintf("doc-%06d", rng.Intn(maxDocs))
				if rng.Float64() < 0.3 {
					if err := tx.Delete(ns, []byte(key)); err == nil {
						delete(expected, key)
					}
				} else {
					valSize := 1200 + rng.Intn(4000)
					val := makeRandomValue(rng, valSize)
					if err := tx.Put(ns, []byte(key), val); err != nil {
						t.Fatalf("tx %d: post Put(%s) failed: %v", txIdx, key, err)
					}
					expected[key] = val
				}
			}
		}

		// Rollback entire tx (10% chance)
		if rng.Float64() < 0.1 {
			require.NoError(t, tx.Rollback())
			expected = expectedSnap
		} else {
			require.NoError(t, tx.Commit())
		}

		// Verify data after every commit
		verifyOverflowData(t, db, expected, txIdx)

		// Periodic checkpoint (30% chance)
		if rng.Float64() < 0.3 {
			modes := []CheckpointMode{CheckpointPassive, CheckpointFull, CheckpointRestart}
			_ = db.Checkpoint(modes[rng.Intn(len(modes))])
		}
	}

	// Final integrity check
	require.NoError(t, db.IntegrityCheck())
}

// verifyOverflowData reads all data in the DB and compares with expected.
func verifyOverflowData(t *testing.T, db *DB, expected map[string][]byte, txIdx int) {
	t.Helper()

	ns, err := db.getNamespaceLocked("docs")
	if err != nil {
		return
	}

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	found := make(map[string]bool)
	cursor := rtx.NewCursor(ns)
	if err := cursor.First(); err != nil {
		cursor.Close()
		return
	}

	for cursor.Valid() {
		key, kErr := cursor.Key()
		if kErr != nil {
			cursor.Close()
			t.Fatalf("tx %d verify: Key() error: %v", txIdx, kErr)
		}
		val, vErr := cursor.Value()
		if vErr != nil {
			cursor.Close()
			t.Fatalf("tx %d verify: Value() error at key=%s: %v", txIdx, string(key), vErr)
		}

		keyStr := string(key)
		found[keyStr] = true

		expectedVal, exists := expected[keyStr]
		if !exists {
			cursor.Close()
			t.Fatalf("tx %d verify: unexpected key %s in DB", txIdx, keyStr)
		}

		if !bytes.Equal(val, expectedVal) {
			// Check if val is all zeros (the specific corruption pattern)
			allZero := true
			for _, b := range val {
				if b != 0 {
					allZero = false
					break
				}
			}
			cursor.Close()
			if allZero {
				t.Fatalf("tx %d verify: key %s has ALL-ZERO value (len=%d), expected non-zero value (len=%d)\n"+
					"  Expected first 32 bytes: %s",
					txIdx, keyStr, len(val), len(expectedVal),
					hex.EncodeToString(expectedVal[:min(32, len(expectedVal))]))
			} else {
				t.Fatalf("tx %d verify: key %s value mismatch (len got=%d want=%d)\n"+
					"  Got first 32: %s\n"+
					"  Want first 32: %s",
					txIdx, keyStr, len(val), len(expectedVal),
					hex.EncodeToString(val[:min(32, len(val))]),
					hex.EncodeToString(expectedVal[:min(32, len(expectedVal))]))
			}
		}

		if err := cursor.Next(); err != nil {
			cursor.Close()
			t.Fatalf("tx %d verify: Next() error: %v", txIdx, err)
		}
	}
	cursor.Close()

	// Check for missing keys
	for key := range expected {
		if !found[key] {
			t.Fatalf("tx %d verify: expected key %s not found in DB", txIdx, key)
		}
	}
}

func makeRandomValue(rng *mrand.Rand, size int) []byte {
	val := make([]byte, size)
	// Fill with random data to ensure non-zero
	rand.Read(val)
	// Also write a recognizable header
	copy(val, []byte(fmt.Sprintf("val-%08d-", rng.Int())))
	return val
}
