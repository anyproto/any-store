package anystore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	mrand "math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/btree"
)

// TestSavepointOverflowCorruption replicates the crash test's savepoint-stress
// pattern at the anystore level with documents large enough to trigger overflow
// pages in the btree. This tests for Bug 9: zero-content overflow pages after
// a clean commit with savepoint rollback.
func TestSavepointOverflowCorruption(t *testing.T) {
	btree.SetDebugOverflowReadErrors(true)
	defer btree.SetDebugOverflowReadErrors(false)

	seeds := []int64{42, 12345, 55443322, 88776655, 99887766, 31337}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			testSavepointOverflowWithSeed(t, seed)
		})
	}
}

// TestSavepointOverflowConcurrent adds concurrent readers and background
// checkpoints to the overflow savepoint test, matching the crash test pattern.
func TestSavepointOverflowConcurrent(t *testing.T) {
	btree.SetDebugOverflowReadErrors(true)
	defer btree.SetDebugOverflowReadErrors(false)

	seeds := []int64{42, 12345, 55443322, 88776655}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			testSavepointOverflowConcurrent(t, seed)
		})
	}
}

// TestSavepointOverflowCloseReopen exercises the full crash-test pattern:
// savepoint stress → close → reopen → verify. This catches corruption that
// only manifests after checkpoint + WAL truncation during close.
func TestSavepointOverflowCloseReopen(t *testing.T) {
	btree.SetDebugOverflowReadErrors(true)
	defer btree.SetDebugOverflowReadErrors(false)

	seeds := []int64{42, 12345, 55443322, 88776655, 99887766, 31337, 11223344, 77665544}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			testSavepointOverflowCloseReopen(t, seed)
		})
	}
}

func testSavepointOverflowWithSeed(t *testing.T, seed int64) {
	t.Helper()
	fx := newFixture(t)
	ctx := context.Background()

	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)

	rng := mrand.New(mrand.NewSource(seed))
	expected := make(map[string]string)
	maxDocs := 30
	numTx := 80

	for txIdx := 0; txIdx < numTx; txIdx++ {
		outerTx, err := fx.WriteTx(ctx)
		require.NoError(t, err, "tx %d: WriteTx", txIdx)
		outerCtx := outerTx.Context()

		prevState := snapshotState(expected)

		outerOps := 1 + rng.Intn(5)
		for j := 0; j < outerOps; j++ {
			doCollOp(t, outerCtx, coll, rng, expected, maxDocs)
		}

		innerTx, err := fx.WriteTx(outerCtx)
		require.NoError(t, err, "tx %d: inner WriteTx", txIdx)
		innerCtx := innerTx.Context()

		innerPrev := snapshotState(expected)

		innerOps := 1 + rng.Intn(8)
		for j := 0; j < innerOps; j++ {
			doCollOp(t, innerCtx, coll, rng, expected, maxDocs)
		}

		if rng.Float64() < 0.3 {
			require.NoError(t, innerTx.Rollback(), "tx %d: inner rollback", txIdx)
			expected = innerPrev
		} else {
			require.NoError(t, innerTx.Commit(), "tx %d: inner commit", txIdx)
		}

		if rng.Float64() < 0.2 {
			for j := 0; j < rng.Intn(3); j++ {
				doCollOp(t, outerCtx, coll, rng, expected, maxDocs)
			}
		}

		if rng.Float64() < 0.1 {
			require.NoError(t, outerTx.Rollback(), "tx %d: outer rollback", txIdx)
			expected = prevState
		} else {
			require.NoError(t, outerTx.Commit(), "tx %d: outer commit", txIdx)
		}

		verifySavepointData(t, ctx, coll, expected, txIdx)
	}
}

func testSavepointOverflowConcurrent(t *testing.T, seed int64) {
	t.Helper()
	fx := newFixture(t)
	ctx := context.Background()

	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)

	rng := mrand.New(mrand.NewSource(seed))
	expected := make(map[string]string)
	maxDocs := 30

	var stop atomic.Bool
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for !stop.Load() {
				rtx, err := fx.ReadTx(ctx)
				if err != nil {
					continue
				}
				iter, err := coll.Find(nil).Iter(rtx.Context())
				if err == nil {
					for iter.Next() {
						_, _ = iter.Doc()
					}
					iter.Close()
				}
				_ = rtx.Commit()
			}
		}(r)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			_ = fx.Flush(ctx, 0, FlushModeCheckpointPassive)
			time.Sleep(time.Millisecond)
		}
	}()

	for txIdx := 0; txIdx < 80; txIdx++ {
		outerTx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		outerCtx := outerTx.Context()

		prevState := snapshotState(expected)

		outerOps := 1 + rng.Intn(5)
		for j := 0; j < outerOps; j++ {
			doCollOp(t, outerCtx, coll, rng, expected, maxDocs)
		}

		innerTx, err := fx.WriteTx(outerCtx)
		require.NoError(t, err)
		innerCtx := innerTx.Context()

		innerPrev := snapshotState(expected)

		innerOps := 1 + rng.Intn(8)
		for j := 0; j < innerOps; j++ {
			doCollOp(t, innerCtx, coll, rng, expected, maxDocs)
		}

		if rng.Float64() < 0.3 {
			require.NoError(t, innerTx.Rollback())
			expected = innerPrev
		} else {
			require.NoError(t, innerTx.Commit())
		}

		if rng.Float64() < 0.1 {
			require.NoError(t, outerTx.Rollback())
			expected = prevState
		} else {
			require.NoError(t, outerTx.Commit())
		}

		verifySavepointData(t, ctx, coll, expected, txIdx)
	}

	stop.Store(true)
	wg.Wait()
}

// testSavepointOverflowCloseReopen does savepoint stress, then closes and
// reopens the DB to verify data survives the checkpoint + WAL truncation cycle.
func testSavepointOverflowCloseReopen(t *testing.T, seed int64) {
	t.Helper()
	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "savepoint-overflow-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmpDir) })
	dbPath := filepath.Join(tmpDir, "test.db")

	rng := mrand.New(mrand.NewSource(seed))
	expected := make(map[string]string)
	maxDocs := 100
	numTx := 60

	// Phase 1: open DB, do savepoint stress
	db1, err := Open(ctx, dbPath, nil)
	require.NoError(t, err)

	coll, err := db1.CreateCollection(ctx, "docs")
	require.NoError(t, err)

	// Initial batch insert
	initialBatch := 20 + rng.Intn(30)
	for i := 0; i < initialBatch; i++ {
		doCollOp(t, ctx, coll, rng, expected, maxDocs)
	}

	// Background readers + flusher during savepoint stress
	var stop atomic.Bool
	var wg sync.WaitGroup

	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				rtx, err := db1.ReadTx(ctx)
				if err != nil {
					continue
				}
				iter, err := coll.Find(nil).Iter(rtx.Context())
				if err == nil {
					for iter.Next() {
						_, _ = iter.Doc()
					}
					iter.Close()
				}
				_ = rtx.Commit()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			_ = db1.Flush(ctx, 0, FlushModeCheckpointPassive)
			time.Sleep(time.Millisecond)
		}
	}()

	// Savepoint stress
	for txIdx := 0; txIdx < numTx; txIdx++ {
		outerTx, err := db1.WriteTx(ctx)
		require.NoError(t, err, "tx %d: WriteTx", txIdx)
		outerCtx := outerTx.Context()

		prevState := snapshotState(expected)

		// Outer operations
		outerOps := 1 + rng.Intn(10)
		for j := 0; j < outerOps; j++ {
			doCollOp(t, outerCtx, coll, rng, expected, maxDocs)
		}

		// Inner WriteTx (savepoint)
		innerTx, err := db1.WriteTx(outerCtx)
		require.NoError(t, err, "tx %d: inner WriteTx", txIdx)
		innerCtx := innerTx.Context()

		innerPrev := snapshotState(expected)

		innerOps := 1 + rng.Intn(10)
		for j := 0; j < innerOps; j++ {
			doCollOp(t, innerCtx, coll, rng, expected, maxDocs)
		}

		// 30% rollback inner
		if rng.Float64() < 0.3 {
			require.NoError(t, innerTx.Rollback(), "tx %d: inner rollback", txIdx)
			expected = innerPrev
		} else {
			require.NoError(t, innerTx.Commit(), "tx %d: inner commit", txIdx)
		}

		// Post-savepoint ops (20%)
		if rng.Float64() < 0.2 {
			postOps := rng.Intn(5)
			for j := 0; j < postOps; j++ {
				doCollOp(t, outerCtx, coll, rng, expected, maxDocs)
			}
		}

		// 10% rollback outer
		if rng.Float64() < 0.1 {
			require.NoError(t, outerTx.Rollback(), "tx %d: outer rollback", txIdx)
			expected = prevState
		} else {
			require.NoError(t, outerTx.Commit(), "tx %d: outer commit", txIdx)
		}
	}

	// Stop background goroutines
	stop.Store(true)
	wg.Wait()

	// Verify in-session before close
	verifySavepointData(t, ctx, coll, expected, -1)

	// Phase 2: close DB
	require.NoError(t, db1.Close())

	// Phase 3: reopen and verify
	db2, err := Open(ctx, dbPath, nil)
	require.NoError(t, err)
	defer db2.Close()

	coll2, err := db2.Collection(ctx, "docs")
	require.NoError(t, err)

	verifySavepointData(t, ctx, coll2, expected, -2)
}

// doCollOp performs a random collection operation with an overflow-sized document.
func doCollOp(t *testing.T, ctx context.Context, coll Collection, rng *mrand.Rand, expected map[string]string, maxDocs int) {
	t.Helper()
	id := fmt.Sprintf("doc-%06d", rng.Intn(maxDocs))

	if rng.Float64() < 0.25 {
		err := coll.DeleteId(ctx, id)
		if err == nil {
			delete(expected, id)
		}
		return
	}

	// Insert/update with large document (triggers overflow)
	dataSize := 1200 + rng.Intn(4000)
	data := make([]byte, dataSize)
	rand.Read(data)
	hexData := hex.EncodeToString(data[:16])

	arena := &anyenc.Arena{}
	obj := arena.NewObject()
	obj.Set("id", arena.NewString(id))
	obj.Set("data", arena.NewString(hex.EncodeToString(data)))
	obj.Set("hash", arena.NewString(hexData))

	err := coll.UpsertOne(ctx, obj)
	if err == nil {
		expected[id] = hexData
	}
}

func verifySavepointData(t *testing.T, ctx context.Context, coll Collection, expected map[string]string, txIdx int) {
	t.Helper()

	found := make(map[string]bool)
	iter, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err, "tx %d verify: Find", txIdx)
	defer iter.Close()

	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err, "tx %d verify: Doc", txIdx)

		id := doc.Value().GetStringBytes("id")
		hash := doc.Value().GetStringBytes("hash")

		idStr := string(id)
		found[idStr] = true

		expectedHash, exists := expected[idStr]
		if !exists {
			t.Fatalf("tx %d verify: unexpected doc %s in collection", txIdx, idStr)
		}

		if string(hash) != expectedHash {
			t.Fatalf("tx %d verify: doc %s hash mismatch: got=%s want=%s",
				txIdx, idStr, string(hash), expectedHash)
		}
	}

	for id := range expected {
		if !found[id] {
			t.Fatalf("tx %d verify: expected doc %s not found in collection", txIdx, id)
		}
	}
}

func snapshotState(m map[string]string) map[string]string {
	snap := make(map[string]string, len(m))
	for k, v := range m {
		snap[k] = v
	}
	return snap
}
