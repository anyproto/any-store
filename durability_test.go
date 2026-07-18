package anystore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	mrand "math/rand"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
)

func TestRecovery_SentinelCleanShutdown(t *testing.T) {
	dir := t.TempDir()

	dbPath := filepath.Join(dir, "test.db")
	sentinelPath := dbPath + ".lock"

	config := &Config{
		Durability: DurabilityConfig{
			AutoFlush: true,
			IdleAfter: 100 * time.Millisecond,
			Sentinel:  true,
		},
	}

	ctx := context.Background()
	db, err := Open(ctx, dbPath, config)
	require.NoError(t, err)

	coll, err := db.CreateCollection(ctx, "test")
	require.NoError(t, err)

	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":"doc1", "data":"test"}`))
	require.NoError(t, err)

	_, err = os.Stat(sentinelPath)
	assert.NoError(t, err, "sentinel file should exist after write")

	time.Sleep(200 * time.Millisecond)

	err = db.Close()
	require.NoError(t, err)

	_, err = os.Stat(sentinelPath)
	assert.True(t, os.IsNotExist(err), "sentinel file should be removed after clean shutdown")

	db2, err := Open(ctx, dbPath, config)
	require.NoError(t, err)
	defer db2.Close()

	coll2, err := db2.OpenCollection(ctx, "test")
	require.NoError(t, err)

	iter, err := coll2.Find(`{"id":"doc1"}`).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()

	assert.True(t, iter.Next())
	doc, err := iter.Doc()
	require.NoError(t, err)
	assert.Equal(t, "test", string(doc.Value().GetStringBytes("data")))
}

func TestRecovery_SentinelDirtyShutdown(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	sentinelPath := dbPath + ".lock"
	config := &Config{
		Durability: DurabilityConfig{
			AutoFlush: true,
			IdleAfter: 10 * time.Minute,
			Sentinel:  true,
		},
	}

	ctx := context.Background()

	{
		db, err := Open(ctx, dbPath, config)
		require.NoError(t, err)

		coll, err := db.CreateCollection(ctx, "test")
		require.NoError(t, err)

		err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":"doc1", "data":"test"}`))
		require.NoError(t, err)

		_, err = os.Stat(sentinelPath)
		assert.NoError(t, err, "sentinel file should exist after write")

		db.Close()

		file, err := os.Create(sentinelPath)
		require.NoError(t, err)
		file.Close()
	}

	_, err := os.Stat(sentinelPath)
	require.NoError(t, err, "sentinel should exist to simulate dirty state")

	db2, err := Open(ctx, dbPath, config)
	require.NoError(t, err)
	defer db2.Close()

	coll2, err := db2.OpenCollection(ctx, "test")
	require.NoError(t, err)

	iter, err := coll2.Find(`{"id":"doc1"}`).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()

	assert.True(t, iter.Next())
	doc, err := iter.Doc()
	require.NoError(t, err)
	assert.Equal(t, "test", string(doc.Value().GetStringBytes("data")))
}

func TestRecovery_IdleFlushIntegration(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	config := &Config{
		Durability: DurabilityConfig{
			AutoFlush: true,
			IdleAfter: 100 * time.Millisecond,
		},
	}

	ctx := context.Background()
	db, err := Open(ctx, dbPath, config)
	require.NoError(t, err)
	defer db.Close()

	coll, err := db.CreateCollection(ctx, "test")
	require.NoError(t, err)

	doc := anyenc.MustParseJson(`{"id":1, "data":"test"}`)
	err = coll.Insert(ctx, doc)
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)
}

func TestRecovery_DisableSentinel(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	sentinelPath := dbPath + ".lock"

	config := &Config{
		Durability: DurabilityConfig{
			AutoFlush: true,
			IdleAfter: 100 * time.Millisecond,
		},
	}

	ctx := context.Background()
	db, err := Open(ctx, dbPath, config)
	require.NoError(t, err)
	defer db.Close()

	_, err = os.Stat(sentinelPath)
	assert.True(t, os.IsNotExist(err), "sentinel file should not exist when DisableSentinel is true")

	coll, err := db.CreateCollection(ctx, "test")
	require.NoError(t, err)

	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`))
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	_, err = os.Stat(sentinelPath)
	assert.True(t, os.IsNotExist(err), "sentinel file should not exist after flush when DisableSentinel is true")
}

func TestRecovery_FlushModes(t *testing.T) {
	testCases := []struct {
		name string
		mode FlushMode
	}{
		{"Passive", FlushModeCheckpointPassive},
		{"Full", FlushModeCheckpointFull},
		{"Restart", FlushModeCheckpointRestart},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "test.db")

			config := &Config{
				Durability: DurabilityConfig{
					AutoFlush: true,
					IdleAfter: 100 * time.Millisecond,
					FlushMode: tc.mode,
				},
			}

			ctx := context.Background()
			db, err := Open(ctx, dbPath, config)
			require.NoError(t, err)

			coll, err := db.CreateCollection(ctx, "test")
			require.NoError(t, err)

			err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`))
			require.NoError(t, err)

			time.Sleep(300 * time.Millisecond)

			err = db.Close()
			require.NoError(t, err)
		})
	}
}

func TestRecovery_ManualFlush(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	config := &Config{
		Durability: DurabilityConfig{
			AutoFlush: false,
		},
	}

	ctx := context.Background()
	db, err := Open(ctx, dbPath, config)
	require.NoError(t, err)
	defer db.Close()

	coll, err := db.CreateCollection(ctx, "test")
	require.NoError(t, err)

	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "data":"test"}`))
	require.NoError(t, err)

	time.Sleep(15 * time.Millisecond)

	err = db.Flush(ctx, 10*time.Millisecond, FlushModeCheckpointPassive)
	require.NoError(t, err)

	err = db.Flush(ctx, 10*time.Millisecond, FlushModeCheckpointPassive)
	require.NoError(t, err)
}

func TestRecovery_ForceFlushImmediatelyAfterWrite(t *testing.T) {
	dir := t.TempDir()

	dbPath := filepath.Join(dir, "test.db")
	db, err := Open(context.Background(), dbPath, &Config{
		Durability: DurabilityConfig{
			AutoFlush: true,
			IdleAfter: 10 * time.Second,
			FlushMode: FlushModeCheckpointPassive,
		},
	})
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	coll, err := db.CreateCollection(ctx, "test")
	require.NoError(t, err)

	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`))
	require.NoError(t, err)

	start := time.Now()
	err = db.Flush(ctx, 50*time.Millisecond, FlushModeCheckpointPassive)
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Less(t, elapsed, 100*time.Millisecond, "ForceFlush should complete quickly")
	assert.Greater(t, elapsed, 40*time.Millisecond, "ForceFlush should wait for idle time")
}

func TestRecovery_ForceFlushWithTimeout(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	config := &Config{
		Durability: DurabilityConfig{
			AutoFlush: true,
			IdleAfter: 10 * time.Second,
		},
	}

	ctx := context.Background()
	db, err := Open(ctx, dbPath, config)
	require.NoError(t, err)
	defer db.Close()

	coll, err := db.CreateCollection(ctx, "test")
	require.NoError(t, err)
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`))
	require.NoError(t, err)

	time.Sleep(15 * time.Millisecond)

	ctxTimeout, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	err = db.Flush(ctxTimeout, 10*time.Millisecond, FlushModeCheckpointPassive)
	assert.NoError(t, err)

	stopWrites := make(chan struct{})
	var counter atomic.Int64
	counter.Store(1)
	go func() {
		for {
			select {
			case <-stopWrites:
				return
			default:
				_ = coll.Insert(context.Background(), anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, counter.Add(1))))
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()
	time.Sleep(10 * time.Millisecond)

	ctxTimeout2, cancel2 := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel2()
	err = db.Flush(ctxTimeout2, 50*time.Millisecond, FlushModeCheckpointPassive)
	close(stopWrites)
	assert.Error(t, err)
	if err != nil {
		assert.Contains(t, err.Error(), "cancelled")
	}
}

// --- from savepoint_overflow_test.go ---

// TestSavepointOverflowCorruption replicates the crash test's savepoint-stress
// pattern at the anystore level with documents large enough to trigger overflow
// pages in the btree. This tests for zero-content overflow pages after
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

// --- from savepoint_overflow_stress_test.go ---

// TestSavepointOverflowStress runs many iterations with random seeds,
// close/reopen cycles, and large document counts to reproduce zero-content
// overflow pages.
func TestSavepointOverflowStress(t *testing.T) {
	btree.SetDebugOverflowReadErrors(true)
	defer btree.SetDebugOverflowReadErrors(false)

	masterRng := mrand.New(mrand.NewSource(55443322)) // crash test's problematic seed
	numIterations := 100

	for i := 0; i < numIterations; i++ {
		seed := masterRng.Int63()
		t.Run(fmt.Sprintf("iter_%04d_seed_%d", i, seed), func(t *testing.T) {
			stressSavepointOverflow(t, seed)
		})
	}
}

func stressSavepointOverflow(t *testing.T, seed int64) {
	t.Helper()
	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "sp-stress-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmpDir) })
	dbPath := filepath.Join(tmpDir, "test.db")

	rng := mrand.New(mrand.NewSource(seed))
	expected := make(map[string]string)
	maxDocs := 500
	numTx := 10 + rng.Intn(50) // 10-59 tx, matching crash test

	// Open DB
	db, err := Open(ctx, dbPath, nil)
	require.NoError(t, err)

	coll, err := db.CreateCollection(ctx, "docs")
	require.NoError(t, err)

	// Initial batch (like crash test)
	initialBatch := 50 + rng.Intn(100)
	for i := 0; i < initialBatch; i++ {
		stressDoOp(t, ctx, coll, rng, expected, maxDocs)
	}

	// Background readers
	var stop atomic.Bool
	var wg sync.WaitGroup
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				rtx, err := db.ReadTx(ctx)
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

	// Background flusher
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			_ = db.Flush(ctx, 0, FlushModeCheckpointPassive)
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// Savepoint stress
	for txIdx := 0; txIdx < numTx; txIdx++ {
		outerTx, err := db.WriteTx(ctx)
		if err != nil {
			continue
		}
		outerCtx := outerTx.Context()

		prevState := snapshotState(expected)

		outerOps := 1 + rng.Intn(10)
		for j := 0; j < outerOps; j++ {
			stressDoOp(t, outerCtx, coll, rng, expected, maxDocs)
		}

		innerTx, err := db.WriteTx(outerCtx)
		if err != nil {
			outerTx.Rollback()
			expected = prevState
			continue
		}
		innerCtx := innerTx.Context()

		innerPrev := snapshotState(expected)

		innerOps := 1 + rng.Intn(10)
		for j := 0; j < innerOps; j++ {
			stressDoOp(t, innerCtx, coll, rng, expected, maxDocs)
		}

		// 30% rollback inner
		if rng.Float64() < 0.3 {
			if err := innerTx.Rollback(); err != nil {
				t.Fatalf("tx %d: inner rollback: %v", txIdx, err)
			}
			expected = innerPrev
		} else {
			if err := innerTx.Commit(); err != nil {
				t.Fatalf("tx %d: inner commit: %v", txIdx, err)
			}
		}

		// 10% rollback outer
		if rng.Float64() < 0.1 {
			if err := outerTx.Rollback(); err != nil {
				t.Fatalf("tx %d: outer rollback: %v", txIdx, err)
			}
			expected = prevState
		} else {
			if err := outerTx.Commit(); err != nil {
				t.Fatalf("tx %d: outer commit: %v", txIdx, err)
			}
		}
	}

	stop.Store(true)
	wg.Wait()

	// Verify in-session
	stressVerify(t, ctx, coll, expected, "pre-close")

	// Close and reopen
	require.NoError(t, db.Close())

	db2, err := Open(ctx, dbPath, nil)
	require.NoError(t, err)
	defer db2.Close()

	coll2, err := db2.Collection(ctx, "docs")
	require.NoError(t, err)

	stressVerify(t, ctx, coll2, expected, "post-reopen")
}

func stressDoOp(t *testing.T, ctx context.Context, coll Collection, rng *mrand.Rand, expected map[string]string, maxDocs int) {
	t.Helper()
	id := fmt.Sprintf("doc-%06d", rng.Intn(maxDocs))

	// 10% delete, 20% insert (smaller doc), 40% upsert (large overflow), 30% upsert (medium)
	r := rng.Float64()
	if r < 0.10 {
		err := coll.DeleteId(ctx, id)
		if err == nil {
			delete(expected, id)
		}
		return
	}

	var dataSize int
	if r < 0.30 {
		// Small document (no overflow)
		dataSize = 50 + rng.Intn(200)
	} else if r < 0.70 {
		// Large document (triggers overflow)
		dataSize = 2000 + rng.Intn(6000)
	} else {
		// Medium document (may or may not overflow depending on key size)
		dataSize = 500 + rng.Intn(1500)
	}

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

func stressVerify(t *testing.T, ctx context.Context, coll Collection, expected map[string]string, label string) {
	t.Helper()

	found := make(map[string]bool)
	iter, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err, "%s verify: Find", label)
	defer iter.Close()

	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err, "%s verify: Doc", label)

		id := doc.Value().GetStringBytes("id")
		hash := doc.Value().GetStringBytes("hash")

		idStr := string(id)
		found[idStr] = true

		expectedHash, exists := expected[idStr]
		if !exists {
			t.Fatalf("%s verify: unexpected doc %s in collection", label, idStr)
		}

		if string(hash) != expectedHash {
			t.Fatalf("%s verify: doc %s hash mismatch: got=%s want=%s",
				label, idStr, string(hash), expectedHash)
		}
	}

	for id := range expected {
		if !found[id] {
			t.Fatalf("%s verify: expected doc %s not found in collection", label, id)
		}
	}
}

// --- from savepoint_overflow_multiiter_test.go ---

// TestSavepointOverflowMultiIteration simulates the crash test pattern:
// multiple iterations on the same DB, where some iterations "crash" (close
// mid-transaction) and the DB is reopened for recovery. This tests whether
// accumulated DB state from crash/recovery cycles can trigger zero-content
// overflow pages.
func TestSavepointOverflowMultiIteration(t *testing.T) {
	btree.SetDebugOverflowReadErrors(true)
	defer btree.SetDebugOverflowReadErrors(false)

	seeds := []int64{55443322, 42, 12345, 88776655, 99887766, 31337}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			testMultiIterationOverflow(t, seed)
		})
	}
}

func testMultiIterationOverflow(t *testing.T, masterSeed int64) {
	t.Helper()
	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "multi-iter-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmpDir) })
	dbPath := filepath.Join(tmpDir, "test.db")

	rng := mrand.New(mrand.NewSource(masterSeed))
	maxDocs := 500
	numIterations := 50

	// expected tracks the "verified ground truth" state.
	// After a crash, we rebuild expected from the DB.
	var expected map[string]string

	for iter := 0; iter < numIterations; iter++ {
		iterSeed := rng.Int63()
		crashProb := 0.1 // 10% chance of crash (close without commit)
		isCrash := rng.Float64() < crashProb && iter > 0

		db, err := Open(ctx, dbPath, nil)
		require.NoError(t, err, "iter %d: Open", iter)

		coll, err := db.Collection(ctx, "docs")
		require.NoError(t, err, "iter %d: Collection", iter)

		// After open, rebuild expected from DB if we lost state (first iter or after crash)
		if expected == nil {
			expected = readAllDocs(t, ctx, coll)
		}

		iterRng := mrand.New(mrand.NewSource(iterSeed))

		if isCrash {
			// Simulate a "crash": start a write tx, do some work, then
			// close the DB WITHOUT committing. This simulates SIGKILL.
			crashIteration(t, ctx, db, coll, iterRng, expected, maxDocs)
			// After crash close, expected is unknown — rebuild on next open
			expected = nil
			continue
		}

		// Normal iteration: savepoint stress with background readers/flusher
		var stop atomic.Bool
		var wg sync.WaitGroup

		for r := 0; r < 4; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for !stop.Load() {
					rtx, err := db.ReadTx(ctx)
					if err != nil {
						return
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
				_ = db.Flush(ctx, 0, FlushModeCheckpointPassive)
				time.Sleep(5 * time.Millisecond)
			}
		}()

		numTx := 5 + iterRng.Intn(20)
		for txIdx := 0; txIdx < numTx; txIdx++ {
			outerTx, err := db.WriteTx(ctx)
			if err != nil {
				continue
			}
			outerCtx := outerTx.Context()
			prevState := snapshotState(expected)

			outerOps := 1 + iterRng.Intn(10)
			for j := 0; j < outerOps; j++ {
				multiIterDoOp(t, outerCtx, coll, iterRng, expected, maxDocs)
			}

			innerTx, err := db.WriteTx(outerCtx)
			if err != nil {
				outerTx.Rollback()
				expected = prevState
				continue
			}
			innerCtx := innerTx.Context()
			innerPrev := snapshotState(expected)

			innerOps := 1 + iterRng.Intn(10)
			for j := 0; j < innerOps; j++ {
				multiIterDoOp(t, innerCtx, coll, iterRng, expected, maxDocs)
			}

			if iterRng.Float64() < 0.3 {
				require.NoError(t, innerTx.Rollback(), "iter %d tx %d: inner rollback", iter, txIdx)
				expected = innerPrev
			} else {
				require.NoError(t, innerTx.Commit(), "iter %d tx %d: inner commit", iter, txIdx)
			}

			if iterRng.Float64() < 0.1 {
				require.NoError(t, outerTx.Rollback(), "iter %d tx %d: outer rollback", iter, txIdx)
				expected = prevState
			} else {
				require.NoError(t, outerTx.Commit(), "iter %d tx %d: outer commit", iter, txIdx)
			}
		}

		stop.Store(true)
		wg.Wait()

		// Verify before close
		multiIterVerify(t, ctx, coll, expected, fmt.Sprintf("iter_%d_pre_close", iter))

		require.NoError(t, db.Close(), "iter %d: Close", iter)

		// Reopen and verify
		db2, err := Open(ctx, dbPath, nil)
		require.NoError(t, err, "iter %d: Reopen", iter)

		coll2, err := db2.Collection(ctx, "docs")
		require.NoError(t, err, "iter %d: Collection after reopen", iter)

		multiIterVerify(t, ctx, coll2, expected, fmt.Sprintf("iter_%d_post_reopen", iter))

		require.NoError(t, db2.Close(), "iter %d: Close2", iter)
	}
}

func crashIteration(t *testing.T, ctx context.Context, db DB, coll Collection, rng *mrand.Rand, expected map[string]string, maxDocs int) {
	t.Helper()

	// Start a tx, do some work, then rollback and close.
	// This exercises the WAL with dirty data that gets rolled back,
	// followed by close (checkpoint + WAL truncation).
	outerTx, err := db.WriteTx(ctx)
	if err != nil {
		db.Close()
		return
	}
	outerCtx := outerTx.Context()

	ops := 5 + rng.Intn(20)
	for j := 0; j < ops; j++ {
		id := fmt.Sprintf("doc-%06d", rng.Intn(maxDocs))
		if rng.Float64() < 0.2 {
			coll.DeleteId(outerCtx, id)
			continue
		}
		dataSize := 1000 + rng.Intn(5000)
		data := make([]byte, dataSize)
		rand.Read(data)
		arena := &anyenc.Arena{}
		obj := arena.NewObject()
		obj.Set("id", arena.NewString(id))
		obj.Set("data", arena.NewString(hex.EncodeToString(data)))
		obj.Set("hash", arena.NewString(hex.EncodeToString(data[:16])))
		coll.UpsertOne(outerCtx, obj)
	}

	// Maybe create a nested savepoint too
	if rng.Float64() < 0.5 {
		innerTx, err := db.WriteTx(outerCtx)
		if err == nil {
			innerCtx := innerTx.Context()
			for j := 0; j < rng.Intn(10); j++ {
				id := fmt.Sprintf("doc-%06d", rng.Intn(maxDocs))
				dataSize := 1000 + rng.Intn(5000)
				data := make([]byte, dataSize)
				rand.Read(data)
				arena := &anyenc.Arena{}
				obj := arena.NewObject()
				obj.Set("id", arena.NewString(id))
				obj.Set("data", arena.NewString(hex.EncodeToString(data)))
				obj.Set("hash", arena.NewString(hex.EncodeToString(data[:16])))
				coll.UpsertOne(innerCtx, obj)
			}
			// Rollback inner savepoint
			innerTx.Rollback()
		}
	}

	// Rollback the outer tx (simulating uncommitted work before crash)
	outerTx.Rollback()
	// Close normally — this exercises checkpoint + WAL truncation
	db.Close()
}

func readAllDocs(t *testing.T, ctx context.Context, coll Collection) map[string]string {
	t.Helper()
	result := make(map[string]string)
	iter, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err, "readAllDocs: Find")
	defer iter.Close()

	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err, "readAllDocs: Doc")
		id := string(doc.Value().GetStringBytes("id"))
		hash := string(doc.Value().GetStringBytes("hash"))
		result[id] = hash
	}
	return result
}

func multiIterDoOp(t *testing.T, ctx context.Context, coll Collection, rng *mrand.Rand, expected map[string]string, maxDocs int) {
	t.Helper()
	id := fmt.Sprintf("doc-%06d", rng.Intn(maxDocs))

	if rng.Float64() < 0.15 {
		err := coll.DeleteId(ctx, id)
		if err == nil {
			delete(expected, id)
		}
		return
	}

	dataSize := 1000 + rng.Intn(5000)
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

func multiIterVerify(t *testing.T, ctx context.Context, coll Collection, expected map[string]string, label string) {
	t.Helper()

	found := make(map[string]bool)
	iter, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err, "%s verify: Find", label)
	defer iter.Close()

	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err, "%s verify: Doc", label)

		id := doc.Value().GetStringBytes("id")
		hash := doc.Value().GetStringBytes("hash")

		idStr := string(id)
		found[idStr] = true

		expectedHash, exists := expected[idStr]
		if !exists {
			t.Fatalf("%s verify: unexpected doc %s in collection", label, idStr)
		}
		if string(hash) != expectedHash {
			t.Fatalf("%s verify: doc %s hash mismatch: got=%s want=%s",
				label, idStr, string(hash), expectedHash)
		}
	}

	for id := range expected {
		if !found[id] {
			t.Fatalf("%s verify: expected doc %s not found in collection", label, id)
		}
	}
}
