package anystore

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
)

// ANYSTORE_TEST_PPROF=<addr> serves net/http/pprof for the run, e.g.
// ANYSTORE_TEST_PPROF=localhost:6066. Off by default: an unconditional
// listener collides between concurrent runs and hides the bind error.
func init() {
	addr := os.Getenv("ANYSTORE_TEST_PPROF")
	if addr == "" {
		return
	}
	go func() {
		if err := http.ListenAndServe(addr, nil); err != nil {
			fmt.Fprintf(os.Stderr, "pprof listener on %s: %v\n", addr, err)
		}
	}()
}

var ctx = context.Background()

func TestDb_CreateCollection(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	assert.NotNil(t, coll)

	cNames, err := fx.GetCollectionNames(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"test"}, cNames)

	_, err = fx.CreateCollection(ctx, "test")
	assert.ErrorIs(t, err, ErrCollectionExists)

	require.NoError(t, coll.Close())

	_, err = fx.CreateCollection(ctx, "test")
	assert.ErrorIs(t, err, ErrCollectionExists)
}

func TestDb_OpenCollection(t *testing.T) {
	t.Run("err not found", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.OpenCollection(ctx, "test")
		require.Nil(t, coll)
		require.ErrorIs(t, err, ErrCollectionNotFound)
	})
	t.Run("success", func(t *testing.T) {
		fx := newFixture(t)
		_, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		coll, err := fx.OpenCollection(ctx, "test")
		require.NoError(t, err)
		assert.NotNil(t, coll)
	})
	t.Run("with indexes", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		indexInfo := IndexInfo{Fields: []string{"a", "-b"}, Sparse: true, Unique: true}
		require.NoError(t, coll.EnsureIndex(ctx, indexInfo))
		require.NoError(t, coll.Close())

		coll, err = fx.OpenCollection(ctx, "test")
		require.NoError(t, err)
		assert.NotNil(t, coll)
		indexes := coll.GetIndexes()
		assert.Len(t, indexes, 1)
	})
}

func TestDb_GetCollectionNames(t *testing.T) {
	fx := newFixture(t)
	var collNames = []string{"c1", "c2", "c3"}
	for _, collName := range collNames {
		coll, err := fx.CreateCollection(ctx, collName)
		require.NoError(t, err)
		require.NoError(t, coll.Close())
	}
	names, err := fx.GetCollectionNames(ctx)
	require.NoError(t, err)
	assert.Equal(t, collNames, names)
}

func TestDb_Stats(t *testing.T) {
	skipIfInMemory(t, "Stats checks file sizes on disk")
	fx := newFixture(t)
	stats, err := fx.Stats(ctx)
	require.NoError(t, err)
	assert.Empty(t, 0, stats.IndexesCount)
	assert.Empty(t, 0, stats.CollectionsCount)
	assert.NotEmpty(t, stats.TotalSizeBytes)
	assert.NotEmpty(t, stats.DataSizeBytes)
}

func TestDb_QuickCheck(t *testing.T) {
	fx := newFixture(t)
	assert.NoError(t, fx.QuickCheck(ctx))
}

func TestDb_Flush(t *testing.T) {
	fx := newFixture(t)
	assert.NoError(t, fx.Flush(ctx, 0, FlushModeCheckpointPassive))
	assert.NoError(t, fx.Flush(ctx, 0, FlushModeCheckpointFull))
}

func TestDb_Backup(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.Collection(ctx, "coll")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"doc"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "doc":"a"}`), anyenc.MustParseJson(`{"id":2, "doc":"b"}`)))

	tmpDir, err := os.MkdirTemp("", "any-store-backup-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	require.NoError(t, fx.Backup(ctx, filepath.Join(tmpDir, "any-store-test.db")))

	fx2 := newFixturePath(t, tmpDir)
	coll2, err := fx2.Collection(ctx, "coll")
	require.NoError(t, err)
	assert.Len(t, coll2.GetIndexes(), 1)
	assertCollCount(t, coll2, 2)
}

func TestDb_Backup_OnlineDuringWrites(t *testing.T) {
	skipIfInMemory(t, "concurrent-writer test; file backend only")
	fx := newFixture(t)
	coll, err := fx.Collection(ctx, "coll")
	require.NoError(t, err)

	// Seed 500 docs.
	for i := 0; i < 500; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d, "val":"seed"}`, i))))
	}

	tmpDir, err := os.MkdirTemp("", "any-store-backup-online-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	backupPath := filepath.Join(tmpDir, "any-store-test.db")

	// Start backup and a concurrent writer.
	done := make(chan error, 1)
	go func() { done <- fx.Backup(ctx, backupPath) }()

	for i := 0; i < 50; i++ {
		_ = coll.UpsertOne(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d, "val":"updated"}`, i)))
	}

	require.NoError(t, <-done)

	fx2 := newFixturePath(t, tmpDir)
	coll2, err := fx2.Collection(ctx, "coll")
	require.NoError(t, err)
	assertCollCount(t, coll2, 500)
}

func TestDb_Close(t *testing.T) {
	t.Run("race", func(t *testing.T) {
		fx := newFixture(t, &Config{})

		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		var docs []*anyenc.Value
		for i := range 100 {
			docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(`{"id": %d, "value": %d}`, i, rand.Int())))
		}
		require.NoError(t, coll.Insert(ctx, docs...))
		var results = make(chan error, 2)
		go func() {
			// writing
			for {
				if pErr := coll.UpsertOne(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id": %d, "value": %d}`, rand.Int(), rand.Int()))); pErr != nil {
					results <- errors.Join(pErr, errors.New("upsertOne"))
					return
				}
			}
		}()

		go func() {
			// tx insert
			tx, tErr := coll.WriteTx(ctx)
			if tErr != nil {
				results <- errors.Join(tErr, errors.New("writeTx"))
				return
			}
			tErr = coll.Insert(tx.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id": "%s", "value": %d}`, anyenc.NewObjectID().Hex(), rand.Int())))
			if tErr != nil {
				results <- errors.Join(tErr, errors.New("insert tx"))
				return
			}
			if tErr = tx.Commit(); tErr != nil {
				results <- errors.Join(tErr, errors.New("insert tx commit"))
				return
			}
		}()

		time.Sleep(time.Second / 2)

		require.NoError(t, fx.Close())

		for range len(results) {
			rErr := <-results
			assert.True(t, errors.Is(rErr, btree.ErrClosed), rErr.Error())
		}
	})

}

func newFixture(t testing.TB, c ...*Config) *fixture {
	if os.Getenv("ANYSTORE_TEST_INMEMORY") == "1" {
		var conf *Config
		if len(c) != 0 {
			conf = c[0]
		}
		if conf == nil {
			conf = &Config{}
		}
		conf.InMemory = true
		db, err := Open(ctx, ":memory:", conf)
		require.NoError(t, err)
		fx := &fixture{DB: db, t: t}
		t.Cleanup(fx.finish)
		return fx
	}
	tmpDir, err := os.MkdirTemp("", "any-store-*")
	require.NoError(t, err)
	return newFixturePath(t, tmpDir, c...)
}

func newFixturePath(t testing.TB, tmpDir string, c ...*Config) *fixture {
	var conf *Config
	if len(c) != 0 {
		conf = c[0]
	}

	db, err := Open(ctx, filepath.Join(tmpDir, "any-store-test.db"), conf)
	require.NoError(t, err)

	fx := &fixture{
		DB:     db,
		tmpDir: tmpDir,
		t:      t,
	}
	t.Cleanup(fx.finish)
	return fx
}

type fixture struct {
	DB
	tmpDir string
	t      testing.TB
}

func (fx *fixture) finish() {
	closeErr := fx.Close()
	if !errors.Is(closeErr, ErrDBIsClosed) {
		require.NoError(fx.t, closeErr)
	}
	if fx.tmpDir != "" {
		_ = os.RemoveAll(fx.tmpDir)
	}
}

func TestInspectIndexSketch_BasicReturnsCount(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 50 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5),
		)))
	}

	insp, ok := fx.DB.(IndexSketchInspector)
	require.True(t, ok, "db must implement IndexSketchInspector")

	info, err := insp.InspectIndexSketch(ctx, "test", "a")
	require.NoError(t, err)
	assert.Equal(t, 1024, info.Size)
	assert.Equal(t, uint64(50), info.DocCount)
	require.Len(t, info.Buckets, 1024)

	var sum uint64
	for _, b := range info.Buckets {
		sum += b
	}
	assert.Equal(t, uint64(50), sum)
}

func TestInspectIndexSketch_UnknownIndexReturnsErrNotFound(t *testing.T) {
	fx := newFixture(t)
	_, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	insp := fx.DB.(IndexSketchInspector)
	_, err = insp.InspectIndexSketch(ctx, "test", "nonexistent")
	assert.ErrorIs(t, err, ErrIndexNotFound)
}

func TestInspectIndexSketch_AccumulatesOnInsertAndDelete(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 30 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%3),
		)))
	}

	insp := fx.DB.(IndexSketchInspector)
	info, err := insp.InspectIndexSketch(ctx, "test", "a")
	require.NoError(t, err)
	assert.Equal(t, uint64(30), info.DocCount)

	for i := range 10 {
		require.NoError(t, coll.DeleteId(ctx, i))
	}

	info, err = insp.InspectIndexSketch(ctx, "test", "a")
	require.NoError(t, err)
	assert.Equal(t, uint64(20), info.DocCount)
}

func TestOpen_AnyStoreV1File(t *testing.T) {
	dir := t.TempDir()

	t.Run("v1 file is reported explicitly", func(t *testing.T) {
		path := filepath.Join(dir, "v1.db")
		// The SQLite header every any-store v1 database starts with. Literal on
		// purpose: a rename or edit of v1Magic must fail here rather than pass
		// by comparing the constant against itself.
		page := make([]byte, 4096)
		copy(page, "SQLite format 3\x00")
		require.NoError(t, os.WriteFile(path, page, 0o600))

		db, err := Open(ctx, path, nil)
		require.ErrorIs(t, err, ErrV1Database)
		require.Nil(t, db)
	})

	t.Run("v2 file still opens", func(t *testing.T) {
		path := filepath.Join(dir, "v2.db")
		db, err := Open(ctx, path, nil)
		require.NoError(t, err)
		coll, err := db.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.Close())
		require.NoError(t, db.Close())

		db, err = Open(ctx, path, nil)
		require.NoError(t, err)
		require.NoError(t, db.Close())
	})

	t.Run("other garbage keeps the generic error", func(t *testing.T) {
		path := filepath.Join(dir, "garbage.db")
		require.NoError(t, os.WriteFile(path, []byte("this is not a database at all"), 0o600))

		_, err := Open(ctx, path, nil)
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrV1Database)
	})
}

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

// TestSavepointOverflowStress runs many iterations with random seeds,
// close/reopen cycles, and large document counts to reproduce zero-content
// overflow pages.
func TestSavepointOverflowStress(t *testing.T) {
	btree.SetDebugOverflowReadErrors(true)
	defer btree.SetDebugOverflowReadErrors(false)

	masterRng := rand.New(rand.NewSource(55443322)) // crash test's problematic seed
	numIterations := 100

	for i := 0; i < numIterations; i++ {
		seed := masterRng.Int63()
		t.Run(fmt.Sprintf("iter_%04d_seed_%d", i, seed), func(t *testing.T) {
			stressSavepointOverflow(t, seed)
		})
	}
}

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

func testSavepointOverflowWithSeed(t *testing.T, seed int64) {
	t.Helper()
	fx := newFixture(t)
	ctx := context.Background()

	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)

	rng := rand.New(rand.NewSource(seed))
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

	rng := rand.New(rand.NewSource(seed))
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

	rng := rand.New(rand.NewSource(seed))
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

func stressSavepointOverflow(t *testing.T, seed int64) {
	t.Helper()
	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "sp-stress-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmpDir) })
	dbPath := filepath.Join(tmpDir, "test.db")

	rng := rand.New(rand.NewSource(seed))
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

func testMultiIterationOverflow(t *testing.T, masterSeed int64) {
	t.Helper()
	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "multi-iter-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmpDir) })
	dbPath := filepath.Join(tmpDir, "test.db")

	rng := rand.New(rand.NewSource(masterSeed))
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

		iterRng := rand.New(rand.NewSource(iterSeed))

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

func snapshotState(m map[string]string) map[string]string {
	snap := make(map[string]string, len(m))
	for k, v := range m {
		snap[k] = v
	}
	return snap
}

// doCollOp performs a random collection operation with an overflow-sized document.
func doCollOp(t *testing.T, ctx context.Context, coll Collection, rng *rand.Rand, expected map[string]string, maxDocs int) {
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

func stressDoOp(t *testing.T, ctx context.Context, coll Collection, rng *rand.Rand, expected map[string]string, maxDocs int) {
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

func crashIteration(t *testing.T, ctx context.Context, db DB, coll Collection, rng *rand.Rand, expected map[string]string, maxDocs int) {
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

func multiIterDoOp(t *testing.T, ctx context.Context, coll Collection, rng *rand.Rand, expected map[string]string, maxDocs int) {
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

// These tests guard the create-in-flight gap in reconcileIndexSet: a read tx
// whose staleness verdict was baked at begin (an unconsumed schema-cookie
// delta — a peer commit, or a local commit racing beginRead between snapshot
// capture and the local-counter load) reconciles ALL registered handles
// against its own, older snapshot. A collection created after that snapshot
// has no catalog key there yet, so collectionVanished reports true via the
// keyNotFound branch and the staleness pass invalidates a live handle. The
// create commits fine; disk is correct; the cached handle is dead — every
// later op fails ErrCollectionClosed until the caller reopens.
//
// reconcileIndexSet guards rename-in-flight explicitly but not
// create-in-flight, though the same safety argument transfers: the creating
// writer holds the cross-process write lock, so any cookie bump a concurrent
// read pass consumes predates the creating tx's begin.

// The in-flight variant: the create sits inside an open write tx (handle
// registered mid-tx, catalog write uncommitted) while a plain read op begins
// with a stale local-counter cache and runs the full production
// ReadTx→checkStale path.
func TestReconcile_ReadTxSparesCreateInFlightHandle(t *testing.T) {
	skipIfInMemory(t, "staleness counters model cross-process commits; not applicable in-memory")
	fx := newFixture(t)
	dbi := fx.DB.(*db)

	seed, err := fx.CreateCollection(ctx, "seed")
	require.NoError(t, err)

	// Committed counters, to rewind against below.
	rtx, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	fcc, sc := rtx.DiskFileChangeCounter(), rtx.DiskSchemaCookie()
	require.NoError(t, rtx.Rollback())

	// Create in-flight: handle registered in openedCollections, catalog write
	// pending until wtx.Commit. The write tx's own begin ran checkStale with
	// counters in sync, so nothing is consumed yet.
	wtx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	fresh, err := fx.CreateCollection(wtx.Context(), "fresh")
	require.NoError(t, err)

	// Emulate an unconsumed cookie bump: the state a reader captures when its
	// beginRead races a commit (snapshot fields baked before the writer's
	// counter store lands) or when a peer process committed DDL.
	dbi.btreeDB.UpdateLocalCounters(fcc, sc-1)

	// Any read op now begins a read tx that sees IsSchemaStale and runs
	// reconcileIndexSet against a snapshot that predates the create.
	_, err = seed.Count(ctx)
	require.NoError(t, err)

	require.NoError(t, wtx.Commit())

	// The create committed; the handle must still be live.
	_, err = fresh.Count(ctx)
	require.NoError(t, err, "read-tx staleness pass invalidated a create-in-flight handle")
}

// The post-commit variant: the create commits normally, but the reader's
// snapshot predates it (begun before the create, checkStale preempted until
// after — the gap between BeginRead and checkStale inside db.ReadTx is a real
// preemption point). The committed handle must survive the pass.
func TestReconcile_ReadTxSparesFreshlyCommittedHandle(t *testing.T) {
	skipIfInMemory(t, "staleness counters model cross-process commits; not applicable in-memory")
	fx := newFixture(t)
	dbi := fx.DB.(*db)

	_, err := fx.CreateCollection(ctx, "seed")
	require.NoError(t, err)

	rtx, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	fcc, sc := rtx.DiskFileChangeCounter(), rtx.DiskSchemaCookie()
	require.NoError(t, rtx.Rollback())

	// Stale verdict baked at begin; snapshot predates the create below.
	dbi.btreeDB.UpdateLocalCounters(fcc, sc-1)
	reader, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	require.True(t, reader.IsSchemaStale())

	fresh, err := fx.CreateCollection(ctx, "fresh")
	require.NoError(t, err)

	// The reader resumes exactly where db.ReadTx would: checkStale on its own
	// pre-create snapshot.
	dbi.checkStale(reader)
	require.NoError(t, reader.Rollback())

	_, err = fresh.Count(ctx)
	require.NoError(t, err, "read-tx staleness pass invalidated a freshly committed handle")
}

// The rename analog: after Rename commits, the handle is registered under its
// NEW name, which a pre-rename snapshot cannot resolve (keyNotFound on the
// new key). Rename's publication raises the handle's visibility bound to the
// rename commit cookie so older-snapshot passes skip it.
func TestReconcile_ReadTxSparesFreshlyRenamedHandle(t *testing.T) {
	skipIfInMemory(t, "staleness counters model cross-process commits; not applicable in-memory")
	fx := newFixture(t)
	dbi := fx.DB.(*db)

	coll, err := fx.CreateCollection(ctx, "before")
	require.NoError(t, err)

	rtx, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	fcc, sc := rtx.DiskFileChangeCounter(), rtx.DiskSchemaCookie()
	require.NoError(t, rtx.Rollback())

	// Stale verdict baked at begin; snapshot predates the rename below.
	dbi.btreeDB.UpdateLocalCounters(fcc, sc-1)
	reader, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	require.True(t, reader.IsSchemaStale())

	require.NoError(t, coll.Rename(ctx, "after"))

	dbi.checkStale(reader)
	require.NoError(t, reader.Rollback())

	_, err = coll.Count(ctx)
	require.NoError(t, err, "read-tx staleness pass invalidated a freshly renamed handle")
}

// The consumption pin: a stale pass on a reader whose snapshot predates
// later DDL must consume only up to the SNAPSHOT counters — recording the
// raised/live values would mark the later DDL as reconciled though this pass
// never saw it, and no subsequent tx would ever reconcile it. The next begin
// must still report schema staleness and converge.
func TestCheckStale_ConsumesOnlySnapshotCounters(t *testing.T) {
	skipIfInMemory(t, "staleness counters model cross-process commits; not applicable in-memory")
	fx := newFixture(t)
	dbi := fx.DB.(*db)

	_, err := fx.CreateCollection(ctx, "seed")
	require.NoError(t, err)

	rtx, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	fcc, sc := rtx.DiskFileChangeCounter(), rtx.DiskSchemaCookie()
	require.NoError(t, rtx.Rollback())

	// Stale verdict baked at begin; snapshot predates the DDL below.
	// Assertions run after each tx is released so a failure cannot leave a
	// reader open (Close would block in the fixture cleanup).
	dbi.btreeDB.UpdateLocalCounters(fcc, sc-1)
	reader, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	readerStale := reader.IsSchemaStale()

	later, err := fx.CreateCollection(ctx, "later")
	require.NoError(t, err)

	// The pass runs with a snapshot that lacks the create: it must neither
	// invalidate the new handle nor consume the create's cookie bump.
	dbi.checkStale(reader)
	require.NoError(t, reader.Rollback())
	require.True(t, readerStale)

	next, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	nextStale := next.IsSchemaStale()
	// A full-vision pass converges.
	dbi.checkStale(next)
	require.NoError(t, next.Rollback())
	require.True(t, nextStale,
		"stale pass must not consume DDL its snapshot never contained")

	settled, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	settledStale := settled.IsSchemaStale()
	require.NoError(t, settled.Rollback())
	require.False(t, settledStale)

	_, err = later.Count(ctx)
	require.NoError(t, err)
}

// These tests guard the index-set publication ratchet: a staleness pass
// whose snapshot is older than the published sets must not republish from
// its older catalog. Rebuilt from that snapshot it would resurrect a
// dropped index (readers at current snapshots then plan against freed
// namespaces — silently wrong results) or evict a fresher one (a concurrent
// DropIndex fails ErrIndexNotFound; writes stop maintaining it).

func ratchetFixture(t *testing.T) (*fixture, *db, Collection) {
	t.Helper()
	skipIfInMemory(t, "staleness counters model cross-process commits; not applicable in-memory")
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	docs := make([]*anyenc.Value, 0, 10)
	for i := 0; i < 10; i++ {
		docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i)))
	}
	require.NoError(t, coll.Insert(ctx, docs...))
	return fx, fx.DB.(*db), coll
}

// A stale pass with a snapshot from when the index still existed must not
// resurrect it after DropIndex committed.
func TestReconcileRatchet_NoResurrectionAfterDrop(t *testing.T) {
	_, dbi, coll := ratchetFixture(t)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Reader begun while the index exists, with a stale-baked verdict.
	rtx, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	fcc, sc := rtx.DiskFileChangeCounter(), rtx.DiskSchemaCookie()
	require.NoError(t, rtx.Rollback())
	dbi.btreeDB.UpdateLocalCounters(fcc, sc-1)
	reader, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)

	require.NoError(t, coll.DropIndex(ctx, "a"))

	dbi.checkStale(reader)
	require.NoError(t, reader.Rollback())

	require.Empty(t, coll.(*collection).loadIndexes(),
		"stale pass resurrected a dropped index into the live set")
	n, err := coll.Find(`{"a":{"$gte":0}}`).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 10, n)
}

// A stale pass with a snapshot predating CreateIndex must not evict the
// freshly created index from the live set.
func TestReconcileRatchet_NoEvictionAfterCreate(t *testing.T) {
	_, dbi, coll := ratchetFixture(t)

	// Reader begun before the index exists, with a stale-baked verdict.
	rtx, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	fcc, sc := rtx.DiskFileChangeCounter(), rtx.DiskSchemaCookie()
	require.NoError(t, rtx.Rollback())
	dbi.btreeDB.UpdateLocalCounters(fcc, sc-1)
	reader, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	dbi.checkStale(reader)
	require.NoError(t, reader.Rollback())

	require.Len(t, coll.(*collection).loadIndexes(), 1,
		"stale pass evicted a freshly created index from the live set")
	// The observed production symptom of the eviction: DropIndex not found.
	require.NoError(t, coll.DropIndex(ctx, "a"))
}

// A rolled-back DDL tx must leave the ratchet where it was: the bump happens
// only in the commit publication, which a rollback drops. A moved ratchet
// would make later same-cookie staleness passes skip reconciles for state
// that never committed.
func TestReconcileRatchet_RollbackLeavesRatchetUntouched(t *testing.T) {
	fx, dbi, coll := ratchetFixture(t)
	before := coll.(*collection).indexSetCookie

	wtx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(wtx.Context(), IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, wtx.Rollback())

	cc := coll.(*collection)
	require.Empty(t, cc.loadIndexes(), "rollback must restore the published set")
	require.Equal(t, before, cc.indexSetCookie, "rollback must not move the ratchet")

	// The unmoved ratchet must not block a genuine staleness reconcile at the
	// unchanged cookie (peer-bump emulation), and real DDL still works.
	rtx, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	fcc, sc := rtx.DiskFileChangeCounter(), rtx.DiskSchemaCookie()
	require.NoError(t, rtx.Rollback())
	dbi.btreeDB.UpdateLocalCounters(fcc, sc-1)
	n, err := coll.Find(`{"a":{"$gte":0}}`).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 10, n)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.DropIndex(ctx, "a"))
}
