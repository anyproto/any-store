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

// TestSavepointOverflowMultiIteration simulates the crash test pattern:
// multiple iterations on the same DB, where some iterations "crash" (close
// mid-transaction) and the DB is reopened for recovery. This tests whether
// accumulated DB state from crash/recovery cycles can trigger Bug 9.
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
