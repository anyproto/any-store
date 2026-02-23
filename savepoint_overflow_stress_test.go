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

// TestSavepointOverflowStress runs many iterations with random seeds,
// close/reopen cycles, and large document counts to reproduce Bug 9.
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
