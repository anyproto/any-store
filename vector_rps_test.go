package anystore

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/require"
)

// TestVectorRPSAndSize reports operations/second and storage for a vector
// collection across three modes (no index, float32 index, int8 index), comparing
// per-transaction vs batched writes.
//
// The DB is file-backed. Set ASV_RPS_DIR to a REAL-disk directory to measure the
// true per-commit fsync cost (default os.TempDir(), which is often tmpfs/RAM and
// hides fsync). Tunable via ASV_RPS_N / ASV_RPS_OPS / ASV_RPS_DIM.
//
// Skipped in -short; it is a measurement, not an assertion.
func TestVectorRPSAndSize(t *testing.T) {
	if testing.Short() {
		t.Skip("measurement test")
	}
	dim := envIntDefault("ASV_RPS_DIM", 768)
	n := envIntDefault("ASV_RPS_N", 3000)
	ops := envIntDefault("ASV_RPS_OPS", 500)

	baseDir := os.Getenv("ASV_RPS_DIR")
	if baseDir == "" {
		baseDir = os.TempDir()
	}
	dir, err := os.MkdirTemp(baseDir, "asvrps")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	t.Logf("db dir: %s  (set ASV_RPS_DIR to real disk for true fsync cost)", dir)

	vecs := vrand(n+8*ops, dim, 1)
	doc := func(id int) *anyenc.Value { return anyenc.MustParseJson(vecDocJSON(id, vecs[id])) }

	db, err := Open(ctx, filepath.Join(dir, "rps.db"), nil)
	require.NoError(t, err)
	defer db.Close()

	rpsPerTx := func(fn func(c context.Context, i int)) float64 {
		t0 := time.Now()
		for i := 0; i < ops; i++ {
			fn(ctx, i)
		}
		return float64(ops) / time.Since(t0).Seconds()
	}
	rpsBatched := func(fn func(c context.Context, i int)) float64 {
		tx, err := db.WriteTx(ctx)
		require.NoError(t, err)
		t0 := time.Now()
		for i := 0; i < ops; i++ {
			fn(tx.Context(), i)
		}
		require.NoError(t, tx.Commit())
		return float64(ops) / time.Since(t0).Seconds()
	}

	insertOp := func(c Collection, start int) func(context.Context, int) {
		return func(cx context.Context, i int) { require.NoError(t, c.Insert(cx, doc(start+i))) }
	}
	updateOp := func(c Collection, start int) func(context.Context, int) {
		return func(cx context.Context, i int) {
			require.NoError(t, c.UpsertOne(cx, anyenc.MustParseJson(vecDocJSON(start+i, vecs[n+4*ops+start+i]))))
		}
	}
	deleteOp := func(c Collection, start int) func(context.Context, int) {
		return func(cx context.Context, i int) { _ = c.DeleteId(cx, start+i) }
	}

	build := func(name string, withIndex bool, q VectorQuantization) Collection {
		c, err := db.CreateCollection(ctx, name)
		require.NoError(t, err)
		if withIndex {
			require.NoError(t, c.CreateIndex(ctx, IndexInfo{
				Name: "emb", Kind: IndexKindVector,
				Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorCosine, EfSearch: 64, Quantization: q},
			}))
		}
		tx, _ := db.WriteTx(ctx)
		for i := 0; i < n; i++ {
			require.NoError(t, c.Insert(tx.Context(), doc(i)))
		}
		require.NoError(t, tx.Commit())
		return c
	}

	report := func(label string, c Collection, withIndex bool) {
		t.Logf("--- %s ---", label)
		t.Logf("  insert  per-tx %8.0f /s   batched %9.0f /s", rpsPerTx(insertOp(c, n)), rpsBatched(insertOp(c, n+ops)))
		t.Logf("  update  per-tx %8.0f /s   batched %9.0f /s", rpsPerTx(updateOp(c, 0)), rpsBatched(updateOp(c, ops)))
		t.Logf("  delete  per-tx %8.0f /s   batched %9.0f /s", rpsPerTx(deleteOp(c, 2*ops)), rpsBatched(deleteOp(c, 3*ops)))
		if withIndex {
			// flush the WAL so the query reads from the main db file, not a WAL
			// overlay bloated by the preceding inserts/updates/deletes.
			require.NoError(t, db.Flush(ctx, 0, FlushModeCheckpointTruncate))
			t0 := time.Now()
			for i := 0; i < ops; i++ {
				_, err := c.VectorSearch(ctx, "emb", vecs[i%n], 10, 0)
				require.NoError(t, err)
			}
			t.Logf("  search         %8.0f /s", float64(ops)/time.Since(t0).Seconds())
		}
	}

	plain := build("plain", false, 0)
	report("WITHOUT index", plain, false)
	f32 := build("vec_f32", true, VectorQuantNone)
	report("WITH index (float32)", f32, true)
	i8 := build("vec_i8", true, VectorQuantInt8)
	report("WITH index (int8)", i8, true)

	mib := func(b int) float64 { return float64(b) / (1 << 20) }
	sF, _ := f32.Stats(ctx)
	sI, _ := i8.Stats(ctx)
	t.Logf("=== storage (%d docs, dim %d) ===", sF.DocCount, dim)
	t.Logf("  documents (no index)       : %6.1f MiB", mib(sF.DocsSizeBytes))
	t.Logf("  vector index float32       : %6.1f MiB  (vec %.1f + graph %.1f + map %.1f)",
		mib(sF.VectorIndexes[0].SizeBytes), mib(sF.VectorIndexes[0].VectorBytes), mib(sF.VectorIndexes[0].GraphBytes), mib(sF.VectorIndexes[0].MappingBytes))
	t.Logf("  vector index int8          : %6.1f MiB  (vec %.1f + graph %.1f + map %.1f)",
		mib(sI.VectorIndexes[0].SizeBytes), mib(sI.VectorIndexes[0].VectorBytes), mib(sI.VectorIndexes[0].GraphBytes), mib(sI.VectorIndexes[0].MappingBytes))
	t.Logf("  total: docs+f32=%.1f MiB   docs+int8=%.1f MiB", mib(sF.DocsSizeBytes+sF.VectorIndexes[0].SizeBytes), mib(sF.DocsSizeBytes+sI.VectorIndexes[0].SizeBytes))
}

func envIntDefault(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}
