package anystore

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/require"
)

// TestVectorRPSAndSize reports operations/second and storage for a vector
// collection, comparing with vs without the index and per-transaction vs batched
// writes. File-backed (real durability: fsync per commit), dim 768.
//
// Skipped unless -run is given explicitly; it is a measurement, not an assertion.
func TestVectorRPSAndSize(t *testing.T) {
	if testing.Short() {
		t.Skip("measurement test")
	}
	const (
		dim = 768
		n   = 3000 // base docs
		ops = 500  // ops per measurement
	)
	vecs := vrand(n+8*ops, dim, 1)
	doc := func(id int) *anyenc.Value {
		return anyenc.MustParseJson(vecDocJSON(id, vecs[id]))
	}

	db, err := Open(ctx, filepath.Join(t.TempDir(), "rps.db"), nil)
	require.NoError(t, err)
	defer db.Close()

	rpsPerTx := func(fn func(c context.Context, i int)) float64 {
		t0 := time.Now()
		for i := 0; i < ops; i++ {
			fn(ctx, i)
		}
		return float64(ops) / time.Since(t0).Seconds()
	}
	rpsBatched := func(c Collection, fn func(c context.Context, i int)) float64 {
		tx, err := db.WriteTx(ctx)
		require.NoError(t, err)
		t0 := time.Now()
		for i := 0; i < ops; i++ {
			fn(tx.Context(), i)
		}
		require.NoError(t, tx.Commit())
		return float64(ops) / time.Since(t0).Seconds()
	}

	// helper closures parameterised by collection + starting id
	insertOp := func(c Collection, start int) func(context.Context, int) {
		return func(cx context.Context, i int) { require.NoError(t, c.Insert(cx, doc(start+i))) }
	}
	updateOp := func(c Collection, start int) func(context.Context, int) {
		return func(cx context.Context, i int) {
			// change the vector to force a real (re)index in the indexed case
			d := anyenc.MustParseJson(vecDocJSON(start+i, vecs[n+4*ops+start+i]))
			require.NoError(t, c.UpsertOne(cx, d))
		}
	}
	deleteOp := func(c Collection, start int) func(context.Context, int) {
		return func(cx context.Context, i int) { _ = c.DeleteId(cx, start+i) }
	}

	build := func(name string, withIndex bool) Collection {
		c, err := db.CreateCollection(ctx, name)
		require.NoError(t, err)
		if withIndex {
			require.NoError(t, c.CreateIndex(ctx, IndexInfo{
				Name: "emb", Kind: IndexKindVector,
				Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorCosine, EfSearch: 64},
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
		// id ranges: insert uses n.., update/delete use existing 0..4ops
		insPerTx := rpsPerTx(insertOp(c, n))
		insBatch := rpsBatched(c, insertOp(c, n+ops))
		updPerTx := rpsPerTx(updateOp(c, 0))
		updBatch := rpsBatched(c, updateOp(c, ops))
		delPerTx := rpsPerTx(deleteOp(c, 2*ops))
		delBatch := rpsBatched(c, deleteOp(c, 3*ops))

		t.Logf("--- %s ---", label)
		t.Logf("  insert  per-tx %8.0f /s   batched %9.0f /s", insPerTx, insBatch)
		t.Logf("  update  per-tx %8.0f /s   batched %9.0f /s", updPerTx, updBatch)
		t.Logf("  delete  per-tx %8.0f /s   batched %9.0f /s", delPerTx, delBatch)
		if withIndex {
			rtx, _ := db.WriteTx(ctx) // not needed; search uses its own tx
			_ = rtx.Rollback()
			t0 := time.Now()
			for i := 0; i < ops; i++ {
				_, err := c.VectorSearch(ctx, "emb", vecs[i%n], 10, 0)
				require.NoError(t, err)
			}
			t.Logf("  search         %8.0f /s", float64(ops)/time.Since(t0).Seconds())
		}
	}

	// ---- without index ----
	plain := build("plain", false)
	report("WITHOUT vector index", plain, false)

	// ---- with index ----
	vec := build("vec", true)
	report("WITH vector index", vec, true)

	// ---- storage ----
	st, err := vec.Stats(ctx)
	require.NoError(t, err)
	mib := func(b int) float64 { return float64(b) / (1 << 20) }
	t.Logf("=== storage (%d docs, dim %d) ===", st.DocCount, dim)
	t.Logf("  documents (no index)     : %6.1f MiB", mib(st.DocsSizeBytes))
	if len(st.VectorIndexes) > 0 {
		vi := st.VectorIndexes[0]
		t.Logf("  vector index             : %6.1f MiB  (vec %.1f + graph %.1f + map %.1f)",
			mib(vi.SizeBytes), mib(vi.VectorBytes), mib(vi.GraphBytes), mib(vi.MappingBytes))
	}
	t.Logf("  total with index         : %6.1f MiB", mib(st.DocsSizeBytes+st.VectorIndexesSizeBytes))
}
