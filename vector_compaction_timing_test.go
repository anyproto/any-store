package anystore

import (
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/require"
)

func vecDocArena(a *anyenc.Arena, id int, vec []float32) *anyenc.Value {
	a.Reset()
	obj := a.NewObject()
	obj.Set("id", a.NewNumberInt(id))
	arr := a.NewArray()
	for i, f := range vec {
		arr.SetArrayItem(i, a.NewNumberFloat64(float64(f)))
	}
	obj.Set("v", arr)
	return obj
}

// TestVectorCompactTiming builds an index of ASV_COMPACT_N (default 100k) vectors
// at ASV_COMPACT_DIM (default 768), deletes ASV_COMPACT_DELPCT% (default 50) to
// create tombstones, then measures how long a full CompactVectorIndex takes.
// Skipped in -short. In-memory DB so the timing reflects rebuild CPU, not fsync.
func TestVectorCompactTiming(t *testing.T) {
	if testing.Short() {
		t.Skip("skip heavy compaction timing in -short")
	}
	// Small defaults so a plain `go test` is a fast smoke run; scale up via env
	// (e.g. ASV_COMPACT_N=100000 ASV_COMPACT_DIM=768) for a real measurement.
	n := envIntDefault("ASV_COMPACT_N", 2000)
	dim := envIntDefault("ASV_COMPACT_DIM", 128)
	delPct := envIntDefault("ASV_COMPACT_DELPCT", 50)

	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorCosine, EfSearch: 64},
	}))

	vecs := vrand(n, dim, 42)
	a := &anyenc.Arena{}

	tBuild := time.Now()
	tx, err := coll.WriteTx(ctx)
	require.NoError(t, err)
	for i, v := range vecs {
		require.NoError(t, coll.Insert(tx.Context(), vecDocArena(a, i, v)))
		if (i+1)%10000 == 0 {
			require.NoError(t, tx.Commit())
			tx, err = coll.WriteTx(ctx)
			require.NoError(t, err)
		}
	}
	require.NoError(t, tx.Commit())
	buildDur := time.Since(tBuild)

	// Delete delPct% of docs to accumulate tombstones (batched; no auto-compaction
	// — CompactRatio is unset and these run inside an explicit tx).
	ndel := n * delPct / 100
	tx, err = coll.WriteTx(ctx)
	require.NoError(t, err)
	for i := 0; i < ndel; i++ {
		require.NoError(t, coll.DeleteId(tx.Context(), i))
		if (i+1)%10000 == 0 {
			require.NoError(t, tx.Commit())
			tx, err = coll.WriteTx(ctx)
			require.NoError(t, err)
		}
	}
	require.NoError(t, tx.Commit())

	pre := vstat(t, coll, "emb")
	require.Greater(t, pre.DeletedCount, 0)

	tComp := time.Now()
	require.NoError(t, coll.CompactVectorIndex(ctx, "emb"))
	compDur := time.Since(tComp)
	post := vstat(t, coll, "emb")
	require.Equal(t, 0, post.DeletedCount)

	t.Logf("build: %d vecs dim=%d in %s (%.0f ins/s)",
		n, dim, buildDur.Round(time.Millisecond), float64(n)/buildDur.Seconds())
	t.Logf("pre-compact:  live=%d deleted=%d node=%d  size=%.1f MB",
		pre.LiveCount, pre.DeletedCount, pre.NodeCount, float64(pre.SizeBytes)/1e6)
	t.Logf("COMPACTION:   rebuilt %d live (dropped %d tombstones) in %s  (%.0f nodes/s)",
		post.LiveCount, pre.DeletedCount, compDur.Round(time.Millisecond), float64(post.LiveCount)/compDur.Seconds())
	t.Logf("post-compact: live=%d deleted=%d node=%d  size=%.1f MB  (reclaimed %.1f MB)",
		post.LiveCount, post.DeletedCount, post.NodeCount, float64(post.SizeBytes)/1e6,
		float64(pre.SizeBytes-post.SizeBytes)/1e6)
}
