package anystore

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func vstat(t *testing.T, coll Collection, name string) VectorIndexStats {
	t.Helper()
	st, err := coll.Stats(ctx)
	require.NoError(t, err)
	for _, vs := range st.VectorIndexes {
		if vs.Name == name {
			return vs
		}
	}
	t.Fatalf("vector index %q not present in stats", name)
	return VectorIndexStats{}
}

// TestVectorIndex_CompactManual churns an index with deletes and replaces, then
// compacts and asserts the tombstones are reclaimed, the graph is dense, and
// search still serves the live set correctly.
func TestVectorIndex_CompactManual(t *testing.T) {
	const (
		n   = 600
		dim = 16
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 64},
	}))

	vecs := vrand(n, dim, 7)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	// Churn: delete every 3rd, replace every 3rd (i%3==1) onto a fresh vector.
	live := make(map[int][]float32, n)
	for i := range vecs {
		live[i] = vecs[i]
	}
	repl := vrand(n, dim, 71)
	for i := 0; i < n; i++ {
		switch i % 3 {
		case 0:
			require.NoError(t, coll.DeleteId(ctx, i))
			delete(live, i)
		case 1:
			require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(vecDocJSON(i, repl[i]))))
			live[i] = repl[i]
		}
	}

	pre := vstat(t, coll, "emb")
	require.Greater(t, pre.DeletedCount, 0, "expected tombstones before compaction")
	require.Greater(t, pre.NodeCount, pre.LiveCount)
	require.Equal(t, len(live), pre.LiveCount)

	require.NoError(t, coll.CompactVectorIndex(ctx, "emb"))

	post := vstat(t, coll, "emb")
	assert.Equal(t, 0, post.DeletedCount, "no tombstones after compaction")
	assert.Equal(t, len(live), post.LiveCount, "live count must survive compaction")
	assert.Equal(t, len(live), post.NodeCount, "label space must be dense after compaction")
	assert.Less(t, post.SizeBytes, pre.SizeBytes, "compaction should reclaim storage")

	// Self-retrieval: each survivor's own vector retrieves itself.
	for i, v := range live {
		hits, herr := vsearch(coll, "v", v, 1, 64)
		require.NoError(t, herr)
		require.Len(t, hits, 1)
		assert.Equal(t, idBytesOf(i), hits[0].DocId, "self-retrieval mismatch for doc %d", i)
	}
	// Deleted docs never reappear.
	for i := 0; i < n; i++ {
		if _, ok := live[i]; ok {
			continue
		}
		hits, herr := vsearch(coll, "v", vecs[i], 5, 64)
		require.NoError(t, herr)
		for _, h := range hits {
			assert.NotEqual(t, idBytesOf(i), h.DocId, "deleted doc %d leaked", i)
		}
	}
}

// TestVectorIndex_CompactMovesRootAndDetects checks the cross-process plumbing:
// compaction must move the index's namespace root pages, and the pre-compaction
// vectorIndex object must detect that (rootUnchanged == false) so a peer's
// reconcile reopens it with fresh handles instead of reading freed pages.
func TestVectorIndex_CompactMovesRootAndDetects(t *testing.T) {
	const dim = 16
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 64},
	}))
	vecs := vrand(300, dim, 7)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	for i := 0; i < 100; i++ { // create tombstones so compaction isn't a no-op
		require.NoError(t, coll.DeleteId(ctx, i))
	}

	c := coll.(*collection)
	oldVI := c.loadVectorIndexes()[0]
	oldRoot := oldVI.ix.MetaRoot()

	require.NoError(t, coll.CompactVectorIndex(ctx, "emb"))

	newVI := c.loadVectorIndexes()[0]
	require.NotEqual(t, oldRoot, newVI.ix.MetaRoot(), "compaction must move the meta root page")

	require.NoError(t, c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		assert.False(t, oldVI.rootUnchanged(tx, c.name), "stale object must detect the moved root")
		assert.True(t, newVI.rootUnchanged(tx, c.name), "fresh object must be current")
		return nil
	}))
}

// TestVectorIndex_CompactReopen verifies a compacted index persists correctly:
// a fresh handle reopening the file reads the rebuilt graph (no tombstones) and
// serves search correctly.
func TestVectorIndex_CompactReopen(t *testing.T) {
	const (
		n   = 400
		dim = 16
	)
	tmpDir := t.TempDir()
	fx := newFixturePath(t, tmpDir)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 64},
	}))
	vecs := vrand(n, dim, 7)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	for i := 0; i < n/2; i++ {
		require.NoError(t, coll.DeleteId(ctx, i))
	}
	require.NoError(t, coll.CompactVectorIndex(ctx, "emb"))
	require.NoError(t, fx.Close())

	db2, err := Open(ctx, filepath.Join(tmpDir, "any-store-test.db"), nil)
	require.NoError(t, err)
	defer db2.Close()
	coll2, err := db2.Collection(ctx, "docs")
	require.NoError(t, err)

	st := vstat(t, coll2, "emb")
	assert.Equal(t, 0, st.DeletedCount, "reopened compacted index has no tombstones")
	assert.Equal(t, n/2, st.LiveCount)
	assert.Equal(t, n/2, st.NodeCount)

	for i := n / 2; i < n; i++ {
		hits, herr := vsearch(coll2, "v", vecs[i], 1, 64)
		require.NoError(t, herr)
		require.Len(t, hits, 1)
		require.Equal(t, idBytesOf(i), hits[0].DocId, "survivor %d after reopen", i)
	}
	for i := 0; i < n/2; i++ {
		hits, herr := vsearch(coll2, "v", vecs[i], 3, 64)
		require.NoError(t, herr)
		for _, h := range hits {
			require.NotEqual(t, idBytesOf(i), h.DocId, "deleted doc %d survived reopen", i)
		}
	}
}

// TestVectorIndex_CompactAuto verifies CompactRatio fires a synchronous rebuild
// once tombstones reach the ratio, off the self-contained write that crossed it.
func TestVectorIndex_CompactAuto(t *testing.T) {
	const (
		n   = 400
		dim = 16
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 64, CompactRatio: 0.5},
	}))

	vecs := vrand(n, dim, 5)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	peakDeleted := 0
	for i := 0; i < n/2; i++ {
		require.NoError(t, coll.DeleteId(ctx, i))
		if d := vstat(t, coll, "emb").DeletedCount; d > peakDeleted {
			peakDeleted = d
		}
	}
	final := vstat(t, coll, "emb")
	t.Logf("auto-compact: live=%d deleted=%d node=%d (peak deleted=%d)",
		final.LiveCount, final.DeletedCount, final.NodeCount, peakDeleted)

	require.GreaterOrEqual(t, peakDeleted, 100, "threshold should have been approached")
	assert.Less(t, final.DeletedCount, peakDeleted, "auto-compaction should have reset tombstones")
	assert.Less(t, final.NodeCount, n, "node space should be densified below the original allocation")
	assert.Equal(t, final.LiveCount, n/2)

	// Search still correct after the in-flight rebuilds.
	hits, err := vsearch(coll, "v", vecs[n-1], 1, 64)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, idBytesOf(n-1), hits[0].DocId)
}

// TestVectorIndex_CompactAuto_BulkDelete verifies CompactRatio also fires after a
// query-based bulk delete (coll.Find(...).Delete()), not just the single-doc
// mutators — the tombstones a bulk delete creates are reclaimed once it commits.
func TestVectorIndex_CompactAuto_BulkDelete(t *testing.T) {
	const (
		n   = 400
		dim = 16
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 64, CompactRatio: 0.5},
	}))
	vecs := vrand(n, dim, 13)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	// One bulk delete of half the corpus — well past the 0.5 ratio — in a single
	// self-contained write. Auto-compaction must reclaim the tombstones it creates.
	res, err := coll.Find(`{"id":{"$lt":200}}`).Delete(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 200, res.Modified)

	final := vstat(t, coll, "emb")
	t.Logf("bulk-delete auto-compact: live=%d deleted=%d node=%d",
		final.LiveCount, final.DeletedCount, final.NodeCount)
	assert.Equal(t, 200, final.LiveCount)
	assert.Equal(t, 0, final.DeletedCount, "bulk delete should have triggered auto-compaction")
	assert.LessOrEqual(t, final.NodeCount, 200, "node space should be densified to the live set")

	// Search still serves the surviving live set.
	hits, err := vsearch(coll, "v", vecs[n-1], 1, 64)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, idBytesOf(n-1), hits[0].DocId)
}

// TestVectorIndex_CompactAuto_DeferredInUserTx verifies auto-compaction does NOT
// run inside a caller-managed transaction (it needs its own committed tx), but
// does run on the next self-contained write.
func TestVectorIndex_CompactAuto_DeferredInUserTx(t *testing.T) {
	const (
		n   = 200
		dim = 16
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 64, CompactRatio: 0.5},
	}))
	vecs := vrand(n, dim, 9)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	// Delete 150/200 inside ONE user-managed tx — well past the 0.5 ratio.
	tx, err := coll.WriteTx(ctx)
	require.NoError(t, err)
	for i := 0; i < 150; i++ {
		require.NoError(t, coll.DeleteId(tx.Context(), i))
	}
	require.NoError(t, tx.Commit())

	inTx := vstat(t, coll, "emb")
	require.Greater(t, inTx.DeletedCount, 100,
		"auto-compaction must be deferred inside a user tx (tombstones should remain)")

	// A self-contained write now triggers the deferred compaction.
	require.NoError(t, coll.DeleteId(ctx, 150))
	after := vstat(t, coll, "emb")
	assert.Less(t, after.DeletedCount, inTx.DeletedCount, "self-contained write should compact")
	assert.Less(t, after.NodeCount, n)
}

// A compaction inside a caller-managed tx that rolls back must restore the
// pre-compaction handle: the rollback reverts the namespace recreation and
// frees the compacted roots, so a published compacted handle would fail every
// subsequent vector op with "btree: key not found" until reopen.
func TestVectorIndex_CompactAmbientRollback_RestoresHandle(t *testing.T) {
	const (
		n   = 200
		dim = 8
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 64},
	}))
	vecs := vrand(n, dim, 7)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	for i := 0; i < n; i += 3 {
		require.NoError(t, coll.DeleteId(ctx, i))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.CompactVectorIndex(tx.Context(), "emb"))
	require.NoError(t, tx.Rollback())

	// The restored handle must serve both sides of the index.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(n+1, vecs[1]))))
	hits, err := vsearch(coll, "v", vecs[1], 2, 64)
	require.NoError(t, err)
	require.Len(t, hits, 2)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// During an uncommitted ambient-tx compaction the compacted roots exist only
// in the writer's view; a concurrent reader must keep searching through the
// replaced handle (still valid in its committed snapshot) instead of failing
// on the recreated namespaces. After commit the compacted handle serves all.
func TestVectorIndex_CompactAmbientCommit_ConcurrentReader(t *testing.T) {
	const (
		n   = 200
		dim = 8
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 64},
	}))
	vecs := vrand(n, dim, 7)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	for i := 0; i < n; i += 3 {
		require.NoError(t, coll.DeleteId(ctx, i))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.CompactVectorIndex(tx.Context(), "emb"))

	// Window: concurrent reader searches through the pre-compaction handle.
	hits, err := vsearch(coll, "v", vecs[1], 2, 64)
	require.NoError(t, err)
	require.Len(t, hits, 2)
	assert.Equal(t, idBytesOf(1), hits[0].DocId)

	// Explain must agree with execution: the reader is index-served (via the
	// pre-compaction handle), so the index is listed.
	explain, err := coll.Find(fmt.Sprintf(`{"v":%s}`, vknnJSON(vecs[1], 2, 64))).Explain(ctx)
	require.NoError(t, err)
	listed := false
	for _, ie := range explain.Indexes {
		if ie.Name == "emb" {
			listed = true
		}
	}
	assert.True(t, listed, "Explain must list the index execution serves via the pre-compaction handle")

	// The compacting tx itself searches the compacted handle.
	fq := coll.Find(fmt.Sprintf(`{"v":%s}`, vknnJSON(vecs[1], 2, 64)))
	iterTx, err := fq.Iter(tx.Context())
	require.NoError(t, err)
	var gotTx int
	for iterTx.Next() {
		gotTx++
	}
	require.NoError(t, iterTx.Err())
	require.NoError(t, iterTx.Close())
	assert.Equal(t, 2, gotTx)

	require.NoError(t, tx.Commit())

	hits, err = vsearch(coll, "v", vecs[1], 2, 64)
	require.NoError(t, err)
	require.Len(t, hits, 2)
	assert.Equal(t, idBytesOf(1), hits[0].DocId)
	require.NoError(t, fx.IntegrityCheck(ctx))
}
