package anystore

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// These tests guard the snapshot side of the DDL visibility gate: visibility
// is decided against the READER'S OWN snapshot (validFromCookie fast path +
// per-snapshot namespace resolution), so a read tx opened BEFORE a DDL commit
// that plans AFTER it behaves exactly as if the DDL never happened — for a
// local commit, a compaction, and a peer process's commit adopted by
// reconcile. A wall-clock publication flag cannot pass these: after the
// commit the flag is down for everyone, including readers whose snapshots
// predate the index.

// vsearchCtx is vsearch through an explicit context (an open tx's view).
func vsearchCtx(qctx context.Context, coll Collection, field string, q []float32, k, ef int) ([]vhit, error) {
	iter, err := coll.Find(fmt.Sprintf(`{%q:%s}`, field, vknnJSON(q, k, ef))).Iter(qctx)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []vhit
	for iter.Next() {
		d, derr := iter.Doc()
		if derr != nil {
			return nil, derr
		}
		out = append(out, vhit{DocId: d.Value().Get("id").MarshalTo(nil), Distance: iter.Distance()})
	}
	return out, iter.Err()
}

func countIterCtx(t *testing.T, qctx context.Context, q Query) int {
	t.Helper()
	iter, err := q.Iter(qctx)
	require.NoError(t, err)
	defer iter.Close()
	var n int
	for iter.Next() {
		n++
	}
	require.NoError(t, iter.Err())
	return n
}

func TestStaleReaderAcrossCreateIndexCommit(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		name := "other"
		if i < 30 {
			name = "x"
		}
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"name":%q}`, i, name))))
	}

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Commit()) }()

	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{Fields: []string{"name"}}))

	const filter = `{"name":"x"}`

	// The stale reader's snapshot has no index namespace: the boost hint pins
	// the planner to the index IF it is a candidate — served by wall-clock
	// state it seeks an empty namespace and Count returns 0 silently.
	hint := IndexHint{IndexName: "name", Boost: 1_000_000}
	cnt, err := coll.Find(filter).IndexHint(hint).Count(rtx.Context())
	require.NoError(t, err)
	assert.Equal(t, 30, cnt)
	assert.Equal(t, 30, countIterCtx(t, rtx.Context(), coll.Find(filter)))
	explain, err := coll.Find(filter).Explain(rtx.Context())
	require.NoError(t, err)
	assert.False(t, explainHasIndex(explain, "name"),
		"an index the snapshot predates must not be a candidate")

	// A fresh tx (snapshot cookie at the commit) uses the index.
	cnt, err = coll.Find(filter).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 30, cnt)
	explain, err = coll.Find(filter).Explain(ctx)
	require.NoError(t, err)
	assert.True(t, explainHasIndex(explain, "name"))
}

func TestStaleReaderAcrossFtsCreateCommit(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"a","body":"london crash report"}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"b","body":"paris sunshine"}`)))

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Commit()) }()

	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{Kind: IndexKindFulltext, Fields: []string{"body"}}))

	// Stale reader: exactly the pre-DDL behavior — no full-text index (served
	// by wall-clock state, its snapshot's meta doc-count reads as 0: silent
	// empty matches).
	iter, err := coll.Find(`{"$text":{"$search":"london"}}`).Iter(rtx.Context())
	if err == nil {
		for iter.Next() {
		}
		err = iter.Err()
		require.NoError(t, iter.Close())
	}
	assert.ErrorIs(t, err, ErrNoFulltextIndex)

	ids, _ := collectIter(t, coll.Find(`{"$text":{"$search":"london"}}`))
	assert.Equal(t, []string{"a"}, ids)
}

func TestStaleReaderAcrossVectorCreateCommit(t *testing.T) {
	const dim = 8
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	vecs := vrand(20, dim, 3)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Commit()) }()

	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name:   "emb",
		Kind:   IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 32},
	}))

	// Stale reader: the graph namespaces do not exist in its snapshot —
	// exactly the pre-DDL behavior.
	_, err = vsearchCtx(rtx.Context(), coll, "v", vecs[0], 3, 32)
	assert.ErrorIs(t, err, ErrIndexNotFound)

	hits, err := vsearch(coll, "v", vecs[0], 3, 32)
	require.NoError(t, err)
	assert.Len(t, hits, 3)
}

func TestStaleReaderAcrossVectorCompactCommit(t *testing.T) {
	const dim = 8
	const n = 40
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "emb",
		Kind:   IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 64},
	}))
	vecs := vrand(n, dim, 7)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	for i := 0; i < 15; i++ {
		require.NoError(t, coll.DeleteId(ctx, i))
	}

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Commit()) }()

	// The compaction re-roots the graph namespaces and its commit clears
	// prev, so the stale reader below is served by a transient handle opened
	// from its own snapshot — searching the identical pre-compaction graph,
	// it must return identical results.
	before, err := vsearchCtx(rtx.Context(), coll, "v", vecs[n-1], 5, 64)
	require.NoError(t, err)
	require.Len(t, before, 5)

	require.NoError(t, coll.CompactVectorIndex(ctx, "emb"))

	after, err := vsearchCtx(rtx.Context(), coll, "v", vecs[n-1], 5, 64)
	require.NoError(t, err)
	assert.Equal(t, before, after,
		"a reader's snapshot must serve identical results across a compaction commit")

	// A fresh tx searches the compacted graph.
	hits, err := vsearch(coll, "v", vecs[n-1], 5, 64)
	require.NoError(t, err)
	assert.Len(t, hits, 5)
}

// TestStaleReaderAcrossPeerCreateReconcile: a PEER process commits CreateIndex;
// the local reconcile (triggered by the schema-cookie bump on the next tx)
// adopts the handle into the shared CoW set while an older local read tx is
// still open. The old reader must not plan with the adopted index — its
// snapshot has no index namespace (silent Count=0 served by wall-clock state;
// a publication flag cannot mark reconciled peer handles at all).
func TestStaleReaderAcrossPeerCreateReconcile(t *testing.T) {
	if path := os.Getenv("IDXVIS_MP_PATH"); path != "" {
		idxVisMpChild(t, path)
		return
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "idxvis_mp.db")

	db, err := Open(ctx, path, nil)
	require.NoError(t, err)
	defer db.Close()
	coll, err := db.CreateCollection(ctx, "test")
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		name := "other"
		if i < 30 {
			name = "x"
		}
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"name":%q}`, i, name))))
	}

	rtx, err := db.ReadTx(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Commit()) }()

	// Peer process creates the index and commits.
	cmd := exec.Command(os.Args[0], "-test.run=^TestStaleReaderAcrossPeerCreateReconcile$", "-test.v=true")
	cmd.Env = append(os.Environ(), "IDXVIS_MP_PATH="+path)
	done := make(chan struct{})
	var out []byte
	var cerr error
	go func() { out, cerr = cmd.CombinedOutput(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("child timed out")
	}
	require.NoError(t, cerr, "child failed:\n%s", out)

	const filter = `{"name":"x"}`

	// A fresh local tx reconciles (cookie bump) and adopts the peer's index.
	explain, err := coll.Find(filter).Explain(ctx)
	require.NoError(t, err)
	require.True(t, explainHasIndex(explain, "name"),
		"fresh tx must adopt the peer's index via reconcile")

	// The old reader — now planning with the reconciled CoW set — must still
	// answer from its own snapshot.
	hint := IndexHint{IndexName: "name", Boost: 1_000_000}
	cnt, err := coll.Find(filter).IndexHint(hint).Count(rtx.Context())
	require.NoError(t, err)
	assert.Equal(t, 30, cnt)
	explain, err = coll.Find(filter).Explain(rtx.Context())
	require.NoError(t, err)
	assert.False(t, explainHasIndex(explain, "name"),
		"an index adopted from a peer must stay invisible to a reader whose snapshot predates it")
}

func idxVisMpChild(t *testing.T, path string) {
	db, err := Open(ctx, path, nil)
	require.NoError(t, err)
	defer db.Close()
	coll, err := db.OpenCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{Fields: []string{"name"}}))
}
