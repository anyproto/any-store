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

// A same-tx drop+recreate under one name can land the recreated tree on the
// freed old root's page number (freelist-first allocation), so root-page
// equality alone would admit the pending handle to a concurrent reader whose
// snapshot holds the OLD tree at that page — the catalog-identity half of the
// slow path must exclude it (the snapshot's row carries the old definition).
func TestConcurrentReaderAcrossDropRecreateSameTx(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{Name: "nm", Fields: []string{"a"}}))
	for i := 0; i < 200; i++ {
		a := "other"
		if i < 30 {
			a = "x"
		}
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%q,"b":"y%d"}`, i, a, i%7))))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.DropIndex(tx.Context(), "nm"))
	require.NoError(t, coll.CreateIndex(tx.Context(), IndexInfo{Name: "nm", Fields: []string{"b"}}))

	// Concurrent reader during the window: the pending fields-b handle must
	// not serve a fields-a query even if its recreated root reuses the freed
	// page number the reader's snapshot still maps to the fields-a tree.
	const filter = `{"a":"x"}`
	hint := IndexHint{IndexName: "nm", Boost: 1_000_000}
	cnt, err := coll.Find(filter).IndexHint(hint).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 30, cnt)
	explain, err := coll.Find(filter).Explain(ctx)
	require.NoError(t, err)
	assert.False(t, explainHasIndex(explain, "nm"),
		"a pending redefinition must not be a candidate for a concurrent reader")

	require.NoError(t, tx.Commit())

	cnt, err = coll.Find(`{"b":"y3"}`).Count(ctx)
	require.NoError(t, err)
	assert.NotZero(t, cnt)
	explain, err = coll.Find(`{"b":"y3"}`).Explain(ctx)
	require.NoError(t, err)
	assert.True(t, explainHasIndex(explain, "nm"))
}

// Fts flavor of the same hazard: the five recreated namespaces can reuse
// freed page numbers, and a partial or full root coincidence must never let
// a concurrent reader search old postings through the new handle's field
// configuration — the definition mismatch excludes it (ErrNoFulltextIndex).
func TestConcurrentReaderAcrossFtsDropRecreateSameTx(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{Name: "t", Kind: IndexKindFulltext, Fields: []string{"body"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"a","body":"london crash report","title":"weather"}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"b","body":"paris sunshine","title":"london"}`)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.DropIndex(tx.Context(), "t"))
	require.NoError(t, coll.CreateIndex(tx.Context(), IndexInfo{Name: "t", Kind: IndexKindFulltext, Fields: []string{"title"}}))

	iter, err := coll.Find(`{"$text":{"$search":"london"}}`).Iter(ctx)
	if err == nil {
		for iter.Next() {
		}
		err = iter.Err()
		require.NoError(t, iter.Close())
	}
	assert.ErrorIs(t, err, ErrNoFulltextIndex,
		"a pending fts redefinition must be invisible, never garbled results")

	require.NoError(t, tx.Commit())

	ids, _ := collectIter(t, coll.Find(`{"$text":{"$search":"london"}}`))
	assert.Equal(t, []string{"b"}, ids, "committed: the title index answers")
}

// A brute-force handle has no namespaces to resolve, but a stale reader whose
// snapshot contains the committed index must still be served: the slow path
// rebuilds from the snapshot's catalog row (metadata-only handle → scan).
// Restamp trigger: collection reopened after an unrelated cookie bump.
func TestStaleReaderBruteForceAfterReopen(t *testing.T) {
	const dim = 8
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "emb",
		Kind:   IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, Mode: VectorModeBruteForce},
	}))
	vecs := vrand(20, dim, 11)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	require.NoError(t, coll.Close())

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Commit()) }()

	// Unrelated schema commit bumps the cookie past the reader's snapshot.
	_, err = fx.CreateCollection(ctx, "other")
	require.NoError(t, err)

	// Reopen stamps the reloaded handles at the newer cookie.
	coll2, err := fx.OpenCollection(ctx, "docs")
	require.NoError(t, err)

	hits, err := vsearchCtx(rtx.Context(), coll2, "v", vecs[0], 3, 0)
	require.NoError(t, err, "a committed brute-force index must serve a reader its snapshot contains")
	assert.Len(t, hits, 3)
}

// A mid-tx collection reopen through an ambient write tx that already ran DDL
// sees that tx's own uncommitted index: the reloaded handle must be stamped
// for the COMMIT's cookie (init's SchemaChanged branch), or a concurrent
// reader at the begin cookie would seek a namespace that exists only in the
// writer's view — the phantom would even survive a rollback.
func TestAmbientReopenPendingRangeInvisible(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":"x%d"}`, i, i%5))))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(tx.Context(), IndexInfo{Name: "nm", Fields: []string{"a"}}))
	require.NoError(t, coll.Close())
	coll2, err := fx.OpenCollection(tx.Context(), "docs")
	require.NoError(t, err)

	// The ambient tx sees and uses its own index through the reloaded handle.
	explainTx, err := coll2.Find(`{"a":"x1"}`).Explain(tx.Context())
	require.NoError(t, err)
	assert.True(t, explainHasIndex(explainTx, "nm"))

	// A concurrent reader must not: correct count via scan, no candidate.
	cnt, err := coll2.Find(`{"a":"x1"}`).IndexHint(IndexHint{IndexName: "nm", Boost: 1_000_000}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, cnt)
	explain, err := coll2.Find(`{"a":"x1"}`).Explain(ctx)
	require.NoError(t, err)
	assert.False(t, explainHasIndex(explain, "nm"),
		"an index reloaded from the writer's uncommitted view must stay invisible to concurrent readers")

	require.NoError(t, tx.Rollback())

	// The rolled-back index never becomes visible.
	cnt, err = coll2.Find(`{"a":"x1"}`).IndexHint(IndexHint{IndexName: "nm", Boost: 1_000_000}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, cnt)
	explain, err = coll2.Find(`{"a":"x1"}`).Explain(ctx)
	require.NoError(t, err)
	assert.False(t, explainHasIndex(explain, "nm"))
}

// The vector flavor of the mid-tx reopen: writable-aware namespace
// resolution lets the reopen see the tx's own uncommitted graph namespaces,
// and init's SchemaChanged stamp keeps the reloaded handle invisible to
// concurrent readers — the phantom never escapes, even across a rollback.
func TestAmbientReopenPendingVectorInvisible(t *testing.T) {
	const dim = 8
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	vecs := vrand(20, dim, 5)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(tx.Context(), IndexInfo{
		Name:   "emb",
		Kind:   IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 32},
	}))
	require.NoError(t, coll.Close())
	coll2, err := fx.OpenCollection(tx.Context(), "docs")
	require.NoError(t, err)

	// The ambient tx sees its own index through the reloaded handle.
	hitsTx, err := vsearchCtx(tx.Context(), coll2, "v", vecs[7], 3, 32)
	require.NoError(t, err)
	assert.Len(t, hitsTx, 3)

	// A concurrent reader must not: the graph exists only in the writer's
	// uncommitted view.
	_, err = vsearchCtx(ctx, coll2, "v", vecs[0], 3, 32)
	assert.ErrorIs(t, err, ErrIndexNotFound)

	require.NoError(t, tx.Rollback())

	// The rolled-back index never becomes visible.
	_, err = vsearchCtx(ctx, coll2, "v", vecs[0], 3, 32)
	assert.ErrorIs(t, err, ErrIndexNotFound)
}

// A stale reader on a redefined index (drop+recreate same name, different
// definition) fails noisy: its snapshot's catalog row no longer matches the
// current handle, and old data must never be served under a new definition
// (the SQLITE_SCHEMA posture).
func TestStaleReaderAcrossVectorDropRecreateDef(t *testing.T) {
	const dim = 8
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "emb",
		Kind:   IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 32},
	}))
	vecs := vrand(20, dim, 13)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Commit()) }()

	require.NoError(t, coll.DropIndex(ctx, "emb"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "emb",
		Kind:   IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, Mode: VectorModeBruteForce},
	}))

	_, err = vsearchCtx(rtx.Context(), coll, "v", vecs[0], 3, 32)
	assert.ErrorIs(t, err, ErrIndexNotFound,
		"a redefined index must fail noisy for a reader on the old definition's snapshot")

	hits, err := vsearch(coll, "v", vecs[0], 3, 0)
	require.NoError(t, err)
	assert.Len(t, hits, 3)
}

// A same-definition drop+recreate moves the roots; the stale reader is served
// by the transient rebuild from its OWN snapshot and must see exactly the
// results it saw before the DDL — even if the recreated roots collide with
// freed page numbers (the rebuild never trusts the handle's roots).
func TestStaleReaderAcrossVectorSameDefRecreate(t *testing.T) {
	const dim = 8
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	info := IndexInfo{
		Name:   "emb",
		Kind:   IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 64},
	}
	require.NoError(t, coll.EnsureIndex(ctx, info))
	vecs := vrand(30, dim, 17)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Commit()) }()

	before, err := vsearchCtx(rtx.Context(), coll, "v", vecs[29], 5, 64)
	require.NoError(t, err)
	require.Len(t, before, 5)

	require.NoError(t, coll.DropIndex(ctx, "emb"))
	require.NoError(t, coll.EnsureIndex(ctx, info))

	after, err := vsearchCtx(rtx.Context(), coll, "v", vecs[29], 5, 64)
	require.NoError(t, err)
	assert.Equal(t, before, after,
		"a reader's snapshot must serve identical results across a same-definition recreate")
}
