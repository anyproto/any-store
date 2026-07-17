package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// These tests guard the DDL visibility gate (visibleIndexes / forTx / the fts
// Search gate): createIndexes publishes new handles into the shared CoW
// snapshot at EXECUTION time — same-tx writes must maintain the new index —
// but their namespaces exist only in the creating write tx's uncommitted
// view. A concurrent tx that planned with such a handle would scan an empty
// namespace and return wrong results with no error (Count = 0 while Iter,
// picking another plan, finds the rows). The gate keeps pending handles
// invisible to every tx but the creator's until the commit publication.

func explainHasIndex(e Explain, name string) bool {
	for _, ie := range e.Indexes {
		if ie.Name == name {
			return true
		}
	}
	return false
}

func TestCreateIndexUncommitted_ConcurrentReaderConsistent(t *testing.T) {
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

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(tx.Context(), IndexInfo{Fields: []string{"name"}}))

	const filter = `{"name":"x"}`

	// Concurrent reader during the uncommitted window: Count and Iter agree
	// on the correct answer, and the pending index is not even a candidate.
	// The boost hint pins the planner to the index IF it is a candidate —
	// pre-gate that forced the empty-namespace seek and Count returned 0.
	hint := IndexHint{IndexName: "name", Boost: 1_000_000}
	cnt, err := coll.Find(filter).IndexHint(hint).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 30, cnt)
	ids, _ := collectIter(t, coll.Find(filter))
	assert.Len(t, ids, 30)
	explain, err := coll.Find(filter).Explain(ctx)
	require.NoError(t, err)
	assert.False(t, explainHasIndex(explain, "name"),
		"pending index must not be a candidate for a concurrent reader")

	// The creating tx keeps seeing its own index.
	cntTx, err := coll.Find(filter).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 30, cntTx)
	explainTx, err := coll.Find(filter).Explain(tx.Context())
	require.NoError(t, err)
	assert.True(t, explainHasIndex(explainTx, "name"),
		"creating tx must plan with its own uncommitted index")

	require.NoError(t, tx.Commit())

	// Committed: the gate lifts for everyone.
	cnt, err = coll.Find(filter).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 30, cnt)
	explain, err = coll.Find(filter).Explain(ctx)
	require.NoError(t, err)
	assert.True(t, explainHasIndex(explain, "name"))
}

func TestCreateFtsIndexUncommitted_ConcurrentReader(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"a","body":"london crash report"}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"b","body":"paris sunshine"}`)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(tx.Context(), IndexInfo{Kind: IndexKindFulltext, Fields: []string{"body"}}))

	// Concurrent reader: exactly the pre-DDL behavior — no full-text index.
	iter, err := coll.Find(`{"$text":{"$search":"london"}}`).Iter(ctx)
	if err == nil {
		for iter.Next() {
		}
		err = iter.Err()
		require.NoError(t, iter.Close())
	}
	assert.ErrorIs(t, err, ErrNoFulltextIndex)

	require.NoError(t, tx.Commit())

	ids, _ := collectIter(t, coll.Find(`{"$text":{"$search":"london"}}`))
	assert.Equal(t, []string{"a"}, ids)
}

func TestCreateVectorIndexUncommitted_ConcurrentReader(t *testing.T) {
	const dim = 8
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	vecs := vrand(20, dim, 3)
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

	// Concurrent reader: exactly the pre-DDL behavior — no vector index
	// (prev == nil, nothing to substitute).
	_, err = vsearch(coll, "v", vecs[0], 3, 32)
	assert.ErrorIs(t, err, ErrIndexNotFound)

	// The creating tx searches its own uncommitted index.
	fq := coll.Find(fmt.Sprintf(`{"v":%s}`, vknnJSON(vecs[7], 3, 32)))
	iterTx, err := fq.Iter(tx.Context())
	require.NoError(t, err)
	var gotTx int
	for iterTx.Next() {
		gotTx++
	}
	require.NoError(t, iterTx.Err())
	require.NoError(t, iterTx.Close())
	assert.Equal(t, 3, gotTx)

	require.NoError(t, tx.Commit())

	hits, err := vsearch(coll, "v", vecs[0], 3, 32)
	require.NoError(t, err)
	assert.Len(t, hits, 3)
}
