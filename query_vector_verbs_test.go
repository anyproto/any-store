package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// A $knn clause must denote the same document set on every verb. Before the
// shared compiler, only Iter ran vector detection: a valid clause was a
// silent no-op on Update/Delete (matched nothing as a literal filter), and an
// invalid one degraded to a scalar comparison. Now the write verbs compile
// through the same seam as Iter, and $k bounds the blast radius of a write to
// a number the caller typed.

func vectorVerbColl(t *testing.T) Collection {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "vec_verbs")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "v", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: 3, Metric: VectorL2, EfSearch: 64},
	}))
	for i := 0; i < 20; i++ {
		doc := fmt.Sprintf(`{"id":%d,"v":[%d,1,2],"n":0}`, i, i)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(doc)))
	}
	return coll
}

func vectorVerbClause(k int) string {
	return fmt.Sprintf(`{"v":{"$knn":{"$query":[3,1,2],"$k":%d}}}`, k)
}

func TestVectorClauseDelete_RemovesExactlyIterSelection(t *testing.T) {
	coll := vectorVerbColl(t)

	want := writeOrderIterIds(t, coll.Find(vectorVerbClause(5)))
	require.Len(t, want, 5, "test premise: $k bounds the denoted set")

	res, err := coll.Find(vectorVerbClause(5)).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, res.Modified, "a valid $knn clause must not be a silent no-op on Delete")

	survivors := writeOrderSurvivors(t, coll)
	for _, id := range want {
		assert.NotContains(t, survivors, id, "doc %d was in Iter's selection and must be deleted", id)
	}
	assert.Len(t, survivors, 15)
}

func TestVectorClauseUpdate_ModifiesExactlyIterSelection(t *testing.T) {
	coll := vectorVerbColl(t)

	want := writeOrderIterIds(t, coll.Find(vectorVerbClause(4)))
	require.Len(t, want, 4)

	res, err := coll.Find(vectorVerbClause(4)).Update(ctx, anyenc.MustParseJson(`{"$inc":{"n":1}}`))
	require.NoError(t, err)
	assert.Equal(t, 4, res.Modified, "a valid $knn clause must not be a silent no-op on Update")

	touched := writeOrderIterIds(t, coll.Find(`{"n":1}`))
	assert.ElementsMatch(t, want, touched)
}

func TestVectorClauseInvalid_WriteVerbsAgreeWithIter(t *testing.T) {
	// Whatever Iter decides about a clause on the vector field — reject it or
	// treat it as an ordinary (never-matching) filter — the write verbs must
	// decide identically, and the collection must stay intact either way.
	// Note on `{"v":{"$gt":…}}`: these docs store v as a PLAIN array, so an
	// ordering op compares element-wise (array semantics) — a deliberately
	// ordinary filter now, not an ANN shape. $gt:5000 exceeds every element,
	// so it legally matches nothing; Rule V (vector-vs-scalar) applies only to
	// packed TypeVectorF32 values and is pinned in query/filter_vector_test.go.
	coll := vectorVerbColl(t)
	for _, cond := range []string{
		`{"v":{"$gt":5000}}`,                     // ordering op on the vector field: ordinary filter, matches nothing
		`{"v":[1,2]}`,                            // wrong dim: not the ANN shape — ordinary filter
		`{"v":[3,1,2]}`,                          // dim-sized bare array: the legacy ANN spelling — hard error
		`{"v":{"$knn":{"$query":[1,2],"$k":5}}}`, // wrong-dim $query — hard error
	} {
		iterOK := true
		if it, ierr := coll.Find(cond).Iter(ctx); ierr != nil {
			iterOK = false
		} else {
			require.NoError(t, it.Close())
		}

		res, derr := coll.Find(cond).Delete(ctx)
		assert.Equal(t, iterOK, derr == nil,
			"cond=%s: Delete's accept/reject decision must match Iter's (iterOK=%v, deleteErr=%v)", cond, iterOK, derr)
		if derr == nil {
			assert.Zero(t, res.Modified, "cond=%s matches no documents", cond)
		}

		_, cerr := coll.Find(cond).Count(ctx)
		assert.Equal(t, iterOK, cerr == nil,
			"cond=%s: Count's accept/reject decision must match Iter's", cond)

		remaining, err := coll.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 20, remaining, "cond=%s: collection must be intact", cond)
	}
}

func TestVectorClauseCount_MatchesIter(t *testing.T) {
	coll := vectorVerbColl(t)
	for name, q := range map[string]func() Query{
		"plain":  func() Query { return coll.Find(vectorVerbClause(7)) },
		"limit":  func() Query { return coll.Find(vectorVerbClause(7)).Limit(5) },
		"offset": func() Query { return coll.Find(vectorVerbClause(7)).Offset(3).Limit(3) },
	} {
		t.Run(name, func(t *testing.T) {
			ids := writeOrderIterIds(t, q())
			count, err := q().Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, len(ids), count, "Count and Iter must denote the same set")
		})
	}
}

func TestVectorClauseUnboundedWrite_BoundedByK(t *testing.T) {
	// $k is the blast radius: an Update/Delete with no .Limit() mutates exactly
	// the k nearest — never "however many candidates the search yields". The
	// old ErrVectorWriteWithoutLimit guard is retired by construction: the
	// unbounded-delete query it rejected is unrepresentable under $knn.
	coll := vectorVerbColl(t)

	res, err := coll.Find(vectorVerbClause(5)).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, res.Modified, "Delete removes exactly $k documents")

	remaining, err := coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 15, remaining)

	res, err = coll.Find(vectorVerbClause(4)).Update(ctx, anyenc.MustParseJson(`{"$inc":{"n":1}}`))
	require.NoError(t, err)
	assert.Equal(t, 4, res.Modified, "Update visits exactly $k documents")
}
