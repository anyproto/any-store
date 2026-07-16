package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// A vector clause must denote the same document set on every verb. Before the
// shared compiler, only Iter ran detectVectorQuery: a valid clause was a
// silent no-op on Update/Delete (matched nothing as a literal filter), and an
// invalid one degraded to a scalar comparison. Now the write verbs compile
// through the same seam as Iter.

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

const vectorVerbClause = `{"v":[3,1,2]}`

func TestVectorClauseDelete_RemovesExactlyIterSelection(t *testing.T) {
	coll := vectorVerbColl(t)

	want := writeOrderIterIds(t, coll.Find(vectorVerbClause).Limit(5))
	require.Len(t, want, 5, "test premise: the ANN query returns a bounded candidate set")

	res, err := coll.Find(vectorVerbClause).Limit(5).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, res.Modified, "a valid vector clause must not be a silent no-op on Delete")

	survivors := writeOrderSurvivors(t, coll)
	for _, id := range want {
		assert.NotContains(t, survivors, id, "doc %d was in Iter's selection and must be deleted", id)
	}
	assert.Len(t, survivors, 15)
}

func TestVectorClauseUpdate_ModifiesExactlyIterSelection(t *testing.T) {
	coll := vectorVerbColl(t)

	want := writeOrderIterIds(t, coll.Find(vectorVerbClause).Limit(4))
	require.Len(t, want, 4)

	res, err := coll.Find(vectorVerbClause).Limit(4).Update(ctx, anyenc.MustParseJson(`{"$inc":{"n":1}}`))
	require.NoError(t, err)
	assert.Equal(t, 4, res.Modified, "a valid vector clause must not be a silent no-op on Update")

	touched := writeOrderIterIds(t, coll.Find(`{"n":1}`))
	assert.ElementsMatch(t, want, touched)
}

func TestVectorClauseInvalid_WriteVerbsAgreeWithIter(t *testing.T) {
	// Whatever Iter decides about a malformed vector query — reject or treat
	// as an ordinary (never-matching) filter — the write verbs must decide
	// identically, and the collection must stay intact either way.
	coll := vectorVerbColl(t)
	for _, cond := range []string{
		`{"v":{"$gt":1}}`,
		`{"v":[1,2]}`, // wrong dimension
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

		remaining, err := coll.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 20, remaining, "cond=%s: collection must be intact", cond)
	}
}
