package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

// GO-7510: the planner never reads IndexInfo.Sparse, so a negative predicate is
// answered by joining the sparse index table -- which holds only the documents
// that DO carry the field, the complement of what `!= true` matches.
func go7510Seed(t *testing.T, coll Collection) {
	t.Helper()
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "isUninstalled", Fields: []string{"isUninstalled"}, Sparse: true}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "resolvedLayout", Fields: []string{"resolvedLayout"}}))
	for i := range 24 {
		layout := "page"
		if i >= 12 {
			layout = "note"
		}
		j := fmt.Sprintf(`{"id":%d,"resolvedLayout":%q}`, i, layout)
		switch i {
		case 20, 21:
			j = fmt.Sprintf(`{"id":%d,"resolvedLayout":%q,"isUninstalled":true}`, i, layout)
		case 22, 23:
			j = fmt.Sprintf(`{"id":%d,"resolvedLayout":%q,"isUninstalled":false}`, i, layout)
		}
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(j)))
	}
}

func go7510Count(t *testing.T, coll Collection, cond string) int {
	t.Helper()
	c, err := coll.Find(cond).Count(ctx)
	require.NoError(t, err)
	iter, err := coll.Find(cond).Iter(ctx)
	require.NoError(t, err)
	n := 0
	for iter.Next() {
		n++
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	assert.Equalf(t, c, n, "Count and Iter disagree for %s", cond)
	return c
}

func TestGO7510_V1_SparseNotEqual(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "objects")
	require.NoError(t, err)
	go7510Seed(t, coll)

	assert.Equal(t, 22, go7510Count(t, coll, `{"isUninstalled":{"$ne":true}}`))
	assert.Equal(t, 10, go7510Count(t, coll, `{"resolvedLayout":"note","isUninstalled":{"$ne":true}}`))
	assert.Equal(t, 12, go7510Count(t, coll, `{"resolvedLayout":"page","isUninstalled":{"$ne":true}}`))
}

func TestGO7510_V1_NoDocumentCarriesTheField(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "objects")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "isUninstalled", Fields: []string{"isUninstalled"}, Sparse: true}))
	for i := range 24 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"resolvedLayout":"note"}`, i))))
	}
	assert.Equal(t, 24, go7510Count(t, coll, `{"isUninstalled":{"$ne":true}}`))
}

// Sort over a sparse field must not drop the documents the index never stored.
func TestGO7510_V1_SortOverSparseField(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "objects")
	require.NoError(t, err)
	go7510Seed(t, coll)

	iter, err := coll.Find(`{}`).Sort("isUninstalled").Iter(ctx)
	require.NoError(t, err)
	n := 0
	for iter.Next() {
		n++
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	assert.Equal(t, 24, n, "Sort over a sparse index dropped documents")
}

// The gate must not cost us the optimization it is guarding: a predicate that
// DOES guarantee presence must still be answered from the sparse index.
func TestGO7510_V1_SparseIndexStillUsedWhenPresenceGuaranteed(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "objects")
	require.NoError(t, err)
	go7510Seed(t, coll)

	for _, tc := range []struct {
		cond      string
		want      int
		useSparse bool
	}{
		{`{"isUninstalled":true}`, 2, true},                 // eq: present
		{`{"isUninstalled":false}`, 2, true},                // eq: present
		{`{"isUninstalled":{"$in":[true,false]}}`, 4, true}, // both present
		// $exists:true matches a field explicitly set to null, which a sparse
		// index also skips (index.go: sparse drops nil AND TypeNull), so it
		// cannot serve this either. Conservative, and still correct.
		{`{"isUninstalled":{"$exists":true}}`, 4, false},
		{`{"isUninstalled":{"$ne":true}}`, 22, false},      // matches absent
		{`{"isUninstalled":{"$exists":false}}`, 20, false}, // matches absent
	} {
		assert.Equalf(t, tc.want, go7510Count(t, coll, tc.cond), "count for %s", tc.cond)

		explain, err := coll.Find(tc.cond).Explain(ctx)
		require.NoError(t, err)
		usedSparse := false
		for _, idx := range explain.Indexes {
			if idx.Name == "isUninstalled" && idx.Used {
				usedSparse = true
			}
		}
		assert.Equalf(t, tc.useSparse, usedSparse,
			"sparse index usage for %s (sql: %s)", tc.cond, explain.Sql)
	}
}
