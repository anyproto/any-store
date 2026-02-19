/*
Index/Planner tests inspired by SQLite: where.test, where9.test

Test scenario:
Tests $or with mixed indexed/non-indexed fields and $exists with sparse
indexes. These require imperative logic (multi-collection comparison,
assertIndexLen).

These tests verify our custom index and query planner implementation.
While inspired by SQLite test patterns, our system has a different
architecture (document-oriented with weight-based planner vs SQL VDBE).
*/
package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

// setupComplexFilterColl creates a collection with 100 docs where a=i%10, b=i%7
// and applies the given indexes.
func setupComplexFilterColl(t testing.TB, indexes ...IndexInfo) Collection {
	t.Helper()
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	for _, idx := range indexes {
		require.NoError(t, coll.EnsureIndex(ctx, idx))
	}
	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7,
		))
		require.NoError(t, coll.Insert(ctx, doc))
	}
	return coll
}

func TestIndex_ComplexFilter_OrMixedFields(t *testing.T) {
	// Index on "a" only; "b" is not indexed
	coll := setupComplexFilterColl(t, IndexInfo{Fields: []string{"a"}})

	// {$or: [{a:1}, {b:2}]}
	// a=1 → 10 docs (i%10==1)
	// b=2 → ~14-15 docs (i%7==2)
	// overlap: docs where a=1 AND b=2 → i≡1(mod10), i≡2(mod7) → i=51 → 1 doc
	count, err := coll.Find(`{"$or":[{"a":1},{"b":2}]}`).Count(ctx)
	require.NoError(t, err)

	// Compare with no-index collection to get exact answer
	collNoIdx := setupComplexFilterColl(t)
	countNoIdx, err := collNoIdx.Find(`{"$or":[{"a":1},{"b":2}]}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countNoIdx, count)
}

func TestIndex_ComplexFilter_ExistsWithSparseIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"optional"}, Sparse: true}))

	// Insert 50 docs with "optional" field and 50 without
	for i := range 100 {
		var doc *anyenc.Value
		if i%2 == 0 {
			doc = anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"optional":%d}`, i, i*10))
		} else {
			doc = anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, i))
		}
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Query {optional: {$exists: true}} → 50 docs
	count, err := coll.Find(`{"optional":{"$exists":true}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 50, count)

	// Sparse index should only have entries for docs with the field
	indexes := coll.GetIndexes()
	require.Len(t, indexes, 1)
	idxLen, err := indexes[0].Len(ctx)
	require.NoError(t, err)
	assert.Equal(t, 50, idxLen, "sparse index should only contain docs with the field")
}
