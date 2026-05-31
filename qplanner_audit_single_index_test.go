package anystore

/*
Audit tests for the "single-index" domain (docs/qplanner/audit/actionable_by_domain.json).

  act-01  $ne on an indexed scalar uses a two-bound seek and still includes
          null/missing (non-sparse): the index IS used (IndexScan, not FullScan),
          value 5 is excluded, and null/missing docs survive the residual Filter.
  act-03  Cross-type ordering: equality is type-strict (number 5 != string "5");
          $gte:0 sweeps strings/bools because number-0 sorts before them; the
          ascending sort order is Null<Number<String<False<True.
  act-04  Negative numbers round-trip through the order-preserving encoding for
          equality, two-sided range and asc/desc sort.

All helpers used here (newFixture, collectField, collectIdsString, assertIndexLen)
are defined elsewhere in the package test suite and reused as-is.
*/

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// act-01
func TestIndex_Single_Ne_TwoBoundSeek_IncludesNullAndMissing(t *testing.T) {
	// Builds a collection; when withIndex is true a non-sparse index {a} is added.
	mk := func(withIndex bool, docs ...string) Collection {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		if withIndex {
			require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		}
		for _, d := range docs {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
		}
		return coll
	}

	sortedIds := func(c Collection, filter string) []string {
		ids := collectIdsString(t, c.Find(filter).Sort("id"))
		sort.Strings(ids)
		return ids
	}

	t.Run("null and missing survive $ne", func(t *testing.T) {
		// id values are JSON strings so collectIdsString (GetStringBytes) resolves them.
		docs := []string{
			`{"id":"1","a":5}`,
			`{"id":"2","a":7}`,
			`{"id":"3","a":null}`,
			`{"id":"4"}`, // missing a
		}
		idx := mk(true, docs...)
		noidx := mk(false, docs...)

		// (1) Count == 3, identical with and without the index.
		idxCount, err := idx.Find(`{"a":{"$ne":5}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 3, idxCount)
		noidxCount, err := noidx.Find(`{"a":{"$ne":5}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, idxCount, noidxCount)

		// (2) Same id set for indexed and unindexed: 2 (a=7), 3 (null), 4 (missing).
		want := []string{"2", "3", "4"}
		assert.ElementsMatch(t, want, sortedIds(idx, `{"a":{"$ne":5}}`))
		assert.ElementsMatch(t, want, sortedIds(noidx, `{"a":{"$ne":5}}`))

		// (3) Both null and missing are indexed (non-sparse) => 4 entries.
		assertIndexLen(t, idx.GetIndexes()[0], 4)

		// (4) Explain: index IS used via the two-bound seek, with a residual Filter.
		explain, err := idx.Find(`{"a":{"$ne":5}}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "IndexScan(a)")
		assert.Contains(t, explain.Sql, "[-inf,'5'),('5',inf]")
		assert.Contains(t, explain.Sql, "-> Filter")
		assert.NotContains(t, explain.Sql, "FullScan")
	})

	t.Run("dense 0..9 dataset", func(t *testing.T) {
		var docs []string
		for i := 0; i < 10; i++ {
			docs = append(docs, fmt.Sprintf(`{"id":"%d","a":%d}`, i, i))
		}
		idx := mk(true, docs...)
		noidx := mk(false, docs...)

		// $ne:5 excludes exactly a=5; ascending a-values are 0,1,2,3,4,6,7,8,9.
		gotIdx := collectField(t, idx.Find(`{"a":{"$ne":5}}`).Sort("a"), "a")
		gotNoidx := collectField(t, noidx.Find(`{"a":{"$ne":5}}`).Sort("a"), "a")
		want := []string{"0", "1", "2", "3", "4", "6", "7", "8", "9"}
		assert.Equal(t, want, gotIdx)
		assert.Equal(t, want, gotNoidx)

		cnt, err := idx.Find(`{"a":{"$ne":5}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 9, cnt)
	})
}

// act-03
func TestIndex_Single_MixedTypeOrdering(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"1","a":5}`),
		anyenc.MustParseJson(`{"id":"2","a":"5"}`),
		anyenc.MustParseJson(`{"id":"3","a":"hello"}`),
		anyenc.MustParseJson(`{"id":"4","a":true}`),
	))

	// Equality is type-strict: number 5 matches only the numeric doc.
	cnt, err := coll.Find(`{"a":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
	assert.Equal(t, []string{"1"}, collectIdsString(t, coll.Find(`{"a":5}`)))

	// String "5" matches only the string doc.
	cnt, err = coll.Find(`{"a":"5"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
	assert.Equal(t, []string{"2"}, collectIdsString(t, coll.Find(`{"a":"5"}`)))

	// $gte:0 sweeps all four docs (number 0 sorts before all strings/bools).
	cnt, err = coll.Find(`{"a":{"$gte":0}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 4, cnt)

	// Ascending sort order: Number(5) < String("5") < String("hello") < True.
	assert.Equal(t,
		[]string{"5", "\"5\"", "\"hello\"", "true"},
		collectField(t, coll.Find(nil).Sort("a"), "a"),
	)
	// And the corresponding id order.
	assert.Equal(t,
		[]string{"1", "2", "3", "4"},
		collectIdsString(t, coll.Find(nil).Sort("a")),
	)
}

// act-04
func TestIndex_Single_NegativeNumbers(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for _, v := range []int{-5, -3, -1, 0, 2, 4} {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, v+100, v))))
	}

	// (1) Two-sided range across the sign boundary, ascending.
	assert.Equal(t,
		[]string{"-3", "-1", "0"},
		collectField(t, coll.Find(`{"a":{"$gte":-3,"$lt":2}}`).Sort("a"), "a"),
	)

	// (2) Full ascending sort across the sign boundary.
	assert.Equal(t,
		[]string{"-5", "-3", "-1", "0", "2", "4"},
		collectField(t, coll.Find(nil).Sort("a"), "a"),
	)

	// (3) Equality on a negative value.
	cnt, err := coll.Find(`{"a":-3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)

	// (4) Full descending sort across the sign boundary.
	assert.Equal(t,
		[]string{"4", "2", "0", "-1", "-3", "-5"},
		collectField(t, coll.Find(nil).Sort("-a"), "a"),
	)
}
