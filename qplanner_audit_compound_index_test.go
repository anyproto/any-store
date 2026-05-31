package anystore

/*
Audit tests for the "compound-index" domain (docs/qplanner/audit/actionable_by_domain.json).
Rewritten for the btree/v2 cost-based planner (the agent draft targeted SQLite EXPLAIN).

  act-43  Whole-tuple reverse of an all-ascending compound index IS realizable by a
          single reverse scan (no in-memory Sort) — distinct from the mixed-direction
          bug which now falls back to an in-memory sort.
  act-44  Equality on the first field + range on the second, with sort on the second:
          index-covered ascending or reverse, no in-memory sort.
*/

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// act-43
func TestIndex_Compound_SortReversedIndex_WholeTuple(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))
	id := 0
	for a := 0; a < 3; a++ {
		for b := 0; b < 3; b++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, id, a, b))))
			id++
		}
	}
	pairs := func(sort ...any) []string {
		iter, err := coll.Find(nil).Sort(sort...).Iter(ctx)
		require.NoError(t, err)
		defer iter.Close()
		var out []string
		for iter.Next() {
			d, derr := iter.Doc()
			require.NoError(t, derr)
			out = append(out, fmt.Sprintf("%d,%d", d.Value().GetInt("a"), d.Value().GetInt("b")))
		}
		require.NoError(t, iter.Err())
		return out
	}
	assert.Equal(t, []string{"2,2", "2,1", "2,0", "1,2", "1,1", "1,0", "0,2", "0,1", "0,0"},
		pairs("-a", "-b"))

	explain, err := coll.Find(nil).Sort("-a", "-b").Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan(a,b)")
	assert.Contains(t, explain.Sql, "(reverse)")
	assert.NotContains(t, explain.Sql, "-> Sort")
	assert.NotContains(t, explain.Sql, "TopK")
}

// act-44
func TestIndex_Compound_FilterAndRangeOnSecond(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))
	for a := 1; a <= 3; a++ {
		for b := 1; b <= 10; b++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, a*100+b, a, b))))
		}
	}

	// equality a + range b ($gte:5), sort b ascending → index-covered, no in-memory sort.
	assert.Equal(t, []string{"5", "6", "7", "8", "9", "10"},
		collectField(t, coll.Find(`{"a":2,"b":{"$gte":5}}`).Sort("b"), "b"))
	ex1, err := coll.Find(`{"a":2,"b":{"$gte":5}}`).Sort("b").Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, ex1.Sql, "IndexScan(a,b)")
	assert.NotContains(t, ex1.Sql, "-> Sort")
	c1, err := coll.Find(`{"a":2,"b":{"$gte":5}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 6, c1)

	// sort b descending → reverse scan, still no in-memory sort.
	assert.Equal(t, []string{"10", "9", "8", "7", "6", "5"},
		collectField(t, coll.Find(`{"a":2,"b":{"$gte":5}}`).Sort("-b"), "b"))
	ex2, err := coll.Find(`{"a":2,"b":{"$gte":5}}`).Sort("-b").Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, ex2.Sql, "IndexScan(a,b)")
	assert.Contains(t, ex2.Sql, "(reverse)")
	assert.NotContains(t, ex2.Sql, "-> Sort")

	// upper-bound range.
	assert.Equal(t, []string{"1", "2", "3", "4"},
		collectField(t, coll.Find(`{"a":2,"b":{"$lt":5}}`).Sort("b"), "b"))
}
