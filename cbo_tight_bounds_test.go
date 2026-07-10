package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
)

// fillRange inserts n docs {"id":i,"a":i} into a fresh collection with an
// index on the given field spec.
func fillRange(t *testing.T, fx *fixture, name, indexField string, n int) Collection {
	t.Helper()
	coll, err := fx.CreateCollection(ctx, name)
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{indexField}}))
	docs := make([]*anyenc.Value, 0, n)
	for i := 0; i < n; i++ {
		docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i)))
	}
	require.NoError(t, coll.Insert(ctx, docs...))
	return coll
}

// TestCBO_TightBounds_TwoSidedRange: the estimation channel must rate a
// two-sided range by BOTH ends. Before the tight channel, And.IndexBounds
// dropped the $lt end, interpolation ranked (lo,+inf) (~60% for a mid-keyspace
// lo), the <0.5 adoption ratchet rejected it, and the flat 0.5 default sent a
// ~2%-selective query to a full scan.
func TestCBO_TightBounds_TwoSidedRange(t *testing.T) {
	fx := newFixture(t)

	run := func(t *testing.T, coll Collection) {
		// a in (2000,2100) → ~2% of 5000 docs, mid-keyspace. Index must win.
		explain, err := coll.Find(`{"a":{"$gt":2000,"$lt":2100}}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "Index",
			"mid-keyspace two-sided range must use the index, got: %s", explain.Sql)
		// The coarse per-field estimate (the "Selectivity:" header) must adopt
		// the interpolated fraction instead of the flat 0.50 default.
		assert.NotContains(t, explain.Plan, "Selectivity: 0.50",
			"two-sided range selectivity must not be the 0.5 default:\n%s", explain.Plan)

		// Broad two-sided range (~98%) must still favor the full scan.
		explain, err = coll.Find(`{"a":{"$gt":50,"$lt":4950}}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "FullScan",
			"broad two-sided range must stay on full scan, got: %s", explain.Sql)
	}

	t.Run("ascending index", func(t *testing.T) {
		run(t, fillRange(t, fx, "asc", "a", 5000))
	})
	t.Run("descending index", func(t *testing.T) {
		// Reverse-flagged fields are stored bit-inverted; the estimation
		// bounds must be transformed into stored-key space before ranking,
		// or the fraction clamps to ~0 and the fix silently goes inert.
		run(t, fillRange(t, fx, "desc", "-a", 5000))
	})
}

// TestQuery_PkContradiction_Unsat: a contradictory constraint on the primary
// key provably matches nothing — pk values are scalars by construction (array
// pks are rejected on write), so all four verbs short-circuit to zero.
func TestQuery_PkContradiction_Unsat(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "pkunsat")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":1}`),
		anyenc.MustParseJson(`{"id":6,"a":2}`),
	))

	for _, filter := range []string{
		`{"id":{"$gt":5,"$lt":3}}`,
		`{"$and":[{"id":1},{"id":2}]}`,
		`{"id":{"$in":[1,2],"$gte":5}}`,
	} {
		t.Run(filter, func(t *testing.T) {
			assertQueryCount(t, coll.Find(filter), 0)

			it, err := coll.Find(filter).Iter(ctx)
			require.NoError(t, err)
			assert.False(t, it.Next(), "Iter must be empty")
			require.NoError(t, it.Err())
			require.NoError(t, it.Close())

			res, err := coll.Find(filter).Update(ctx, `{"$set":{"a":9}}`)
			require.NoError(t, err)
			assert.Equal(t, 0, res.Matched)

			dres, err := coll.Find(filter).Delete(ctx)
			require.NoError(t, err)
			assert.Equal(t, 0, dres.Modified)

			// Explain deliberately bypasses the short-circuit and still
			// produces a plan for debugging.
			explain, err := coll.Find(filter).Explain(ctx)
			require.NoError(t, err)
			assert.NotEmpty(t, explain.Sql)
		})
	}
	assertCollCount(t, coll, 2)
}

// TestQuery_ArrayContradiction_MustMatch pins the soundness boundary of the
// unsat short-circuit: a "contradictory" range on a NON-pk field still matches
// array values element-wise ({a:[6,1]}: 6>5 via one element, 1<3 via another),
// so it must NOT be short-circuited — on any verb, with or without an index.
func TestQuery_ArrayContradiction_MustMatch(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "arrunsat")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":[6,1]}`),
		anyenc.MustParseJson(`{"id":2,"a":4}`),
	))

	const filter = `{"a":{"$gt":5,"$lt":3}}`
	hint := IndexHint{IndexName: "a", Boost: 1_000_000}

	// Sanity: the filter really matches the array doc.
	f := query.MustParseCondition(filter)
	require.True(t, f.Ok(anyenc.MustParseJson(`{"a":[6,1]}`), nil))

	for _, q := range []func() Query{
		func() Query { return coll.Find(filter) },
		func() Query { return coll.Find(filter).IndexHint(hint) },
	} {
		assertQueryCount(t, q(), 1)

		it, err := q().Iter(ctx)
		require.NoError(t, err)
		n := 0
		for it.Next() {
			n++
		}
		require.NoError(t, it.Err())
		require.NoError(t, it.Close())
		assert.Equal(t, 1, n, "Iter must yield the array doc")

		res, err := q().Update(ctx, `{"$set":{"touched":true}}`)
		require.NoError(t, err)
		assert.Equal(t, 1, res.Matched, "Update must match the array doc")
	}

	dres, err := coll.Find(filter).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, dres.Modified, "Delete must remove the array doc")
	assertCollCount(t, coll, 1)
}

// TestCBO_TightBounds_AscAndDescIndexesOneField pins calculateSelectivity's
// usedFields dedup when BOTH an ascending and a descending index cover the
// same field: whichever index enumerates first, the two-sided range must not
// fall back to the flat 0.5 default (each index's tight estimation bounds go
// through its own reverse-flag transform).
func TestCBO_TightBounds_AscAndDescIndexesOneField(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "ascdesc")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx,
		IndexInfo{Name: "asc", Fields: []string{"a"}},
		IndexInfo{Name: "desc", Fields: []string{"-a"}},
	))
	docs := make([]*anyenc.Value, 0, 5000)
	for i := 0; i < 5000; i++ {
		docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i)))
	}
	require.NoError(t, coll.Insert(ctx, docs...))

	explain, err := coll.Find(`{"a":{"$gt":2000,"$lt":2100}}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "Index", "plan: %s", explain.Sql)
	assert.NotContains(t, explain.Plan, "Selectivity: 0.50",
		"two-sided range selectivity must be interpolated regardless of index order:\n%s", explain.Plan)
}
