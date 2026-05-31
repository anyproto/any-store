package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// =============================================================================
// Domain: or-complex-filter
//
// These tests pin the soundness of the planner's handling of filter operators
// that intentionally produce NO index bounds ($nor, $not, $exists, cross-field
// $or), plus the over-approximating two-range $and. For every such operator the
// contract is: the index cannot narrow the candidate set, so the planner must
// FullScan-and-filter (or, for a partially-indexable predicate, IndexScan the
// indexable conjunct and apply the rest as a residual Filter), and the result
// must be byte-for-byte identical to an unindexed twin collection.
//
// Ground-truth references (verified in query/filter.go in this worktree):
//   - Nor.IndexBounds    returns bounds unchanged  (pure $nor => FullScan)
//   - Not.IndexBounds    returns bounds unchanged  (negation  => FullScan)
//   - Exists.IndexBounds returns bounds unchanged  ($exists   => FullScan)
//   - Or.IndexBounds     returns bounds unchanged when any branch yields no
//                        bounds for the field (cross-field $or => FullScan)
//   - And.IndexBounds    returns the FIRST conjunct's bounds (over-approx);
//                        remaining conjuncts are a residual Filter.
//
// The FullScan token in Explain.Sql prints as "FullScan(filtered)" whenever a
// residual filter is present (fullscan_iter.go String()).
// =============================================================================

// act-33: $nor over an indexed collection is sound and fullscans (pure $nor);
// mixed equality+$nor narrows on the equality field and applies NOR as a
// residual filter.
func TestIndex_ComplexFilter_NorIsSoundAndFullScans(t *testing.T) {
	// Local closure: build a collection of 100 docs (a=i%10, b=i%7), optionally
	// indexed on "a". Twin (unindexed) collection is built by passing nil.
	build := func(t *testing.T, indexes ...IndexInfo) Collection {
		t.Helper()
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		for _, idx := range indexes {
			require.NoError(t, coll.EnsureIndex(ctx, idx))
		}
		for i := 0; i < 100; i++ {
			doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))
			require.NoError(t, coll.Insert(ctx, doc))
		}
		return coll
	}

	idx := build(t, IndexInfo{Fields: []string{"a"}})
	noidx := build(t)

	t.Run("pure nor fullscans and agrees with unindexed", func(t *testing.T) {
		f := `{"$nor":[{"a":1},{"a":2}]}`

		// Nor.IndexBounds returns bounds unchanged => no narrowing => FullScan.
		ex, err := idx.Find(f).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, ex.Sql, "FullScan")
		assert.NotContains(t, ex.Sql, "IndexScan")

		// a=1 (10 docs) + a=2 (10 docs) excluded => 80 docs remain.
		cntIdx, err := idx.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 80, cntIdx)

		cntNo, err := noidx.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, cntIdx, cntNo, "indexed Count must equal unindexed Count")

		// Exact id-set parity between indexed and unindexed collections.
		idsIdx := collectIntField(t, idx.Find(f), "id")
		idsNo := collectIntField(t, noidx.Find(f), "id")
		assert.Len(t, idsIdx, 80)
		assert.ElementsMatch(t, idsNo, idsIdx)
	})

	t.Run("mixed equality+nor narrows on indexed field, nor is residual", func(t *testing.T) {
		f := `{"a":5,"$nor":[{"b":1},{"b":2}]}`

		// The equality on "a" narrows via the index; the $nor is a residual Filter.
		ex, err := idx.Find(f).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, ex.Sql, "IndexScan(a)")
		assert.Contains(t, ex.Sql, "-> Filter")

		// a=5 => 10 docs (ids 5,15,...,95). Of those, b=i%7 in {1,2} are removed.
		// ids with a=5 and (b==1 or b==2): 15(b=1), 65(b=2), 85(b=1) => 3 removed
		// => 7 remain.
		cntIdx, err := idx.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 7, cntIdx)

		cntNo, err := noidx.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, cntIdx, cntNo)

		idsIdx := collectIntField(t, idx.Find(f), "id")
		idsNo := collectIntField(t, noidx.Find(f), "id")
		assert.Len(t, idsIdx, 7)
		assert.ElementsMatch(t, idsNo, idsIdx)
	})
}

// act-34: $not operator form over an index is sound and fullscans.
func TestIndex_ComplexFilter_NotOperatorSound(t *testing.T) {
	build := func(t *testing.T, indexes ...IndexInfo) Collection {
		t.Helper()
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		for _, idx := range indexes {
			require.NoError(t, coll.EnsureIndex(ctx, idx))
		}
		for i := 0; i < 100; i++ {
			doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))
			require.NoError(t, coll.Insert(ctx, doc))
		}
		return coll
	}

	idx := build(t, IndexInfo{Fields: []string{"a"}})
	noidx := build(t)

	t.Run("not eq fullscans, 90 docs, agrees with unindexed", func(t *testing.T) {
		f := `{"a":{"$not":{"$eq":5}}}`

		// Not.IndexBounds returns bounds unchanged => FullScan, never index access.
		ex, err := idx.Find(f).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, ex.Sql, "FullScan(filtered)")
		assert.NotContains(t, ex.Sql, "IndexScan")
		assert.NotContains(t, ex.Sql, "IndexSeek")
		assert.NotContains(t, ex.Sql, "CoverLookup")

		// a=5 => 10 docs excluded => 90 remain.
		cntIdx, err := idx.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 90, cntIdx)

		cntNo, err := noidx.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, cntIdx, cntNo)

		idsIdx := collectIntField(t, idx.Find(f), "id")
		idsNo := collectIntField(t, noidx.Find(f), "id")
		assert.Len(t, idsIdx, 90)
		assert.ElementsMatch(t, idsNo, idsIdx)
	})

	t.Run("not gte negated range, 80 docs", func(t *testing.T) {
		f := `{"a":{"$not":{"$gte":8}}}`

		ex, err := idx.Find(f).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, ex.Sql, "FullScan")
		assert.NotContains(t, ex.Sql, "IndexScan")

		// !(a>=8) => a in 0..7 => 80 docs.
		cntIdx, err := idx.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 80, cntIdx)

		cntNo, err := noidx.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, cntIdx, cntNo)

		idsIdx := collectIntField(t, idx.Find(f), "id")
		idsNo := collectIntField(t, noidx.Find(f), "id")
		assert.ElementsMatch(t, idsNo, idsIdx)
	})
}

// act-35: $exists:false and $exists:true over a non-sparse index both fullscan
// (the answer is NOT derived from index entries). A buggy index-based answer to
// $exists:true would yield 100 (non-sparse index len==100), so the FullScan and
// the exact count of 50 are the load-bearing assertions.
func TestIndex_ComplexFilter_ExistsFalseAndNonSparse(t *testing.T) {
	// Local closure: 100 docs; even i has "opt", odd i does not. Index on "opt".
	build := func(t *testing.T, sparse bool) Collection {
		t.Helper()
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"opt"}, Sparse: sparse}))
		for i := 0; i < 100; i++ {
			var doc *anyenc.Value
			if i%2 == 0 {
				doc = anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"opt":%d}`, i, i))
			} else {
				doc = anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, i))
			}
			require.NoError(t, coll.Insert(ctx, doc))
		}
		return coll
	}

	// Unindexed baseline (no index at all) for parity.
	baseline := func(t *testing.T) Collection {
		t.Helper()
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		for i := 0; i < 100; i++ {
			var doc *anyenc.Value
			if i%2 == 0 {
				doc = anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"opt":%d}`, i, i))
			} else {
				doc = anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, i))
			}
			require.NoError(t, coll.Insert(ctx, doc))
		}
		return coll
	}

	idxLen := func(t *testing.T, c Collection) int {
		t.Helper()
		n, err := c.GetIndexes()[0].Len(ctx)
		require.NoError(t, err)
		return n
	}

	nonSparse := build(t, false)
	base := baseline(t)

	t.Run("non-sparse exists:false fullscans, 50 docs", func(t *testing.T) {
		f := `{"opt":{"$exists":false}}`

		ex, err := nonSparse.Find(f).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, ex.Sql, "FullScan")

		// Odd ids (no "opt") => 50.
		cnt, err := nonSparse.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 50, cnt)

		cntBase, err := base.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, cnt, cntBase)

		assert.ElementsMatch(t,
			collectIntField(t, base.Find(f), "id"),
			collectIntField(t, nonSparse.Find(f), "id"))
	})

	t.Run("non-sparse exists:true fullscans, 50 docs not 100", func(t *testing.T) {
		f := `{"opt":{"$exists":true}}`

		// Non-sparse index has 100 entries (one "null" per missing-field doc).
		assert.Equal(t, 100, idxLen(t, nonSparse))

		ex, err := nonSparse.Find(f).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, ex.Sql, "FullScan")
		// Critical: must NOT answer $exists:true from the index (would be 100).
		assert.NotContains(t, ex.Sql, "IndexScan")

		cnt, err := nonSparse.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 50, cnt, "exists:true must be 50, not the index length 100")

		cntBase, err := base.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, cnt, cntBase)

		assert.ElementsMatch(t,
			collectIntField(t, base.Find(f), "id"),
			collectIntField(t, nonSparse.Find(f), "id"))
	})

	t.Run("sparse exists:false fullscans, 50 docs, sparse index len 50", func(t *testing.T) {
		sparse := build(t, true)
		f := `{"opt":{"$exists":false}}`

		// Sparse index skips missing fields => 50 entries.
		assert.Equal(t, 50, idxLen(t, sparse))

		ex, err := sparse.Find(f).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, ex.Sql, "FullScan")

		cnt, err := sparse.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 50, cnt)

		cntBase, err := base.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, cnt, cntBase)

		assert.ElementsMatch(t,
			collectIntField(t, base.Find(f), "id"),
			collectIntField(t, sparse.Find(f), "id"))
	})
}

// act-36: Contradictory two-range $and on one indexed field (a>5 AND a<3)
// returns 0. And.IndexBounds yields only the first conjunct's over-approx seek
// bounds ('5',inf]; the residual FilterIter rejects every row.
func TestIndex_ComplexFilter_ContradictoryRangeAnd(t *testing.T) {
	coll := setupTestCollection(t, 100, IndexInfo{Fields: []string{"a"}})

	// Both the explicit $and form and the inline two-operator form must behave
	// identically: seek the first conjunct, re-filter to empty.
	cases := map[string]string{
		"explicit $and": `{"$and":[{"a":{"$gt":5}},{"a":{"$lt":3}}]}`,
		"inline range":  `{"a":{"$gt":5,"$lt":3}}`,
	}

	for name, f := range cases {
		f := f
		t.Run(name, func(t *testing.T) {
			cnt, err := coll.Find(f).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, 0, cnt)

			// Count must equal the iterated length.
			ids := collectIntField(t, coll.Find(f), "id")
			assert.Len(t, ids, 0)
			assert.Equal(t, cnt, len(ids))
		})
	}

	t.Run("explain explicit $and uses over-approx index seek + residual Filter", func(t *testing.T) {
		ex, err := coll.Find(`{"$and":[{"a":{"$gt":5}},{"a":{"$lt":3}}]}`).Explain(ctx)
		require.NoError(t, err)
		// First conjunct a>5 produces the over-approx seek bounds; second is residual.
		assert.Contains(t, ex.Sql, "IndexScan(a)")
		assert.Contains(t, ex.Sql, "-> Filter")
		// The covering-count fast path must NOT fire (two predicates on a).
		assert.NotContains(t, ex.Sql, "CoverLookup")
	})
}

// act-37: $or whose two branches are on two SEPARATE indexed fields must
// fullscan. Or.IndexBounds returns bounds unchanged whenever any branch yields
// no bounds for a given field, so a cross-field $or produces empty bounds for
// every index. Guards against a future change that wrongly seeks one index and
// silently drops the other branch.
func TestIndex_ComplexFilter_OrTwoIndexedFieldsUnion(t *testing.T) {
	// Local closure: 100 docs (a=i%10, b=i%7); optionally index BOTH a and b.
	build := func(t *testing.T, indexed bool) Collection {
		t.Helper()
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		if indexed {
			require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
			require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))
		}
		for i := 0; i < 100; i++ {
			doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))
			require.NoError(t, coll.Insert(ctx, doc))
		}
		return coll
	}

	idx := build(t, true)
	noidx := build(t, false)

	f := `{"$or":[{"a":1},{"b":2}]}`

	// a=1 => 10 docs (i%10==1). b=2 => 15 docs (i%7==2). Overlap a=1 AND b=2
	// (i%10==1 AND i%7==2 => i=51) => 2 docs counted in both branches, so the
	// union is 10+15-2 = 23. Exact value verified empirically.
	cntIdx, err := idx.Find(f).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 23, cntIdx)

	cntNo, err := noidx.Find(f).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 23, cntNo)
	assert.Equal(t, cntNo, cntIdx, "indexed must equal unindexed")

	// Exact id-set parity.
	assert.ElementsMatch(t,
		collectIntField(t, noidx.Find(f), "id"),
		collectIntField(t, idx.Find(f), "id"))

	// The plan must FullScan: no index candidate may be Used, and no IndexScan
	// token may appear (a single-index seek would silently drop the other branch).
	ex, err := idx.Find(f).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, ex.Sql, "FullScan")
	assert.NotContains(t, ex.Sql, "IndexScan")

	// Both indexes are reported as present but unused candidates.
	require.Len(t, ex.Indexes, 2)
	for _, ie := range ex.Indexes {
		assert.False(t, ie.Used, "index %q must not be used for a cross-field $or", ie.Name)
	}
}
