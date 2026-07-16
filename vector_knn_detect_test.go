package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
)

// knnDetectColl builds a 3-dim vector-indexed collection ("v", index "emb")
// with a handful of docs, for exercising detectKnnQuery through the verbs.
func knnDetectColl(t *testing.T) Collection {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "knn_detect")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: 3, Metric: VectorL2, EfSearch: 64},
	}))
	for i := 0; i < 10; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"v":[%d,1,2],"a":%d,"b":%d}`, i, i, i%2, i%3))))
	}
	return coll
}

const kd = `{"$knn":{"$query":[3,1,2],"$k":4}}`

// TestDetectKnn_Placement pins the detection rows of the $knn design (T2.4):
// legal placements search; illegal ones hard-error with the named sentinel, on
// every verb (Iter and Count exercised here; the verb-agreement property is
// TestVectorClauseInvalid_WriteVerbsAgreeWithIter).
func TestDetectKnn_Placement(t *testing.T) {
	coll := knnDetectColl(t)

	legal := []struct {
		name string
		cond string
	}{
		{"plain", fmt.Sprintf(`{"v":%s}`, kd)},
		{"flat residual", fmt.Sprintf(`{"v":%s,"a":1}`, kd)},
		{"$and single", fmt.Sprintf(`{"$and":[{"v":%s}]}`, kd)},
		{"$and multi (*And)", fmt.Sprintf(`{"$and":[{"v":%s},{"a":1}]}`, kd)},
		{"nested *And (the residual-leak shape)", fmt.Sprintf(`{"$and":[{"$and":[{"v":%s},{"a":1}]},{"b":2}]}`, kd)},
	}
	for _, tc := range legal {
		t.Run("legal/"+tc.name, func(t *testing.T) {
			ids := writeOrderIterIds(t, coll.Find(tc.cond))
			assert.NotEmpty(t, ids, "a legal $knn placement must search, not silently return 0 rows")
			n, err := coll.Find(tc.cond).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, len(ids), n)
			assert.LessOrEqual(t, n, 4, "the k-cut bounds the denoted set")
		})
	}

	illegal := []struct {
		name   string
		cond   string
		wantIs error
	}{
		{"$or", fmt.Sprintf(`{"$or":[{"v":%s},{"a":1}]}`, kd), ErrKnnBadPlacement},
		{"$nor", fmt.Sprintf(`{"$nor":[{"v":%s}]}`, kd), ErrKnnBadPlacement},
		{"$or nested under $and", fmt.Sprintf(`{"$and":[{"$or":[{"v":%s},{"a":1}]},{"b":2}]}`, kd), ErrKnnBadPlacement},
		{"two $knn", fmt.Sprintf(`{"$and":[{"v":%s},{"v":%s}]}`, kd, kd), ErrMultipleVectorClauses},
		{"$knn with $text", fmt.Sprintf(`{"v":%s,"$text":{"$search":"x"}}`, kd), ErrKnnWithText},
		{"unindexed field", fmt.Sprintf(`{"w":%s}`, kd), ErrNoVectorIndex},
		{"$index names no index", `{"v":{"$knn":{"$query":[3,1,2],"$k":4,"$index":"nope"}}}`, ErrNoVectorIndex},
		{"wrong dim", `{"v":{"$knn":{"$query":[3,1],"$k":4}}}`, ErrInvalidVectorQuery},
		{"legacy bare array", `{"v":[3,1,2]}`, ErrLegacyVectorClause},
		{"legacy under $not", `{"v":{"$not":{"$eq":[3,1,2]}}}`, ErrLegacyVectorClause},
		{"_distance without $knn", `{"_distance":{"$lt":1.0},"a":1}`, ErrDistanceWithoutVector},
		{"_distance inside $or without $knn", `{"$or":[{"_distance":{"$lt":1.0}},{"a":1}]}`, ErrDistanceWithoutVector},
		{"_distance inside $not without $knn", `{"a":{"$gt":0},"$nor":[{"_distance":{"$lt":1.0}}]}`, ErrDistanceWithoutVector},
	}
	for _, tc := range illegal {
		t.Run("illegal/"+tc.name, func(t *testing.T) {
			_, err := coll.Find(tc.cond).Iter(ctx)
			require.ErrorIs(t, err, tc.wantIs, "Iter")
			_, err = coll.Find(tc.cond).Count(ctx)
			require.ErrorIs(t, err, tc.wantIs, "Count")
			_, err = coll.Find(tc.cond).Delete(ctx)
			require.ErrorIs(t, err, tc.wantIs, "Delete")
			_, err = coll.Find(tc.cond).Explain(ctx)
			require.ErrorIs(t, err, tc.wantIs, "Explain")
		})
	}

	// An invalid source errors even when an unrelated clause makes the filter
	// unsatisfiable — validation precedes the unsatisfiable() short-circuit.
	t.Run("validation precedes unsatisfiable", func(t *testing.T) {
		cond := `{"v":{"$knn":{"$query":[3,1],"$k":4}},"a":{"$in":[]}}`
		_, err := coll.Find(cond).Iter(ctx)
		require.ErrorIs(t, err, ErrInvalidVectorQuery, "Iter")
		_, err = coll.Find(cond).Count(ctx)
		require.ErrorIs(t, err, ErrInvalidVectorQuery, "Count")
		_, err = coll.Find(cond).Delete(ctx)
		require.ErrorIs(t, err, ErrInvalidVectorQuery, "Delete")
		_, err = coll.Find(cond).Explain(ctx)
		require.ErrorIs(t, err, ErrInvalidVectorQuery, "Explain")
	})
}

// TestDetectKnn_HandBuilt pins the programmatic path: a pre-built query.Filter
// bypasses the parser entirely (ParseCondition short-circuits), so detection is
// the only validation it ever sees — every parse rule must hold there too.
func TestDetectKnn_HandBuilt(t *testing.T) {
	coll := knnDetectColl(t)
	knn := func() query.Filter {
		return query.Key{Path: []string{"v"}, Filter: query.NewKnn([]float32{3, 1, 2}, 4)}
	}

	t.Run("legal Key{v,Knn}", func(t *testing.T) {
		ids := writeOrderIterIds(t, coll.Find(knn()))
		assert.NotEmpty(t, ids)
	})
	t.Run("legal And{Key{v,Knn},Key{v,Comp}} — strip by node, not path", func(t *testing.T) {
		// Two Keys on ONE path: the $ne residual must survive the Knn strip.
		// A path-keyed strip would drop it and widen the deleted set.
		f := query.And{
			knn(),
			query.Key{Path: []string{"v"}, Filter: query.NewComp(query.CompOpNe, "nothing-stored-matches")},
		}
		ids := writeOrderIterIds(t, coll.Find(f))
		assert.NotEmpty(t, ids, "the $ne residual matches every doc; the knn still selects k")
		f2 := query.And{
			knn(),
			// $eq "x" on v matches nothing → residual excludes everything.
			query.Key{Path: []string{"v"}, Filter: query.NewComp(query.CompOpEq, "x")},
		}
		n, err := coll.Find(f2).Count(ctx)
		require.NoError(t, err)
		assert.Zero(t, n, "the co-path residual must be preserved and applied")
	})

	for _, tc := range []struct {
		name   string
		f      query.Filter
		wantIs error
	}{
		{"bare Knn (no Key)", query.NewKnn([]float32{3, 1, 2}, 4), ErrKnnBadPlacement},
		{"Not{Key{v,Knn}}", query.Not{Filter: knn()}, ErrKnnBadPlacement},
		{"Key{v,Not{Knn}}", query.Key{Path: []string{"v"}, Filter: query.Not{Filter: query.NewKnn([]float32{3, 1, 2}, 4)}}, ErrKnnBadPlacement},
		{"Or{Key{v,Knn},…}", query.Or{knn(), query.MustParseCondition(`{"a":1}`)}, ErrKnnBadPlacement},
		{"k=0", query.Key{Path: []string{"v"}, Filter: query.Knn{Query: []float32{3, 1, 2}}}, ErrInvalidVectorQuery},
		{"empty query", query.Key{Path: []string{"v"}, Filter: query.Knn{K: 3}}, ErrInvalidVectorQuery},
		{"ef below k", query.Key{Path: []string{"v"}, Filter: query.NewKnn([]float32{3, 1, 2}, 4, query.KnnEf(2))}, ErrInvalidVectorQuery},
		{"legacy programmatic (the downstream indexer shape)", legacyEqFilter([]float32{3, 1, 2}), ErrLegacyVectorClause},
	} {
		t.Run("illegal/"+tc.name, func(t *testing.T) {
			_, err := coll.Find(tc.f).Iter(ctx)
			require.ErrorIs(t, err, tc.wantIs, "Iter")
			_, err = coll.Find(tc.f).Count(ctx)
			require.ErrorIs(t, err, tc.wantIs, "Count")
			_, err = coll.Find(tc.f).Delete(ctx)
			require.ErrorIs(t, err, tc.wantIs, "Delete")
		})
	}

	t.Run("non-ANN predicates on the vector field stay ordinary on every verb", func(t *testing.T) {
		// The downstream EnsureVectorIndex pattern: Count with {v:{$exists}}.
		n, err := coll.Find(query.Key{Path: []string{"v"}, Filter: query.Exists{}}).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 10, n)
	})
}

// legacyEqFilter reproduces the pre-$knn programmatic ANN construction
// (query.NewCompValue(CompOpEq, <plain array>)) that must now hard-error.
func legacyEqFilter(qv []float32) query.Filter {
	a := &anyenc.Arena{}
	arr := a.NewArray()
	for i, f := range qv {
		arr.SetArrayItem(i, a.NewNumberFloat64(float64(f)))
	}
	return query.Key{Path: []string{"v"}, Filter: query.NewCompValue(query.CompOpEq, arr)}
}

// TestDetectKnn_AmbiguousIndex: two vector indexes on one field require $index;
// either name resolves; a $knn without it errors rather than searching
// whichever index loaded first.
func TestDetectKnn_AmbiguousIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "knn_ambig")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb_l2", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: 3, Metric: VectorL2},
	}))
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb_cos", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: 3, Metric: VectorCosine},
	}))
	for i := 0; i < 6; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"v":[%d,1,2]}`, i, i))))
	}

	_, err = coll.Find(fmt.Sprintf(`{"v":%s}`, kd)).Iter(ctx)
	require.ErrorIs(t, err, ErrAmbiguousVectorIndex)
	_, err = coll.Find(fmt.Sprintf(`{"v":%s}`, kd)).Count(ctx)
	require.ErrorIs(t, err, ErrAmbiguousVectorIndex)

	for _, name := range []string{"emb_l2", "emb_cos"} {
		cond := fmt.Sprintf(`{"v":{"$knn":{"$query":[3,1,2],"$k":2,"$index":%q}}}`, name)
		ids := writeOrderIterIds(t, coll.Find(cond))
		assert.NotEmpty(t, ids, "$index %s must resolve", name)
		exp, eerr := coll.Find(cond).Explain(ctx)
		require.NoError(t, eerr)
		var used []string
		for _, ix := range exp.Indexes {
			if ix.Used {
				used = append(used, ix.Name)
			}
		}
		assert.Equal(t, []string{name}, used, "Explain must report the resolved index as used")
	}
}

// TestDetectKnn_ExplainReportsKnn: Explain names the ANN plan and the driving
// vector index — Explain printing FullScan for a working ANN query is why the
// verb divergence survived as long as it did.
func TestDetectKnn_ExplainReportsKnn(t *testing.T) {
	coll := knnDetectColl(t)
	exp, err := coll.Find(fmt.Sprintf(`{"v":%s,"a":1}`, kd)).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, exp.Sql, "KnnSearch(k=4,ef=", "the plan names the ANN source: %s", exp.Sql)
	assert.Contains(t, exp.Sql, "Filter", "the residual filter appears in the plan: %s", exp.Sql)
	var used []string
	for _, ix := range exp.Indexes {
		if ix.Used {
			used = append(used, ix.Name)
		}
	}
	assert.Equal(t, []string{"emb"}, used)
}

// TestKnnResidual_NoKnnSurvives is the hard post-condition behind the
// fail-closed design: for every legal placement, the residual handed to the
// FilterIter contains no Knn (a leaked Knn rejects every candidate — 0 rows,
// no-op Delete, err == nil — silently, on all verbs).
func TestKnnResidual_NoKnnSurvives(t *testing.T) {
	for _, cond := range []string{
		fmt.Sprintf(`{"v":%s}`, kd),
		fmt.Sprintf(`{"v":%s,"a":1}`, kd),
		fmt.Sprintf(`{"$and":[{"v":%s}]}`, kd),
		fmt.Sprintf(`{"$and":[{"v":%s},{"a":1}]}`, kd),
		fmt.Sprintf(`{"$and":[{"$and":[{"v":%s},{"a":1}]},{"b":2}]}`, kd),
		fmt.Sprintf(`{"$and":[{"$and":[{"v":%s}]},{"$and":[{"a":1},{"b":2}]}]}`, kd),
	} {
		f := query.MustParseCondition(cond)
		require.True(t, query.ContainsKnn(f), cond)
		residual := knnResidualFilter(f)
		assert.False(t, residual != nil && query.ContainsKnn(residual),
			"the Knn must be stripped from the residual: %s -> %v", cond, residual)
	}

	// Empty residual collapses to nil, NOT All{}: ef sizing and the
	// brute-force topK both key off "no residual" — All{} would flip every
	// bare $knn onto the ×10 over-fetch / full-ranking path invisibly.
	assert.Nil(t, knnResidualFilter(query.MustParseCondition(fmt.Sprintf(`{"v":%s}`, kd))))
	assert.Nil(t, knnResidualFilter(query.MustParseCondition(fmt.Sprintf(`{"$and":[{"v":%s}]}`, kd))))
}
