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

// TestDetectKnn_PointerBuiltFilters pins the pointer-form walk: every
// composite filter has value-receiver methods, so &Not{…}/&Key{…}/&Or{…}
// satisfy query.Filter too, and a value-only type switch would skip them —
// letting &Not{Key{v,Knn}} reflect fail-closed Ok into a full-collection
// Delete, and &Key{v,Knn} silently return 0 rows instead of searching.
func TestDetectKnn_PointerBuiltFilters(t *testing.T) {
	coll := knnDetectColl(t)
	knnLeaf := func() query.Knn { return query.NewKnn([]float32{3, 1, 2}, 4) }

	t.Run("legal pointer forms search like their value forms", func(t *testing.T) {
		for name, f := range map[string]query.Filter{
			"&Key{v,Knn}": &query.Key{Path: []string{"v"}, Filter: knnLeaf()},
			"Key{v,&Knn}": query.Key{Path: []string{"v"}, Filter: ptrKnn(knnLeaf())},
			"&And{Key{v,Knn},residual}": func() query.Filter {
				a := query.And{query.Key{Path: []string{"v"}, Filter: knnLeaf()}, query.MustParseCondition(`{"a":1}`)}
				return &a
			}(),
		} {
			ids := writeOrderIterIds(t, coll.Find(f))
			require.NotEmpty(t, ids, "%s must search, not silently return 0 rows", name)
			n, err := coll.Find(f).Count(ctx)
			require.NoError(t, err, name)
			assert.Equal(t, len(ids), n, name)
		}
	})

	t.Run("illegal pointer forms error instead of bypassing detection", func(t *testing.T) {
		for name, f := range map[string]query.Filter{
			"&Not{Key{v,Knn}}": &query.Not{Filter: query.Key{Path: []string{"v"}, Filter: knnLeaf()}},
			"&Or{Key{v,Knn},…}": func() query.Filter {
				o := query.Or{query.Key{Path: []string{"v"}, Filter: knnLeaf()}, query.MustParseCondition(`{"a":1}`)}
				return &o
			}(),
			"&Nor{Key{v,Knn}}": func() query.Filter {
				n := query.Nor{query.Key{Path: []string{"v"}, Filter: knnLeaf()}}
				return &n
			}(),
			"&Knn bare": ptrKnn(knnLeaf()),
		} {
			_, err := coll.Find(f).Iter(ctx)
			require.ErrorIs(t, err, ErrKnnBadPlacement, "%s: Iter", name)
			res, err := coll.Find(f).Delete(ctx)
			require.ErrorIs(t, err, ErrKnnBadPlacement, "%s: Delete", name)
			require.Zero(t, res.Modified, name)
			remaining, cerr := coll.Find(nil).Count(ctx)
			require.NoError(t, cerr)
			require.Equal(t, 10, remaining, "%s: collection must be intact — this exact shape deleted everything before the pointer-walk fix", name)
		}
	})
}

func ptrKnn(k query.Knn) *query.Knn { return &k }

// neArrComp builds Comp{Ne, <plain array>} — the programmatic twin of the JSON
// {"$ne":[...]} spelling.
func neArrComp(qv []float32) *query.Comp {
	a := &anyenc.Arena{}
	arr := a.NewArray()
	for i, f := range qv {
		arr.SetArrayItem(i, a.NewNumberFloat64(float64(f)))
	}
	return query.NewCompValue(query.CompOpNe, arr)
}

// TestDetectKnn_NeDimArrayIsLegacy: $ne with a dim-sized plain array on a
// vector-indexed field is the match-all mirror of the legacy $eq spelling —
// on packed storage the operand never byte-equals any stored value, so the
// $ne would silently select EVERY document ("delete all but this vector"
// removes that vector too). Loud error, both spellings.
func TestDetectKnn_NeDimArrayIsLegacy(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "knn_ne")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: 3, Metric: VectorL2},
	}))
	// Packed storage — the encoding for which the $ne is a silent match-all.
	a := &anyenc.Arena{}
	for i := 0; i < 8; i++ {
		obj := a.NewObject()
		obj.Set("id", a.NewNumberInt(i))
		obj.Set("v", a.NewVectorF32([]float32{float32(i), 1, 2}))
		require.NoError(t, coll.Insert(ctx, obj))
		a.Reset()
	}

	for name, f := range map[string]any{
		"json $ne":         `{"v":{"$ne":[3,1,2]}}`,
		"programmatic $ne": query.Key{Path: []string{"v"}, Filter: neArrComp([]float32{3, 1, 2})},
	} {
		_, err = coll.Find(f).Iter(ctx)
		require.ErrorIs(t, err, ErrLegacyVectorClause, "%s: Iter", name)
		res, derr := coll.Find(f).Delete(ctx)
		require.ErrorIs(t, derr, ErrLegacyVectorClause, "%s: Delete", name)
		require.Zero(t, res.Modified, name)
	}
	remaining, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 8, remaining, "the $ne must never have executed as a match-all literal")

	// Wrong-dim $ne stays an ordinary filter (not ANN-shaped).
	n, err := coll.Find(`{"v":{"$ne":[1,2]}}`).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 8, n, "non-dim-sized $ne is an ordinary literal filter")
}

// TestDetectKnn_NestedKeyDistancePath: a nested-Key filter on a real stored
// sub-field literally named _distance (a._distance) is NOT the synthetic
// top-level _distance — the reference walk descends with the path suffix.
func TestDetectKnn_NestedKeyDistancePath(t *testing.T) {
	coll := knnDetectColl(t)
	f := query.Key{Path: []string{"a"}, Filter: query.Key{Path: []string{"_distance"}, Filter: query.NewComp(query.CompOpGt, 1)}}
	n, err := coll.Find(f).Count(ctx)
	require.NoError(t, err, "a._distance is a stored sub-field, not the synthetic _distance")
	assert.Zero(t, n)
	// The synthetic reference itself still errors, wherever it hides.
	_, err = coll.Find(query.Not{Filter: query.Key{Path: []string{"_distance"}, Filter: query.NewComp(query.CompOpLt, 1)}}).Count(ctx)
	require.ErrorIs(t, err, ErrDistanceWithoutVector)
}

// TestKnn_SortedResultsKeepDistance: SortIter clears the in-flight decorated
// doc, so the public iterator re-fetches — the _distance field must be
// re-injected from the sidecar, as docs/vector-search.md promises.
func TestKnn_SortedResultsKeepDistance(t *testing.T) {
	coll := knnDetectColl(t)
	iter, err := coll.Find(fmt.Sprintf(`{"v":%s}`, kd)).Sort("-_distance").Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	rows := 0
	for iter.Next() {
		doc, derr := iter.Doc()
		require.NoError(t, derr)
		dv := doc.Value().Get("_distance")
		require.NotNil(t, dv, "sorted $knn result lost the documented _distance field")
		assert.InDelta(t, float64(iter.Distance()), dv.GetFloat64(), 1e-6)
		rows++
	}
	require.NoError(t, iter.Err())
	require.NotZero(t, rows)
}

// TestExplain_UnconsideredSourceIndexesCarryNoCost: vector/fts indexes are
// listed for visibility, but only the driving index carries the plan's cost —
// an unconsidered index with a phantom cost would read as a CBO candidate.
func TestExplain_UnconsideredSourceIndexesCarryNoCost(t *testing.T) {
	coll := knnDetectColl(t)
	exp, err := coll.Find(`{"a":1}`).Explain(ctx)
	require.NoError(t, err)
	var sawVector bool
	for _, ix := range exp.Indexes {
		if ix.Name == "emb" {
			sawVector = true
			assert.False(t, ix.Used)
			assert.Zero(t, ix.Cost, "the CBO never costed the vector index for a plain range query")
		}
	}
	assert.True(t, sawVector, "the vector index must still be listed")
}
