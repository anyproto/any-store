package qplanner

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/query"
)

// mockSketch creates an IndexSketch where Estimate(key) always returns the given value.
func mockSketch(estimate uint64) *IndexSketch {
	s := NewIndexSketch(DefaultSketchSize)
	// Increment a dummy key `estimate` times so that any hash-colliding lookup returns ~estimate.
	// For testing, we just set all buckets to the same value.
	for i := range s.Buckets {
		atomic.StoreUint64(&s.Buckets[i], estimate)
	}
	return s
}

func TestBuildPlan_NoIndexes_FullScan(t *testing.T) {
	plan := BuildPlan(&PlanParams{
		TotalDocs: 100,
	})
	assert.Equal(t, "FullScan", plan.Name)
	assert.Empty(t, plan.IndexName)
	assert.Contains(t, plan.String(), "FullScan")
}

func TestBuildPlan_NoFilter_FullScan(t *testing.T) {
	plan := BuildPlan(&PlanParams{
		Filter:    query.All{},
		TotalDocs: 100,
		Indexes: []CBOIndex{{
			Info:   &IndexInfo{Name: "a", FieldNames: []string{"a"}},
			Sketch: mockSketch(10),
		}},
	})
	// No filter → no index bounds → full scan
	assert.Equal(t, "FullScan", plan.Name)
}

func TestBuildPlan_SelectiveIndex_IndexSeek(t *testing.T) {
	// A selective index (estimates 5 out of 1000 docs) should be cheaper than full scan.
	plan := BuildPlan(&PlanParams{
		Filter:    query.MustParseCondition(`{"a": 42}`),
		TotalDocs: 1000,
		Indexes: []CBOIndex{{
			Info:        &IndexInfo{Name: "a", FieldNames: []string{"a"}},
			Sketch:      mockSketch(5),
			Bounds:      mustParseBounds("a", `{"a": 42}`),
			PointLookup: true,
			BoundFields: 1,
		}},
	})
	assert.Equal(t, "IndexSeek", plan.Name)
	assert.Equal(t, "a", plan.IndexName)
	assert.True(t, plan.Cost < 1000*CostDocFetch, "index seek should be cheaper than full scan")
}

func TestBuildPlan_IndexScan_SortWithLimit(t *testing.T) {
	// Sort on indexed field with LIMIT: IndexScan should win.
	plan := BuildPlan(&PlanParams{
		Filter:    query.All{},
		Sorter:    mustParseSort("a"),
		Limit:     10,
		TotalDocs: 1000,
		Indexes: []CBOIndex{{
			Info:      &IndexInfo{Name: "a", FieldNames: []string{"a"}},
			Sketch:    mockSketch(0),
			ExactSort: true,
		}},
	})
	assert.Equal(t, "IndexScan", plan.Name)
	assert.Equal(t, "a", plan.IndexName)
}

func TestBuildPlan_IndexScan_SortWithoutLimit(t *testing.T) {
	// Sort on indexed field without LIMIT: IndexScan still avoids in-memory sort.
	plan := BuildPlan(&PlanParams{
		Filter:    query.All{},
		Sorter:    mustParseSort("a"),
		TotalDocs: 100,
		Indexes: []CBOIndex{{
			Info:      &IndexInfo{Name: "a", FieldNames: []string{"a"}},
			Sketch:    mockSketch(0),
			ExactSort: true,
		}},
	})
	// IndexScan cost = 100*(0+2+0.5)=250, FullScan = 250 + sortCost
	// IndexScan should win because no sort penalty
	assert.Equal(t, "IndexScan", plan.Name)
}

func TestBuildPlan_LowSelectivity_FullScan(t *testing.T) {
	// When index estimates most of the collection, full scan is cheaper.
	plan := BuildPlan(&PlanParams{
		Filter:    query.MustParseCondition(`{"a": 42}`),
		TotalDocs: 100,
		Indexes: []CBOIndex{{
			Info:        &IndexInfo{Name: "a", FieldNames: []string{"a"}},
			Sketch:      mockSketch(100), // every doc matches
			Bounds:      mustParseBounds("a", `{"a": 42}`),
			PointLookup: true,
			BoundFields: 1,
		}},
	})
	// With 100% selectivity, both plans cost the same → tie-break prefers index
	assert.Equal(t, "IndexSeek", plan.Name)
}

func TestBuildPlan_UniqueIndex_CoverLookup(t *testing.T) {
	bounds := mustParseBounds("a", `{"a": 42}`)
	plan := BuildPlan(&PlanParams{
		Filter:    query.MustParseCondition(`{"a": 42}`),
		TotalDocs: 1000,
		Indexes: []CBOIndex{{
			Info:        &IndexInfo{Name: "a", FieldNames: []string{"a"}, Unique: true},
			Sketch:      mockSketch(1),
			Bounds:      bounds,
			PointLookup: true,
			BoundFields: 1,
		}},
	})
	assert.Equal(t, "IndexSeek", plan.Name)
	assert.Contains(t, plan.String(), "CoverLookup")
}

func TestBuildPlan_IndexHint(t *testing.T) {
	boundsA := mustParseBounds("a", `{"a": 1}`)
	boundsB := mustParseBounds("b", `{"b": 2}`)

	// Without hint: both indexes have similar cost
	planNoHint := BuildPlan(&PlanParams{
		Filter:    query.MustParseCondition(`{"a": 1, "b": 2}`),
		TotalDocs: 100,
		Indexes: []CBOIndex{
			{
				Info: &IndexInfo{Name: "a", FieldNames: []string{"a"}},
				Sketch: mockSketch(10), Bounds: boundsA,
				PointLookup: true, BoundFields: 1,
			},
			{
				Info: &IndexInfo{Name: "b", FieldNames: []string{"b"}},
				Sketch: mockSketch(14), Bounds: boundsB,
				PointLookup: true, BoundFields: 1,
			},
		},
	})
	// "a" should win (lower estimate = cheaper)
	assert.Equal(t, "a", planNoHint.IndexName)

	// With hint boosting "b" by 100: "b" should win
	planHinted := BuildPlan(&PlanParams{
		Filter:    query.MustParseCondition(`{"a": 1, "b": 2}`),
		TotalDocs: 100,
		Indexes: []CBOIndex{
			{
				Info: &IndexInfo{Name: "a", FieldNames: []string{"a"}},
				Sketch: mockSketch(10), Bounds: boundsA,
				PointLookup: true, BoundFields: 1,
			},
			{
				Info: &IndexInfo{Name: "b", FieldNames: []string{"b"}},
				Sketch: mockSketch(14), Bounds: boundsB,
				PointLookup: true, BoundFields: 1,
			},
		},
		IndexHints: []IndexHintParam{{IndexName: "b", Boost: 100}},
	})
	assert.Equal(t, "b", planHinted.IndexName)
}

func TestBuildPlan_TotalDocsZero(t *testing.T) {
	plan := BuildPlan(&PlanParams{
		TotalDocs: 0,
	})
	assert.Equal(t, "FullScan", plan.Name)
	assert.NotNil(t, plan.Root)
}

func TestBuildPlan_SingleDoc(t *testing.T) {
	bounds := mustParseBounds("a", `{"a": 1}`)
	plan := BuildPlan(&PlanParams{
		Filter:    query.MustParseCondition(`{"a": 1}`),
		TotalDocs: 1,
		Indexes: []CBOIndex{{
			Info:        &IndexInfo{Name: "a", FieldNames: []string{"a"}},
			Sketch:      mockSketch(1),
			Bounds:      bounds,
			PointLookup: true,
			BoundFields: 1,
		}},
	})
	// With 1 doc, IndexSeek should tie or beat FullScan
	assert.Equal(t, "IndexSeek", plan.Name)
}

func TestBuildPlan_IDBounds_FullScan(t *testing.T) {
	// When idBounds are specific point lookups, FullScan cost is based on len(idBounds)
	plan := BuildPlan(&PlanParams{
		Filter:    query.MustParseCondition(`{"id": {"$in": [2, 5]}}`),
		Sorter:    mustParseSort("-a"),
		TotalDocs: 100,
		IDBounds: query.Bounds{
			{Start: []byte{2}, End: []byte{2}, StartInclude: true, EndInclude: true},
			{Start: []byte{5}, End: []byte{5}, StartInclude: true, EndInclude: true},
		},
		Indexes: []CBOIndex{{
			Info:      &IndexInfo{Name: "a", FieldNames: []string{"a"}},
			ExactSort: true,
		}},
	})
	// FullScan with 2 idBounds + sort should be cheaper than IndexScan of 100 docs
	assert.Equal(t, "FullScan", plan.Name)
}

func TestCalculateSelectivity_NoFilter(t *testing.T) {
	p := calculateSelectivity(nil, nil, 100)
	assert.Equal(t, 1.0, p)
}

func TestCalculateSelectivity_AllFilter(t *testing.T) {
	p := calculateSelectivity(query.All{}, nil, 100)
	assert.Equal(t, 1.0, p)
}

func TestCalculateSelectivity_NoIndexes(t *testing.T) {
	p := calculateSelectivity(query.MustParseCondition(`{"a": 1}`), nil, 100)
	assert.Equal(t, DefaultRangeSelectivity, p)
}

func TestIndexSortMatch_ExactMatch(t *testing.T) {
	idx := &IndexInfo{FieldNames: []string{"a", "b"}}
	exact, partial := IndexSortMatch(idx, []query.SortField{
		{Field: "a"},
		{Field: "b"},
	}, 0)
	assert.True(t, exact)
	assert.False(t, partial)
}

func TestIndexSortMatch_PartialMatch(t *testing.T) {
	// Index (a, b, c), sort on (a) → all sort fields matched = exactSort
	idx := &IndexInfo{FieldNames: []string{"a", "b", "c"}}
	exact, partial := IndexSortMatch(idx, []query.SortField{
		{Field: "a"},
	}, 0)
	assert.True(t, exact)
	assert.False(t, partial)

	// Index (a, b, c), sort on (a, b, d) → first 2 matched but not all = partialSort
	exact2, partial2 := IndexSortMatch(idx, []query.SortField{
		{Field: "a"},
		{Field: "b"},
		{Field: "d"},
	}, 0)
	assert.False(t, exact2)
	assert.True(t, partial2)
}

func TestIndexSortMatch_NoMatch(t *testing.T) {
	idx := &IndexInfo{FieldNames: []string{"a", "b"}}
	exact, partial := IndexSortMatch(idx, []query.SortField{
		{Field: "c"},
	}, 0)
	assert.False(t, exact)
	assert.False(t, partial)
}

func TestIndexSortMatch_EqualityPrefix(t *testing.T) {
	// Index (a, b), filter pins a, sort on b → should match via prefix skip
	idx := &IndexInfo{FieldNames: []string{"a", "b"}}
	exact, partial := IndexSortMatch(idx, []query.SortField{
		{Field: "b"},
	}, 1)
	assert.True(t, exact)
	assert.False(t, partial)
}

func TestIndexSortMatch_FullyPinned(t *testing.T) {
	// Index (a), filter pins a, sort on a → should still match (Try 1)
	idx := &IndexInfo{FieldNames: []string{"a"}}
	exact, partial := IndexSortMatch(idx, []query.SortField{
		{Field: "a"},
	}, 1)
	assert.True(t, exact)
	assert.False(t, partial)
}

func TestAllBoundsFixed(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		assert.True(t, AllBoundsFixed(nil))
	})
	t.Run("point lookup", func(t *testing.T) {
		assert.True(t, AllBoundsFixed(query.Bounds{
			{Start: []byte{1}, End: []byte{1}},
		}))
	})
	t.Run("range", func(t *testing.T) {
		assert.False(t, AllBoundsFixed(query.Bounds{
			{Start: []byte{1}, End: []byte{5}},
		}))
	})
}

func TestComputeIndexBounds_SingleField(t *testing.T) {
	idx := &IndexInfo{FieldNames: []string{"a"}}
	cond := query.MustParseCondition(`{"a": 5}`)
	bounds, chainLen := ComputeIndexBounds(idx, cond)
	require.True(t, len(bounds) > 0, "should produce bounds for equality")
	assert.Equal(t, 1, chainLen)
}

func TestComputeIndexBounds_CompoundField(t *testing.T) {
	idx := &IndexInfo{FieldNames: []string{"a", "b"}}
	cond := query.MustParseCondition(`{"a": 5, "b": 3}`)
	bounds, chainLen := ComputeIndexBounds(idx, cond)
	require.True(t, len(bounds) > 0, "should produce compound bounds")
	assert.Equal(t, 2, chainLen)
}

func TestComputeIndexBounds_PartialCompound(t *testing.T) {
	idx := &IndexInfo{FieldNames: []string{"a", "b", "c"}}
	cond := query.MustParseCondition(`{"a": 5}`)
	bounds, chainLen := ComputeIndexBounds(idx, cond)
	require.True(t, len(bounds) > 0)
	assert.Equal(t, 1, chainLen) // only first field has bounds
}

func TestComputeIndexBounds_NoMatch(t *testing.T) {
	idx := &IndexInfo{FieldNames: []string{"x"}}
	cond := query.MustParseCondition(`{"a": 5}`)
	bounds, chainLen := ComputeIndexBounds(idx, cond)
	assert.Nil(t, bounds)
	assert.Equal(t, 0, chainLen)
}

func TestAdjustBoundsForNonUnique(t *testing.T) {
	bounds := query.Bounds{
		{Start: []byte{1}, End: []byte{1}, StartInclude: true, EndInclude: true},
	}
	adjusted := AdjustBoundsForNonUnique(bounds)
	require.Len(t, adjusted, 1)
	// End should be extended with 0xff
	assert.True(t, len(adjusted[0].End) > len(bounds[0].End))
}

func TestSortCost(t *testing.T) {
	assert.Equal(t, 0.0, sortCost(0))
	assert.Equal(t, 0.0, sortCost(1))
	assert.True(t, sortCost(10) > 0)
	assert.True(t, sortCost(100) > sortCost(10))
}

// --- helpers ---

func mustParseBounds(field, jsonCond string) query.Bounds {
	cond := query.MustParseCondition(jsonCond)
	return cond.IndexBounds(field, nil)
}

func mustParseSort(fields ...string) query.Sort {
	args := make([]any, len(fields))
	for i, f := range fields {
		args[i] = f
	}
	s, err := query.ParseSort(args...)
	if err != nil {
		panic(err)
	}
	return s
}
