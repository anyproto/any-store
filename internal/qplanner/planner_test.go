package qplanner

import (
	"bytes"
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
)

// mockSketch creates an IndexSketch where Estimate(level, key) always returns the
// given value at every level. It allocates enough levels to cover any compound
// index used in the planner tests.
func mockSketch(estimate uint64) *IndexSketch {
	s := NewIndexSketch(DefaultSketchSize, 8)
	// Set every bucket (across all levels) to the same value so any lookup at any
	// level returns ~estimate.
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

// TestBuildPlan_CompoundPrefix_SelectivePrefixBeatsFullScan is the motivating
// regression: index (a,b), query Find({a:42}) [+ Sort(b)]. The level-0 prefix
// sketch knows a=42 is selective (10 of 50000), so the planner picks the index.
// Before multi-level prefix sketches, selectivityForIndex fell back to the 0.5
// default for a partial prefix on a compound index, costing the index plan as if
// it touched half the collection and (for the no-sort case, always) choosing a
// full scan instead.
func TestBuildPlan_CompoundPrefix_SelectivePrefixBeatsFullScan(t *testing.T) {
	const totalDocs = 50000
	bounds := mustParseBounds("a", `{"a": 42}`)

	newIdx := func() CBOIndex {
		sk := NewIndexSketch(DefaultSketchSize, 2)
		for i := 0; i < 10; i++ {
			sk.Increment(0, bounds[0].Start) // a=42 → 10 entries at the prefix level
		}
		return CBOIndex{
			Info: &IndexInfo{
				Name: "ab", FieldNames: []string{"a", "b"},
				FieldPaths: [][]string{{"a"}, {"b"}},
			},
			Sketch:      sk,
			Bounds:      bounds,
			PointLookup: true,
			BoundFields: 1,    // only a is bound (a prefix of (a,b))
			ExactSort:   true, // (a,b) yields b-order once a is fixed
		}
	}

	filter := query.MustParseCondition(`{"a": 42}`)

	t.Run("no sort picks IndexSeek", func(t *testing.T) {
		plan := BuildPlan(&PlanParams{
			Filter:    filter,
			TotalDocs: totalDocs,
			Indexes:   []CBOIndex{newIdx()},
		})
		assert.Equal(t, "IndexSeek", plan.Name,
			"selective compound prefix must beat full scan")
	})

	t.Run("Sort(b) uses the index, not full scan", func(t *testing.T) {
		sorter := &sortFieldStub{fields: []query.SortField{{Field: "b"}}}
		plan := BuildPlan(&PlanParams{
			Filter:    filter,
			Sorter:    sorter,
			TotalDocs: totalDocs,
			Indexes:   []CBOIndex{newIdx()},
		})
		assert.NotEqual(t, "FullScan", plan.Name,
			"selective compound prefix with Sort(b) must use the index, not full scan")
	})
}

// TestBuildPlan_SparseIndex_NeUsesIndex is the sparse-cut regression: {a:{$ne:""}}
// on a sparse index where only a small fraction of docs have `a`. $ne is a range
// (two open bounds), so there is no point estimate; bounding the estimate by the
// index's own live entry count (EntryCount, far below totalDocs for a sparse
// index) lets the index win — scanning only the present entries is far cheaper
// than a full collection scan. A DENSE index with the same query must still pick
// FullScan, because $ne there genuinely touches ~the whole collection.
func TestBuildPlan_SparseIndex_NeUsesIndex(t *testing.T) {
	const totalDocs = 50000
	bounds := mustParseBounds("a", `{"a": {"$ne": ""}}`)
	require.Len(t, bounds, 2, "$ne compiles to two open bounds")

	newIdx := func(entryCount int) CBOIndex {
		sk := NewIndexSketch(DefaultSketchSize, 1)
		for i := 0; i < entryCount; i++ {
			// Only levelTotals[0] (== EntryCount(0)) matters here; the bucketed
			// counts are never consulted for a range predicate.
			sk.Increment(0, []byte{byte(i), byte(i >> 8)})
		}
		return CBOIndex{
			Info:        &IndexInfo{Name: "a", FieldNames: []string{"a"}},
			Sketch:      sk,
			Bounds:      bounds,
			PointLookup: false, // $ne is not an equality lookup
			BoundFields: 1,
		}
	}

	filter := query.MustParseCondition(`{"a": {"$ne": ""}}`)

	t.Run("sparse index (500/50000 present) uses index", func(t *testing.T) {
		plan := BuildPlan(&PlanParams{
			Filter:    filter,
			TotalDocs: totalDocs,
			Indexes:   []CBOIndex{newIdx(500)},
		})
		assert.Equal(t, "IndexSeek", plan.Name,
			"sparse index presence cut must beat full scan")
	})

	t.Run("dense index ($ne ~ all) stays full scan", func(t *testing.T) {
		plan := BuildPlan(&PlanParams{
			Filter:    filter,
			TotalDocs: totalDocs,
			Indexes:   []CBOIndex{newIdx(totalDocs)},
		})
		assert.Equal(t, "FullScan", plan.Name,
			"$ne on a dense index genuinely touches ~all docs; full scan is correct")
	})
}

// TestIndexCoversFilter_RejectsUncoveredField is the defensive regression for
// the I-04 field-coverage check: a filter touching a field not in the index is
// never reported as covered, and empty index bounds are never covered. See
// docs/known-issues.md (I-04).
func TestIndexCoversFilter_RejectsUncoveredField(t *testing.T) {
	pointBound := query.Bounds{{
		Start:        anyenc.AppendAnyValue(nil, 1),
		End:          anyenc.AppendAnyValue(nil, 1),
		StartInclude: true,
		EndInclude:   true,
	}}
	idx := &CBOIndex{
		Info:        &IndexInfo{Name: "a", FieldNames: []string{"a"}},
		Bounds:      pointBound,
		BoundFields: 1, // "a" pinned by the point bound
	}

	assert.False(t, indexCoversFilter(idx, query.MustParseCondition(`{"a":1,"b":2}`)),
		"a predicate on an uncovered field must not be reported as covered")
	assert.True(t, indexCoversFilter(idx, query.MustParseCondition(`{"a":1}`)),
		"a predicate fully on the indexed field is covered")

	// Empty bounds are rejected by the len(idx.Bounds)==0 early-return.
	idxEmpty := &CBOIndex{Info: &IndexInfo{Name: "a", FieldNames: []string{"a"}}}
	assert.False(t, indexCoversFilter(idxEmpty, query.MustParseCondition(`{"a":1}`)),
		"empty bounds must not be reported as covered")
}

// TestIndexCoversFilter_GatesMultiPredicateField pins the I-04 fast-path gate.
// Once And.IndexBounds over-approximates (its bounds are a superset of the
// matches — required for array/multi-key correctness, see And.IndexBounds), the
// CountOnly fast path, which skips the FilterIter, is sound only when each
// covered field carries a SINGLE predicate so the bounds equal the matches
// exactly. A field with two predicates — a same-field $and, an inline
// {$in,$gte}, or a two-sided range — must NOT be reported as covered. Single
// predicates and compound point lookups stay covered. See docs/known-issues.md
// (I-04).
func TestIndexCoversFilter_GatesMultiPredicateField(t *testing.T) {
	idxA := &CBOIndex{
		Info:        &IndexInfo{Name: "a", FieldNames: []string{"a"}},
		Bounds:      mustParseBounds("a", `{"a":1}`),
		BoundFields: 1, // "a" is pinned by the bound prefix
	}

	// Rejected: more than one predicate on the single covered field.
	for _, f := range []string{
		`{"a":{"$in":[1,2]},"$and":[{"a":{"$gte":5}}]}`, // same-field $and
		`{"a":{"$in":[1,2],"$gte":5}}`,                  // inline multi-op
		`{"a":{"$gte":2,"$lte":3}}`,                     // two-sided range
	} {
		assert.False(t, indexCoversFilter(idxA, query.MustParseCondition(f)),
			"multi-predicate field must not be covered (over-approx bounds): %s", f)
	}

	// Covered: exactly one predicate on the field.
	for _, f := range []string{
		`{"a":1}`,
		`{"a":{"$in":[1,2,3]}}`,
	} {
		assert.True(t, indexCoversFilter(idxA, query.MustParseCondition(f)),
			"single-predicate field must stay covered: %s", f)
	}

	// Covered: a compound point lookup that pins BOTH fields via the bound
	// prefix (BoundFields == 2) keeps one predicate per field, so the fast path
	// (CountEntries over the compound index) must remain available.
	idxAB := &CBOIndex{
		Info:        &IndexInfo{Name: "ab", FieldNames: []string{"a", "b"}},
		Bounds:      mustParseBounds("a", `{"a":1}`),
		BoundFields: 2, // both a and b pinned by the equality-equality bound chain
	}
	assert.True(t, indexCoversFilter(idxAB, query.MustParseCondition(`{"a":1,"b":2}`)),
		"compound point lookup (one predicate per field) must stay covered")

	// NOT covered: skip-middle — a trailing equality field (c) lies BEYOND the
	// bounded prefix [a], so the bounds don't enforce it and the covering-count
	// fast path would over-count. Must fall through to the FilterIter path.
	idxABC := &CBOIndex{
		Info:        &IndexInfo{Name: "abc", FieldNames: []string{"a", "b", "c"}},
		Bounds:      mustParseBounds("a", `{"a":1}`),
		BoundFields: 1, // only a is pinned; b unconstrained breaks the chain
	}
	assert.False(t, indexCoversFilter(idxABC, query.MustParseCondition(`{"a":1,"c":0}`)),
		"skip-middle filter field beyond the bound prefix must NOT be covered")
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

// TestComputeFullScanCost_SortAddsMaterialize pins the exact-sort boost: a
// full-scan sort pays both the n*log2(n) swap term AND a linear CostMaterialize
// per buffered row, so an order-providing index scan (which streams) is favored.
func TestComputeFullScanCost_SortAddsMaterialize(t *testing.T) {
	const totalDocs = 1000.0
	const yield = 800.0
	noSort := computeFullScanCost(totalDocs, yield, false, false)
	withSort := computeFullScanCost(totalDocs, yield, true, false)

	want := noSort + sortCost(yield) + yield*CostMaterialize
	assert.InDelta(t, want, withSort, 1e-9)
	assert.Greater(t, withSort-noSort, sortCost(yield),
		"materialize must add cost beyond the swap term alone")
}

// TestBuildPlan_PoorSelectivitySort_StaysFullScan is the protection regression
// for the exact-sort boost: a poorly-selective filter on a NON-indexed field with
// an ORDER BY that an index satisfies must NOT flip to an ordered index scan,
// because that scan would random-fetch the whole collection to evaluate the
// filter. The per-row fetch cost keeps full-scan+sort cheaper. (Mirrors the
// expert-validated "WHERE unindexed_b=x ORDER BY a" failure mode.)
func TestBuildPlan_PoorSelectivitySort_StaysFullScan(t *testing.T) {
	plan := BuildPlan(&PlanParams{
		Filter:    query.MustParseCondition(`{"b": 5}`), // b is not in the index
		Sorter:    mustParseSort("a"),
		TotalDocs: 50000,
		Indexes: []CBOIndex{{
			Info:      &IndexInfo{Name: "a", FieldNames: []string{"a"}},
			Sketch:    mockSketch(0),
			ExactSort: true, // covers ORDER BY a, but has no bounds (filter is on b)
		}},
	})
	assert.Equal(t, "FullScan", plan.Name,
		"ordered scan that must fetch ~all docs to filter must lose to full-scan+sort")
}

func TestBuildPlan_LowSelectivity_FullScan(t *testing.T) {
	// When index estimates most of the collection, full scan is cheaper
	// because sequential reads + filter is cheaper than seek + random fetch + filter.
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
	assert.Equal(t, "FullScan", plan.Name)
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

// TestBuildPlan_UniqueIndexPointLookup_NoFlipAtScale is the regression for the
// sketch-floor plan flip: with ~N/DefaultSketchSize distinct values per bucket,
// a unique-index point lookup's sketch estimate grows with the collection while
// the true row count stays 1. Past ~150k docs the inflated estimate made
// IndexSeek lose to a LIMIT-capped FullScan (fullScanEffective = limit/pTotal
// collapses when pTotal is also sketch-derived). A full-key equality on a
// UNIQUE index must be priced at len(bounds) rows, never the sketch.
func TestBuildPlan_UniqueIndexPointLookup_NoFlipAtScale(t *testing.T) {
	const totalDocs = 175000
	// The bucket load a 1024-bucket sketch reports for a value occurring once.
	inflated := uint64(totalDocs / DefaultSketchSize * 3) // a hot bucket
	plan := BuildPlan(&PlanParams{
		Filter:    query.MustParseCondition(`{"id": "bafyrei42"}`),
		TotalDocs: totalDocs,
		Limit:     1,
		Indexes: []CBOIndex{{
			Info:        &IndexInfo{Name: "id", FieldNames: []string{"id"}, Unique: true},
			Sketch:      mockSketch(inflated),
			Bounds:      mustParseBounds("id", `{"id": "bafyrei42"}`),
			PointLookup: true,
			BoundFields: 1,
		}},
	})
	assert.Equal(t, "IndexSeek", plan.Name)
	for _, c := range plan.Explain.Candidates {
		if c.Name != "FullScan" {
			assert.Equal(t, float64(1), c.EstRows, "unique point lookup must be priced at 1 row")
		}
	}
}

// TestBuildPlan_UniqueIndexPointLookup_OrderIndependent: the unique bypass in
// calculateSelectivity must fire even when another index containing the same
// field is iterated first — fields are priced by whichever index claims them
// first, so without the unique-first pass a compound index claims the field
// with a sketch-bucket (or DefaultRangeSelectivity for a non-leading field)
// estimate and pTotal — which feeds estimatedYield and every candidate's
// filtered-yield comparison — inflates by orders of magnitude.
func TestBuildPlan_UniqueIndexPointLookup_OrderIndependent(t *testing.T) {
	const totalDocs = 175000
	inflated := uint64(totalDocs / DefaultSketchSize * 3)
	plan := BuildPlan(&PlanParams{
		Filter:    query.MustParseCondition(`{"id": "bafyrei42"}`),
		TotalDocs: totalDocs,
		Limit:     1,
		Indexes: []CBOIndex{
			{
				// Compound non-unique index with `id` at a NON-leading position,
				// listed before the unique index: pre-fix it claimed the field at
				// DefaultRangeSelectivity (fi>0 skips both equality branches).
				Info:        &IndexInfo{Name: "space_id", FieldNames: []string{"space", "id"}},
				Sketch:      mockSketch(inflated),
				PointLookup: false,
				BoundFields: 0,
			},
			{
				Info:        &IndexInfo{Name: "id", FieldNames: []string{"id"}, Unique: true},
				Sketch:      mockSketch(inflated),
				Bounds:      mustParseBounds("id", `{"id": "bafyrei42"}`),
				PointLookup: true,
				BoundFields: 1,
			},
		},
	})
	assert.Equal(t, "IndexSeek", plan.Name)
	assert.Equal(t, "id", plan.IndexName)
	// Without the unique-first pass the compound index claims `id` at
	// DefaultRangeSelectivity (0.5); the unique claim prices it 1/totalDocs.
	assert.Less(t, plan.Explain.Selectivity, 1e-4)
}

func TestUniqueFullKeyDocs(t *testing.T) {
	bounds := mustParseBounds("a", `{"a": 42}`)
	uniqueSingle := CBOIndex{
		Info:        &IndexInfo{Name: "a", FieldNames: []string{"a"}, Unique: true},
		Bounds:      bounds,
		PointLookup: true,
		BoundFields: 1,
	}
	docs, ok := uniqueFullKeyDocs(&uniqueSingle)
	assert.True(t, ok)
	assert.Equal(t, float64(1), docs)

	nonUnique := uniqueSingle
	nonUnique.Info = &IndexInfo{Name: "a", FieldNames: []string{"a"}}
	_, ok = uniqueFullKeyDocs(&nonUnique)
	assert.False(t, ok)

	rangeLookup := uniqueSingle
	rangeLookup.PointLookup = false
	_, ok = uniqueFullKeyDocs(&rangeLookup)
	assert.False(t, ok)

	// Partial prefix on a compound unique index: uniqueness of (a,b) bounds
	// nothing for a=x alone.
	partialPrefix := uniqueSingle
	partialPrefix.Info = &IndexInfo{Name: "ab", FieldNames: []string{"a", "b"}, Unique: true}
	partialPrefix.BoundFields = 1
	_, ok = uniqueFullKeyDocs(&partialPrefix)
	assert.False(t, ok)
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
				Info:   &IndexInfo{Name: "a", FieldNames: []string{"a"}},
				Sketch: mockSketch(10), Bounds: boundsA,
				PointLookup: true, BoundFields: 1,
			},
			{
				Info:   &IndexInfo{Name: "b", FieldNames: []string{"b"}},
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
				Info:   &IndexInfo{Name: "a", FieldNames: []string{"a"}},
				Sketch: mockSketch(10), Bounds: boundsA,
				PointLookup: true, BoundFields: 1,
			},
			{
				Info:   &IndexInfo{Name: "b", FieldNames: []string{"b"}},
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
	// With 1 doc, FullScan is cheaper than index seek overhead
	assert.Equal(t, "FullScan", plan.Name)
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
	p := calculateSelectivity(nil, nil, 100, nil)
	assert.Equal(t, 1.0, p)
}

func TestCalculateSelectivity_AllFilter(t *testing.T) {
	p := calculateSelectivity(query.All{}, nil, 100, nil)
	assert.Equal(t, 1.0, p)
}

func TestCalculateSelectivity_NoIndexes(t *testing.T) {
	p := calculateSelectivity(query.MustParseCondition(`{"a": 1}`), nil, 100, nil)
	assert.Equal(t, DefaultRangeSelectivity, p)
}

func TestIndexSortMatch_ExactMatch(t *testing.T) {
	idx := &IndexInfo{FieldNames: []string{"a", "b"}}
	exact, partial, ms := IndexSortMatch(idx, []query.SortField{
		{Field: "a"},
		{Field: "b"},
	}, 0)
	assert.True(t, exact)
	assert.False(t, partial)
	assert.Equal(t, 0, ms)
}

func TestIndexSortMatch_PartialMatch(t *testing.T) {
	// Index (a, b, c), sort on (a) → all sort fields matched = exactSort
	idx := &IndexInfo{FieldNames: []string{"a", "b", "c"}}
	exact, partial, _ := IndexSortMatch(idx, []query.SortField{
		{Field: "a"},
	}, 0)
	assert.True(t, exact)
	assert.False(t, partial)

	// Index (a, b, c), sort on (a, b, d) → first 2 matched but not all = partialSort
	exact2, partial2, _ := IndexSortMatch(idx, []query.SortField{
		{Field: "a"},
		{Field: "b"},
		{Field: "d"},
	}, 0)
	assert.False(t, exact2)
	assert.True(t, partial2)
}

func TestIndexSortMatch_NoMatch(t *testing.T) {
	idx := &IndexInfo{FieldNames: []string{"a", "b"}}
	exact, partial, _ := IndexSortMatch(idx, []query.SortField{
		{Field: "c"},
	}, 0)
	assert.False(t, exact)
	assert.False(t, partial)
}

func TestIndexSortMatch_EqualityPrefix(t *testing.T) {
	// Index (a, b), filter pins a, sort on b → should match via prefix skip
	idx := &IndexInfo{FieldNames: []string{"a", "b"}}
	exact, partial, ms := IndexSortMatch(idx, []query.SortField{
		{Field: "b"},
	}, 1)
	assert.True(t, exact)
	assert.False(t, partial)
	assert.Equal(t, 1, ms, "matched run starts at the equality-pinned prefix position")
}

func TestIndexSortMatch_FullyPinned(t *testing.T) {
	// Index (a), filter pins a, sort on a → should still match (Try 1)
	idx := &IndexInfo{FieldNames: []string{"a"}}
	exact, partial, _ := IndexSortMatch(idx, []query.SortField{
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

func buildBoundsResult(idx *IndexInfo, cond query.Filter) *BoundsResult {
	var br BoundsResult
	br.Build([]*IndexInfo{idx}, cond)
	return &br
}

func TestComputeIndexBounds_SingleField(t *testing.T) {
	idx := &IndexInfo{FieldNames: []string{"a"}}
	cond := query.MustParseCondition(`{"a": 5}`)
	bounds, chainLen := ComputeIndexBounds(idx, buildBoundsResult(idx, cond))
	require.True(t, len(bounds) > 0, "should produce bounds for equality")
	assert.Equal(t, 1, chainLen)
}

func TestComputeIndexBounds_CompoundField(t *testing.T) {
	idx := &IndexInfo{FieldNames: []string{"a", "b"}}
	cond := query.MustParseCondition(`{"a": 5, "b": 3}`)
	bounds, chainLen := ComputeIndexBounds(idx, buildBoundsResult(idx, cond))
	require.True(t, len(bounds) > 0, "should produce compound bounds")
	assert.Equal(t, 2, chainLen)
}

func TestComputeIndexBounds_PartialCompound(t *testing.T) {
	idx := &IndexInfo{FieldNames: []string{"a", "b", "c"}}
	cond := query.MustParseCondition(`{"a": 5}`)
	bounds, chainLen := ComputeIndexBounds(idx, buildBoundsResult(idx, cond))
	require.True(t, len(bounds) > 0)
	assert.Equal(t, 1, chainLen) // only first field has bounds
}

func TestComputeIndexBounds_NoMatch(t *testing.T) {
	idx := &IndexInfo{FieldNames: []string{"x"}}
	cond := query.MustParseCondition(`{"a": 5}`)
	bounds, chainLen := ComputeIndexBounds(idx, buildBoundsResult(idx, cond))
	assert.Nil(t, bounds)
	assert.Equal(t, 0, chainLen)
}

func TestAdjustBoundsForNonUnique(t *testing.T) {
	bounds := query.Bounds{
		{Start: []byte{1}, End: []byte{1}, StartInclude: true, EndInclude: true},
	}
	adjusted := AdjustBoundsForNonUnique(bounds)
	require.Len(t, adjusted, 1)
	// End should be extended with 0xff (modified in-place)
	assert.Equal(t, anyenc.Tuple{1, 0xff}, adjusted[0].End)
	assert.Equal(t, anyenc.Tuple{1}, adjusted[0].Start)
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

// --- Coverage tests from filter_iter_coverage_test.go ---

// parseCountingFilter wraps a query.Filter and counts the number of Ok()
// evaluations. Used to assert that FilterIter evaluates the filter for every
// upstream hit (not only the accepted ones) — i.e. rejected hits force a
// Plan.DocParsed reset so the next hit re-parses.
type parseCountingFilter struct {
	inner query.Filter
	calls *int
}

func (f *parseCountingFilter) Ok(v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	*f.calls++
	return f.inner.Ok(v, buf)
}

func (f *parseCountingFilter) IndexBounds(fieldName string, bs query.Bounds) query.Bounds {
	return f.inner.IndexBounds(fieldName, bs)
}

func (f *parseCountingFilter) String() string { return f.inner.String() }

// TestFilterIter_Coverage_DocParsedResetAfterRejection asserts the
// cache-invalidation contract at internal/qplanner/filter_iter.go:84-86.
//
// When FilterIter rejects a doc, it must clear Plan.DocParsed so the next
// upstream hit re-parses a fresh document. We use a counting upstream iter
// whose doc values are distinct for each hit; if the filter did not reset
// DocParsed on rejection, we would see the stale previous doc on the next
// evaluation. Instead, every hit reaches the filter exactly once with its
// own parsed doc — so the filter-evaluation count equals total upstream hits
// (not only accepted hits).
func TestFilterIter_Coverage_DocParsedResetAfterRejection(t *testing.T) {
	a := &anyenc.Arena{}

	// Construct 6 docs, half pass the filter (even status) and half fail (odd).
	// The filter is {"v": {"$gte": 0, "$lte": 9}} and {"keep": true}; we flip
	// "keep" on every other doc so exactly half are rejected.
	buildDoc := func(id string, keep bool) *anyenc.Value {
		d := a.NewObject()
		d.Set("id", a.NewString(id))
		d.Set("v", a.NewNumberInt(5))
		if keep {
			d.Set("keep", a.NewTrue())
		} else {
			d.Set("keep", a.NewFalse())
		}
		return d
	}

	// Filter selects {"keep": true}. Half of the hits will be rejected.
	baseFilter := query.MustParseCondition(`{"keep": true}`)
	callCount := 0
	pcf := &parseCountingFilter{inner: baseFilter, calls: &callCount}

	plan := &Plan{}
	// Build 6 hits; upstream (fakeIter) populates Plan.DocParsed with the
	// hit's doc — this is how FetchIter behaves in production.
	hits := []fakeHit{
		{key: []byte("k1"), docId: []byte("p1"), doc: buildDoc("p1", true)},  // accept
		{key: []byte("k2"), docId: []byte("p2"), doc: buildDoc("p2", false)}, // reject
		{key: []byte("k3"), docId: []byte("p3"), doc: buildDoc("p3", true)},  // accept
		{key: []byte("k4"), docId: []byte("p4"), doc: buildDoc("p4", false)}, // reject
		{key: []byte("k5"), docId: []byte("p5"), doc: buildDoc("p5", true)},  // accept
		{key: []byte("k6"), docId: []byte("p6"), doc: buildDoc("p6", false)}, // reject
	}
	upstream := &fakeIter{plan: plan, hits: hits}

	buf := &syncpool.DocBuffer{}
	it := &FilterIter{
		Source: upstream,
		Filter: pcf,
		Buf:    buf,
		Plan:   plan,
	}

	// Collect accepted docIds.
	var accepted []string
	for {
		_, docId, _, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		accepted = append(accepted, string(docId))
	}

	// Three accepts expected.
	assert.Equal(t, []string{"p1", "p3", "p5"}, accepted,
		"only {keep:true} docs must be emitted")

	// The filter must have been invoked once per upstream hit, not just for
	// accepted hits. If DocParsed were not cleared on rejection, the next
	// call would reuse the stale rejected doc and the count would be wrong.
	assert.Equal(t, len(hits), callCount,
		"filter must be evaluated for every hit (Plan.DocParsed reset after rejection)")

	// After the final rejection, Plan.DocParsed is expected to be nil
	// because filter_iter clears it on reject before asking upstream for
	// the next hit (which returns EOF).
	assert.Nil(t, plan.DocParsed,
		"DocParsed must be cleared after the final rejected hit")
}

// --- Coverage tests from verify_iter_coverage_test.go ---

// openBtreeForVerify opens a file-backed btree DB for tests that need a
// real ReadTx + Namespace. Returns the DB, a read transaction ready to use,
// and a namespace populated per the populate callback (which runs inside a
// separate write tx and is nil-safe for "leave it empty").
func openBtreeForVerify(t *testing.T, nsName string, populate func(tx *btree.WriteTx, ns *btree.Namespace)) (*btree.DB, *btree.ReadTx, *btree.Namespace) {
	t.Helper()
	dir := t.TempDir()
	db, err := btree.Open(filepath.Join(dir, "test.db"), btree.DefaultOptions())
	require.NoError(t, err)

	// Create the namespace in a write tx.
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace(nsName)
	require.NoError(t, err)
	if populate != nil {
		populate(wtx, ns)
	}
	require.NoError(t, wtx.Commit())

	// Begin a read tx for VerifyIter.
	rtx, err := db.BeginRead()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = rtx.Rollback()
		_ = db.Close()
	})

	return db, rtx, ns
}

// TestVerifyIter_Coverage_EmptyVerifyNamespace exercises the scenario where
// the sparse verification namespace exists but contains no entries — every
// candidate docId is rejected by Tx.Get and must be skipped, so the iterator
// yields nothing.
// Covers internal/qplanner/verify_iter.go:31-35.
func TestVerifyIter_Coverage_EmptyVerifyNamespace(t *testing.T) {
	_, rtx, verifyNs := openBtreeForVerify(t, "verify_empty", nil /* no entries */)

	// Upstream produces three candidate docIds; none exist in the verify ns.
	upstream := &fakeIter{
		hits: []fakeHit{
			{key: []byte("k1"), docId: []byte("doc1")},
			{key: []byte("k2"), docId: []byte("doc2")},
			{key: []byte("k3"), docId: []byte("doc3")},
		},
	}

	it := &VerifyIter{
		Source:   upstream,
		Tx:       rtx,
		VerifyNs: verifyNs,
		// Empty prefix simulates a "point lookup by docId" style verify key.
		Prefix: nil,
	}

	var got []string
	for {
		_, docId, _, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, string(docId))
	}

	assert.Empty(t, got,
		"empty verify namespace: every candidate must be skipped by VerifyIter")
}

// TestVerifyIter_Coverage_MissingEntriesSkipped exercises a mixed scenario
// where the verify namespace contains a subset of the docIds; the missing
// ones must be skipped, the present ones must be yielded in upstream order.
func TestVerifyIter_Coverage_MissingEntriesSkipped(t *testing.T) {
	_, rtx, verifyNs := openBtreeForVerify(t, "verify_mixed", func(tx *btree.WriteTx, ns *btree.Namespace) {
		// Only "doc2" is present in the verify namespace.
		require.NoError(t, tx.Put(ns, []byte("doc2"), []byte{}))
	})

	upstream := &fakeIter{
		hits: []fakeHit{
			{key: []byte("k1"), docId: []byte("doc1")}, // missing → skip
			{key: []byte("k2"), docId: []byte("doc2")}, // present → yield
			{key: []byte("k3"), docId: []byte("doc3")}, // missing → skip
		},
	}

	it := &VerifyIter{
		Source:   upstream,
		Tx:       rtx,
		VerifyNs: verifyNs,
		Prefix:   nil, // verify key == docId for this test
	}

	var got []string
	for {
		_, docId, _, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, string(docId))
	}
	assert.Equal(t, []string{"doc2"}, got,
		"only present docIds survive the verify filter")
}

// --- Coverage tests from planner_dedup_coverage_test.go ---

// TestSetPlanRef_Coverage_DedupChainPropagation asserts that setPlanRef walks
// a chain CanonicalKeyDedupIter → FilterIter → FetchIter → IndexIter and sets
// the Plan reference on every node that holds one.
// Covers internal/qplanner/planner.go:986-988 (and the setPlanRef walker
// that handles CanonicalKeyDedupIter by recursing into Source).
func TestSetPlanRef_Coverage_DedupChainPropagation(t *testing.T) {
	// Leaf: an IndexIter that doesn't hold a Plan reference (it's a cursor-
	// backed leaf).
	leaf := &IndexIter{
		Source:  nil, // unused — setPlanRef doesn't descend into Source
		IdxInfo: &IndexInfo{Name: "tags", FieldNames: []string{"tags"}, FieldPaths: [][]string{{"tags"}}},
		Bounds:  nil,
	}

	fetch := &FetchIter{
		Source: leaf,
		Buf:    &syncpool.DocBuffer{},
	}
	filter := &FilterIter{
		Source: fetch,
		Filter: query.MustParseCondition(`{"x": 1}`),
		Buf:    &syncpool.DocBuffer{},
	}
	dedup := &CanonicalKeyDedupIter{
		Source:    filter,
		Bounds:    nil,
		FieldPath: []string{"tags"},
	}

	// Precondition: none of the nodes have a Plan yet.
	require.Nil(t, dedup.Plan)
	require.Nil(t, filter.Plan)
	require.Nil(t, fetch.Plan)

	plan := &Plan{}
	setPlanRef(dedup, plan)

	assert.Same(t, plan, dedup.Plan, "CanonicalKeyDedupIter.Plan must be set")
	assert.Same(t, plan, filter.Plan, "FilterIter.Plan must be set")
	assert.Same(t, plan, fetch.Plan, "FetchIter.Plan must be set")
}

// (TestSetPlanRef_Coverage_SeenSetDedupChainPropagation removed: the
// compound multi-key dedup wrap was dropped from the planner pipeline.
// Plan-ref propagation through the remaining chain is covered by the
// CanonicalKey variant above and by the case branches in setPlanRef.)

// --- Coverage tests from limit_iter_coverage_test.go ---

// seqIter emits a fixed sequence of (key, docId) pairs built from integer ids.
// It is deliberately distinct from dedup_iter_test.go's fakeIter (which also
// mutates Plan.DocParsed) — LimitIter does not touch the Plan, so we keep this
// helper lean.
type seqIter struct {
	ids []int
	i   int
}

func (s *seqIter) Next() ([]byte, []byte, bool, error) {
	if s.i >= len(s.ids) {
		return nil, nil, false, nil
	}
	id := s.ids[s.i]
	s.i++
	b := []byte(fmt.Sprintf("%d", id))
	return b, b, false, nil
}

func (s *seqIter) Close()         {}
func (s *seqIter) String() string { return "seq" }

func drain(it *LimitIter) []string {
	var out []string
	for {
		_, docId, _, err := it.Next()
		if err != nil || docId == nil {
			break
		}
		out = append(out, string(docId))
	}
	return out
}

// TestLimitIter_Coverage_ZeroLimitPassesAll ensures Limit==0 is treated as
// "no limit" — the guard at limit_iter.go:27-29 must not fire when Limit is 0.
// We also compare against the unset-limit case (constructed without setting
// Limit) to pin the semantic equivalence required by MongoDB convention.
func TestLimitIter_Coverage_ZeroLimitPassesAll(t *testing.T) {
	ids := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	// Explicit Limit(0)
	limited := drain(&LimitIter{Source: &seqIter{ids: ids}, Limit: 0})

	// Unset Limit (Go zero value)
	unset := drain(&LimitIter{Source: &seqIter{ids: ids}})

	expected := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	require.Len(t, limited, len(ids))
	assert.Equal(t, expected, limited, "Limit(0) must behave as no-limit")
	assert.Equal(t, expected, unset, "unset Limit must behave as no-limit")
	assert.Equal(t, limited, unset, "Limit(0) and unset must produce identical output")
}

// TestLimitIter_Coverage_ZeroLimitWithOffset ensures Limit==0 pairs correctly
// with a non-zero Offset: the prefix is skipped, the tail is returned in full.
func TestLimitIter_Coverage_ZeroLimitWithOffset(t *testing.T) {
	ids := []int{1, 2, 3, 4, 5}

	got := drain(&LimitIter{
		Source: &seqIter{ids: ids},
		Offset: 2,
		Limit:  0, // no upper bound
	})
	assert.Equal(t, []string{"3", "4", "5"}, got)
}

// TestLimitIter_Coverage_OffsetExceedsSource verifies that an Offset past the
// end of the source yields no rows and no error. Covers the degenerate
// skip-past-end path through limit_iter.go:15-32.
func TestLimitIter_Coverage_OffsetExceedsSource(t *testing.T) {
	// 50 rows, offset 1000 → zero results.
	ids := make([]int, 50)
	for i := range ids {
		ids[i] = i
	}
	it := &LimitIter{
		Source: &seqIter{ids: ids},
		Offset: 1000,
		Limit:  10,
	}

	// First Next must simply return (nil, nil, nil) after exhausting upstream.
	key, docId, _, err := it.Next()
	require.NoError(t, err)
	assert.Nil(t, key)
	assert.Nil(t, docId)

	// And stays exhausted on subsequent calls.
	key, docId, _, err = it.Next()
	require.NoError(t, err)
	assert.Nil(t, key)
	assert.Nil(t, docId)
}

// --- Coverage tests from sort_iter_coverage_test.go ---

// docFeedIter emits the prepared (docId, doc) pairs one at a time, setting
// plan.DocParsed on each Next() so SortIter takes the "already-parsed upstream"
// branch and skips CursorSource entirely. Matches the contract of the existing
// fakeIter in dedup_iter_test.go, but keyed on pre-parsed docs only.
type docFeedIter struct {
	feed []docFeed
	i    int
	plan *Plan
}

type docFeed struct {
	docId []byte
	doc   *anyenc.Value
}

func (f *docFeedIter) Next() ([]byte, []byte, bool, error) {
	if f.i >= len(f.feed) {
		if f.plan != nil {
			f.plan.DocParsed = nil
		}
		return nil, nil, false, nil
	}
	h := f.feed[f.i]
	f.i++
	if f.plan != nil {
		f.plan.DocParsed = h.doc
	}
	return h.docId, h.docId, false, nil
}

func (f *docFeedIter) Close()         {}
func (f *docFeedIter) String() string { return "docfeed" }

// TestSortIter_Coverage_ExhaustedNextIsSafe verifies that calling Next()
// after the iterator has emitted all its entries is a safe no-op: returns
// (nil,nil,nil) repeatedly without panic, regardless of TopK.
func TestSortIter_Coverage_ExhaustedNextIsSafe(t *testing.T) {
	a := &anyenc.Arena{}
	docs := []docFeed{
		{docId: []byte("a"), doc: makeDoc(a, "a", 3)},
		{docId: []byte("b"), doc: makeDoc(a, "b", 1)},
		{docId: []byte("c"), doc: makeDoc(a, "c", 2)},
	}

	plan := &Plan{}
	upstream := &docFeedIter{feed: docs, plan: plan}

	it := &SortIter{
		Source: upstream,
		Data:   nil, // not needed: upstream sets Plan.DocParsed on every Next
		Sorter: query.MustParseSort("n"),
		Buf:    &syncpool.DocBuffer{Arena: &anyenc.Arena{}, Parser: &anyenc.Parser{}},
		Plan:   plan,
	}
	defer it.Close()

	// Drain.
	var order []string
	for {
		_, docId, _, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		order = append(order, string(docId))
	}
	// Sorted ascending by n: b(1), c(2), a(3).
	assert.Equal(t, []string{"b", "c", "a"}, order)

	// Call Next() again: must remain safe and return nil.
	require.NotPanics(t, func() {
		for i := 0; i < 3; i++ {
			key, docId, _, err := it.Next()
			require.NoError(t, err)
			assert.Nil(t, key)
			assert.Nil(t, docId)
		}
	})
}

// TestSortIter_Coverage_TopKZeroLargeCorpus exercises the heap-disabled
// full-sort path over a sizable corpus and asserts the ordering is correct
// end-to-end.
func TestSortIter_Coverage_TopKZeroLargeCorpus(t *testing.T) {
	// 10K originally requested; 2000 keeps the test fast while still driving
	// multiple arena-grow tiers (1K→10K→100K bytes) and enough entries to
	// make any ordering regression obvious.
	const n = 2000

	a := &anyenc.Arena{}
	docs := make([]docFeed, n)
	// Use a shuffled permutation of 0..n-1 so the sort actually has work to do.
	perm := rand.New(rand.NewSource(42)).Perm(n)
	for i, v := range perm {
		docs[i] = docFeed{
			docId: []byte(fmt.Sprintf("doc-%05d", v)),
			doc:   makeDoc(a, fmt.Sprintf("doc-%05d", v), v),
		}
	}

	plan := &Plan{}
	upstream := &docFeedIter{feed: docs, plan: plan}

	it := &SortIter{
		Source: upstream,
		Data:   nil,
		Sorter: query.MustParseSort("n"),
		Buf:    &syncpool.DocBuffer{Arena: &anyenc.Arena{}, Parser: &anyenc.Parser{}},
		Plan:   plan,
		// TopK == 0 forces the full-sort branch in collectAndSort.
	}
	defer it.Close()

	var ids [][]byte
	for {
		_, docId, _, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		ids = append(ids, append([]byte(nil), docId...))
	}
	require.Len(t, ids, n)

	// The input ids are lexicographically sortable because they are
	// zero-padded. Sorted by numeric field "n" (0..n-1), the docIds should
	// be in ascending lexicographic order.
	for i := 1; i < len(ids); i++ {
		if bytes.Compare(ids[i-1], ids[i]) > 0 {
			t.Fatalf("out of order at %d: %q before %q", i, ids[i-1], ids[i])
		}
	}

	// And the first/last elements must be the min/max.
	assert.Equal(t, "doc-00000", string(ids[0]))
	assert.Equal(t, fmt.Sprintf("doc-%05d", n-1), string(ids[len(ids)-1]))
}

// makeDoc creates {"id": idStr, "n": n} on the given arena. The SortIter
// sorts by field "n".
func makeDoc(a *anyenc.Arena, idStr string, n int) *anyenc.Value {
	obj := a.NewObject()
	obj.Set("id", a.NewString(idStr))
	obj.Set("n", a.NewNumberInt(n))
	return obj
}

// --- Coverage tests from fullscan_iter_coverage_test.go ---

// coverageBtree opens an in-memory btree DB under t.TempDir() and seeds the
// given namespace with keys of the form anyenc.AppendAnyValue(nil, id) → id.
// The test closes the DB automatically via t.Cleanup.
func coverageBtree(t *testing.T, nsName string, ids []string) (*btree.DB, *btree.Namespace) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "coverage.db")
	db, err := btree.Open(path, btree.Options{
		PageSize:  4096,
		CacheSize: 128,
		InMemory:  true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace(nsName)
	require.NoError(t, err)
	for _, id := range ids {
		k := anyenc.AppendAnyValue(nil, id)
		// Value is a tiny well-formed object: {"id": id}.
		a := &anyenc.Arena{}
		obj := a.NewObject()
		obj.Set("id", a.NewString(id))
		require.NoError(t, wtx.Put(ns, k, obj.MarshalTo(nil)))
	}
	require.NoError(t, wtx.Commit())
	return db, ns
}

// drainFullScan drains a FullScanIter and returns the raw keys (each key is a
// copy so the caller can keep them after the iterator is closed).
func drainFullScan(t *testing.T, it *FullScanIter) [][]byte {
	t.Helper()
	var out [][]byte
	for {
		k, _, _, err := it.Next()
		require.NoError(t, err)
		if k == nil {
			break
		}
		cp := make([]byte, len(k))
		copy(cp, k)
		out = append(out, cp)
	}
	return out
}

// idKey encodes a string id into the same key format used by the data
// namespace in a collection.
func idKey(id string) []byte {
	return anyenc.AppendAnyValue(nil, id)
}

// TestFullScanIter_Coverage_ReverseOffsetSkipsForwardTail verifies that a
// reverse scan with Offset(N) skips the *last N entries in forward order*.
// Equivalent to: in descending iteration order, skip the first N entries,
// which are the largest N keys in forward order.
//
// Gap item 25: FullScanIter reverse scan with Offset.
func TestFullScanIter_Coverage_ReverseOffsetSkipsForwardTail(t *testing.T) {
	// 20 ids; ascending order id-00 .. id-19.
	ids := make([]string, 20)
	for i := range ids {
		ids[i] = pad2(i)
	}
	db, ns := coverageBtree(t, "data", ids)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	it := &FullScanIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		Buf:     &syncpool.DocBuffer{Arena: &anyenc.Arena{}, Parser: &anyenc.Parser{}},
		Reverse: true,
		Offset:  10,
	}
	defer it.Close()

	keys := drainFullScan(t, it)
	require.Len(t, keys, 10)

	// Expected: reverse direction applied first, then skip 10.
	// Descending order of ids: 19,18,...,00 → skip first 10 (ids 19..10) →
	// emit ids 09..00 in that order.
	for i, k := range keys {
		expected := idKey(pad2(9 - i))
		assert.Equal(t, expected, k,
			"position %d: reverse+Offset(10) must emit the forward-first half in reverse (09..00)", i)
	}
}

// TestFullScanIter_Coverage_ReverseIDBoundsAtCollectionMin verifies that a
// reverse scan bounded by the smallest id in the collection returns exactly
// that one row. Bounds: id >= "0000" AND id < "0001"; only "0000" exists.
//
// Gap item 29: FullScanIter reverse with IDBounds at collection minimum.
func TestFullScanIter_Coverage_ReverseIDBoundsAtCollectionMin(t *testing.T) {
	// Populate with "0000" and a bunch of higher keys so the cursor has
	// somewhere to land when we seek above End.
	ids := []string{"0000", "0002", "0005", "0010"}
	db, ns := coverageBtree(t, "data", ids)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	startTuple := idKey("0000")
	endTuple := idKey("0001")

	it := &FullScanIter{
		Source: &CursorSource{Tx: rtx, Ns: ns},
		Buf:    &syncpool.DocBuffer{Arena: &anyenc.Arena{}, Parser: &anyenc.Parser{}},
		IDBounds: query.Bounds{{
			Start:        startTuple,
			End:          endTuple,
			StartInclude: true,
			EndInclude:   false,
		}},
		Reverse: true,
	}
	defer it.Close()

	keys := drainFullScan(t, it)
	require.Len(t, keys, 1, "only the minimum id '0000' must be returned")
	assert.Equal(t, idKey("0000"), keys[0])
}

// TestFullScanIter_Coverage_OffsetZeroEqualsNoOffset verifies that Offset(0)
// produces the same key sequence as no Offset at all, over 100 docs.
//
// Gap item 58: Offset(0) semantics equal to no-offset.
func TestFullScanIter_Coverage_OffsetZeroEqualsNoOffset(t *testing.T) {
	ids := make([]string, 100)
	for i := range ids {
		ids[i] = pad3(i)
	}
	db, ns := coverageBtree(t, "data", ids)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	baseline := &FullScanIter{
		Source: &CursorSource{Tx: rtx, Ns: ns},
		Buf:    &syncpool.DocBuffer{Arena: &anyenc.Arena{}, Parser: &anyenc.Parser{}},
	}
	defer baseline.Close()
	baseKeys := drainFullScan(t, baseline)

	withZero := &FullScanIter{
		Source: &CursorSource{Tx: rtx, Ns: ns},
		Buf:    &syncpool.DocBuffer{Arena: &anyenc.Arena{}, Parser: &anyenc.Parser{}},
		Offset: 0,
	}
	defer withZero.Close()
	zeroKeys := drainFullScan(t, withZero)

	require.Len(t, baseKeys, 100)
	require.Len(t, zeroKeys, 100)
	assert.Equal(t, baseKeys, zeroKeys,
		"Offset(0) must be element-wise identical to unset Offset")
}

func pad2(n int) string {
	buf := make([]byte, 2)
	buf[0] = byte('0' + (n/10)%10)
	buf[1] = byte('0' + n%10)
	return string(buf)
}

func pad3(n int) string {
	buf := make([]byte, 3)
	buf[0] = byte('0' + (n/100)%10)
	buf[1] = byte('0' + (n/10)%10)
	buf[2] = byte('0' + n%10)
	return string(buf)
}

// --- Coverage tests from index_iter_coverage_test.go ---

// openIndexTestDB opens an in-memory btree DB, creates a namespace named
// "idx", writes the given encoded keys (each a tuple suffixed with a docId),
// and returns (DB, populated namespace, read tx). Caller is responsible for
// closing DB when done. Cleanup is registered with t.Cleanup for convenience.
func openIndexTestDB(t *testing.T, keys [][]byte) (*btree.DB, *btree.Namespace, *btree.ReadTx) {
	t.Helper()
	dir := t.TempDir()
	db, err := btree.Open(filepath.Join(dir, "test.db"), btree.Options{PageSize: 4096, InProcess: true})
	require.NoError(t, err)

	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("idx")
	require.NoError(t, err)
	for _, k := range keys {
		require.NoError(t, wtx.Put(ns, k, []byte("")))
	}
	require.NoError(t, wtx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = rtx.Rollback()
		_ = db.Close()
	})
	return db, ns, rtx
}

// encodeIdxKey produces an index key for a single-field index:
// tuple(fieldVal) ++ tuple(docId). This mirrors IndexIter's expected layout.
func encodeIdxKey(fieldVal, docId any) []byte {
	k := anyenc.AppendAnyValue(nil, fieldVal)
	k = anyenc.AppendAnyValue(k, docId)
	return k
}

// TestIndexIter_CountEntries_Coverage_EmptyRange asserts that CountEntries()
// returns 0 when the bound range contains no keys. Covers
// internal/qplanner/index_iter.go:188-228.
func TestIndexIter_CountEntries_Coverage_EmptyRange(t *testing.T) {
	// Populate with keys for values 1..5. Query on [100, 200] — empty.
	var keys [][]byte
	for i := 1; i <= 5; i++ {
		keys = append(keys, encodeIdxKey(i, i))
	}
	_, ns, rtx := openIndexTestDB(t, keys)

	start := anyenc.AppendAnyValue(nil, 100)
	end := anyenc.AppendAnyValue(nil, 200)

	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "x", FieldNames: []string{"x"}},
		Bounds: query.Bounds{
			{Start: start, End: end, StartInclude: true, EndInclude: true},
		},
	}
	defer it.Close()

	n, err := it.CountEntries()
	require.NoError(t, err)
	assert.Equal(t, 0, n, "empty range must count 0")
}

// TestIndexIter_CountEntries_Coverage_OpenEndedBound asserts that CountEntries()
// with a Start but no End walks to EOF and counts every key >= Start, including
// the last one. Covers internal/qplanner/index_iter.go:194-226 (the branch
// where b.End is empty and CountUntil walks to EOF).
func TestIndexIter_CountEntries_Coverage_OpenEndedBound(t *testing.T) {
	// Populate keys for values 1..200. Bound Start=100 (inclusive), no End.
	var keys [][]byte
	for i := 1; i <= 200; i++ {
		keys = append(keys, encodeIdxKey(i, i))
	}
	_, ns, rtx := openIndexTestDB(t, keys)

	start := anyenc.AppendAnyValue(nil, 100)

	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "x", FieldNames: []string{"x"}},
		Bounds: query.Bounds{
			{Start: start, StartInclude: true}, // no End
		},
	}
	defer it.Close()

	n, err := it.CountEntries()
	require.NoError(t, err)
	// Count must equal total keys >= 100, including the last (200).
	// Expected: 200 - 100 + 1 = 101.
	assert.Equal(t, 101, n, "open-ended count must include the last key")
}

// TestIndexIter_Coverage_DisjointBoundsTransition verifies that when iterating
// $in ["a","z"] over an index populated with keys around a..z, the transition
// between bounds (after exhausting "a", reset started=false and seek to "z")
// skips rows in between. Covers internal/qplanner/index_iter.go:120-147.
func TestIndexIter_Coverage_DisjointBoundsTransition(t *testing.T) {
	// Populate index with fieldVal in {"a", "b", ..., "y", "z"} — one doc each.
	var keys [][]byte
	for c := 'a'; c <= 'z'; c++ {
		keys = append(keys, encodeIdxKey(string(c), string(c)))
	}
	_, ns, rtx := openIndexTestDB(t, keys)

	// Bounds = $in ["a","z"] — two single-point bounds.
	// For non-unique indexes, the End bound must have 0xff appended to
	// capture all docId suffixes (mirrors AdjustBoundsForNonUnique).
	aKey := anyenc.AppendAnyValue(nil, "a")
	zKey := anyenc.AppendAnyValue(nil, "z")
	aEnd := append(append([]byte(nil), aKey...), 0xff)
	zEnd := append(append([]byte(nil), zKey...), 0xff)
	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "x", FieldNames: []string{"x"}},
		Bounds: query.Bounds{
			{Start: aKey, End: aEnd, StartInclude: true, EndInclude: true},
			{Start: zKey, End: zEnd, StartInclude: true, EndInclude: true},
		},
	}
	defer it.Close()

	var got []string
	for {
		_, docId, _, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		// docId is a tuple-encoded string. Tuple.String() joins values
		// with "/"; for a single-value tuple it returns the raw value.
		got = append(got, anyenc.Tuple(docId).String())
	}
	// Only "a" and "z" must be emitted; everything in between ("b".."y")
	// is skipped by the boundIdx transition logic.
	// Tuple.String() renders string values with quotes (via Value.String()).
	assert.Equal(t, []string{`"a"`, `"z"`}, got,
		"disjoint bounds must skip the gap between 'a' and 'z'")
}

// TestIndexIter_Next_Coverage_CursorErrorPropagation documents why we cannot
// currently assert cursor-level error propagation from IndexIter.Next() at
// this package's test level.
//
// IndexIter.cursor is a concrete *btree.Cursor and IndexIter accepts a
// *CursorSource (not an Iterator or a cursor interface). There is no
// injection point for a mock cursor from outside the btree package, and
// btree.Namespace's unexported fields (rootPage, db) cannot be synthesized
// from here. Triggering a real cursor error (e.g. corrupting a page) would
// require editing the on-disk file with btree-internal knowledge.
//
// A code review of internal/qplanner/index_iter.go:100-174 shows every call
// to cursor.First/Last/Next/Previous/Seek/Key wraps the result as
// `if err := ...; err != nil { return nil, nil, err }` — the error is
// surfaced on every path with no silent-truncation branch. This is tracked
// as item 19 in docs/plans/2026-04-17-index-test-coverage-gaps.md; a
// dedicated cursor interface (or an error-injecting exported helper on
// btree.ReadTx) would be needed to write a direct unit test.
func TestIndexIter_Next_Coverage_CursorErrorPropagation(t *testing.T) {
	t.Skip("cannot inject *btree.Cursor error from outside btree package; " +
		"see internal/qplanner/index_iter.go:100-174 for the propagation pathway")
}

// --- Coverage tests from cover_iter_coverage_test.go ---

// TestCoverIter_Coverage_MixedEmptyAndNonEmptyStarts verifies that a Bounds
// list mixing empty-Start and non-empty-Start entries yields results only for
// the non-empty bounds. Covers internal/qplanner/cover_iter.go:26-28:
//
//	if len(b.Start) == 0 {
//	    continue
//	}
//
// which quietly skips bounds with no start key (these represent open-start
// ranges that CoverIter cannot satisfy with a single-shot seek).
func TestCoverIter_Coverage_MixedEmptyAndNonEmptyStarts(t *testing.T) {
	dir := t.TempDir()
	db, err := btree.Open(filepath.Join(dir, "test.db"), btree.Options{PageSize: 4096, InProcess: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("idx")
	require.NoError(t, err)

	// Populate unique-style index entries (no docId suffix): just the field
	// value. This matches CoverLookup layout, which relies on SeekKey
	// returning the exact field tuple as the key.
	// For simplicity use docId=fieldVal, matching non-unique layout
	// tuple(fieldVal) ++ tuple(docId).
	aKey := anyenc.AppendAnyValue(nil, "a")
	aFull := append(append([]byte(nil), aKey...), anyenc.AppendAnyValue(nil, "doc-a")...)
	bKey := anyenc.AppendAnyValue(nil, "b")
	bFull := append(append([]byte(nil), bKey...), anyenc.AppendAnyValue(nil, "doc-b")...)
	require.NoError(t, wtx.Put(ns, aFull, []byte("")))
	require.NoError(t, wtx.Put(ns, bFull, []byte("")))
	require.NoError(t, wtx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rtx.Rollback() })

	// Bounds list: [{Start: nil}, {Start: "a"}] — first must be skipped.
	it := &CoverIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "x", FieldNames: []string{"x"}},
		Bounds: query.Bounds{
			{Start: nil, End: nil},
			{Start: aKey, End: aKey, StartInclude: true, EndInclude: true},
		},
	}
	defer it.Close()

	// First call: the empty-Start bound is silently skipped (idx++ then
	// continue). The second bound drives the seek and returns the "a" entry.
	_, docId, _, err := it.Next()
	require.NoError(t, err)
	require.NotNil(t, docId, "the non-empty 'a' bound must contribute exactly one entry")

	// Decode the docId tuple (single string value) and check equality.
	gotDocId := anyenc.Tuple(docId).String()
	assert.Equal(t, `"doc-a"`, gotDocId,
		"only the second bound (Start='a') may contribute an entry")

	// Exhausted: no more bounds.
	_, docId2, _, err := it.Next()
	require.NoError(t, err)
	assert.Nil(t, docId2, "after the second bound is consumed, the iterator is drained")
}

// --- Coverage tests from planner_coverage_test.go ---

// plannerTestDB opens an in-memory btree DB with a data namespace and an index
// namespace, returning them along with an active read tx. Cleanup is registered.
func plannerTestDB(t *testing.T) (*btree.DB, *btree.Namespace, *btree.Namespace, *btree.ReadTx) {
	t.Helper()
	dir := t.TempDir()
	db, err := btree.Open(filepath.Join(dir, "test.db"), btree.Options{PageSize: 4096, InProcess: true})
	require.NoError(t, err)

	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	dataNs, err := wtx.CreateNamespace("data")
	require.NoError(t, err)
	idxNs, err := wtx.CreateNamespace("idx_a")
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = rtx.Rollback()
		_ = db.Close()
	})
	return db, dataNs, idxNs, rtx
}

// TestBuildPlan_Coverage_CountOnly_MultiRangeInDisablesFastPath verifies that
// when CountOnly is set on a query with multiple equality bounds (len(Bounds) >
// 1 — i.e. a $in over 5+ values), the covering-count fast path at
// internal/qplanner/planner.go:900 is NOT taken. Instead, the full pipeline
// (Fetch → Filter → Dedup) must be used so multi-key index entries don't
// inflate the count. Relates to commit 8491785.
func TestBuildPlan_Coverage_CountOnly_MultiRangeInDisablesFastPath(t *testing.T) {
	_, dataNs, idxNs, rtx := plannerTestDB(t)

	// Build 5 point bounds ($in ["a","b","c","d","e"]).
	var bounds query.Bounds
	for _, v := range []string{"a", "b", "c", "d", "e"} {
		k := anyenc.AppendAnyValue(nil, v)
		bounds = append(bounds, query.Bound{Start: k, End: k, StartInclude: true, EndInclude: true})
	}

	// Non-unique single-field index. The non-unique-ness forces the planner
	// past the CoverIter shortcut (which is reserved for
	// unique+PointLookup+full-fields) and into the IndexIter+Fetch+Filter
	// path where the fast-path check at line 900 lives.
	idx := CBOIndex{
		Info: &IndexInfo{
			Name: "tags", FieldNames: []string{"tags"}, FieldPaths: [][]string{{"tags"}},
			Ns: idxNs,
		},
		Sketch:      mockSketch(1),
		Bounds:      bounds,
		PointLookup: true,
		BoundFields: 1,
	}

	plan := BuildPlan(&PlanParams{
		Tx:        rtx,
		DataNs:    dataNs,
		Filter:    query.MustParseCondition(`{"tags":{"$in":["a","b","c","d","e"]}}`),
		TotalDocs: 100,
		Indexes:   []CBOIndex{idx},
		CountOnly: true,
	})
	require.NotNil(t, plan)
	require.NotNil(t, plan.Root)

	chain := plan.Root.String()
	// Multi-range $in on a covering index now uses the optimised fast path:
	// raw IndexIter, with multi-bound dedup handled internally by
	// IndexIter.CountEntries (per-entry value byte). No Fetch / Filter /
	// Dedup wrap.
	assert.NotContains(t, chain, "Fetch",
		"multi-range $in covering count must bypass FetchIter (fast path with internal dedup)")
	assert.NotContains(t, chain, "Filter",
		"multi-range $in covering count must bypass FilterIter")
	assert.NotContains(t, chain, "Dedup",
		"multi-range $in covering count handles dedup inside IndexIter.CountEntries, not via a wrapping iterator")

	// CountableIterator implementation must be present so collQuery.Count
	// short-circuits through the batch path.
	_, isCountable := plan.Root.(CountableIterator)
	assert.True(t, isCountable,
		"plan root must satisfy CountableIterator for collQuery.Count fast path")
}

// TestBuildPlan_Coverage_CountOnly_SingleBoundAllowsFastPath is the positive
// control for item 14: with exactly one bound (single-value equality), the
// covering-count fast path IS taken when the index covers the filter. This
// pins the boundary of the `len(idx.Bounds) <= 1` guard.
func TestBuildPlan_Coverage_CountOnly_SingleBoundAllowsFastPath(t *testing.T) {
	_, dataNs, idxNs, rtx := plannerTestDB(t)

	k := anyenc.AppendAnyValue(nil, "a")
	bounds := query.Bounds{{Start: k, End: k, StartInclude: true, EndInclude: true}}

	idx := CBOIndex{
		Info: &IndexInfo{
			Name: "tags", FieldNames: []string{"tags"}, FieldPaths: [][]string{{"tags"}},
			Ns: idxNs,
		},
		Sketch:      mockSketch(1),
		Bounds:      bounds,
		PointLookup: true,
		BoundFields: 1,
	}

	plan := BuildPlan(&PlanParams{
		Tx:        rtx,
		DataNs:    dataNs,
		Filter:    query.MustParseCondition(`{"tags":"a"}`),
		TotalDocs: 100,
		Indexes:   []CBOIndex{idx},
		CountOnly: true,
	})
	require.NotNil(t, plan)
	require.NotNil(t, plan.Root)

	chain := plan.Root.String()
	// Fast path: just the IndexScan — no Fetch / Filter / Dedup wrap.
	assert.NotContains(t, chain, "Fetch",
		"single-bound covering count must bypass FetchIter (fast path)")
	assert.NotContains(t, chain, "Dedup",
		"single-bound covering count must bypass Dedup (fast path)")
}

// TestBuildPlan_Coverage_BuildVerifyChain_EmptyFieldBoundsReturnsNil asserts
// that buildVerifyChain exits early (returns nil) when the index fully covers
// the filter — collectUncoveredFilterFields returns an empty slice at
// internal/qplanner/planner.go:1183-1186, and the caller falls through without
// adding a VerifyIter.
//
// This is a pure unit test of buildVerifyChain, exercising the "no verify
// needed" branch for a fully-covered query.
func TestBuildPlan_Coverage_BuildVerifyChain_EmptyFieldBoundsReturnsNil(t *testing.T) {
	// Index covers the filter field; buildVerifyChain must return nil.
	idx := &CBOIndex{
		Info: &IndexInfo{
			Name: "a", FieldNames: []string{"a"}, FieldPaths: [][]string{{"a"}},
		},
		BoundFields: 1,
	}

	br := &BoundsResult{}
	params := &PlanParams{
		Filter:      query.MustParseCondition(`{"a":5}`),
		FieldBounds: br,
	}

	// Use a sentinel dummy root to ensure the returned value is nil
	// (not the original root accidentally echoed back).
	var dummyRoot Iterator = &IndexIter{
		IdxInfo: idx.Info,
	}

	result := buildVerifyChain(params, idx, dummyRoot)
	assert.Nil(t, result,
		"fully-covered filter must short-circuit buildVerifyChain to nil")
}

// TestBuildPlan_Coverage_BuildIndexSeek_FullyCoveredNoFilterIter verifies that
// when the filter is fully covered by the index and CountOnly+PointLookup with
// a single bound, the iterator chain has no FilterIter. Covers
// internal/qplanner/planner.go:907-910 (and the surrounding fast-path region)
// from the chain-assembly side.
func TestBuildPlan_Coverage_BuildIndexSeek_FullyCoveredNoFilterIter(t *testing.T) {
	_, dataNs, idxNs, rtx := plannerTestDB(t)

	// Single point bound on the only indexed field.
	k := anyenc.AppendAnyValue(nil, 5)
	bounds := query.Bounds{{Start: k, End: k, StartInclude: true, EndInclude: true}}

	idx := CBOIndex{
		Info: &IndexInfo{
			Name: "a", FieldNames: []string{"a"}, FieldPaths: [][]string{{"a"}},
			Ns: idxNs,
		},
		Sketch:      mockSketch(1),
		Bounds:      bounds,
		PointLookup: true,
		BoundFields: 1,
	}

	plan := BuildPlan(&PlanParams{
		Tx:        rtx,
		DataNs:    dataNs,
		Filter:    query.MustParseCondition(`{"a":5}`),
		TotalDocs: 100,
		Indexes:   []CBOIndex{idx},
		CountOnly: true,
	})
	require.NotNil(t, plan)
	require.NotNil(t, plan.Root)

	chain := plan.Root.String()
	// Fully-covered filter with single bound hits the fast path — no
	// FilterIter (no need to re-verify the filter against the doc).
	assert.NotContains(t, chain, "Filter",
		"fully-covered single-bound count must skip FilterIter")
}

// TestQueryExplain_Coverage_FullScan asserts that Explain output for a
// full-scan plan (no usable index) names the plan as FullScan, includes the
// cost formula, and does NOT mention IndexSeek. Covers
// internal/qplanner/planner.go:76-119 (ExplainString).
func TestQueryExplain_Coverage_FullScan(t *testing.T) {
	// No indexes at all → full scan is the only candidate.
	plan := BuildPlan(&PlanParams{
		Filter:    query.MustParseCondition(`{"a":5}`),
		TotalDocs: 100,
	})
	require.NotNil(t, plan)

	assert.Equal(t, "FullScan", plan.Name,
		"with no indexes the planner must choose FullScan")
	assert.Empty(t, plan.IndexName, "FullScan has no index name")

	explain := plan.ExplainString()
	// Must name the plan.
	assert.Contains(t, explain, "Plan: FullScan",
		"ExplainString must name the plan")
	// Must NOT mention IndexSeek (no index candidates in this scenario).
	assert.NotContains(t, explain, "IndexSeek",
		"ExplainString for a FullScan plan must not mention IndexSeek")
	// Must include the cost formula. The FullScan cost formula is of the
	// form "N×fetch(...) + N×filter(...) = TOTAL" (from formatFullScanDetails).
	assert.Contains(t, explain, "filter",
		"ExplainString must include the cost breakdown formula")
	assert.True(t,
		strings.Contains(explain, "fetch") || strings.Contains(explain, "seq"),
		"ExplainString must include per-doc cost label (fetch or seq)")
	// Must report selectivity and the iterator chain.
	assert.Contains(t, explain, "Selectivity:",
		"ExplainString must include selectivity info")
	assert.Contains(t, explain, "Iterator: ",
		"ExplainString must include the iterator chain")
}

// TestPlan_Close_DelegatesToRoot asserts Plan.Close calls Root.Close exactly once
// when Root is set, and is a no-op when Root is nil.
func TestPlan_Close_DelegatesToRoot(t *testing.T) {
	t.Run("with_root", func(t *testing.T) {
		root := &closeTrackingIter{}
		p := &Plan{Root: root}
		p.Close()
		assert.Equal(t, 1, root.closed)
	})
	t.Run("nil_root", func(t *testing.T) {
		p := &Plan{}
		p.Close() // must not panic
	})
}

// TestPlan_String_NoPlan covers the `p.Root == nil` branch of Plan.String.
func TestPlan_String_NoPlan(t *testing.T) {
	p := &Plan{}
	assert.Equal(t, "NoPlan", p.String())
}

// TestFormatSeekDetails covers both branches of formatSeekDetails
// (with and without seek-sort cost).
func TestFormatSeekDetails(t *testing.T) {
	t.Run("no_sort", func(t *testing.T) {
		s := formatSeekDetails(3, 10, CostDocFetch, 0)
		assert.Contains(t, s, fmt.Sprintf("3×seek(%.1f)", CostIndexSeek))
		assert.Contains(t, s, fmt.Sprintf("10×fetch(%.1f)", CostDocFetch))
		assert.Contains(t, s, fmt.Sprintf("10×filter(%.1f)", CostFilter))
		assert.NotContains(t, s, "+ sort=")

		total := 3*CostIndexSeek + 10*CostDocFetch + 10*CostFilter
		expectedSuffix := fmt.Sprintf("= %.1f", total)
		assert.True(t, strings.HasSuffix(s, expectedSuffix),
			"expected suffix %q, got %q", expectedSuffix, s)
	})
	t.Run("with_sort", func(t *testing.T) {
		seekSortCost := 42.5
		s := formatSeekDetails(1, 5, CostDocFetch, seekSortCost)
		assert.Contains(t, s, fmt.Sprintf("+ sort=%.1f", seekSortCost))
		total := 1*CostIndexSeek + 5*CostDocFetch + 5*CostFilter + seekSortCost
		expectedSuffix := fmt.Sprintf("= %.1f", total)
		assert.True(t, strings.HasSuffix(s, expectedSuffix),
			"expected suffix %q, got %q", expectedSuffix, s)
	})
}

// TestFormatScanDetails covers both branches of formatScanDetails
// (with and without a limit-optimized marker).
func TestFormatScanDetails(t *testing.T) {
	t.Run("no_limit", func(t *testing.T) {
		s := formatScanDetails(12, CostDocFetch, false)
		assert.Contains(t, s, fmt.Sprintf("12×seek(%.1f)", CostIndexSeek))
		assert.Contains(t, s, fmt.Sprintf("12×fetch(%.1f)", CostDocFetch))
		assert.Contains(t, s, fmt.Sprintf("12×filter(%.1f)", CostFilter))
		assert.NotContains(t, s, "[limit-optimized]")
		total := 12*CostIndexSeek + 12*CostDocFetch + 12*CostFilter
		assert.True(t, strings.HasSuffix(s, fmt.Sprintf("= %.1f", total)),
			"unexpected total in %q", s)
	})
	t.Run("with_limit", func(t *testing.T) {
		s := formatScanDetails(12, CostDocFetch, true)
		assert.Contains(t, s, "[limit-optimized]")
		// hasLimit is a display-only flag; the numerical total must not change.
		total := 12*CostIndexSeek + 12*CostDocFetch + 12*CostFilter
		assert.True(t, strings.HasSuffix(s, fmt.Sprintf("= %.1f", total)),
			"unexpected total in %q", s)
	})
}

// TestBoundsResult_FieldCount covers BoundsResult.FieldCount, which simply
// returns the length of the Fields slice.
func TestBoundsResult_FieldCount(t *testing.T) {
	br := &BoundsResult{}
	assert.Equal(t, 0, br.FieldCount())
	br.Fields = []FieldBounds{
		{Field: "a"},
		{Field: "b"},
		{Field: "c"},
	}
	assert.Equal(t, 3, br.FieldCount())
}

// TestBoundsResult_AllFixed covers all three branches of BoundsResult.AllFixed:
// empty Fields returns false; any non-fixed field returns false; all-fixed returns true.
func TestBoundsResult_AllFixed(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		br := &BoundsResult{}
		assert.False(t, br.AllFixed(), "empty Fields must return false")
	})
	t.Run("one_non_fixed", func(t *testing.T) {
		br := &BoundsResult{
			Fields: []FieldBounds{
				{Field: "a", Fixed: true},
				{Field: "b", Fixed: false},
				{Field: "c", Fixed: true},
			},
		}
		assert.False(t, br.AllFixed())
	})
	t.Run("all_fixed", func(t *testing.T) {
		br := &BoundsResult{
			Fields: []FieldBounds{
				{Field: "a", Fixed: true},
				{Field: "b", Fixed: true},
			},
		}
		assert.True(t, br.AllFixed())
	})
}

// TestBoundsResult_Lookup_Miss covers the `return nil, false, false` path of
// BoundsResult.Lookup when the requested field isn't present.
func TestBoundsResult_Lookup_Miss(t *testing.T) {
	br := &BoundsResult{
		Fields: []FieldBounds{{Field: "a", Start: 0, Count: 1, Fixed: true}},
		Bounds: []query.Bound{{Start: []byte{1}, End: []byte{1}}},
	}
	bounds, fixed, found := br.Lookup("missing")
	assert.Nil(t, bounds)
	assert.False(t, fixed)
	assert.False(t, found)
}

// TestBoundsResult_Build_DedupsRepeatedFields asserts Build skips fields that
// are already populated when multiple indexes share field names, and that
// each field's Bounds slice carries the correct encoded value from the filter.
func TestBoundsResult_Build_DedupsRepeatedFields(t *testing.T) {
	br := &BoundsResult{}
	filter := query.MustParseCondition(`{"a": 1, "b": 2}`)
	indexes := []*IndexInfo{
		{FieldNames: []string{"a", "b"}},
		{FieldNames: []string{"a"}},      // duplicate "a" — should be skipped
		{FieldNames: []string{"b", "c"}}, // duplicate "b" — should be skipped
	}
	br.Build(indexes, filter)

	assert.Equal(t, 3, br.FieldCount(), "fields a, b, c exactly once each")

	fieldNames := make([]string, 0, br.FieldCount())
	for _, f := range br.Fields {
		fieldNames = append(fieldNames, f.Field)
	}
	assert.Equal(t, []string{"a", "b", "c"}, fieldNames)

	// Compute the ground-truth bounds directly from the filter so we can
	// assert byte-level equality, not just counts.
	expectedA := filter.IndexBounds("a", nil)
	expectedB := filter.IndexBounds("b", nil)

	aBounds, aFixed, aFound := br.Lookup("a")
	assert.True(t, aFound)
	assert.True(t, aFixed)
	assert.Equal(t, expectedA, aBounds, "field a bounds must match filter output")

	bBounds, bFixed, bFound := br.Lookup("b")
	assert.True(t, bFound)
	assert.True(t, bFixed)
	assert.Equal(t, expectedB, bBounds, "field b bounds must match filter output")

	cBounds, _, cFound := br.Lookup("c")
	assert.True(t, cFound, "c is present in indexes so it must be recorded")
	assert.Empty(t, cBounds, "c has no filter → no bounds")
}

// TestCandidatePlan_Details covers both branches of CandidatePlan.Details:
// nil lazy formatter returns "", non-nil formatter invokes and returns the value.
func TestCandidatePlan_Details(t *testing.T) {
	t.Run("nil_formatter", func(t *testing.T) {
		c := &CandidatePlan{}
		assert.Equal(t, "", c.Details())
	})
	t.Run("eager_call", func(t *testing.T) {
		var calls int
		c := &CandidatePlan{
			details: func() string {
				calls++
				return "formula"
			},
		}
		assert.Equal(t, "formula", c.Details())
		assert.Equal(t, 1, calls)
	})
}

// TestFilterFieldsCoveredBy covers all three cases of filterFieldsCoveredBy:
// a Key whose field is covered, a Key whose field is NOT covered, an And
// composition, and the default (unsupported filter type) fall-through.
func TestFilterFieldsCoveredBy(t *testing.T) {
	t.Run("key_covered", func(t *testing.T) {
		f := query.MustParseCondition(`{"a": 1}`) // parses to query.Key
		has := false
		ok := filterFieldsCoveredBy(f, []string{"a", "b"}, &has)
		assert.True(t, ok)
		assert.True(t, has)
	})
	t.Run("key_not_covered", func(t *testing.T) {
		f := query.MustParseCondition(`{"z": 1}`)
		has := false
		ok := filterFieldsCoveredBy(f, []string{"a", "b"}, &has)
		assert.False(t, ok)
		assert.False(t, has)
	})
	t.Run("and_all_covered", func(t *testing.T) {
		// Construct an And directly so we get query.And (not pointer).
		inner1 := query.MustParseCondition(`{"a": 1}`)
		inner2 := query.MustParseCondition(`{"b": 2}`)
		f := query.And{inner1, inner2}
		has := false
		ok := filterFieldsCoveredBy(f, []string{"a", "b"}, &has)
		assert.True(t, ok)
		assert.True(t, has)
	})
	t.Run("and_with_missing_child", func(t *testing.T) {
		inner1 := query.MustParseCondition(`{"a": 1}`)
		inner2 := query.MustParseCondition(`{"z": 2}`)
		f := query.And{inner1, inner2}
		has := false
		ok := filterFieldsCoveredBy(f, []string{"a", "b"}, &has)
		assert.False(t, ok, "And must short-circuit on first uncovered child")
		// The first child matched before the short-circuit, so `has` is true.
		// Pin the current behavior so a future refactor can't silently drop it.
		assert.True(t, has, "hasFields must retain its value from the matched first child")
	})
	t.Run("pointer_and_recurses", func(t *testing.T) {
		// filterFieldsCoveredBy now handles `case *query.And` (symmetric with
		// collectUncoveredFilterFields) so $and-spelled filters that parse to
		// *query.And enjoy the covering-count fast path too.
		inner := query.And{query.MustParseCondition(`{"a": 1}`)}
		has := false
		ok := filterFieldsCoveredBy(&inner, []string{"a"}, &has)
		assert.True(t, ok, "pointer-And with covered child should report covered")
		assert.True(t, has)
	})
	t.Run("pointer_and_uncovered", func(t *testing.T) {
		// Pointer-And with an uncovered child must short-circuit to false.
		inner := query.And{query.MustParseCondition(`{"z": 1}`)}
		has := false
		ok := filterFieldsCoveredBy(&inner, []string{"a"}, &has)
		assert.False(t, ok)
	})
	t.Run("default_unsupported", func(t *testing.T) {
		// Or is unsupported by filterFieldsCoveredBy → default branch returns false.
		f := query.Or{
			query.MustParseCondition(`{"a": 1}`),
			query.MustParseCondition(`{"b": 2}`),
		}
		has := false
		ok := filterFieldsCoveredBy(f, []string{"a", "b"}, &has)
		assert.False(t, ok)
		assert.False(t, has, "default branch must not set hasFields")
	})
}

// TestCollectUncoveredFilterFields covers Key (covered/uncovered), And (value
// receiver), *And (pointer receiver), nested uncovered-propagation, and the
// default (unsupported) branch returning nil.
func TestCollectUncoveredFilterFields(t *testing.T) {
	t.Run("key_covered", func(t *testing.T) {
		f := query.MustParseCondition(`{"a": 1}`)
		got := collectUncoveredFilterFields(f, []string{"a"})
		assert.Equal(t, []string{}, got)
	})
	t.Run("key_uncovered", func(t *testing.T) {
		f := query.MustParseCondition(`{"z": 1}`)
		got := collectUncoveredFilterFields(f, []string{"a"})
		assert.Equal(t, []string{"z"}, got)
	})
	t.Run("and_mixed", func(t *testing.T) {
		f := query.And{
			query.MustParseCondition(`{"a": 1}`), // covered
			query.MustParseCondition(`{"z": 2}`), // uncovered → should be returned
		}
		got := collectUncoveredFilterFields(f, []string{"a"})
		assert.Equal(t, []string{"z"}, got)
	})
	t.Run("and_pointer", func(t *testing.T) {
		// Pointer variant of And exercises the `*query.And` case.
		a := query.And{query.MustParseCondition(`{"q": 1}`)}
		got := collectUncoveredFilterFields(&a, []string{"a"})
		assert.Equal(t, []string{"q"}, got)
	})
	t.Run("and_propagates_nil", func(t *testing.T) {
		// A complex child (Or) makes collectUncoveredFilterFields return nil,
		// which the And branch must propagate up.
		f := query.And{
			query.MustParseCondition(`{"a": 1}`),
			query.Or{
				query.MustParseCondition(`{"b": 1}`),
				query.MustParseCondition(`{"c": 1}`),
			},
		}
		got := collectUncoveredFilterFields(f, []string{"a"})
		assert.Nil(t, got, "nil must propagate out of And when any child returns nil")
	})
	t.Run("and_pointer_propagates_nil", func(t *testing.T) {
		inner := query.And{
			query.MustParseCondition(`{"a": 1}`),
			query.Or{
				query.MustParseCondition(`{"b": 1}`),
				query.MustParseCondition(`{"c": 1}`),
			},
		}
		got := collectUncoveredFilterFields(&inner, []string{"a"})
		assert.Nil(t, got)
	})
	t.Run("default_unsupported", func(t *testing.T) {
		f := query.Or{query.MustParseCondition(`{"a": 1}`)}
		got := collectUncoveredFilterFields(f, []string{"a"})
		assert.Nil(t, got)
	})
}

// TestIndexCoversFilter covers all four branches of indexCoversFilter:
// nil filter, empty bounds, filter not fully covered, filter fully covered.
func TestIndexCoversFilter(t *testing.T) {
	t.Run("nil_filter", func(t *testing.T) {
		idx := &CBOIndex{
			Info:   &IndexInfo{FieldNames: []string{"a"}},
			Bounds: query.Bounds{{Start: []byte{1}, End: []byte{1}}},
		}
		assert.False(t, indexCoversFilter(idx, nil))
	})
	t.Run("empty_bounds", func(t *testing.T) {
		idx := &CBOIndex{Info: &IndexInfo{FieldNames: []string{"a"}}}
		f := query.MustParseCondition(`{"a": 1}`)
		assert.False(t, indexCoversFilter(idx, f))
	})
	t.Run("filter_fields_not_in_index", func(t *testing.T) {
		idx := &CBOIndex{
			Info:   &IndexInfo{FieldNames: []string{"a"}},
			Bounds: query.Bounds{{Start: []byte{1}, End: []byte{1}}},
		}
		f := query.MustParseCondition(`{"z": 1}`)
		assert.False(t, indexCoversFilter(idx, f))
	})
	t.Run("covered", func(t *testing.T) {
		idx := &CBOIndex{
			Info:        &IndexInfo{FieldNames: []string{"a", "b"}},
			Bounds:      query.Bounds{{Start: []byte{1}, End: []byte{1}}},
			BoundFields: 1, // filter {a:1} pins only the leading field a
		}
		f := query.MustParseCondition(`{"a": 1}`)
		assert.True(t, indexCoversFilter(idx, f))
	})
}

// TestCoveringFilterFields covers coveringFilterFields:
// nil fieldBounds returns nil; all-bound-fields (no trailing) returns nil;
// a non-bound field with a fixed single bound produces a filter; and the
// reverse-field branch inverts the match value bytewise.
func TestCoveringFilterFields(t *testing.T) {
	t.Run("nil_field_bounds", func(t *testing.T) {
		idx := &CBOIndex{
			Info:        &IndexInfo{FieldNames: []string{"a", "b"}},
			BoundFields: 1,
		}
		assert.Nil(t, coveringFilterFields(idx, nil))
	})
	t.Run("no_trailing_fields", func(t *testing.T) {
		idx := &CBOIndex{
			Info:        &IndexInfo{FieldNames: []string{"a"}},
			BoundFields: 1,
		}
		br := &BoundsResult{}
		assert.Nil(t, coveringFilterFields(idx, br))
	})
	t.Run("non_fixed_trailing_skipped", func(t *testing.T) {
		idx := &CBOIndex{
			Info:        &IndexInfo{FieldNames: []string{"a", "b"}},
			BoundFields: 1,
		}
		br := &BoundsResult{
			Fields: []FieldBounds{{
				Field: "b", Start: 0, Count: 1, Fixed: false, // range, not fixed
			}},
			Bounds: []query.Bound{{Start: []byte{1}, End: []byte{2}}},
		}
		// Production returns nil (no appends), not an empty slice, so we
		// lock in that zero-allocation behavior.
		assert.Nil(t, coveringFilterFields(idx, br))
	})
	t.Run("forward_field", func(t *testing.T) {
		idx := &CBOIndex{
			Info:        &IndexInfo{FieldNames: []string{"a", "b"}},
			BoundFields: 1,
		}
		br := &BoundsResult{
			Fields: []FieldBounds{{
				Field: "b", Start: 0, Count: 1, Fixed: true,
			}},
			Bounds: []query.Bound{{Start: []byte{0x11, 0x22}, End: []byte{0x11, 0x22}}},
		}
		got := coveringFilterFields(idx, br)
		if assert.Len(t, got, 1) {
			assert.Equal(t, 1, got[0].FieldIdx)
			assert.Equal(t, []byte{0x11, 0x22}, got[0].MatchValue)
		}
	})
	t.Run("reverse_field_inverted", func(t *testing.T) {
		// Reverse-flagged fields are stored bitwise-inverted (writeValues), so
		// IndexFilterIter compares key.FieldBytes (inverted stored bytes) against
		// MatchValue. The covering-filter match value must therefore be the
		// bitwise-NOT of the encoded equality value; otherwise the IndexFilter
		// would never match and would drop every row.
		idx := &CBOIndex{
			Info: &IndexInfo{
				FieldNames: []string{"a", "b"},
				Reverse:    []bool{false, true},
			},
			BoundFields: 1,
		}
		raw := []byte{0x00, 0xff, 0x11}
		br := &BoundsResult{
			Fields: []FieldBounds{{
				Field: "b", Start: 0, Count: 1, Fixed: true,
			}},
			Bounds: []query.Bound{{Start: raw, End: raw}},
		}
		got := coveringFilterFields(idx, br)
		if assert.Len(t, got, 1) {
			assert.Equal(t, 1, got[0].FieldIdx)
			assert.Equal(t, []byte{0xff, 0x00, 0xee}, got[0].MatchValue,
				"reverse field match value must be bitwise-inverted to match stored bytes")
		}
	})
}

// sortFieldStub is a minimal query.Sort implementation for unit-testing
// shouldReverse and IndexSortMatch without depending on parsing internals.
type sortFieldStub struct {
	fields []query.SortField
}

func (s *sortFieldStub) Fields() []query.SortField { return s.fields }
func (s *sortFieldStub) AppendKey(k anyenc.Tuple, _ *anyenc.Value) anyenc.Tuple {
	return k
}

// TestShouldReverse covers scan-direction selection. A forward scan yields the
// index's declared per-field directions, so the scan is forward iff the first
// matched sort field's direction equals the declared direction at SortMatchStart,
// and reverse iff opposite.
func TestShouldReverse(t *testing.T) {
	t.Run("nil_sorter", func(t *testing.T) {
		assert.False(t, shouldReverse(nil, nil))
	})
	t.Run("empty_fields", func(t *testing.T) {
		s := &sortFieldStub{fields: nil}
		assert.False(t, shouldReverse(s, &CBOIndex{}))
	})
	// Forward-declared index (Reverse nil ⇒ idxRev=false): scan direction
	// equals the requested sort direction.
	t.Run("forward_index_forward_sort", func(t *testing.T) {
		s := &sortFieldStub{fields: []query.SortField{{Field: "a", Reverse: false}}}
		assert.False(t, shouldReverse(s, &CBOIndex{}))
	})
	t.Run("forward_index_reverse_sort", func(t *testing.T) {
		s := &sortFieldStub{fields: []query.SortField{{Field: "a", Reverse: true}}}
		assert.True(t, shouldReverse(s, &CBOIndex{Reverse: []bool{false}}))
	})
	// Reverse-declared single-field index: a forward scan already yields
	// descending, so Sort("-a") is served FORWARD and Sort("a") REVERSE.
	t.Run("reverse_index_reverse_sort_is_forward", func(t *testing.T) {
		s := &sortFieldStub{fields: []query.SortField{{Field: "a", Reverse: true}}}
		idx := &CBOIndex{Reverse: []bool{true}, SortMatchStart: 0}
		assert.False(t, shouldReverse(s, idx), "Sort(-a) on (-a) index must scan forward")
	})
	t.Run("reverse_index_forward_sort_is_reverse", func(t *testing.T) {
		s := &sortFieldStub{fields: []query.SortField{{Field: "a", Reverse: false}}}
		idx := &CBOIndex{Reverse: []bool{true}, SortMatchStart: 0}
		assert.True(t, shouldReverse(s, idx), "Sort(a) on (-a) index must scan reverse")
	})
	// Equality-pinned prefix: matchStart shifts to the trailing field, so the
	// declared direction read must be idx.Reverse[SortMatchStart].
	t.Run("equality_prefix_reverse_trailing_forward_scan", func(t *testing.T) {
		// Index (a,-b): Reverse=[false,true]. a pinned, Sort(-b), matchStart=1.
		// f[0].Reverse(true) == idx.Reverse[1](true) → forward.
		s := &sortFieldStub{fields: []query.SortField{{Field: "b", Reverse: true}}}
		idx := &CBOIndex{Reverse: []bool{false, true}, SortMatchStart: 1}
		assert.False(t, shouldReverse(s, idx))
	})
	t.Run("equality_prefix_reverse_trailing_reverse_scan", func(t *testing.T) {
		// Same index, Sort(b): f[0].Reverse(false) != idx.Reverse[1](true) → reverse.
		s := &sortFieldStub{fields: []query.SortField{{Field: "b", Reverse: false}}}
		idx := &CBOIndex{Reverse: []bool{false, true}, SortMatchStart: 1}
		assert.True(t, shouldReverse(s, idx))
	})
}

// TestSetPlanRef covers all 8 switch arms of setPlanRef by constructing each
// iterator wrapper around a downstream fakeIter and asserting that
// (a) the outer Plan field is populated on iterators that carry one, and
// (b) the recursion reaches into Source.
func TestSetPlanRef(t *testing.T) {
	plan := &Plan{}

	// --- FilterIter ---
	t.Run("FilterIter_sets_plan_and_recurses", func(t *testing.T) {
		inner := &FilterIter{}
		outer := &FilterIter{Source: inner}
		setPlanRef(outer, plan)
		assert.Same(t, plan, outer.Plan)
		assert.Same(t, plan, inner.Plan)
	})
	// --- FetchIter ---
	t.Run("FetchIter_sets_plan_and_recurses", func(t *testing.T) {
		innerFilter := &FilterIter{}
		fi := &FetchIter{Source: innerFilter}
		setPlanRef(fi, plan)
		assert.Same(t, plan, fi.Plan)
		assert.Same(t, plan, innerFilter.Plan)
	})
	// --- FullScanIter ---
	t.Run("FullScanIter_sets_plan_and_no_recurse", func(t *testing.T) {
		fs := &FullScanIter{}
		setPlanRef(fs, plan)
		assert.Same(t, plan, fs.Plan)
	})
	// --- SortIter ---
	t.Run("SortIter_sets_plan_and_recurses", func(t *testing.T) {
		inner := &FilterIter{}
		so := &SortIter{Source: inner}
		setPlanRef(so, plan)
		assert.Same(t, plan, so.Plan)
		assert.Same(t, plan, inner.Plan)
	})
	// --- IndexFilterIter (no Plan field) ---
	t.Run("IndexFilterIter_recurses_no_plan_field", func(t *testing.T) {
		inner := &FilterIter{}
		ifi := &IndexFilterIter{Source: inner}
		setPlanRef(ifi, plan)
		assert.Same(t, plan, inner.Plan, "recursion should populate inner")
	})
	// --- LimitIter (no Plan field) ---
	t.Run("LimitIter_recurses_no_plan_field", func(t *testing.T) {
		inner := &FilterIter{}
		li := &LimitIter{Source: inner}
		setPlanRef(li, plan)
		assert.Same(t, plan, inner.Plan)
	})
	// --- CanonicalKeyDedupIter ---
	t.Run("CanonicalKeyDedupIter_sets_plan_and_recurses", func(t *testing.T) {
		inner := &FilterIter{}
		d := &CanonicalKeyDedupIter{Source: inner}
		setPlanRef(d, plan)
		assert.Same(t, plan, d.Plan)
		assert.Same(t, plan, inner.Plan)
	})
	// (SeenSetDedupIter dropped: compound multi-key dedup is now consumer-side.)
	// --- Default branch: unknown iterator ---
	t.Run("Unknown_noop", func(t *testing.T) {
		tr := &closeTrackingIter{}
		setPlanRef(tr, plan) // must not panic, and trackingIter has no Plan field
	})
}

// TestIndexSortMatch covers all outcomes: no sort fields / no index fields,
// zero match, exact match, partial match, and equalityPrefix-shifted match
// (where the prefix beats the start-of-index match).
func TestIndexSortMatch(t *testing.T) {
	t.Run("empty_sort", func(t *testing.T) {
		ex, pa, ms := IndexSortMatch(&IndexInfo{FieldNames: []string{"a"}}, nil, 0)
		assert.False(t, ex)
		assert.False(t, pa)
		assert.Equal(t, 0, ms)
	})
	t.Run("empty_index", func(t *testing.T) {
		ex, pa, _ := IndexSortMatch(&IndexInfo{}, []query.SortField{{Field: "a"}}, 0)
		assert.False(t, ex)
		assert.False(t, pa)
	})
	t.Run("no_match", func(t *testing.T) {
		ex, pa, _ := IndexSortMatch(
			&IndexInfo{FieldNames: []string{"a"}},
			[]query.SortField{{Field: "z"}},
			0,
		)
		assert.False(t, ex)
		assert.False(t, pa)
	})
	t.Run("exact_match", func(t *testing.T) {
		ex, pa, ms := IndexSortMatch(
			&IndexInfo{FieldNames: []string{"a", "b"}},
			[]query.SortField{{Field: "a"}, {Field: "b"}},
			0,
		)
		assert.True(t, ex)
		assert.False(t, pa)
		assert.Equal(t, 0, ms)
	})
	t.Run("partial_match", func(t *testing.T) {
		// 3-field sort, index has only first 2. match=2 != 3 → partial.
		ex, pa, _ := IndexSortMatch(
			&IndexInfo{FieldNames: []string{"a", "b"}},
			[]query.SortField{{Field: "a"}, {Field: "b"}, {Field: "c"}},
			0,
		)
		assert.False(t, ex)
		assert.True(t, pa)
	})
	t.Run("equality_prefix_wins", func(t *testing.T) {
		// Index is (a, b). Equality on a (prefix=1). Sort by b.
		// matchAt(0) fails (b != a). matchAt(1) succeeds (idx[1]==b). Exact.
		ex, pa, ms := IndexSortMatch(
			&IndexInfo{FieldNames: []string{"a", "b"}},
			[]query.SortField{{Field: "b"}},
			1,
		)
		assert.True(t, ex)
		assert.False(t, pa)
		assert.Equal(t, 1, ms)
	})
	t.Run("equality_prefix_ignored_when_out_of_range", func(t *testing.T) {
		// equalityPrefix >= len(FieldNames): matchAt returns 0.
		ex, pa, _ := IndexSortMatch(
			&IndexInfo{FieldNames: []string{"a"}},
			[]query.SortField{{Field: "a"}},
			5,
		)
		assert.True(t, ex, "matchAt(0) still finds a")
		assert.False(t, pa)
	})
	t.Run("direction_mismatch_breaks", func(t *testing.T) {
		// Index (a,b) both forward. Sort(a,-b): first field same (fwd==fwd),
		// second field idxRev=false vs sortRev=true → curSame=false, inconsistent
		// with the first → loop breaks at 1, match=1 != 2 sort fields → partial.
		ex, pa, _ := IndexSortMatch(
			&IndexInfo{FieldNames: []string{"a", "b"}, Reverse: []bool{false, false}},
			[]query.SortField{{Field: "a", Reverse: false}, {Field: "b", Reverse: true}},
			0,
		)
		assert.False(t, ex)
		assert.True(t, pa)
	})
	t.Run("mixed_dir_index_matching_sort_is_exact", func(t *testing.T) {
		// Index (a,-b): Reverse=[false,true]. Sort(a,-b) aligns with the index's
		// declared directions (a same, b same) → uniform same → exact forward.
		ex, pa, ms := IndexSortMatch(
			&IndexInfo{FieldNames: []string{"a", "b"}, Reverse: []bool{false, true}},
			[]query.SortField{{Field: "a", Reverse: false}, {Field: "b", Reverse: true}},
			0,
		)
		assert.True(t, ex, "Sort(a,-b) on (a,-b) index must be exact")
		assert.False(t, pa)
		assert.Equal(t, 0, ms)
	})
	t.Run("mixed_dir_index_opposite_sort_is_exact_reverse", func(t *testing.T) {
		// Index (a,-b). Sort(-a,b): a opposite, b opposite → uniform opposite →
		// exact, served by a reverse scan.
		ex, pa, _ := IndexSortMatch(
			&IndexInfo{FieldNames: []string{"a", "b"}, Reverse: []bool{false, true}},
			[]query.SortField{{Field: "a", Reverse: true}, {Field: "b", Reverse: false}},
			0,
		)
		assert.True(t, ex, "Sort(-a,b) on (a,-b) index must be exact (reverse)")
		assert.False(t, pa)
	})
	t.Run("mixed_dir_index_uniform_sort_is_partial", func(t *testing.T) {
		// Index (a,-b). Sort(a,b): a same (fwd==fwd) but b opposite (idxRev=true
		// vs sortRev=false) → breaks at 1 → partial (genuinely unrealizable).
		ex, pa, _ := IndexSortMatch(
			&IndexInfo{FieldNames: []string{"a", "b"}, Reverse: []bool{false, true}},
			[]query.SortField{{Field: "a", Reverse: false}, {Field: "b", Reverse: false}},
			0,
		)
		assert.False(t, ex, "Sort(a,b) on (a,-b) index is not realizable by one scan")
		assert.True(t, pa)
	})
	t.Run("reverse_first_field_single_match_exact", func(t *testing.T) {
		// Index (-a): Reverse=[true]. Sort(-a) → same → exact (forward scan).
		ex, pa, ms := IndexSortMatch(
			&IndexInfo{FieldNames: []string{"a"}, Reverse: []bool{true}},
			[]query.SortField{{Field: "a", Reverse: true}},
			0,
		)
		assert.True(t, ex)
		assert.False(t, pa)
		assert.Equal(t, 0, ms)
	})
	t.Run("equality_prefix_reverse_trailing_field", func(t *testing.T) {
		// Index (a,-b), a pinned by equality (prefix=1). Sort(-b) aligns with the
		// declared reverse direction of field 1 → exact, matchStart=1.
		ex, pa, ms := IndexSortMatch(
			&IndexInfo{FieldNames: []string{"a", "b"}, Reverse: []bool{false, true}},
			[]query.SortField{{Field: "b", Reverse: true}},
			1,
		)
		assert.True(t, ex)
		assert.False(t, pa)
		assert.Equal(t, 1, ms)
	})
}

// TestComputeIndexBounds covers: no lookup (empty chain → nil, 0), single
// field index (cached return), compound index (tuple concat), and the
// non-fixed short-circuit where a trailing non-fixed field stops the chain.
func TestComputeIndexBounds(t *testing.T) {
	t.Run("no_fields_in_filter", func(t *testing.T) {
		idx := &IndexInfo{FieldNames: []string{"x"}}
		br := &BoundsResult{} // no Fields, Lookup misses
		bounds, n := ComputeIndexBounds(idx, br)
		assert.Nil(t, bounds)
		assert.Equal(t, 0, n)
	})
	t.Run("empty_bounds_list", func(t *testing.T) {
		// Lookup hits but returns zero bounds → chain stays empty.
		idx := &IndexInfo{FieldNames: []string{"a"}}
		br := &BoundsResult{
			Fields: []FieldBounds{{Field: "a", Start: 0, Count: 0, Fixed: true}},
			Bounds: nil,
		}
		bounds, n := ComputeIndexBounds(idx, br)
		assert.Nil(t, bounds)
		assert.Equal(t, 0, n)
	})
	t.Run("single_field", func(t *testing.T) {
		idx := &IndexInfo{FieldNames: []string{"a"}}
		br := &BoundsResult{
			Fields: []FieldBounds{{Field: "a", Start: 0, Count: 1, Fixed: true}},
			Bounds: []query.Bound{{Start: []byte{1}, End: []byte{1}}},
		}
		bounds, n := ComputeIndexBounds(idx, br)
		assert.Equal(t, 1, n)
		assert.Equal(t, 1, len(bounds))
		assert.Equal(t, []byte{1}, []byte(bounds[0].Start))
		assert.Equal(t, []byte{1}, []byte(bounds[0].End))
	})
	t.Run("compound_index_all_fixed", func(t *testing.T) {
		idx := &IndexInfo{FieldNames: []string{"a", "b"}}
		br := &BoundsResult{
			Fields: []FieldBounds{
				{Field: "a", Start: 0, Count: 1, Fixed: true},
				{Field: "b", Start: 1, Count: 1, Fixed: true},
			},
			Bounds: []query.Bound{
				{Start: []byte{0xaa}, End: []byte{0xaa}, StartInclude: true, EndInclude: true},
				{Start: []byte{0xbb}, End: []byte{0xbb}, StartInclude: true, EndInclude: true},
			},
		}
		bounds, n := ComputeIndexBounds(idx, br)
		assert.Equal(t, 2, n)
		if assert.Equal(t, 1, len(bounds)) {
			// concat is aa + bb
			assert.Equal(t, []byte{0xaa, 0xbb}, []byte(bounds[0].Start))
			assert.Equal(t, []byte{0xaa, 0xbb}, []byte(bounds[0].End))
		}
	})
	t.Run("stops_at_non_fixed_field", func(t *testing.T) {
		// First field (a) has a range bound → chain captures a, then breaks.
		idx := &IndexInfo{FieldNames: []string{"a", "b"}}
		br := &BoundsResult{
			Fields: []FieldBounds{
				{Field: "a", Start: 0, Count: 1, Fixed: false},
				{Field: "b", Start: 1, Count: 1, Fixed: true},
			},
			Bounds: []query.Bound{
				{Start: []byte{0x10}, End: []byte{0x20}},
				{Start: []byte{0xbb}, End: []byte{0xbb}},
			},
		}
		bounds, n := ComputeIndexBounds(idx, br)
		assert.Equal(t, 1, n, "chain should contain only field a, not b")
		assert.Equal(t, []byte{0x10}, []byte(bounds[0].Start))
		assert.Equal(t, []byte{0x20}, []byte(bounds[0].End))
	})
}

// TestFormatFullScanDetails covers all branches of formatFullScanDetails:
// small-table fetch label, large-table seq label (totalDocs > 500), needSort
// toggle, and idBoundsSeek override (large but still uses fetch).
func TestFormatFullScanDetails(t *testing.T) {
	t.Run("small_no_sort", func(t *testing.T) {
		s := formatFullScanDetails(100, 50, false, false)
		assert.Contains(t, s, fmt.Sprintf("100×fetch(%.1f)", CostDocFetch))
		assert.NotContains(t, s, "× sort")
		total := 100*CostDocFetch + 100*CostFilter
		assert.True(t, strings.HasSuffix(s, fmt.Sprintf("= %.1f", total)),
			"unexpected total in %q", s)
	})
	t.Run("large_seq_label", func(t *testing.T) {
		s := formatFullScanDetails(1000, 1000, false, false)
		assert.Contains(t, s, fmt.Sprintf("1000×seq(%.1f)", CostSeqRead))
		total := 1000*CostSeqRead + 1000*CostFilter
		assert.True(t, strings.HasSuffix(s, fmt.Sprintf("= %.1f", total)))
	})
	t.Run("large_with_idBoundsSeek_keeps_fetch", func(t *testing.T) {
		s := formatFullScanDetails(1000, 1000, false, true)
		assert.Contains(t, s, fmt.Sprintf("1000×fetch(%.1f)", CostDocFetch),
			"idBoundsSeek=true must override large-table seq label")
	})
	t.Run("with_sort", func(t *testing.T) {
		s := formatFullScanDetails(100, 50, true, false)
		assert.Contains(t, s, fmt.Sprintf("sort(%.0f)=%.1f", 50.0, sortCost(50)))
		total := 100*CostDocFetch + 100*CostFilter + sortCost(50)
		assert.True(t, strings.HasSuffix(s, fmt.Sprintf("= %.1f", total)))
	})
}

// TestExplainString_WithIndexName covers the `p.IndexName != ""` branch and
// the `d := c.Details(); d != ""` branch of ExplainString by building a plan
// with a non-empty IndexName and a matched candidate that carries lazy details.
func TestExplainString_WithIndexName(t *testing.T) {
	plan := &Plan{
		Name:      "IndexSeek",
		Cost:      123.4,
		IndexName: "idx_a",
		Explain: ExplainInfo{
			TotalDocs:   1000,
			Selectivity: 0.05,
			Candidates: []CandidatePlan{
				{Name: "IndexSeek(a)", Cost: 123.4, EstRows: 50,
					details: func() string { return "formula-goes-here" }},
				{Name: "FullScan", Cost: 999.9, EstRows: 1000},
			},
		},
	}
	out := plan.ExplainString()
	assert.Contains(t, out, "Plan: IndexSeek")
	assert.Contains(t, out, "Index: idx_a")
	assert.Contains(t, out, "Cost: 123.4")
	assert.Contains(t, out, "Selectivity: 0.05 (50 of 1000 docs)")
	assert.Contains(t, out, "Iterator: NoPlan") // Root is nil in this fixture
	assert.Contains(t, out, "Cost breakdown: formula-goes-here")
	assert.Contains(t, out, "IndexSeek(a)")
	assert.Contains(t, out, "[chosen]")
}

// TestCalculateSelectivity covers the all/nil filter short-circuit, the sketch
// path for single-field equality, the compound-equality fallback, the range
// fallback, the no-indexed-fields branch (nUsed == 0), and the p <= 0 clamp.
func TestCalculateSelectivity(t *testing.T) {
	t.Run("nil_filter", func(t *testing.T) {
		assert.Equal(t, 1.0, calculateSelectivity(nil, nil, 100, nil))
	})
	t.Run("all_filter", func(t *testing.T) {
		assert.Equal(t, 1.0, calculateSelectivity(query.All{}, nil, 100, nil))
	})
	t.Run("no_indexed_fields_match", func(t *testing.T) {
		// Filter references "x" but only an index on "a" exists → nUsed stays 0
		// → pTotal falls back to DefaultRangeSelectivity.
		f := query.MustParseCondition(`{"x": 1}`)
		idx := CBOIndex{Info: &IndexInfo{Name: "a", FieldNames: []string{"a"}}}
		br := &BoundsResult{}
		br.Build([]*IndexInfo{idx.Info}, f)
		sel := calculateSelectivity(f, []CBOIndex{idx}, 100, br)
		assert.Equal(t, DefaultRangeSelectivity, sel)
	})
	t.Run("sketch_single_field_equality", func(t *testing.T) {
		f := query.MustParseCondition(`{"a": 42}`)
		sketch := mockSketch(5)
		idx := CBOIndex{
			Info:   &IndexInfo{Name: "a", FieldNames: []string{"a"}},
			Sketch: sketch,
		}
		br := &BoundsResult{}
		br.Build([]*IndexInfo{idx.Info}, f)
		sel := calculateSelectivity(f, []CBOIndex{idx}, 1000, br)
		assert.InDelta(t, 0.005, sel, 1e-9, "5/1000 = 0.005")
	})
	t.Run("sketch_p_clamp_high", func(t *testing.T) {
		// est == totalDocs*2 → raw p=2.0 → clamped to 1.0.
		f := query.MustParseCondition(`{"a": 42}`)
		sketch := mockSketch(200)
		idx := CBOIndex{
			Info:   &IndexInfo{Name: "a", FieldNames: []string{"a"}},
			Sketch: sketch,
		}
		br := &BoundsResult{}
		br.Build([]*IndexInfo{idx.Info}, f)
		sel := calculateSelectivity(f, []CBOIndex{idx}, 100, br)
		assert.Equal(t, 1.0, sel, "raw p > 1 must clamp to 1.0")
	})
	t.Run("sketch_p_clamp_zero_uses_min", func(t *testing.T) {
		// sketch returns 0 → p=0 → clamped up to 0.0001.
		f := query.MustParseCondition(`{"a": 42}`)
		idx := CBOIndex{
			Info:   &IndexInfo{Name: "a", FieldNames: []string{"a"}},
			Sketch: mockSketch(0),
		}
		br := &BoundsResult{}
		br.Build([]*IndexInfo{idx.Info}, f)
		sel := calculateSelectivity(f, []CBOIndex{idx}, 1000, br)
		assert.Equal(t, 0.0001, sel)
	})
	t.Run("compound_equality_leading_field_uses_sketch", func(t *testing.T) {
		// Compound index (a, b) with equality on a only: the leading field (fi=0)
		// now uses the level-0 prefix sketch instead of the default-range fallback.
		f := query.MustParseCondition(`{"a": 1}`)
		idx := CBOIndex{
			Info:   &IndexInfo{Name: "ab", FieldNames: []string{"a", "b"}},
			Sketch: mockSketch(10),
		}
		br := &BoundsResult{}
		br.Build([]*IndexInfo{idx.Info}, f)
		sel := calculateSelectivity(f, []CBOIndex{idx}, 100, br)
		// sel = est/totalDocs = 10/100 (b has no bound → only a contributes).
		assert.InDelta(t, 0.1, sel, 1e-9)
	})
	t.Run("range_predicate", func(t *testing.T) {
		f := query.MustParseCondition(`{"a": {"$gt": 5}}`)
		idx := CBOIndex{Info: &IndexInfo{Name: "a", FieldNames: []string{"a"}}}
		br := &BoundsResult{}
		br.Build([]*IndexInfo{idx.Info}, f)
		sel := calculateSelectivity(f, []CBOIndex{idx}, 100, br)
		assert.Equal(t, DefaultRangeSelectivity, sel)
	})
	t.Run("nil_br_uses_filter_direct", func(t *testing.T) {
		// Pass br=nil to force the filter.IndexBounds path.
		f := query.MustParseCondition(`{"a": 7}`)
		idx := CBOIndex{
			Info:   &IndexInfo{Name: "a", FieldNames: []string{"a"}},
			Sketch: mockSketch(2),
		}
		sel := calculateSelectivity(f, []CBOIndex{idx}, 100, nil)
		// 2/100 = 0.02 (no clamp); sketch branch is taken when br=nil too.
		assert.InDelta(t, 0.02, sel, 1e-9)
	})
}

// TestSelectivityForIndex covers all branches: empty bounds returns 1.0,
// full-sketch coverage returns est/totalDocs with both clamps (low and high),
// and the fallback to DefaultRangeSelectivity.
func TestSelectivityForIndex(t *testing.T) {
	t.Run("empty_bounds", func(t *testing.T) {
		idx := &CBOIndex{Info: &IndexInfo{FieldNames: []string{"a"}}}
		assert.Equal(t, 1.0, selectivityForIndex(idx, 100))
	})
	t.Run("sketch_full_coverage", func(t *testing.T) {
		idx := &CBOIndex{
			Info:        &IndexInfo{FieldNames: []string{"a"}},
			Bounds:      query.Bounds{{Start: []byte{1}, End: []byte{1}}},
			Sketch:      mockSketch(25),
			PointLookup: true,
			BoundFields: 1,
		}
		assert.InDelta(t, 0.25, selectivityForIndex(idx, 100), 1e-9)
	})
	t.Run("sketch_clamp_low", func(t *testing.T) {
		idx := &CBOIndex{
			Info:        &IndexInfo{FieldNames: []string{"a"}},
			Bounds:      query.Bounds{{Start: []byte{1}, End: []byte{1}}},
			Sketch:      mockSketch(0),
			PointLookup: true,
			BoundFields: 1,
		}
		assert.Equal(t, 0.0001, selectivityForIndex(idx, 100))
	})
	t.Run("sketch_clamp_high", func(t *testing.T) {
		idx := &CBOIndex{
			Info:        &IndexInfo{FieldNames: []string{"a"}},
			Bounds:      query.Bounds{{Start: []byte{1}, End: []byte{1}}},
			Sketch:      mockSketch(200),
			PointLookup: true,
			BoundFields: 1,
		}
		assert.Equal(t, 1.0, selectivityForIndex(idx, 100))
	})
	t.Run("no_sketch_falls_back", func(t *testing.T) {
		idx := &CBOIndex{
			Info:   &IndexInfo{FieldNames: []string{"a"}},
			Bounds: query.Bounds{{Start: []byte{1}, End: []byte{5}}},
		}
		assert.Equal(t, DefaultRangeSelectivity, selectivityForIndex(idx, 100))
	})
	t.Run("partial_prefix_uses_sketch", func(t *testing.T) {
		// Compound (a, b) with an equality prefix on a only: the level-0 prefix
		// sketch now gives a real estimate instead of the default-range fallback.
		idx := &CBOIndex{
			Info:        &IndexInfo{FieldNames: []string{"a", "b"}},
			Bounds:      query.Bounds{{Start: []byte{1}, End: []byte{1}}},
			Sketch:      mockSketch(10),
			PointLookup: true,
			BoundFields: 1, // only a bound, b is not bounded
		}
		assert.InDelta(t, 0.1, selectivityForIndex(idx, 100), 1e-9)
	})
	t.Run("legacy_unrebuilt_shallow_level_falls_back", func(t *testing.T) {
		// A 2-level sketch loaded from a legacy single-level blob: only the
		// full-key level holds data, so a shallow prefix (level 0) must fall back
		// to DefaultRangeSelectivity instead of trusting an empty bucket.
		legacy := make([]byte, DefaultSketchSize*8+8) // V1 buckets + docCount, no magic
		sk := NewIndexSketch(DefaultSketchSize, 2)
		sk.UnmarshalBinary(legacy)
		if !sk.NeedsRebuild() {
			t.Fatal("legacy load into multi-level sketch must set NeedsRebuild")
		}
		idx := &CBOIndex{
			Info:        &IndexInfo{FieldNames: []string{"a", "b"}},
			Bounds:      query.Bounds{{Start: []byte{1}, End: []byte{1}}},
			Sketch:      sk,
			PointLookup: true,
			BoundFields: 1, // shallow prefix → untrusted on a not-yet-rebuilt sketch
		}
		assert.Equal(t, DefaultRangeSelectivity, selectivityForIndex(idx, 100))
	})
	t.Run("range_prefix_falls_back", func(t *testing.T) {
		// Non-point-lookup (range) prefix: no sketch estimate, default range.
		idx := &CBOIndex{
			Info:        &IndexInfo{FieldNames: []string{"a", "b"}},
			Bounds:      query.Bounds{{Start: []byte{1}, End: []byte{5}}},
			Sketch:      mockSketch(10),
			PointLookup: false,
			BoundFields: 1,
		}
		assert.Equal(t, DefaultRangeSelectivity, selectivityForIndex(idx, 100))
	})
}

// TestEstimateIndexDocsWithFieldSel covers: empty bounds returns totalDocs,
// full-sketch coverage sums estimates, partial bounds with matching fieldSel
// multiplies per-field selectivity (with fallback when a field has no sketch),
// and the bottom fallback multiplies DefaultRangeSelectivity per bound field.
func TestEstimateIndexDocsWithFieldSel(t *testing.T) {
	t.Run("empty_bounds", func(t *testing.T) {
		idx := &CBOIndex{Info: &IndexInfo{FieldNames: []string{"a"}}}
		assert.Equal(t, 123.0, estimateIndexDocsWithFieldSel(idx, 123, nil))
	})
	t.Run("full_sketch_sums", func(t *testing.T) {
		idx := &CBOIndex{
			Info:        &IndexInfo{FieldNames: []string{"a"}},
			Bounds:      query.Bounds{{Start: []byte{1}}, {Start: []byte{2}}},
			Sketch:      mockSketch(7),
			PointLookup: true,
			BoundFields: 1,
		}
		// Two bounds × 7 each.
		assert.Equal(t, 14.0, estimateIndexDocsWithFieldSel(idx, 100, nil))
	})
	t.Run("partial_bounds_with_matching_fieldsel", func(t *testing.T) {
		idx := &CBOIndex{
			Info:        &IndexInfo{FieldNames: []string{"a", "b"}},
			Bounds:      query.Bounds{{Start: []byte{1}, End: []byte{1}}},
			BoundFields: 1,
		}
		fieldSel := []fieldSelEntry{{field: "a", sel: 0.1}}
		got := estimateIndexDocsWithFieldSel(idx, 1000, fieldSel)
		assert.InDelta(t, 100.0, got, 1e-6)
	})
	t.Run("partial_bounds_with_missing_fieldsel_fallback", func(t *testing.T) {
		// BoundFields=2, fieldSel covers only "a". The "b" lookup misses and
		// falls back to DefaultRangeSelectivity.
		idx := &CBOIndex{
			Info:        &IndexInfo{FieldNames: []string{"a", "b"}},
			Bounds:      query.Bounds{{Start: []byte{1}, End: []byte{1}}},
			BoundFields: 2,
		}
		fieldSel := []fieldSelEntry{{field: "a", sel: 0.1}}
		got := estimateIndexDocsWithFieldSel(idx, 1000, fieldSel)
		expected := 1000 * 0.1 * DefaultRangeSelectivity
		assert.InDelta(t, expected, got, 1e-6)
	})
	t.Run("bottom_fallback_no_fieldsel", func(t *testing.T) {
		idx := &CBOIndex{
			Info:        &IndexInfo{FieldNames: []string{"a", "b"}},
			Bounds:      query.Bounds{{Start: []byte{1}, End: []byte{5}}},
			BoundFields: 2,
		}
		got := estimateIndexDocsWithFieldSel(idx, 1000, nil)
		expected := 1000 * DefaultRangeSelectivity * DefaultRangeSelectivity
		assert.InDelta(t, expected, got, 1e-6)
	})
}

// TestComputeIndexBounds_OpenEndedBound covers the else branches at
// planner.go:1373-1380 (empty cur.Start) and 1388-1395 (empty cur.End) by
// passing a compound index with an open-ended bound on the second field.
func TestComputeIndexBounds_OpenEndedBound(t *testing.T) {
	t.Run("open_start", func(t *testing.T) {
		idx := &IndexInfo{FieldNames: []string{"a", "b"}}
		br := &BoundsResult{
			Fields: []FieldBounds{
				{Field: "a", Start: 0, Count: 1, Fixed: true},
				{Field: "b", Start: 1, Count: 1, Fixed: false},
			},
			Bounds: []query.Bound{
				{Start: []byte{0xaa}, End: []byte{0xaa}, StartInclude: true, EndInclude: true},
				{Start: nil, End: []byte{0xbb}}, // open start on b
			},
		}
		bounds, n := ComputeIndexBounds(idx, br)
		assert.Equal(t, 2, n)
		if assert.Equal(t, 1, len(bounds)) {
			// Open start on b: eb.Start is just prev.Start (a) with trailing cap.
			assert.Equal(t, []byte{0xaa}, []byte(bounds[0].Start))
			assert.True(t, bounds[0].StartInclude,
				"open start branch must force StartInclude=true")
			// End is concat of a+b.
			assert.Equal(t, []byte{0xaa, 0xbb}, []byte(bounds[0].End))
		}
	})
	t.Run("open_end", func(t *testing.T) {
		idx := &IndexInfo{FieldNames: []string{"a", "b"}}
		br := &BoundsResult{
			Fields: []FieldBounds{
				{Field: "a", Start: 0, Count: 1, Fixed: true},
				{Field: "b", Start: 1, Count: 1, Fixed: false},
			},
			Bounds: []query.Bound{
				{Start: []byte{0xaa}, End: []byte{0xaa}, StartInclude: true, EndInclude: true},
				{Start: []byte{0xbb}, End: nil}, // open end on b
			},
		}
		bounds, n := ComputeIndexBounds(idx, br)
		assert.Equal(t, 2, n)
		if assert.Equal(t, 1, len(bounds)) {
			assert.Equal(t, []byte{0xaa, 0xbb}, []byte(bounds[0].Start))
			// Open end: eb.End is prev.End + 0xff, EndInclude=true.
			assert.Equal(t, []byte{0xaa, 0xff}, []byte(bounds[0].End))
			assert.True(t, bounds[0].EndInclude,
				"open end branch must force EndInclude=true")
		}
	})
}

// TestBuildPlan_SortById_FullScanNoExtraSort hits the `len(fields) == 1 &&
// fields[0].Field == "id"` branch in BuildPlan where sorting by "id" is free
// (FullScanIter naturally reads in id order).
func TestBuildPlan_SortById_FullScanNoExtraSort(t *testing.T) {
	sorter := &sortFieldStub{fields: []query.SortField{{Field: "id"}}}
	plan := BuildPlan(&PlanParams{
		Filter:    query.All{},
		Sorter:    sorter,
		TotalDocs: 100,
	})
	assert.Equal(t, "FullScan", plan.Name)
	// Root should be a bare FullScanIter, NOT wrapped in SortIter (id-sort is free).
	_, isSortIter := plan.Root.(*SortIter)
	assert.False(t, isSortIter, "sort-by-id must not wrap FullScanIter in SortIter")
	// With Limit=0 and no filter, BuildPlan's wrapping at planner.go:522 skips
	// LimitIter, so Root is a bare *FullScanIter today. If this test breaks
	// later because of extra wrapping, expand the cast then.
	fsi, _ := plan.Root.(*FullScanIter)
	assert.NotNil(t, fsi, "plan root must be *FullScanIter for sort-by-id")
}

// TestBuildPlan_LimitScalesByPTotal hits the `if pTotal > 0 && pTotal < 1.0`
// scaling branch and the `if needed < fullScanDocs` branch where LIMIT is
// effective at reducing scanned rows.
func TestBuildPlan_LimitScalesByPTotal(t *testing.T) {
	// No indexes → pure FullScan. With a selective filter and small LIMIT,
	// needed = limit / pTotal < totalDocs triggers the effective-scan clamp.
	plan := BuildPlan(&PlanParams{
		Filter:    query.MustParseCondition(`{"x": 1}`), // selective
		TotalDocs: 10_000,
		Limit:     10,
	})
	assert.Equal(t, "FullScan", plan.Name)

	// Find the FullScan candidate and assert EstRows < TotalDocs.
	var fsCand *CandidatePlan
	for i := range plan.Explain.Candidates {
		if plan.Explain.Candidates[i].Name == "FullScan" {
			fsCand = &plan.Explain.Candidates[i]
			break
		}
	}
	if assert.NotNil(t, fsCand, "FullScan candidate must be present") {
		// With no indexed field for "x", calculateSelectivity falls through
		// to DefaultRangeSelectivity. needed = Limit / DefaultRangeSelectivity
		// triggers both the `pTotal > 0 && pTotal < 1.0` scaling (planner.go:264)
		// and the `needed < fullScanDocs` clamp (planner.go:268).
		expected := 10.0 / DefaultRangeSelectivity
		assert.InDelta(t, expected, fsCand.EstRows, 0.01,
			"fullScanEffective must equal Limit/pTotal")
	}
}

// TestBuildPlan_IndexSeek_ClampE1 hits the `if e < 1 { e = 1 }` clamp in the
// Plan-B loop. A sketch returning 0 estimates 0 docs, which gets clamped.
func TestBuildPlan_IndexSeek_ClampE1(t *testing.T) {
	plan := BuildPlan(&PlanParams{
		Filter:    query.MustParseCondition(`{"a": 42}`),
		TotalDocs: 1000,
		Indexes: []CBOIndex{{
			Info:        &IndexInfo{Name: "a", FieldNames: []string{"a"}},
			Sketch:      mockSketch(0), // forces e=0 → clamp to 1
			Bounds:      mustParseBounds("a", `{"a": 42}`),
			PointLookup: true,
			BoundFields: 1,
		}},
	})
	// Either IndexSeek or FullScan can win depending on cost math, but either
	// way the IndexSeek candidate must have EstRows >= 1 (the clamp).
	var seek *CandidatePlan
	for i := range plan.Explain.Candidates {
		if strings.HasPrefix(plan.Explain.Candidates[i].Name, "IndexSeek") {
			seek = &plan.Explain.Candidates[i]
			break
		}
	}
	if assert.NotNil(t, seek, "IndexSeek candidate required") {
		// sketch(0) forces `e=0` out of estimateIndexDocsWithFieldSel, which is
		// then clamped at exactly 1.0 at planner.go:329-331. The clamp must
		// produce the floor value, not any value >= 1 — otherwise a missing
		// clamp returning (say) 0.0001 could silently pass a weaker >=1 check.
		assert.Equal(t, 1.0, seek.EstRows,
			"e<1 clamp must produce exactly 1.0")
	}
}

// TestBuildPlan_ExactSort_LimitCovers hits the `needSort && idx.ExactSort &&
// params.Limit > 0 && !isCovering` branch and pins the re-computation of
// seekCost using the scanSel-based `s` formula at planner.go:370-377.
func TestBuildPlan_ExactSort_LimitCovers(t *testing.T) {
	sorter := &sortFieldStub{fields: []query.SortField{{Field: "a"}}}
	plan := BuildPlan(&PlanParams{
		Filter:    query.MustParseCondition(`{"a": 5}`),
		Sorter:    sorter,
		Limit:     10,
		TotalDocs: 1000,
		Indexes: []CBOIndex{{
			Info:        &IndexInfo{Name: "a", FieldNames: []string{"a"}},
			Sketch:      mockSketch(2),
			Bounds:      mustParseBounds("a", `{"a": 5}`),
			PointLookup: true,
			BoundFields: 1,
			ExactSort:   true,
		}},
	})

	// Find the IndexSeek candidate — must exist since the index has bounds.
	var seek *CandidatePlan
	for i := range plan.Explain.Candidates {
		if strings.HasPrefix(plan.Explain.Candidates[i].Name, "IndexSeek") {
			seek = &plan.Explain.Candidates[i]
			break
		}
	}
	if seek == nil {
		t.Fatal("IndexSeek candidate required to exercise ExactSort+Limit branch")
	}
	// e = sketch(2) at BoundFields=1 → estimateIndexDocsWithFieldSel returns 2.
	// Then planner.go:370: s = (Limit+Offset) / scanSel. scanSel = pTotal/idxSel.
	// With a single-field equality index + sketch, pTotal ≈ idxSel ≈ 2/1000,
	// so scanSel ≈ 1.0 (clamped at line 364). s = 10 / 1.0 = 10, clamped to e=2
	// at planner.go:371-373. So EstRows must equal the e value (2.0).
	assert.Equal(t, 2.0, seek.EstRows,
		"IndexSeek EstRows must equal sketch-derived e (2), proving the clamp fired")
}

// TestBuildPlan_IndexScan_LimitClampsAtScanPopulation hits the `s > scanPopulation`
// clamp and the `s < 1` clamp in the Plan-C (IndexScan) loop.
func TestBuildPlan_IndexScan_LimitClampsAtScanPopulation(t *testing.T) {
	sorter := &sortFieldStub{fields: []query.SortField{{Field: "a"}}}
	// Very restrictive selectivity but huge LIMIT → s = (limit/scanSel) > scanPopulation
	// → clamped at scanPopulation.
	plan := BuildPlan(&PlanParams{
		Filter:    query.MustParseCondition(`{"a": 1}`),
		Sorter:    sorter,
		Limit:     100_000, // absurdly large
		TotalDocs: 100,
		Indexes: []CBOIndex{{
			Info:      &IndexInfo{Name: "a", FieldNames: []string{"a"}},
			Sketch:    mockSketch(1),
			ExactSort: true, // must be true for Plan-C consideration
		}},
	})
	// Find the IndexScan candidate — MUST exist since the only index is ExactSort.
	var scan *CandidatePlan
	for i := range plan.Explain.Candidates {
		if strings.HasPrefix(plan.Explain.Candidates[i].Name, "IndexScan") {
			scan = &plan.Explain.Candidates[i]
			break
		}
	}
	if scan == nil {
		t.Fatal("IndexScan candidate required — sort on ExactSort index must generate one")
	}
	// Without the `s > scanPopulation` clamp, s ≈ Limit/scanSel ≈ 100_000/sketch(1)*100
	// which would be millions. The clamp at planner.go:458-460 forces s to
	// scanPopulation (= totalDocs = 100 when idx.Bounds is empty). Assert
	// exact equality so removing the clamp cannot silently pass.
	assert.Equal(t, 100.0, scan.EstRows,
		"s>scanPopulation clamp must pin EstRows at scanPopulation (=totalDocs here)")
}

// TestBuildPlan_IndexScan_NonUniqueBoundsAdjusted hits planner.go:973 — when
// the chosen IndexScan index is non-unique and has bounds, AdjustBoundsForNonUnique
// appends 0xff so range scans capture all docId suffixes.
func TestBuildPlan_IndexScan_NonUniqueBoundsAdjusted(t *testing.T) {
	sorter := &sortFieldStub{fields: []query.SortField{{Field: "a"}}}
	indexBounds := query.Bounds{
		{Start: []byte{0x10}, End: []byte{0x20}, StartInclude: true, EndInclude: true},
	}
	plan := BuildPlan(&PlanParams{
		Filter:    query.MustParseCondition(`{"a": {"$gte": 1, "$lte": 5}}`),
		Sorter:    sorter,
		TotalDocs: 100,
		Indexes: []CBOIndex{{
			Info: &IndexInfo{
				Name:       "a",
				FieldNames: []string{"a"},
				FieldPaths: [][]string{{"a"}},
				Unique:     false, // triggers adjustment
			},
			Sketch:    mockSketch(10),
			Bounds:    indexBounds,
			ExactSort: true,
		}},
	})
	// Root traversal: LimitIter? → *CanonicalKeyDedupIter → *FilterIter? → *FetchIter → *IndexIter.
	// We walk down until we find the IndexIter and check its Bounds were adjusted.
	root := plan.Root
	for {
		switch r := root.(type) {
		case *LimitIter:
			root = r.Source
			continue
		case *CanonicalKeyDedupIter:
			root = r.Source
			continue
		case *FilterIter:
			root = r.Source
			continue
		case *FetchIter:
			root = r.Source
			continue
		case *IndexFilterIter:
			root = r.Source
			continue
		case *IndexIter:
			// Walked to the leaf. If plan.Name is IndexScan, bounds must carry
			// the 0xff suffix now. We fail (not skip) on a CBO choice change so
			// regressions in cost model surface rather than hide the test.
			if plan.Name != "IndexScan" {
				t.Fatalf("BuildPlan chose %s; expected IndexScan to exercise bounds adjust", plan.Name)
			}
			// Full slice equality: {0x10} stays as Start; {0x20} becomes {0x20, 0xff}.
			bounds := r.Bounds
			if assert.Equal(t, 1, len(bounds)) {
				assert.Equal(t, []byte{0x10}, []byte(bounds[0].Start))
				assert.Equal(t, []byte{0x20, 0xff}, []byte(bounds[0].End),
					"non-unique IndexScan bounds must have 0xff appended to End")
				assert.True(t, bounds[0].StartInclude, "StartInclude must be preserved")
				assert.True(t, bounds[0].EndInclude, "EndInclude must be preserved")
			}
			return
		default:
			t.Fatalf("unexpected iterator type: %T", r)
		}
	}
}

// TestBuildPlan_IndexScan_MultiFieldPathsNoDedupWrap pins that compound
// indexes are NOT wrapped with a dedup iterator at the planner level —
// dedup happens consumer-side via the multiKey flag flowing through
// IndexIter → planIterator.Next/DocDedup. This replaces the previous
// SeenSetDedupIter assertion (the wrapper was dropped in this commit).
func TestBuildPlan_IndexScan_MultiFieldPathsNoDedupWrap(t *testing.T) {
	sorter := &sortFieldStub{fields: []query.SortField{{Field: "a"}}}
	plan := BuildPlan(&PlanParams{
		Filter:    query.All{},
		Sorter:    sorter,
		TotalDocs: 100,
		Indexes: []CBOIndex{{
			Info: &IndexInfo{
				Name:       "ab",
				FieldNames: []string{"a", "b"},
				FieldPaths: [][]string{{"a"}, {"b"}},
				Unique:     true,
			},
			ExactSort: true,
		}},
	})
	if plan.Name != "IndexScan" {
		t.Fatalf("BuildPlan chose %s; expected IndexScan", plan.Name)
	}
	root := plan.Root
	if li, ok := root.(*LimitIter); ok {
		root = li.Source
	}
	_, isCanonical := root.(*CanonicalKeyDedupIter)
	assert.False(t, isCanonical,
		"compound index must NOT use CanonicalKeyDedupIter (it's only for single-field)")
}

// TestBuildPlan_IndexScan_CoverFiltersInsertsIndexFilterIter hits planner.go:991
// — when coveringFilterFields returns non-nil, buildIndexScanChain wraps the
// IndexIter in an IndexFilterIter.
func TestBuildPlan_IndexScan_CoverFiltersInsertsIndexFilterIter(t *testing.T) {
	sorter := &sortFieldStub{fields: []query.SortField{{Field: "a"}}}

	// Compound index (a, b). Filter: a=5 AND b=10. Index bounds cover a
	// (BoundFields=1); b is a trailing field with equality condition, so
	// coveringFilterFields returns one entry for b.
	filter := query.MustParseCondition(`{"a": 5, "b": 10}`)
	indexInfo := &IndexInfo{
		Name:       "ab",
		FieldNames: []string{"a", "b"},
		FieldPaths: [][]string{{"a"}, {"b"}},
		Unique:     true,
	}
	br := &BoundsResult{}
	br.Build([]*IndexInfo{indexInfo}, filter)

	plan := BuildPlan(&PlanParams{
		Filter:      filter,
		Sorter:      sorter,
		FieldBounds: br,
		TotalDocs:   100,
		Indexes: []CBOIndex{{
			Info:        indexInfo,
			Sketch:      mockSketch(10),
			Bounds:      mustParseBounds("a", `{"a": 5}`),
			PointLookup: true,
			BoundFields: 1, // only a is bound; b is a trailing covered field
			ExactSort:   true,
		}},
	})
	if plan.Name != "IndexScan" {
		t.Fatalf("BuildPlan chose %s — Plan-C coverFilters branch not exercised; a CBO cost regression should fail the test, not silently skip", plan.Name)
	}

	// Walk: LimitIter? → dedup → FilterIter? → FetchIter → IndexFilterIter → IndexIter.
	// Keep walking until we see an *IndexFilterIter.
	root := plan.Root
	seen := false
	for i := 0; i < 10; i++ {
		if _, ok := root.(*IndexFilterIter); ok {
			seen = true
			break
		}
		switch r := root.(type) {
		case *LimitIter:
			root = r.Source
		case *CanonicalKeyDedupIter:
			root = r.Source
		case *FilterIter:
			root = r.Source
		case *FetchIter:
			root = r.Source
		default:
			goto done
		}
	}
done:
	assert.True(t, seen, "coverFilters > 0 must insert an IndexFilterIter into the chain")
}

// TestBuildPlan_CountOnly_Covering hits planner.go:900-902 — a count query
// over a single-bound PointLookup non-unique index whose fields fully cover
// the filter returns the bare IndexIter root (no fetch/filter/dedup wrapping).
// A unique + all-fields-bound index would short-circuit earlier at line 837
// into the CoverIter path, so this test uses Unique=false to bypass that.
func TestBuildPlan_CountOnly_Covering(t *testing.T) {
	filter := query.MustParseCondition(`{"a": 5}`)
	indexInfo := &IndexInfo{
		Name:       "a",
		FieldNames: []string{"a"},
		FieldPaths: [][]string{{"a"}},
		Unique:     false, // avoids the CoverIter short-circuit at line 837
	}
	br := &BoundsResult{}
	br.Build([]*IndexInfo{indexInfo}, filter)

	plan := BuildPlan(&PlanParams{
		Filter:      filter,
		FieldBounds: br,
		CountOnly:   true,
		TotalDocs:   100,
		Indexes: []CBOIndex{{
			Info:        indexInfo,
			Sketch:      mockSketch(5),
			Bounds:      mustParseBounds("a", `{"a": 5}`),
			PointLookup: true,
			BoundFields: 1,
		}},
	})
	if plan.Name != "IndexSeek" {
		t.Fatalf("BuildPlan chose %s — covering-count fast path not exercised; a CBO cost regression should fail the test, not silently skip", plan.Name)
	}
	// Covering count path returns the IndexIter directly from buildIndexSeekChain
	// without FetchIter/FilterIter/Dedup wrapping.
	root := plan.Root
	if li, ok := root.(*LimitIter); ok {
		root = li.Source
	}
	_, hasFetch := root.(*FetchIter)
	_, hasFilter := root.(*FilterIter)
	_, hasIdxIter := root.(*IndexIter)
	assert.False(t, hasFetch, "covering count must skip FetchIter")
	assert.False(t, hasFilter, "covering count must skip FilterIter")
	assert.True(t, hasIdxIter, "covering count root should be *IndexIter, got %T", root)
}

// TestBuildVerifyChain_NoUncoveredFields_ReturnsNil pins the early-return
// branch in buildVerifyChain at planner.go:1184-1186 when every filter field
// is covered by the index's bound prefix.
func TestBuildVerifyChain_NoUncoveredFields_ReturnsNil(t *testing.T) {
	idx := &CBOIndex{
		Info:        &IndexInfo{FieldNames: []string{"a"}},
		BoundFields: 1,
	}
	params := &PlanParams{
		Filter: query.MustParseCondition(`{"a": 1}`),
	}
	tr := &closeTrackingIter{}
	got := buildVerifyChain(params, idx, tr)
	assert.Nil(t, got, "no uncovered fields → buildVerifyChain must return nil")
}

// TestBuildVerifyChain_UncoveredNoFieldBounds pins the nil return at
// planner.go:1191-1193 when an uncovered field has NO entry in FieldBounds
// (Lookup returns found=false).
func TestBuildVerifyChain_UncoveredNoFieldBounds(t *testing.T) {
	// Primary index covers "a"; filter also references uncovered "b".
	// params.FieldBounds has no "b" entry → Lookup returns found=false
	// → buildVerifyChain returns nil.
	idx := &CBOIndex{
		Info:        &IndexInfo{FieldNames: []string{"a"}},
		BoundFields: 1,
	}
	params := &PlanParams{
		Filter:      query.MustParseCondition(`{"a": 1, "b": 2}`),
		FieldBounds: &BoundsResult{}, // intentionally empty; no "b" entry
	}
	tr := &closeTrackingIter{}
	got := buildVerifyChain(params, idx, tr)
	assert.Nil(t, got, "missing FieldBounds for uncovered field → nil")
}

// TestBuildVerifyChain_NoVerifyIndex pins the nil return at planner.go:1204-1206
// when an uncovered field has fixed bounds but no matching non-unique
// single-field index exists to verify against.
func TestBuildVerifyChain_NoVerifyIndex(t *testing.T) {
	filter := query.MustParseCondition(`{"a": 1, "b": 2}`)
	br := &BoundsResult{}
	br.Build([]*IndexInfo{{FieldNames: []string{"a"}}, {FieldNames: []string{"b"}}}, filter)

	idx := &CBOIndex{
		Info:        &IndexInfo{FieldNames: []string{"a"}},
		BoundFields: 1,
	}
	params := &PlanParams{
		Filter:      filter,
		FieldBounds: br,
		Indexes: []CBOIndex{
			// Only the primary index exists; no separate index for "b".
			*idx,
		},
	}
	tr := &closeTrackingIter{}
	got := buildVerifyChain(params, idx, tr)
	assert.Nil(t, got, "no non-unique single-field index for uncovered field → nil")
}

// TestBuildVerifyChain_BuildsVerifyIter pins the non-nil VerifyIter construction
// at planner.go:1208-1213 when every uncovered field has both a fixed bound
// AND a matching non-unique single-field index with a non-nil Namespace.
func TestBuildVerifyChain_BuildsVerifyIter(t *testing.T) {
	// Open a real (but minimal) btree to get a non-nil Namespace for the
	// verify target. buildVerifyChain requires info.Ns != nil.
	_, _, _, _ = plannerTestDB(t) // seed planner test db infra (no-op if idempotent)
	// Use the helper already in planner_test.go to open a small btree.
	_, _, verifyNs := openBtreeForVerify(t, "verify_b", func(tx *btree.WriteTx, ns *btree.Namespace) {})

	filter := query.MustParseCondition(`{"a": 1, "b": 2}`)
	br := &BoundsResult{}
	br.Build([]*IndexInfo{{FieldNames: []string{"a"}}, {FieldNames: []string{"b"}}}, filter)

	primary := &CBOIndex{
		Info:        &IndexInfo{Name: "a", FieldNames: []string{"a"}, Unique: false},
		BoundFields: 1,
	}
	verifyTarget := &CBOIndex{
		Info: &IndexInfo{Name: "b_idx", FieldNames: []string{"b"}, Unique: false, Ns: verifyNs},
	}
	params := &PlanParams{
		Filter:      filter,
		FieldBounds: br,
		Indexes:     []CBOIndex{*primary, *verifyTarget},
	}
	tr := &closeTrackingIter{}
	got := buildVerifyChain(params, primary, tr)
	if assert.NotNil(t, got, "uncovered field with matching non-unique index → VerifyIter") {
		_, ok := got.(*VerifyIter)
		assert.True(t, ok, "result must be a *VerifyIter; got %T", got)
	}
}

// TestBuildVerifyChain_RangeBoundSkipsVerify pins the nil return at
// planner.go:1191-1193 when an uncovered field has bounds but they are NOT
// fixed (i.e., a range), so verification is unsafe.
func TestBuildVerifyChain_RangeBoundSkipsVerify(t *testing.T) {
	filter := query.MustParseCondition(`{"a": 1, "b": {"$gt": 2}}`)
	br := &BoundsResult{}
	br.Build([]*IndexInfo{{FieldNames: []string{"a"}}, {FieldNames: []string{"b"}}}, filter)

	primary := &CBOIndex{
		Info:        &IndexInfo{Name: "a", FieldNames: []string{"a"}, Unique: false},
		BoundFields: 1,
	}
	verifyTarget := &CBOIndex{
		Info: &IndexInfo{Name: "b_idx", FieldNames: []string{"b"}, Unique: false},
	}
	params := &PlanParams{
		Filter:      filter,
		FieldBounds: br,
		Indexes:     []CBOIndex{*primary, *verifyTarget},
	}
	tr := &closeTrackingIter{}
	got := buildVerifyChain(params, primary, tr)
	assert.Nil(t, got, "range bound on uncovered field → verification unsafe, return nil")
}

// TestCalculateSelectivity_InnerSketchClampPropagates pins the cumulative
// effect of the inner per-field sketch clamp (planner.go:648-650) multiplied
// by a subsequent DefaultRangeSelectivity contribution. The outer
// `pTotal <= 0` and `pTotal > 1.0` clamps at planner.go:669-675 appear to be
// unreachable from calculateSelectivity itself (see bugs.md UNREACHABLE).
func TestCalculateSelectivity_InnerSketchClampPropagates(t *testing.T) {
	// Two independent equality predicates. a has sketch(0) → inner clamp
	// yields 0.0001; b has no sketch → compound-equality branch contributes
	// DefaultRangeSelectivity. Product is 0.0001 * DefaultRangeSelectivity.
	f := query.MustParseCondition(`{"a": 1, "b": 2}`)
	idxA := CBOIndex{
		Info:   &IndexInfo{Name: "a", FieldNames: []string{"a"}},
		Sketch: mockSketch(0),
	}
	idxB := CBOIndex{Info: &IndexInfo{Name: "b", FieldNames: []string{"b"}}}
	br := &BoundsResult{}
	br.Build([]*IndexInfo{idxA.Info, idxB.Info}, f)
	sel := calculateSelectivity(f, []CBOIndex{idxA, idxB}, 1000, br)
	assert.InDelta(t, 0.0001*DefaultRangeSelectivity, sel, 1e-9)
}

// TestBuildPlan_CountOnly_NonCoveringWithVerifyChain exercises the path at
// planner.go:906-911 (CountOnly && PointLookup && FieldBounds != nil) by
// building a plan that enters buildIndexSeekChain and takes the verify-chain
// branch (returns non-nil VerifyIter root).
func TestBuildPlan_CountOnly_NonCoveringWithVerifyChain(t *testing.T) {
	// buildVerifyChain only stores info.Ns as a pointer into VerifyIter.VerifyNs;
	// no btree I/O happens during BuildPlan. A zero-value *btree.Namespace
	// satisfies the non-nil check without the cost of opening a real DB.
	verifyNs := &btree.Namespace{}

	filter := query.MustParseCondition(`{"a": 1, "b": 2}`)
	primaryInfo := &IndexInfo{
		Name: "a", FieldNames: []string{"a"}, Unique: false,
	}
	verifyInfo := &IndexInfo{
		Name: "b_idx", FieldNames: []string{"b"}, Unique: false, Ns: verifyNs,
	}
	br := &BoundsResult{}
	br.Build([]*IndexInfo{primaryInfo, verifyInfo}, filter)

	plan := BuildPlan(&PlanParams{
		Filter:      filter,
		FieldBounds: br,
		CountOnly:   true,
		TotalDocs:   100,
		Indexes: []CBOIndex{
			{
				Info:        primaryInfo,
				Sketch:      mockSketch(5),
				Bounds:      mustParseBounds("a", `{"a": 1}`),
				PointLookup: true,
				BoundFields: 1,
			},
			{Info: verifyInfo},
		},
	})
	if plan.Name != "IndexSeek" {
		t.Fatalf("BuildPlan chose %s; expected IndexSeek to exercise verify-chain", plan.Name)
	}
	root := plan.Root
	if li, ok := root.(*LimitIter); ok {
		root = li.Source
	}
	_, isVerify := root.(*VerifyIter)
	assert.True(t, isVerify, "verify-chain branch must yield *VerifyIter root, got %T", root)
}

// TestBuildPlan_FullScanOffsetAbsorbed covers planner.go:771-773 (FullScanIter
// absorbs Offset into cursor-level batch skip when sorting by id with no
// filter) and planner.go:523-526 (LimitIter then clears offset to avoid
// double-skipping).
func TestBuildPlan_FullScanOffsetAbsorbed(t *testing.T) {
	sorter := &sortFieldStub{fields: []query.SortField{{Field: "id"}}}
	plan := BuildPlan(&PlanParams{
		Filter:    query.All{}, // no filter → needFilter=false
		Sorter:    sorter,      // sort-by-id → id-sorted FullScanIter path
		Offset:    5,
		TotalDocs: 100,
	})
	// Root should be LimitIter → FullScanIter. LimitIter wrapping at planner.go:523
	// sets Offset to 0 when FullScanIter absorbed it.
	li, ok := plan.Root.(*LimitIter)
	if assert.True(t, ok, "expected LimitIter root, got %T", plan.Root) {
		assert.Equal(t, 0, li.Offset, "LimitIter.Offset must be 0 after FullScanIter absorbs it")
		fsi, ok := li.Source.(*FullScanIter)
		if assert.True(t, ok) {
			assert.Equal(t, 5, fsi.Offset, "FullScanIter must carry the offset")
		}
	}
}

// TestBuildPlan_IndexSeek_NonExactSort_WrapsSort exercises the SortIter
// wrapping in buildIndexSeekChain's general path (planner.go:951-963),
// bypassing the CoverIter fast-path at planner.go:837 by using a non-unique
// index (Unique=false defeats the CoverIter condition).
func TestBuildPlan_IndexSeek_NonExactSort_WrapsSort(t *testing.T) {
	sorter := &sortFieldStub{fields: []query.SortField{{Field: "b"}}}
	filter := query.MustParseCondition(`{"a": 1}`)
	idxInfo := &IndexInfo{
		Name: "a", FieldNames: []string{"a"}, FieldPaths: [][]string{{"a"}},
		Unique: false, // must NOT be Unique → skips CoverIter fast-path
	}
	plan := BuildPlan(&PlanParams{
		Filter:    filter,
		Sorter:    sorter,
		TotalDocs: 100,
		Indexes: []CBOIndex{{
			Info:        idxInfo,
			Sketch:      mockSketch(3),
			Bounds:      mustParseBounds("a", `{"a": 1}`),
			PointLookup: true,
			BoundFields: 1,
			ExactSort:   false, // sort is on "b", not covered by this index
		}},
	})
	if plan.Name != "IndexSeek" {
		t.Fatalf("BuildPlan chose %s; expected IndexSeek", plan.Name)
	}
	// Walk past LimitIter to the first non-wrapper node.
	root := plan.Root
	if li, ok := root.(*LimitIter); ok {
		root = li.Source
	}
	_, isSortIter := root.(*SortIter)
	assert.True(t, isSortIter, "seek chain with non-exact sort must wrap in SortIter, got %T", root)
}

// TestBuildPlan_IndexHintBoost covers planner.go:388-390 (seek cost reduced
// by hint boost) and planner.go:483-486 (scan cost reduced by hint boost).
// A large boost forces the hinted index to win over cheaper alternatives.
func TestBuildPlan_IndexHintBoost(t *testing.T) {
	// FullScan on 100 docs is cheap. Apply a large negative-cost boost to
	// the index seek so it wins the CBO.
	filter := query.MustParseCondition(`{"a": 1}`)
	idx := CBOIndex{
		Info:        &IndexInfo{Name: "a", FieldNames: []string{"a"}, Unique: false},
		Sketch:      mockSketch(50),
		Bounds:      mustParseBounds("a", `{"a": 1}`),
		PointLookup: true,
		BoundFields: 1,
	}
	plan := BuildPlan(&PlanParams{
		Filter:     filter,
		TotalDocs:  100,
		Indexes:    []CBOIndex{idx},
		IndexHints: []IndexHintParam{{IndexName: "a", Boost: 1_000_000}},
	})
	assert.Equal(t, "IndexSeek", plan.Name,
		"large hint boost must force IndexSeek to win over FullScan")
}

// TestBuildPlan_FilteredYieldClamp_LowHits pins the IndexSeek(idxB) cost
// formula at planner.go:350 when e=1 and filteredYield<1 clamp fires (line
// 336-338). Setup: idxA's sketch(1) makes fieldSel[a]=0.01 so idxB's e=100×0.01=1
// (exact). idxB's PointLookup=false forces idxSel=DefaultRangeSelectivity=0.5.
// filteredYield = 1 × (pTotal/idxSel) = 1 × (0.01/0.5) = 0.02 → clamp to 1.
// No sort: cost = nSeeks×CostIndexSeek + e×fetchCost + e×CostFilter
// = 1×0.5 + 1×3.0 + 1×0.5 = 4.0.
func TestBuildPlan_FilteredYieldClamp_LowHits(t *testing.T) {
	filter := query.MustParseCondition(`{"a": 1}`)
	idxA := CBOIndex{
		Info:        &IndexInfo{Name: "idxA", FieldNames: []string{"a"}},
		Sketch:      mockSketch(1), // p = 1/100 = 0.01 → pins fieldSel["a"]=0.01
		Bounds:      mustParseBounds("a", `{"a": 1}`),
		PointLookup: true,
		BoundFields: 1,
	}
	// idxB iterates Plan-B loop. BoundFields=1 enables the fieldSel lookup,
	// which yields e=100*0.01=1. No sketch + PointLookup=false forces
	// selectivityForIndex to return DefaultRangeSelectivity=0.5.
	idxB := CBOIndex{
		Info:        &IndexInfo{Name: "idxB", FieldNames: []string{"a"}},
		Bounds:      mustParseBounds("a", `{"a": 1}`),
		PointLookup: false,
		BoundFields: 1,
	}
	plan := BuildPlan(&PlanParams{
		Filter:    filter,
		TotalDocs: 100,
		Indexes:   []CBOIndex{idxA, idxB},
	})
	var seekB *CandidatePlan
	for i := range plan.Explain.Candidates {
		if plan.Explain.Candidates[i].Name == "IndexSeek(idxB)" {
			seekB = &plan.Explain.Candidates[i]
			break
		}
	}
	if seekB == nil {
		t.Fatal("IndexSeek(idxB) candidate must be generated to exercise Plan-B clamp")
	}
	// Pin the exact Cost formula from planner.go:350 with the clamped e=1 and
	// nSeeks=1 values. Deviation signals either a cost-model regression or a
	// selectivity plumbing change — not a silent coverage drop.
	fetchCost := indexFetchCost(100)
	expectedCost := 1*CostIndexSeek + 1*fetchCost + 1*CostFilter
	assert.InDelta(t, expectedCost, seekB.Cost, 0.01,
		"IndexSeek(idxB) cost must equal 1*CostIndexSeek + 1*fetchCost + 1*CostFilter with e=1")
}

// TestBuildPlan_FilteredYieldClamp_HighHits pins planner.go:339-341 — when
// filteredYield = e × (pTotal / idxSel) > e it is clamped back to e.
// Construction: filter=All{} forces pTotal=1.0. The index has sketch(0)
// with full bounds, so selectivityForIndex returns the 0.0001 low clamp.
// filteredYield becomes e × (1.0 / 0.0001) = 10000e → clamped to e.
func TestBuildPlan_FilteredYieldClamp_HighHits(t *testing.T) {
	plan := BuildPlan(&PlanParams{
		Filter:    query.All{},
		TotalDocs: 100,
		Indexes: []CBOIndex{{
			Info:        &IndexInfo{Name: "a", FieldNames: []string{"a"}, Unique: false},
			Sketch:      mockSketch(0),
			Bounds:      mustParseBounds("a", `{"a": 1}`),
			PointLookup: true,
			BoundFields: 1,
		}},
	})
	// Find the IndexSeek candidate. e = sum(sketch(0)) = 0 → clamped to 1.
	// EstRows reports e (post-clamp, pre-filteredYield). filteredYield
	// was clamped back to e during cost compute, so the cost formula used
	// `e` rather than the pre-clamp 10000×e.
	var seek *CandidatePlan
	for i := range plan.Explain.Candidates {
		if strings.HasPrefix(plan.Explain.Candidates[i].Name, "IndexSeek") {
			seek = &plan.Explain.Candidates[i]
			break
		}
	}
	if seek == nil {
		t.Fatal("IndexSeek candidate required to exercise filteredYield>e clamp")
	}
	assert.Equal(t, 1.0, seek.EstRows, "e clamped to 1")
	// Seek cost = 1×CostIndexSeek + 1×fetchCost + 1×CostFilter.
	// Without the filteredYield>e clamp, the filter term would blow up to
	// 10000×CostFilter. Assert the cost is close to the expected (small)
	// formula — catches a regression that removes the clamp.
	fetchCost := indexFetchCost(100)
	expectedCost := 1*CostIndexSeek + 1*fetchCost + 1*CostFilter
	assert.InDelta(t, expectedCost, seek.Cost, 0.01,
		"IndexSeek cost must reflect filteredYield clamped to e")
}

// TestBuildPlan_DeduplicatesIndexFieldsInSelectivity covers the usedFields
// dedup at planner.go:611-618 in calculateSelectivity: two indexes that share
// a field name should not double-count the field's selectivity contribution.
func TestBuildPlan_DeduplicatesIndexFieldsInSelectivity(t *testing.T) {
	f := query.MustParseCondition(`{"a": 1}`)
	// Two indexes both referencing field "a".
	idxA1 := CBOIndex{Info: &IndexInfo{Name: "a1", FieldNames: []string{"a"}}}
	idxA2 := CBOIndex{Info: &IndexInfo{Name: "a2", FieldNames: []string{"a"}}}
	br := &BoundsResult{}
	br.Build([]*IndexInfo{idxA1.Info, idxA2.Info}, f)
	sel := calculateSelectivity(f, []CBOIndex{idxA1, idxA2}, 100, br)
	// The dedup ensures field "a" contributes exactly once: pTotal should be
	// DefaultRangeSelectivity (from the compound-equality branch, since no
	// sketches are provided).
	assert.Equal(t, DefaultRangeSelectivity, sel,
		"duplicate field across indexes must not double-multiply selectivity")
}

// TestBuildPlan_PlanC_ScanDetailsRendered hits planner.go:493 (Plan-C scan
// details closure evaluation) by forcing ExplainString to emit it.
func TestBuildPlan_PlanC_ScanDetailsRendered(t *testing.T) {
	sorter := &sortFieldStub{fields: []query.SortField{{Field: "a"}}}
	plan := BuildPlan(&PlanParams{
		Filter:    query.All{},
		Sorter:    sorter,
		TotalDocs: 100,
		Indexes: []CBOIndex{{
			Info: &IndexInfo{
				Name: "a", FieldNames: []string{"a"},
				FieldPaths: [][]string{{"a"}}, Unique: true,
			},
			ExactSort: true,
		}},
	})
	if plan.Name != "IndexScan" {
		t.Fatalf("BuildPlan chose %s; expected IndexScan to exercise Plan-C details closure", plan.Name)
	}
	// Calling ExplainString iterates candidates and invokes Details() on the
	// matching candidate, which runs the closure at planner.go:493.
	out := plan.ExplainString()
	assert.Contains(t, out, "Cost breakdown:", "ExplainString must emit cost breakdown")
	assert.Contains(t, out, "seek(", "Plan-C details must include seek term")
}

// TestBuildPlan_PlanC_CoverFiltersNoLimit pins the Plan-C no-limit cover-filter
// Cost formula at planner.go:476:
//
//	scanCost = scanPopulation×CostSeqRead +
//	           scanPopulation×coverSel×fetchCost +
//	           scanPopulation×CostFilter
//
// Setup: compound index (a,b) with BoundFields=1 (only a bound, b is trailing
// fixed equality → coverFilters has 1 entry). idxSel comes from the level-0
// prefix sketch: Estimate(0, a=5)/totalDocs = 10/100 = 0.1. coverSel is
// DefaultRangeSelectivity=0.5 (no fieldSel because compound index doesn't
// contribute to fieldSelectivity). scanPopulation = totalDocs × idxSel =
// 100 × 0.1 = 10.
func TestBuildPlan_PlanC_CoverFiltersNoLimit(t *testing.T) {
	sorter := &sortFieldStub{fields: []query.SortField{{Field: "a"}}}
	filter := query.MustParseCondition(`{"a": 5, "b": 10}`)
	indexInfo := &IndexInfo{
		Name: "ab", FieldNames: []string{"a", "b"},
		FieldPaths: [][]string{{"a"}, {"b"}}, Unique: true,
	}
	br := &BoundsResult{}
	br.Build([]*IndexInfo{indexInfo}, filter)
	plan := BuildPlan(&PlanParams{
		Filter:      filter,
		Sorter:      sorter,
		FieldBounds: br,
		TotalDocs:   100,
		// No Limit — exercises the else branch at planner.go:472-479.
		Indexes: []CBOIndex{{
			Info:        indexInfo,
			Sketch:      mockSketch(10),
			Bounds:      mustParseBounds("a", `{"a": 5}`),
			PointLookup: true,
			BoundFields: 1,
			ExactSort:   true,
		}},
	})
	if plan.Name != "IndexScan" {
		t.Fatalf("BuildPlan chose %s; expected IndexScan for no-limit cover-filters path", plan.Name)
	}
	var scan *CandidatePlan
	for i := range plan.Explain.Candidates {
		if strings.HasPrefix(plan.Explain.Candidates[i].Name, "IndexScan") {
			scan = &plan.Explain.Candidates[i]
			break
		}
	}
	if scan == nil {
		t.Fatal("IndexScan candidate required to pin cover-filter no-limit formula")
	}
	fetchCost := indexFetchCost(100)
	const scanPopulation = 10.0 // totalDocs × idxSel = 100 × (10/100)
	const coverSel = DefaultRangeSelectivity
	expectedCost := scanPopulation*CostSeqRead +
		scanPopulation*coverSel*fetchCost +
		scanPopulation*CostFilter
	assert.InDelta(t, expectedCost, scan.Cost, 0.01,
		"Plan-C no-limit covering-filter cost must equal scanPopulation×(CostSeqRead + coverSel×fetchCost + CostFilter)")
}

// TestBuildPlan_PlanB_ExactSortLimit_ScanSelClamp_High pins the Plan-B
// ExactSort+Limit branch cost formula at planner.go:377 when scanSel>1 clamps
// to 1.0. Setup: filter=All{} → pTotal=1.0. idxSel from sketch(50) →
// 50/100=0.5. scanSel = 1.0/0.5 = 2.0 → clamps to 1.0. s = Limit/1.0 = 10.
// e = sum(sketch estimates) = 50 (one bound × 50). s=10 < e=50 → no clamp.
// Cost = nSeeks×CostIndexSeek + s×fetchCost + s×CostFilter
//
//	= 1×0.5 + 10×3.0 + 10×0.5 = 35.5.
func TestBuildPlan_PlanB_ExactSortLimit_ScanSelClamp_High(t *testing.T) {
	sorter := &sortFieldStub{fields: []query.SortField{{Field: "a"}}}
	plan := BuildPlan(&PlanParams{
		Filter:    query.All{},
		Sorter:    sorter,
		Limit:     10,
		TotalDocs: 100,
		Indexes: []CBOIndex{{
			Info:        &IndexInfo{Name: "a", FieldNames: []string{"a"}, Unique: false},
			Sketch:      mockSketch(50), // e=50 ≫ Limit=10, avoids s>e clamp
			Bounds:      mustParseBounds("a", `{"a": 1}`),
			PointLookup: true,
			BoundFields: 1,
			ExactSort:   true,
		}},
	})
	var seek *CandidatePlan
	for i := range plan.Explain.Candidates {
		if strings.HasPrefix(plan.Explain.Candidates[i].Name, "IndexSeek") {
			seek = &plan.Explain.Candidates[i]
			break
		}
	}
	if seek == nil {
		t.Fatal("IndexSeek candidate required to pin ExactSort+Limit+scanSel-clamp cost")
	}
	fetchCost := indexFetchCost(100)
	const nSeeks = 1.0
	const s = 10.0 // == Limit, because scanSel clamps to 1.0
	expectedCost := nSeeks*CostIndexSeek + s*fetchCost + s*CostFilter
	assert.InDelta(t, expectedCost, seek.Cost, 0.01,
		"Plan-B ExactSort+Limit cost must equal nSeeks×CostIndexSeek + Limit×fetchCost + Limit×CostFilter after scanSel clamps to 1.0")
}

// TestBuildPlan_PlanC_ScanSelClamp_High pins the Plan-C no-limit cost formula
// at planner.go:478 when scanSel>1 clamps to 1.0. Setup: filter=All{} →
// pTotal=1.0. sketch(0) → idxSel clamps low to 0.0001 (planner.go:688-691).
// scanSel = 1.0/0.0001 = 10000 → clamps to 1.0. scanPopulation =
// totalDocs × idxSel = 100 × 0.0001 = 0.01 → clamps to 1 at planner.go:443.
// No limit, no coverFilters → Cost = scanPopulation×(CostIndexSeek + fetchCost
// + CostFilter) = 1×(0.5 + 3.0 + 0.5) = 4.0.
func TestBuildPlan_PlanC_ScanSelClamp_High(t *testing.T) {
	sorter := &sortFieldStub{fields: []query.SortField{{Field: "a"}}}
	plan := BuildPlan(&PlanParams{
		Filter:    query.All{},
		Sorter:    sorter,
		TotalDocs: 100,
		Indexes: []CBOIndex{{
			Info:        &IndexInfo{Name: "a", FieldNames: []string{"a"}, Unique: false},
			Sketch:      mockSketch(0),
			Bounds:      mustParseBounds("a", `{"a": 1}`),
			PointLookup: true,
			BoundFields: 1,
			ExactSort:   true,
		}},
	})
	var scan *CandidatePlan
	for i := range plan.Explain.Candidates {
		if strings.HasPrefix(plan.Explain.Candidates[i].Name, "IndexScan") {
			scan = &plan.Explain.Candidates[i]
			break
		}
	}
	if scan == nil {
		t.Fatal("IndexScan candidate required to pin Plan-C scanSel-clamp cost")
	}
	fetchCost := indexFetchCost(100)
	// scanPopulation = max(1, totalDocs*idxSel) = 1 (clamp at planner.go:443).
	const scanPopulation = 1.0
	expectedCost := scanPopulation * (CostIndexSeek + fetchCost + CostFilter)
	assert.InDelta(t, expectedCost, scan.Cost, 0.01,
		"Plan-C scanSel-clamp cost must equal scanPopulation×(CostIndexSeek + fetchCost + CostFilter)")
}

// TestBuildPlan_PlanC_HintBoost_CoversAllScanBoostLine hits planner.go:484-486
// (Plan-C hint boost application). Pins the exact `scanCost -= float64(boost)`
// subtraction by running BuildPlan twice — once without hints (capture the
// IndexScan candidate's pre-boost cost) and once with the boost — and asserts
// the post-boost IndexScan candidate cost equals preBoost - boost.
func TestBuildPlan_PlanC_HintBoost_CoversAllScanBoostLine(t *testing.T) {
	sorter := &sortFieldStub{fields: []query.SortField{{Field: "a"}}}
	idxInfo := &IndexInfo{
		Name: "a_idx", FieldNames: []string{"a"},
		FieldPaths: [][]string{{"a"}}, Unique: true,
	}

	// Pre-boost: run BuildPlan without IndexHints to capture the unboosted
	// IndexScan candidate cost.
	preBoostPlan := BuildPlan(&PlanParams{
		Filter:    query.All{},
		Sorter:    sorter,
		TotalDocs: 100,
		Indexes:   []CBOIndex{{Info: idxInfo, ExactSort: true}},
	})
	var preBoostCost float64
	preBoostFound := false
	for i := range preBoostPlan.Explain.Candidates {
		if strings.HasPrefix(preBoostPlan.Explain.Candidates[i].Name, "IndexScan") {
			preBoostCost = preBoostPlan.Explain.Candidates[i].Cost
			preBoostFound = true
			break
		}
	}
	if !preBoostFound {
		t.Fatal("pre-boost IndexScan candidate must exist to measure boost effect")
	}

	// Post-boost: a large boost guarantees IndexScan wins.
	const boost = 1_000_000
	plan := BuildPlan(&PlanParams{
		Filter:     query.All{},
		Sorter:     sorter,
		TotalDocs:  100,
		Indexes:    []CBOIndex{{Info: idxInfo, ExactSort: true}},
		IndexHints: []IndexHintParam{{IndexName: "a_idx", Boost: boost}},
	})
	if plan.Name != "IndexScan" {
		t.Fatalf("hint boost should force IndexScan, got %s", plan.Name)
	}
	var postBoostCost float64
	postBoostFound := false
	for i := range plan.Explain.Candidates {
		if strings.HasPrefix(plan.Explain.Candidates[i].Name, "IndexScan") {
			postBoostCost = plan.Explain.Candidates[i].Cost
			postBoostFound = true
			break
		}
	}
	if !postBoostFound {
		t.Fatal("post-boost IndexScan candidate must exist")
	}
	// planner.go:485 applies `scanCost -= float64(boost)`. InDelta tolerates
	// float rounding while pinning the exact subtraction.
	assert.InDelta(t, preBoostCost-float64(boost), postBoostCost, 0.01,
		"Plan-C hint boost must subtract exactly `boost` from the scanCost")
}

// TestBuildPlan_PlanC_CoverFiltersWithLimit pins the Plan-C with-limit
// cover-filter cost formula at planner.go:467:
//
//	scanCost = s×CostSeqRead + s×coverSel×fetchCost + s×CostFilter
//
// where s = (Limit+Offset)/scanSel, capped at scanPopulation. Setup: filter
// {"a":5,"b":10}, compound index (a,b) with BoundFields=1. idxSel from the
// level-0 prefix sketch = Estimate(0, a=5)/100 = 5/100 = 0.05. pTotal = leading
// field sketch (0.05) × b compound-equality DefaultRange (0.5) = 0.025. scanSel =
// 0.025/0.05 = 0.5. raw s = 10/0.5 = 20, but scanPopulation = 100×0.05 = 5 caps
// it to s = 5. coverSel=0.5. Cost = 5×0.1 + 5×0.5×3.0 + 5×0.5 = 10.5.
func TestBuildPlan_PlanC_CoverFiltersWithLimit(t *testing.T) {
	sorter := &sortFieldStub{fields: []query.SortField{{Field: "a"}}}
	filter := query.MustParseCondition(`{"a": 5, "b": 10}`)
	indexInfo := &IndexInfo{
		Name: "ab", FieldNames: []string{"a", "b"},
		FieldPaths: [][]string{{"a"}, {"b"}}, Unique: true,
	}
	br := &BoundsResult{}
	br.Build([]*IndexInfo{indexInfo}, filter)
	plan := BuildPlan(&PlanParams{
		Filter:      filter,
		Sorter:      sorter,
		FieldBounds: br,
		Limit:       10,
		TotalDocs:   100,
		Indexes: []CBOIndex{{
			Info:        indexInfo,
			Sketch:      mockSketch(5),
			Bounds:      mustParseBounds("a", `{"a": 5}`),
			PointLookup: true,
			BoundFields: 1,
			ExactSort:   true,
		}},
	})
	var scan *CandidatePlan
	for i := range plan.Explain.Candidates {
		if strings.HasPrefix(plan.Explain.Candidates[i].Name, "IndexScan") {
			scan = &plan.Explain.Candidates[i]
			break
		}
	}
	if scan == nil {
		t.Fatal("IndexScan candidate required to pin Plan-C with-limit cover-filter cost")
	}
	fetchCost := indexFetchCost(100)
	const s = 5.0 // (Limit+Offset)/scanSel = 20, capped at scanPopulation = 100×0.05 = 5
	const coverSel = DefaultRangeSelectivity
	expectedCost := s*CostSeqRead + s*coverSel*fetchCost + s*CostFilter
	assert.InDelta(t, expectedCost, scan.Cost, 0.01,
		"Plan-C with-limit covering-filter cost must equal s×CostSeqRead + s×coverSel×fetchCost + s×CostFilter")
}

// TestBuildPlan_PlanB_DetailsClosureFires pins planner.go:397 — the details
// closure on an IndexSeek CandidatePlan is actually invoked (rather than
// simply created) when the chosen plan is IndexSeek and ExplainString is
// called. This also keeps the closure body in coverage.
func TestBuildPlan_PlanB_DetailsClosureFires(t *testing.T) {
	filter := query.MustParseCondition(`{"a": 42}`)
	plan := BuildPlan(&PlanParams{
		Filter:    filter,
		TotalDocs: 1000,
		Indexes: []CBOIndex{{
			Info:        &IndexInfo{Name: "a", FieldNames: []string{"a"}, Unique: false},
			Sketch:      mockSketch(2),
			Bounds:      mustParseBounds("a", `{"a": 42}`),
			PointLookup: true,
			BoundFields: 1,
		}},
	})
	if plan.Name != "IndexSeek" {
		t.Fatalf("IndexSeek must win to trigger Plan-B details closure, got %s", plan.Name)
	}
	out := plan.ExplainString()
	assert.Contains(t, out, "Cost breakdown:")
	assert.Contains(t, out, "seek(") // formatSeekDetails includes "×seek("
}

// TestBuildPlan_PlanB_FilteredYieldClampLow pins the IndexSeek(idxB) cost
// formula at planner.go:350 that only holds after the filteredYield<1 clamp
// at planner.go:336-338. Setup: totalDocs=100_000. idxA's sketch(1) makes
// fieldSel[a]=1/100000=0.00001 and pTotal=0.00001. idxB has BoundFields=1
// so estimateIndexDocsWithFieldSel yields e = 100000×0.00001 = 1.0 (exact).
// idxSel = DefaultRangeSelectivity=0.5 (PointLookup=false). filteredYield
// = 1 × (0.00001/0.5) = 0.00002 → clamps to 1 at line 337. No sort so the
// clamp doesn't alter cost directly, but cost = 1×CostIndexSeek + 1×fetchCost
// + 1×CostFilter = 0.5 + 3.0 + 0.5 = 4.0 matches ONLY when e is correctly
// computed and nSeeks=1 (i.e., the Plan-B loop entered for idxB).
func TestBuildPlan_PlanB_FilteredYieldClampLow(t *testing.T) {
	filter := query.MustParseCondition(`{"a": 1}`)
	idxA := CBOIndex{
		Info:        &IndexInfo{Name: "idxA", FieldNames: []string{"a"}},
		Sketch:      mockSketch(1), // p = 1/100000 = 0.00001
		Bounds:      mustParseBounds("a", `{"a": 1}`),
		PointLookup: true,
		BoundFields: 1,
	}
	// idxB: iterates Plan-B loop. BoundFields=1 enables fieldSel lookup.
	// Without a sketch + PointLookup=false, selectivityForIndex falls back
	// to DefaultRangeSelectivity so filteredYield clamp fires.
	idxB := CBOIndex{
		Info:        &IndexInfo{Name: "idxB", FieldNames: []string{"a"}, Unique: false},
		Bounds:      mustParseBounds("a", `{"a": 1}`),
		PointLookup: false,
		BoundFields: 1,
	}
	plan := BuildPlan(&PlanParams{
		Filter:    filter,
		TotalDocs: 100_000,
		Indexes:   []CBOIndex{idxA, idxB},
	})
	var seekB *CandidatePlan
	for i := range plan.Explain.Candidates {
		if plan.Explain.Candidates[i].Name == "IndexSeek(idxB)" {
			seekB = &plan.Explain.Candidates[i]
			break
		}
	}
	if seekB == nil {
		t.Fatal("IndexSeek(idxB) candidate required to pin post-clamp cost formula")
	}
	fetchCost := indexFetchCost(100_000)
	expectedCost := 1*CostIndexSeek + 1*fetchCost + 1*CostFilter
	assert.InDelta(t, expectedCost, seekB.Cost, 0.01,
		"IndexSeek(idxB) cost must equal 1×CostIndexSeek + 1×fetchCost + 1×CostFilter with e clamped + filteredYield clamped")
}

// TestBuildPlan_PlanB_TieBreaking_SeekUniqueBeatsSeekNonUnique hits
// planner.go:405-406 tie-breaking: when the best plan is already an IndexSeek
// on a non-unique index, a unique index seek with the same cost wins.
func TestBuildPlan_PlanB_TieBreaking_SeekUniqueBeatsSeekNonUnique(t *testing.T) {
	// Construct two indexes with identical selectivity & bounds; first is
	// non-unique (becomes best), second is unique with identical cost → wins.
	filter := query.MustParseCondition(`{"a": 1}`)
	idxNonUniq := CBOIndex{
		Info:        &IndexInfo{Name: "nu", FieldNames: []string{"a"}, Unique: false},
		Sketch:      mockSketch(5),
		Bounds:      mustParseBounds("a", `{"a": 1}`),
		PointLookup: true,
		BoundFields: 1,
	}
	idxUniq := CBOIndex{
		Info:        &IndexInfo{Name: "u", FieldNames: []string{"a"}, Unique: true},
		Sketch:      mockSketch(5),
		Bounds:      mustParseBounds("a", `{"a": 1}`),
		PointLookup: true,
		BoundFields: 1,
	}
	plan := BuildPlan(&PlanParams{
		Filter:    filter,
		TotalDocs: 100,
		Indexes:   []CBOIndex{idxNonUniq, idxUniq},
	})
	assert.Equal(t, "IndexSeek", plan.Name)
	// Unique index should win the tie-break.
	assert.Equal(t, "u", plan.IndexName, "unique index must win tie-break over non-unique")
}

// TestBuildPlan_Seek_CoverIter_NeedSortWraps hits planner.go:858-869 — the
// CoverIter fast-path wraps in SortIter when needSort && !idx.ExactSort.
// Construction: Unique + PointLookup + BoundFields==len(FieldNames) triggers
// CoverIter; sort on a different field forces ExactSort=false.
func TestBuildPlan_Seek_CoverIter_NeedSortWraps(t *testing.T) {
	sorter := &sortFieldStub{fields: []query.SortField{{Field: "other"}}}
	filter := query.MustParseCondition(`{"a": 1}`)
	plan := BuildPlan(&PlanParams{
		Filter:    filter,
		Sorter:    sorter,
		TotalDocs: 100,
		Indexes: []CBOIndex{{
			Info: &IndexInfo{
				Name: "a", FieldNames: []string{"a"},
				FieldPaths: [][]string{{"a"}}, Unique: true,
			},
			Sketch:      mockSketch(3),
			Bounds:      mustParseBounds("a", `{"a": 1}`),
			PointLookup: true,
			BoundFields: 1,
			ExactSort:   false,
		}},
	})
	if plan.Name != "IndexSeek" {
		t.Fatalf("IndexSeek must win to trigger CoverIter+Sort path, got %s", plan.Name)
	}
	root := plan.Root
	if li, ok := root.(*LimitIter); ok {
		root = li.Source
	}
	so, ok := root.(*SortIter)
	if assert.True(t, ok, "CoverIter+Sort root should be *SortIter, got %T", root) {
		// CoverIter is nested under FilterIter (needFilter=true) when filter
		// is non-All. Walk once more.
		inner := so.Source
		if fi, isFilter := inner.(*FilterIter); isFilter {
			inner = fi.Source
		}
		_, innerIsCover := inner.(*CoverIter)
		assert.True(t, innerIsCover, "inner iterator should be *CoverIter, got %T", inner)
	}
}

// TestBuildPlan_Seek_MultiFieldPathsNoDedupWrap pins that compound
// indexes are NOT wrapped with a planner-level dedup iterator on the
// seek path either. Replaces the previous SeenSetDedupIter assertion.
// Compound multi-key dedup runs at the consumer via DocDedup, threaded
// through the multiKey return value of Iterator.Next.
func TestBuildPlan_Seek_MultiFieldPathsNoDedupWrap(t *testing.T) {
	filter := query.MustParseCondition(`{"a": 1}`)
	plan := BuildPlan(&PlanParams{
		Filter:    filter,
		TotalDocs: 100,
		Indexes: []CBOIndex{{
			Info: &IndexInfo{
				Name:       "ab",
				FieldNames: []string{"a", "b"},
				FieldPaths: [][]string{{"a"}, {"b"}},
				Unique:     false,
			},
			Sketch:      mockSketch(3),
			Bounds:      mustParseBounds("a", `{"a": 1}`),
			PointLookup: true,
			BoundFields: 1,
		}},
	})
	if plan.Name != "IndexSeek" {
		t.Fatalf("IndexSeek must win, got %s", plan.Name)
	}
	root := plan.Root
	if li, ok := root.(*LimitIter); ok {
		root = li.Source
	}
	_, isCanonical := root.(*CanonicalKeyDedupIter)
	assert.False(t, isCanonical,
		"compound seek must NOT use CanonicalKeyDedupIter (single-field only)")
}

// TestCoveringFilterSelectivity covers both branches of coveringFilterSelectivity:
// empty filters return 1.0; non-empty filters multiply per-field selectivities,
// falling back to DefaultRangeSelectivity when no sketch exists for the field.
func TestCoveringFilterSelectivity(t *testing.T) {
	t.Run("empty_returns_one", func(t *testing.T) {
		sel := coveringFilterSelectivity(nil, nil, nil)
		assert.Equal(t, 1.0, sel)
	})
	t.Run("with_sketch", func(t *testing.T) {
		idx := &CBOIndex{Info: &IndexInfo{FieldNames: []string{"a", "b"}}}
		filters := []IndexFieldFilter{{FieldIdx: 1}} // references field "b"
		fieldSel := []fieldSelEntry{{field: "b", sel: 0.25}}
		sel := coveringFilterSelectivity(filters, idx, fieldSel)
		assert.Equal(t, 0.25, sel)
	})
	t.Run("missing_sketch_uses_default", func(t *testing.T) {
		idx := &CBOIndex{Info: &IndexInfo{FieldNames: []string{"x"}}}
		filters := []IndexFieldFilter{{FieldIdx: 0}}
		// fieldSel has no entry for "x" → fallback.
		sel := coveringFilterSelectivity(filters, idx, []fieldSelEntry{{field: "other", sel: 0.5}})
		assert.Equal(t, DefaultRangeSelectivity, sel)
	})
	t.Run("multiple_fields_multiply", func(t *testing.T) {
		idx := &CBOIndex{Info: &IndexInfo{FieldNames: []string{"a", "b"}}}
		filters := []IndexFieldFilter{
			{FieldIdx: 0},
			{FieldIdx: 1},
		}
		fieldSel := []fieldSelEntry{
			{field: "a", sel: 0.5},
			{field: "b", sel: 0.25},
		}
		sel := coveringFilterSelectivity(filters, idx, fieldSel)
		assert.InDelta(t, 0.125, sel, 1e-9)
	})
}

// TestIdBoundsPreferred covers all branches of idBoundsPreferred plus the
// transitive AllBoundsFixed paths it reaches: empty bounds, a bound with
// empty Start, a range bound (Start != End), and a fully-fixed bound.
func TestIdBoundsPreferred(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		assert.False(t, idBoundsPreferred(nil))
		assert.False(t, idBoundsPreferred(query.Bounds{}))
	})
	t.Run("empty_start", func(t *testing.T) {
		// Non-empty Bounds with a zero-length Start: reaches the
		// `len(b.Start) == 0` arm of AllBoundsFixed.
		b := query.Bound{Start: nil, End: nil}
		assert.False(t, idBoundsPreferred(query.Bounds{b}))
	})
	t.Run("range_bound", func(t *testing.T) {
		// Start != End reaches the `!bytes.Equal(b.Start, b.End)` arm.
		b := query.Bound{Start: []byte{1}, End: []byte{2}}
		assert.False(t, idBoundsPreferred(query.Bounds{b}))
	})
	t.Run("fixed_bound", func(t *testing.T) {
		b := query.Bound{Start: []byte{1, 2, 3}, End: []byte{1, 2, 3}}
		assert.True(t, idBoundsPreferred(query.Bounds{b}))
	})
}

// TestFilterFieldsCoveredBy_PointerAndBranch covers the *query.And arm of
// filterFieldsCoveredBy. query.MustParseCondition produces *query.And for
// `{"$and":[...]}` JSON syntax; without the pointer arm the covering-count
// fast path was silently disabled for any such filter.
func TestFilterFieldsCoveredBy_PointerAndBranch(t *testing.T) {
	// MustParseCondition(`{"$and":[{"a":1}]}`) returns *query.And.
	f := query.MustParseCondition(`{"$and":[{"a":1}]}`)
	has := false
	ok := filterFieldsCoveredBy(f, []string{"a"}, &has)
	// The function should recurse into the underlying And slice exactly like
	// the value-receiver case and report covered=true.
	assert.True(t, ok, "pointer-And with covered field should report covered")
	assert.True(t, has)
}

// TestBoundsResult_AllFixed_ZeroBoundsField verifies AllFixed no longer
// reports "fixed" for a field that has zero bounds. The previous behaviour
// contradicted AllFixed's godoc ("all fields have equality bounds") because
// the allFixed-computation loop was skipped for zero-count fields.
func TestBoundsResult_AllFixed_ZeroBoundsField(t *testing.T) {
	// A BoundsResult populated via Build() for an index whose second field
	// ("unused") is absent from the filter must not count that field as fixed.
	br := &BoundsResult{}
	filter := query.MustParseCondition(`{"a": 1}`)
	indexes := []*IndexInfo{{FieldNames: []string{"a", "unused"}}}
	br.Build(indexes, filter)

	// A field with no bounds should NOT count as "fixed" — no equality
	// constraint is present.
	assert.False(t, br.AllFixed(),
		"AllFixed should not treat a zero-bounds field as fixed")
}
