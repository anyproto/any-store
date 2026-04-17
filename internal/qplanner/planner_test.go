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

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/btree"
	"github.com/anyproto/any-store/query"
	"github.com/anyproto/any-store/syncpool"
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
		_, docId, err := it.Next()
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
		_, docId, err := it.Next()
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
		_, docId, err := it.Next()
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

// TestSetPlanRef_Coverage_SeenSetDedupChainPropagation asserts the same
// propagation for SeenSetDedupIter-wrapped chains (compound multi-key case).
// SeenSetDedupIter itself has no Plan field, but the setPlanRef case for
// it must still recurse into Source so downstream FilterIter/FetchIter
// receive the Plan reference.
func TestSetPlanRef_Coverage_SeenSetDedupChainPropagation(t *testing.T) {
	leaf := &IndexIter{
		IdxInfo: &IndexInfo{Name: "compound", FieldNames: []string{"a", "b"}},
	}
	fetch := &FetchIter{
		Source: leaf,
		Buf:    &syncpool.DocBuffer{},
	}
	filter := &FilterIter{
		Source: fetch,
		Filter: query.MustParseCondition(`{"a": 1}`),
		Buf:    &syncpool.DocBuffer{},
	}
	dedup := &SeenSetDedupIter{Source: filter}

	plan := &Plan{}
	setPlanRef(dedup, plan)

	assert.Same(t, plan, filter.Plan, "FilterIter.Plan must be set through SeenSetDedupIter")
	assert.Same(t, plan, fetch.Plan, "FetchIter.Plan must be set through SeenSetDedupIter")
}

// --- Coverage tests from limit_iter_coverage_test.go ---

// seqIter emits a fixed sequence of (key, docId) pairs built from integer ids.
// It is deliberately distinct from dedup_iter_test.go's fakeIter (which also
// mutates Plan.DocParsed) — LimitIter does not touch the Plan, so we keep this
// helper lean.
type seqIter struct {
	ids []int
	i   int
}

func (s *seqIter) Next() ([]byte, []byte, error) {
	if s.i >= len(s.ids) {
		return nil, nil, nil
	}
	id := s.ids[s.i]
	s.i++
	b := []byte(fmt.Sprintf("%d", id))
	return b, b, nil
}

func (s *seqIter) Close()         {}
func (s *seqIter) String() string { return "seq" }

func drain(it *LimitIter) []string {
	var out []string
	for {
		_, docId, err := it.Next()
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
	key, docId, err := it.Next()
	require.NoError(t, err)
	assert.Nil(t, key)
	assert.Nil(t, docId)

	// And stays exhausted on subsequent calls.
	key, docId, err = it.Next()
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

func (f *docFeedIter) Next() ([]byte, []byte, error) {
	if f.i >= len(f.feed) {
		if f.plan != nil {
			f.plan.DocParsed = nil
		}
		return nil, nil, nil
	}
	h := f.feed[f.i]
	f.i++
	if f.plan != nil {
		f.plan.DocParsed = h.doc
	}
	return h.docId, h.docId, nil
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
		_, docId, err := it.Next()
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
			key, docId, err := it.Next()
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
		_, docId, err := it.Next()
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
		k, _, err := it.Next()
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
		_, docId, err := it.Next()
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
	_, docId, err := it.Next()
	require.NoError(t, err)
	require.NotNil(t, docId, "the non-empty 'a' bound must contribute exactly one entry")

	// Decode the docId tuple (single string value) and check equality.
	gotDocId := anyenc.Tuple(docId).String()
	assert.Equal(t, `"doc-a"`, gotDocId,
		"only the second bound (Start='a') may contribute an entry")

	// Exhausted: no more bounds.
	_, docId2, err := it.Next()
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
	// Fast-path would yield a bare IndexScan (no Fetch/Dedup). Multi-range
	// $in must route through the full pipeline with a Dedup wrap.
	assert.Contains(t, chain, "Dedup", "multi-range $in must use the dedup pipeline")
	assert.Contains(t, chain, "Fetch", "multi-range $in must fetch docs for filtering/dedup")
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
