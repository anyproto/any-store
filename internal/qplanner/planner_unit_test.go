package qplanner

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/btree"
	"github.com/anyproto/any-store/query"
)

// closeTrackingIter is a minimal Iterator whose Close call is observable.
type closeTrackingIter struct {
	closed int
}

func (c *closeTrackingIter) Next() ([]byte, []byte, error) { return nil, nil, nil }
func (c *closeTrackingIter) Close()                        { c.closed++ }
func (c *closeTrackingIter) String() string                { return "track" }

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
	t.Run("pointer_and_falls_through_default", func(t *testing.T) {
		// filterFieldsCoveredBy has no `case *query.And`, so a pointer-And
		// hits the default branch and returns false. This pins the current
		// behavior; the asymmetry with collectUncoveredFilterFields is
		// documented in bugs.md.
		inner := query.And{query.MustParseCondition(`{"a": 1}`)}
		has := false
		ok := filterFieldsCoveredBy(&inner, []string{"a"}, &has)
		assert.False(t, ok)
		assert.False(t, has, "default branch must not set hasFields")
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
			Info:   &IndexInfo{FieldNames: []string{"a", "b"}},
			Bounds: query.Bounds{{Start: []byte{1}, End: []byte{1}}},
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
		idx := &CBOIndex{
			Info: &IndexInfo{
				FieldNames: []string{"a", "b"},
				Reverse:    []bool{false, true},
			},
			BoundFields: 1,
		}
		br := &BoundsResult{
			Fields: []FieldBounds{{
				Field: "b", Start: 0, Count: 1, Fixed: true,
			}},
			Bounds: []query.Bound{{Start: []byte{0x00, 0xff, 0x11}, End: []byte{0x00, 0xff, 0x11}}},
		}
		got := coveringFilterFields(idx, br)
		if assert.Len(t, got, 1) {
			assert.Equal(t, 1, got[0].FieldIdx)
			// Each byte should be bitwise-NOT of the source.
			assert.Equal(t, []byte{0xff, 0x00, 0xee}, got[0].MatchValue,
				"reverse field must be bitwise-inverted")
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

// TestShouldReverse covers all branches: nil sorter, empty fields, and the
// fields[0].Reverse read-through (both forward and reverse).
func TestShouldReverse(t *testing.T) {
	t.Run("nil_sorter", func(t *testing.T) {
		assert.False(t, shouldReverse(nil, nil))
	})
	t.Run("empty_fields", func(t *testing.T) {
		s := &sortFieldStub{fields: nil}
		assert.False(t, shouldReverse(s, &CBOIndex{}))
	})
	t.Run("forward", func(t *testing.T) {
		s := &sortFieldStub{fields: []query.SortField{{Field: "a", Reverse: false}}}
		assert.False(t, shouldReverse(s, &CBOIndex{}))
	})
	t.Run("reverse", func(t *testing.T) {
		s := &sortFieldStub{fields: []query.SortField{{Field: "a", Reverse: true}}}
		assert.True(t, shouldReverse(s, &CBOIndex{}))
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
	// --- SeenSetDedupIter (no Plan field) ---
	t.Run("SeenSetDedupIter_recurses_no_plan_field", func(t *testing.T) {
		inner := &FilterIter{}
		d := &SeenSetDedupIter{Source: inner}
		setPlanRef(d, plan)
		assert.Same(t, plan, inner.Plan)
	})
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
		ex, pa := IndexSortMatch(&IndexInfo{FieldNames: []string{"a"}}, nil, 0)
		assert.False(t, ex)
		assert.False(t, pa)
	})
	t.Run("empty_index", func(t *testing.T) {
		ex, pa := IndexSortMatch(&IndexInfo{}, []query.SortField{{Field: "a"}}, 0)
		assert.False(t, ex)
		assert.False(t, pa)
	})
	t.Run("no_match", func(t *testing.T) {
		ex, pa := IndexSortMatch(
			&IndexInfo{FieldNames: []string{"a"}},
			[]query.SortField{{Field: "z"}},
			0,
		)
		assert.False(t, ex)
		assert.False(t, pa)
	})
	t.Run("exact_match", func(t *testing.T) {
		ex, pa := IndexSortMatch(
			&IndexInfo{FieldNames: []string{"a", "b"}},
			[]query.SortField{{Field: "a"}, {Field: "b"}},
			0,
		)
		assert.True(t, ex)
		assert.False(t, pa)
	})
	t.Run("partial_match", func(t *testing.T) {
		// 3-field sort, index has only first 2. match=2 != 3 → partial.
		ex, pa := IndexSortMatch(
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
		ex, pa := IndexSortMatch(
			&IndexInfo{FieldNames: []string{"a", "b"}},
			[]query.SortField{{Field: "b"}},
			1,
		)
		assert.True(t, ex)
		assert.False(t, pa)
	})
	t.Run("equality_prefix_ignored_when_out_of_range", func(t *testing.T) {
		// equalityPrefix >= len(FieldNames): matchAt returns 0.
		ex, pa := IndexSortMatch(
			&IndexInfo{FieldNames: []string{"a"}},
			[]query.SortField{{Field: "a"}},
			5,
		)
		assert.True(t, ex, "matchAt(0) still finds a")
		assert.False(t, pa)
	})
	t.Run("direction_mismatch_breaks", func(t *testing.T) {
		// First field matches with sameMode=true (both forward).
		// Second field also sameMode? idxRev=false, sortRev=true → curSame=false.
		// This is inconsistent with first → loop breaks at 1, match=1 partial.
		ex, pa := IndexSortMatch(
			&IndexInfo{FieldNames: []string{"a", "b"}, Reverse: []bool{false, false}},
			[]query.SortField{{Field: "a", Reverse: false}, {Field: "b", Reverse: true}},
			0,
		)
		assert.False(t, ex)
		assert.True(t, pa)
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
	t.Run("compound_equality_uses_default_range", func(t *testing.T) {
		// Compound index (a, b) with equality on a only → first field fi=0
		// but len(FieldNames) != 1 → compound-equality branch.
		f := query.MustParseCondition(`{"a": 1}`)
		idx := CBOIndex{
			Info:   &IndexInfo{Name: "ab", FieldNames: []string{"a", "b"}},
			Sketch: mockSketch(10),
		}
		br := &BoundsResult{}
		br.Build([]*IndexInfo{idx.Info}, f)
		sel := calculateSelectivity(f, []CBOIndex{idx}, 100, br)
		// sel = DefaultRangeSelectivity (compound branch on a, no bounds for b → continues)
		assert.Equal(t, DefaultRangeSelectivity, sel)
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
	t.Run("partial_bounds_falls_back", func(t *testing.T) {
		// PointLookup=false or BoundFields != len(FieldNames) → fallback.
		idx := &CBOIndex{
			Info:        &IndexInfo{FieldNames: []string{"a", "b"}},
			Bounds:      query.Bounds{{Start: []byte{1}, End: []byte{1}}},
			Sketch:      mockSketch(10),
			PointLookup: true,
			BoundFields: 1, // only a bound, b is not bounded
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
		case *SeenSetDedupIter:
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

// TestBuildPlan_IndexScan_MultiFieldPathsUsesSeenSetDedup hits planner.go:1027
// — when idx.FieldPaths has length > 1, buildIndexScanChain wraps in
// SeenSetDedupIter (not CanonicalKeyDedupIter).
func TestBuildPlan_IndexScan_MultiFieldPathsUsesSeenSetDedup(t *testing.T) {
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
		// Skipping masks CBO cost regressions. If this ever trips, the
		// multi-field dedup branch at planner.go:1027 is no longer reached
		// — investigate rather than relax the assertion.
		t.Fatalf("BuildPlan chose %s; expected IndexScan to exercise SeenSetDedupIter", plan.Name)
	}
	// Walk to the topmost dedup wrapper.
	root := plan.Root
	if li, ok := root.(*LimitIter); ok {
		root = li.Source
	}
	_, isSeenSet := root.(*SeenSetDedupIter)
	_, isCanonical := root.(*CanonicalKeyDedupIter)
	assert.True(t, isSeenSet, "compound index must wrap with SeenSetDedupIter")
	assert.False(t, isCanonical, "compound index must NOT use CanonicalKeyDedupIter")
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
		case *SeenSetDedupIter:
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
// fixed equality → coverFilters has 1 entry). idxSel falls back to
// DefaultRangeSelectivity=0.5 (BoundFields != len(FieldNames)). coverSel is
// DefaultRangeSelectivity=0.5 (no fieldSel because compound index doesn't
// contribute to fieldSelectivity). scanPopulation = totalDocs × idxSel =
// 100 × 0.5 = 50.
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
	const scanPopulation = 50.0
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
//      = 1×0.5 + 10×3.0 + 10×0.5 = 35.5.
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
// where s = (Limit+Offset)/scanSel. Setup: filter {"a":5,"b":10}, compound
// index (a,b) with BoundFields=1 → idxSel=DefaultRangeSelectivity=0.5. pTotal
// from two compound-equality branches (a then b) = 0.5×0.5 = 0.25. scanSel =
// 0.25/0.5 = 0.5 (no clamp). s = 10/0.5 = 20. coverSel=0.5 (DefaultRange, no
// fieldSel for compound index). Cost = 20×0.1 + 20×0.5×3.0 + 20×0.5 = 42.0.
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
	const s = 20.0 // (Limit+Offset)/scanSel = 10/0.5
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

// TestBuildPlan_Seek_MultiFieldPathsUsesSeenSetDedup hits planner.go:948-949
// in buildIndexSeekChain — compound FieldPaths wrap in SeenSetDedupIter
// (not CanonicalKeyDedupIter). Forces the main seek path (not CoverIter) by
// using Unique=false.
func TestBuildPlan_Seek_MultiFieldPathsUsesSeenSetDedup(t *testing.T) {
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
		t.Fatalf("IndexSeek must win to reach compound-seek dedup path, got %s", plan.Name)
	}
	root := plan.Root
	if li, ok := root.(*LimitIter); ok {
		root = li.Source
	}
	_, isSeenSet := root.(*SeenSetDedupIter)
	assert.True(t, isSeenSet, "compound seek with Unique=false must wrap in SeenSetDedupIter, got %T", root)
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
