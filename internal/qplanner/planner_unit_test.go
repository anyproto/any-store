package qplanner

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

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

// TestFilterFieldsCoveredBy_PointerAndBranch is expected to FAIL:
// filterFieldsCoveredBy has no *query.And case, so any filter parsed from
// `{"$and":[...]}` (which produces a *query.And) returns false even when the
// index fully covers the filter. See bugs.md.
func TestFilterFieldsCoveredBy_PointerAndBranch(t *testing.T) {
	t.Skip("FAIL: filterFieldsCoveredBy missing *query.And case — see bugs.md")

	// MustParseCondition(`{"$and":[{"a":1}]}`) returns *query.And.
	f := query.MustParseCondition(`{"$and":[{"a":1}]}`)
	has := false
	ok := filterFieldsCoveredBy(f, []string{"a"}, &has)
	// Expectation: the function should recurse into the underlying And slice
	// exactly like the value-receiver case and report covered=true.
	assert.True(t, ok, "pointer-And with covered field should report covered")
	assert.True(t, has)
}

// TestBoundsResult_AllFixed_ZeroBoundsField is expected to FAIL:
// AllFixed returns true when every field has zero bounds, even though there
// are no equality constraints at all. See bugs.md.
func TestBoundsResult_AllFixed_ZeroBoundsField(t *testing.T) {
	t.Skip("FAIL: AllFixed returns true for fields with zero bounds — see bugs.md")

	// The following snapshot reproduces the bug: a BoundsResult populated via
	// Build() for an index whose field is absent from the filter ends up with
	// Fixed=true (the allFixed loop at planner.go:1264 is empty).
	br := &BoundsResult{}
	filter := query.MustParseCondition(`{"a": 1}`)
	indexes := []*IndexInfo{{FieldNames: []string{"a", "unused"}}}
	br.Build(indexes, filter)

	// Expectation the test holds the production to: a field with no bounds
	// should NOT count as "fixed" because it has no equality constraint.
	assert.False(t, br.AllFixed(),
		"AllFixed should not treat a zero-bounds field as fixed")
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
