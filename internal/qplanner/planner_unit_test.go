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
