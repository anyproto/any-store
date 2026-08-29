package qplanner

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
)

// encNum returns the ascending anyenc encoding of an integer (as a query bound
// value would carry it).
func encNum(n int) []byte {
	return anyenc.AppendAnyValue(nil, int64(n))
}

func invAll(b []byte) []byte {
	out := make([]byte, len(b))
	for i, c := range b {
		out[i] = ^c
	}
	return out
}

// TestComputeIndexBounds_ReverseSingleField (Group B) verifies the reverse
// bound transform: for a reverse single field each operator's ascending-space
// bound is bitwise-inverted AND Start/End swapped, with open ends swapping
// sides. The result must be sorted/merged so IndexIter walks it in cursor order.
func TestComputeIndexBounds_ReverseSingleField(t *testing.T) {
	idx := &IndexInfo{FieldNames: []string{"b"}, Reverse: []bool{true}}
	k := encNum(5)

	t.Run("eq", func(t *testing.T) {
		bounds, _ := ComputeIndexBounds(idx, buildBoundsResult(idx, query.MustParseCondition(`{"b":5}`)))
		require.Len(t, bounds, 1)
		assert.Equal(t, invAll(k), []byte(bounds[0].Start))
		assert.Equal(t, invAll(k), []byte(bounds[0].End))
		assert.True(t, bounds[0].StartInclude)
		assert.True(t, bounds[0].EndInclude)
	})
	// Ordering ops are clamped to the operand's type bracket: the number
	// bracket is [0x02, 0x03) in ascending space, so its edges invert to the
	// bare tags ^0x03 = 0xFC (exclusive Start) and ^0x02 = 0xFD (a mid-value
	// prefix End, mapped to its successor 0xFE exclusive by the prefix rule).
	hiEdge := []byte{^byte(anyenc.TypeNumber + 1)}
	loEdgeSucc := []byte{^byte(anyenc.TypeNumber) + 1}
	t.Run("gt_becomes_hi_edge_start_to_inv_excl_end", func(t *testing.T) {
		// Gt 5 ascending = (5, 0x03). Inverted+swapped = (^0x03, ^5).
		bounds, _ := ComputeIndexBounds(idx, buildBoundsResult(idx, query.MustParseCondition(`{"b":{"$gt":5}}`)))
		require.Len(t, bounds, 1)
		assert.Equal(t, hiEdge, []byte(bounds[0].Start))
		assert.False(t, bounds[0].StartInclude)
		assert.Equal(t, invAll(k), []byte(bounds[0].End))
		assert.False(t, bounds[0].EndInclude)
	})
	t.Run("gte_becomes_hi_edge_start_to_inv_incl_end", func(t *testing.T) {
		bounds, _ := ComputeIndexBounds(idx, buildBoundsResult(idx, query.MustParseCondition(`{"b":{"$gte":5}}`)))
		require.Len(t, bounds, 1)
		assert.Equal(t, hiEdge, []byte(bounds[0].Start))
		assert.False(t, bounds[0].StartInclude)
		assert.Equal(t, invAll(k), []byte(bounds[0].End))
		assert.True(t, bounds[0].EndInclude)
	})
	t.Run("lt_becomes_inv_excl_start_to_lo_edge_end", func(t *testing.T) {
		// Lt 5 ascending = [0x02, 5). Inverted+swapped = (^5, succ(^0x02)).
		bounds, _ := ComputeIndexBounds(idx, buildBoundsResult(idx, query.MustParseCondition(`{"b":{"$lt":5}}`)))
		require.Len(t, bounds, 1)
		assert.Equal(t, invAll(k), []byte(bounds[0].Start))
		assert.False(t, bounds[0].StartInclude)
		assert.Equal(t, loEdgeSucc, []byte(bounds[0].End))
		assert.False(t, bounds[0].EndInclude)
	})
	t.Run("lte_becomes_inv_incl_start_to_lo_edge_end", func(t *testing.T) {
		bounds, _ := ComputeIndexBounds(idx, buildBoundsResult(idx, query.MustParseCondition(`{"b":{"$lte":5}}`)))
		require.Len(t, bounds, 1)
		assert.Equal(t, invAll(k), []byte(bounds[0].Start))
		assert.True(t, bounds[0].StartInclude)
		assert.Equal(t, loEdgeSucc, []byte(bounds[0].End))
		assert.False(t, bounds[0].EndInclude)
	})
	t.Run("two_sided_is_transform_of_forward", func(t *testing.T) {
		// query/filter.go And.IndexBounds over-approximates a two-sided scalar
		// range to the FIRST conjunct's bound (here $gte:3 ⇒ [3,+inf)), so the
		// reverse bound must equal the inverted+swapped form of whatever the
		// FORWARD index produces — derive the expectation from the forward path
		// rather than hardcoding [3,7].
		fwdIdx := &IndexInfo{FieldNames: []string{"b"}}
		cond := query.MustParseCondition(`{"b":{"$gte":3,"$lte":7}}`)
		fwd, _ := ComputeIndexBounds(fwdIdx, buildBoundsResult(fwdIdx, cond))
		rev, _ := ComputeIndexBounds(idx, buildBoundsResult(idx, cond))
		require.Len(t, fwd, 1)
		require.Len(t, rev, 1)
		// rev.Start == invert(fwd.End); rev.End == invert(fwd.Start), with the
		// open ends swapping sides (empty <-> empty).
		assert.Equal(t, invertBytes(fwd[0].End), []byte(rev[0].Start))
		assert.Equal(t, invertBytes(fwd[0].Start), []byte(rev[0].End))
		assert.Equal(t, fwd[0].EndInclude, rev[0].StartInclude)
		assert.Equal(t, fwd[0].StartInclude, rev[0].EndInclude)
	})
	t.Run("in_sorted_after_transform", func(t *testing.T) {
		// $in[3,1,2] → three point bounds; after inversion the stored order is
		// ^3 < ^2 < ^1, so SortAndMerge must put ^3 first.
		bounds, _ := ComputeIndexBounds(idx, buildBoundsResult(idx, query.MustParseCondition(`{"b":{"$in":[3,1,2]}}`)))
		require.Len(t, bounds, 3)
		for i := 1; i < len(bounds); i++ {
			assert.True(t, bytes.Compare(bounds[i-1].Start, bounds[i].Start) < 0,
				"transformed $in bounds must be ascending in stored space")
		}
		assert.Equal(t, invAll(encNum(3)), []byte(bounds[0].Start))
		assert.Equal(t, invAll(encNum(1)), []byte(bounds[2].Start))
	})
	t.Run("ne_sorted_after_transform", func(t *testing.T) {
		// $ne 5 ascending = {[-inf,5),(5,+inf]} → stored {[-inf,^5),(^5,+inf]}
		// → after swap+sort, two bounds with the smaller stored Start first.
		bounds, _ := ComputeIndexBounds(idx, buildBoundsResult(idx, query.MustParseCondition(`{"b":{"$ne":5}}`)))
		require.Len(t, bounds, 2)
		for i := 1; i < len(bounds); i++ {
			assert.True(t, bytes.Compare(bounds[i-1].Start, bounds[i].Start) <= 0)
		}
	})
}

// TestComputeIndexBounds_ReverseSingleField_FreshSlice (Group B mutation
// safety) verifies the single-field reverse path returns a slice independent of
// the aliased BoundsResult: mutating the returned bounds must not corrupt a
// second ComputeIndexBounds call's ascending bounds.
func TestComputeIndexBounds_ReverseSingleField_FreshSlice(t *testing.T) {
	br := buildBoundsResult(&IndexInfo{FieldNames: []string{"b"}}, query.MustParseCondition(`{"b":5}`))

	revIdx := &IndexInfo{FieldNames: []string{"b"}, Reverse: []bool{true}}
	fwdIdx := &IndexInfo{FieldNames: []string{"b"}}

	revBounds, _ := ComputeIndexBounds(revIdx, br)
	require.Len(t, revBounds, 1)
	// Corrupt the returned reverse bounds in place.
	for i := range revBounds[0].Start {
		revBounds[0].Start[i] = 0
	}

	// The forward (ascending) bounds from the SAME BoundsResult must be intact.
	fwdBounds, _ := ComputeIndexBounds(fwdIdx, br)
	require.Len(t, fwdBounds, 1)
	assert.Equal(t, encNum(5), []byte(fwdBounds[0].Start),
		"reverse transform must not mutate the shared ascending BoundsResult")
}

// TestComputeIndexBounds_CompoundReverseTrailing (Group B) verifies that only
// the reverse field's segment of a compound tuple bound is inverted, with the
// equality prefix left ascending.
func TestComputeIndexBounds_CompoundReverseTrailing(t *testing.T) {
	idx := &IndexInfo{FieldNames: []string{"a", "b"}, Reverse: []bool{false, true}}
	// a == 1 (equality prefix, ascending), range on b (reverse, inverted).
	// And.IndexBounds over-approximates the two-sided b range to the first
	// conjunct, [2, 0x03) (the number bracket's upper edge), so after transform
	// the b segment is (^0x03, ^2]: Start = ^0x03 exclusive, End = ^2.
	cond := query.MustParseCondition(`{"a":1,"b":{"$gte":2,"$lte":8}}`)
	bounds, chainLen := ComputeIndexBounds(idx, buildBoundsResult(idx, cond))
	require.Equal(t, 2, chainLen)
	require.Len(t, bounds, 1)

	encA := encNum(1)
	// The a segment must stay ascending (forward-declared equality prefix).
	assert.True(t, bytes.HasPrefix(bounds[0].Start, encA), "a segment must be ascending")
	assert.True(t, bytes.HasPrefix(bounds[0].End, encA), "a segment must be ascending")
	// b's transformed Start is the inverted bracket edge → a ++ ^0x03, exclusive.
	assert.Equal(t, append(append([]byte{}, encA...), ^byte(anyenc.TypeNumber+1)), []byte(bounds[0].Start))
	assert.False(t, bounds[0].StartInclude)
	// b's transformed End == ^encNum(2) → combined End == a ++ ^2.
	wantEnd := append(append([]byte{}, encA...), invAll(encNum(2))...)
	assert.Equal(t, wantEnd, []byte(bounds[0].End))
	// Crucially the b segment of End is inverted (descending storage), proving
	// only the reverse field is transformed.
	bSeg := bounds[0].End[len(encA):]
	assert.Equal(t, invAll(encNum(2)), []byte(bSeg))
}
