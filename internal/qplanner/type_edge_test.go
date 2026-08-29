package qplanner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
)

// A bracket edge stays marked through inversion and tuple concatenation, so
// a compound chain whose descending LAST field carries $gt/$gte keeps the
// exact successor Start with no +0x01 pad (which would drop every key whose
// first inverted payload byte is 0x00: doubles >= ~1e308, +Inf, NaN, ...).
func TestFinalizeIndexBounds_CompoundDescTail_EdgeUnpadded(t *testing.T) {
	info := &IndexInfo{FieldNames: []string{"k", "a"}, Reverse: []bool{false, true}}
	cond := query.MustParseCondition(`{"k":1,"a":{"$gt":5}}`)
	bounds, chainLen := ComputeIndexBounds(info, buildBoundsResult(info, cond))
	require.Equal(t, 2, chainLen)
	require.Len(t, bounds, 1)
	assert.True(t, bounds[0].StartIsTypeEdge())
	assert.False(t, bounds[0].EndIsTypeEdge())

	idx := &CBOIndex{Info: info, Reverse: info.Reverse, BoundFields: 2, Bounds: bounds}
	idx.Bounds = AdjustBoundsForNonUnique(idx.Bounds)
	finalizeIndexBounds(idx)
	want := append(append([]byte{}, encNum(1)...), ^byte(anyenc.TypeNumber+1)+1)
	assert.Equal(t, want, []byte(idx.Bounds[0].Start), "successor Start must not be padded")
	assert.True(t, idx.Bounds[0].StartInclude)
	// The value End keeps its exclusive pad.
	assert.Equal(t, append(append(append([]byte{}, encNum(1)...), invAll(encNum(5))...), 0x01), []byte(idx.Bounds[0].End))
}

// Two bounds in one bracket on a descending index: the edge Start [0xFD] is
// a byte-prefix of every inverted number key, so the escape-continuation
// ordering rule must not apply to it — it sorts first.
func TestTransformReverseBounds_EdgeStartOrdersFirst(t *testing.T) {
	idx := &IndexInfo{FieldNames: []string{"a"}, Reverse: []bool{true}}
	cond := query.MustParseCondition(`{"$or":[{"a":{"$gt":5}},{"a":{"$lt":1}}]}`)
	bounds, _ := ComputeIndexBounds(idx, buildBoundsResult(idx, cond))
	require.Len(t, bounds, 2)
	// (5, <string>) inverts to [0xFD, ^5): the numbers above 5 come first in
	// stored order; [<number>, 1) inverts to (^1, 0xFE).
	assert.Equal(t, []byte{^byte(anyenc.TypeNumber+1) + 1}, []byte(bounds[0].Start))
	assert.True(t, bounds[0].StartIsTypeEdge())
	assert.Equal(t, invAll(encNum(5)), []byte(bounds[0].End))
	assert.Equal(t, invAll(encNum(1)), []byte(bounds[1].Start))
	assert.True(t, bounds[1].EndIsTypeEdge())
}

// $gt null on a descending index inverts to Start == End with an exclusive
// side: an empty range, never a point lookup.
func TestComputeIndexBounds_EmptyRangeIsNotFixed(t *testing.T) {
	idx := &IndexInfo{FieldNames: []string{"a"}, Reverse: []bool{true}}
	for _, cond := range []string{`{"a":{"$gt":null}}`, `{"a":{"$gt":true}}`} {
		bounds, _ := ComputeIndexBounds(idx, buildBoundsResult(idx, query.MustParseCondition(cond)))
		require.Len(t, bounds, 1, cond)
		assert.False(t, AllBoundsFixed(bounds), cond)
	}
}
