package qplanner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/query"
)

// A forward (ascending-stored) tail must turn an exclusive Start into the
// enc(v)‖0xFF inclusive pad: entry keys are enc(v)‖suffix and the seek's
// byte-equality skip never removes them, so without the pad $gt v admits v.
func TestFinalizeIndexBounds_ForwardExclusiveStartPad(t *testing.T) {
	k := encNum(5)
	idx := &CBOIndex{
		Info:        &IndexInfo{FieldNames: []string{"a"}, Reverse: []bool{false}},
		Reverse:     []bool{false},
		BoundFields: 1,
	}

	t.Run("gt pads to inclusive 0xff", func(t *testing.T) {
		idx.Bounds = query.Bounds{{Start: k}}
		dedup := finalizeIndexBounds(idx)
		require.Len(t, idx.Bounds, 1)
		assert.Equal(t, append(append([]byte{}, k...), 0xff), []byte(idx.Bounds[0].Start))
		assert.True(t, idx.Bounds[0].StartInclude)
		assert.Empty(t, idx.Bounds[0].End)
		// Pre-pad bounds are handed to CanonicalKeyDedupIter untouched.
		require.Len(t, dedup, 1)
		assert.Equal(t, k, []byte(dedup[0].Start))
		assert.False(t, dedup[0].StartInclude)
	})
	t.Run("gte and ends untouched", func(t *testing.T) {
		in := query.Bounds{
			{Start: k, StartInclude: true},
			{End: k},
			{End: append(append([]byte{}, k...), 0xff), EndInclude: true},
		}
		idx.Bounds = in
		finalizeIndexBounds(idx)
		assert.Equal(t, in, idx.Bounds, "no exclusive Start: bounds returned as-is")
	})
	t.Run("ne pads only the exclusive-start half", func(t *testing.T) {
		idx.Bounds = query.Bounds{{End: k}, {Start: k}}
		finalizeIndexBounds(idx)
		require.Len(t, idx.Bounds, 2)
		assert.Equal(t, k, []byte(idx.Bounds[0].End))
		assert.False(t, idx.Bounds[0].EndInclude)
		assert.Equal(t, append(append([]byte{}, k...), 0xff), []byte(idx.Bounds[1].Start))
		assert.True(t, idx.Bounds[1].StartInclude)
	})
	t.Run("reverse tail leaves a bare-tag Start unpadded", func(t *testing.T) {
		// The successor form of an inverted bracket edge admits exactly the
		// keys of the remaining types; a +0x01 pad would drop the ones whose
		// first inverted payload byte is 0x00.
		rev := &CBOIndex{
			Info:        &IndexInfo{FieldNames: []string{"a"}, Reverse: []bool{true}},
			Reverse:     []bool{true},
			BoundFields: 1,
			Bounds:      query.Bounds{{Start: []byte{0xFD}, StartInclude: true, End: invAll(k)}},
		}
		finalizeIndexBounds(rev)
		require.Len(t, rev.Bounds, 1)
		assert.Equal(t, []byte{0xFD}, []byte(rev.Bounds[0].Start))
		assert.True(t, rev.Bounds[0].StartInclude)
		assert.Equal(t, append(invAll(k), 0x01), []byte(rev.Bounds[0].End))
	})
	t.Run("reverse tail keeps the reverse rule", func(t *testing.T) {
		rev := &CBOIndex{
			Info:        &IndexInfo{FieldNames: []string{"a"}, Reverse: []bool{true}},
			Reverse:     []bool{true},
			BoundFields: 1,
			Bounds:      query.Bounds{{Start: invAll(k)}},
		}
		finalizeIndexBounds(rev)
		require.Len(t, rev.Bounds, 1)
		assert.Equal(t, append(invAll(k), 0xff), []byte(rev.Bounds[0].Start))
		assert.True(t, rev.Bounds[0].StartInclude)
	})
}

// The pad is a stored-key detail: Explain keeps rendering the logical
// exclusive bound.
func TestBoundString_ExclusiveStartPadRendersExclusive(t *testing.T) {
	k := encNum(5)
	padded := query.Bound{Start: append(append([]byte{}, k...), 0xff), StartInclude: true}
	assert.Equal(t, query.Bound{Start: k}.String(), padded.String())
	assert.Contains(t, padded.String(), "('5'")
}
