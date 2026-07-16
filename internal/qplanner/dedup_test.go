package qplanner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDocDedup_Accept_AllScalarBypass pins the zero-allocation path: a
// stream of multiKey=false entries never triggers map allocation.
func TestDocDedup_Accept_AllScalarBypass(t *testing.T) {
	var d DocDedup
	for _, id := range []string{"a", "b", "c", "a"} { // duplicate "a" too
		require.True(t, d.Accept([]byte(id), false),
			"multiKey=false must always Accept, regardless of repeats")
	}
	assert.Nil(t, d.seen,
		"map must remain nil for an all-scalar stream")
}

// TestDocDedup_Accept_LazyAllocOnFirstMultiKey pins that the map is
// allocated lazily only when the first multiKey=true entry arrives.
func TestDocDedup_Accept_LazyAllocOnFirstMultiKey(t *testing.T) {
	var d DocDedup
	require.True(t, d.Accept([]byte("a"), false))
	require.Nil(t, d.seen, "no map yet")

	require.True(t, d.Accept([]byte("b"), true),
		"first multiKey entry must Accept")
	require.NotNil(t, d.seen, "map must be allocated on first multiKey=true")
	assert.Len(t, d.seen, 1)
}

// TestDocDedup_Accept_DeduplicatesMultiKeyByDocId pins the dedup
// behaviour on multi-key entries: first sighting accepted, subsequent
// rejected.
func TestDocDedup_Accept_DeduplicatesMultiKeyByDocId(t *testing.T) {
	var d DocDedup
	assert.True(t, d.Accept([]byte("p1"), true))
	assert.True(t, d.Accept([]byte("p2"), true))
	assert.False(t, d.Accept([]byte("p1"), true), "p1 already seen")
	assert.False(t, d.Accept([]byte("p2"), true), "p2 already seen")
	assert.True(t, d.Accept([]byte("p3"), true))
	assert.Len(t, d.seen, 3)
}

// TestDocDedup_Accept_MixedStream pins the most subtle case: a stream
// where some entries are scalar and some are multi-key. Scalar entries
// bypass the map entirely; the map only contains multi-key docIds.
//
// Important invariant: a scalar entry asserts "this docId will not
// reappear in the rest of the stream". So the helper does NOT need to
// add scalar docIds to the seen map — they're trusted to be unique by
// the iterator that emitted them.
func TestDocDedup_Accept_MixedStream(t *testing.T) {
	var d DocDedup

	// Scalar p1: trusted unique, not added to seen.
	assert.True(t, d.Accept([]byte("p1"), false))
	assert.Nil(t, d.seen)

	// Multi-key p2: map allocated, p2 added.
	assert.True(t, d.Accept([]byte("p2"), true))
	assert.Len(t, d.seen, 1)

	// Multi-key p2 again: rejected.
	assert.False(t, d.Accept([]byte("p2"), true))
	assert.Len(t, d.seen, 1)

	// Scalar p3 after the map exists: still bypasses, not added.
	assert.True(t, d.Accept([]byte("p3"), false))
	assert.Len(t, d.seen, 1, "scalar entries do not enter the map")

	// Multi-key p3 after a scalar p3: a hostile case — the iterator
	// contract says scalar's docId won't reappear, so this would only
	// happen via an iterator bug. Document current behaviour: p3 is
	// accepted (correct: it wasn't in the map). Maps to "trust the
	// scalar promise; multi-key entries dedup independently".
	assert.True(t, d.Accept([]byte("p3"), true),
		"multi-key p3 after scalar p3 still passes — the scalar promise is the iterator's invariant, not the helper's")
}

// TestDocDedup_Reset clears the map for reuse without dropping its
// underlying storage.
func TestDocDedup_Reset(t *testing.T) {
	var d DocDedup
	d.Accept([]byte("a"), true)
	d.Accept([]byte("b"), true)
	require.Len(t, d.seen, 2)

	d.Reset()
	assert.Len(t, d.seen, 0, "Reset must clear seen entries")
	assert.NotNil(t, d.seen, "Reset must keep the map allocated for reuse")

	// Same ids should now be accepted again.
	assert.True(t, d.Accept([]byte("a"), true))
	assert.True(t, d.Accept([]byte("b"), true))
}

// stubIter feeds ForEachDistinct a fixed (docId, multiKey) sequence, then EOI.
type stubIter struct {
	rows []stubRow
	pos  int
	err  error
}

type stubRow struct {
	id string
	mk bool
}

func (s *stubIter) Next() ([]byte, []byte, bool, error) {
	if s.pos >= len(s.rows) {
		if s.err != nil {
			return nil, nil, false, s.err
		}
		return nil, nil, false, nil
	}
	r := s.rows[s.pos]
	s.pos++
	return nil, []byte(r.id), r.mk, nil
}

func (s *stubIter) Close()         {}
func (s *stubIter) String() string { return "stub" }

func TestForEachDistinct_ScalarPassthrough(t *testing.T) {
	it := &stubIter{rows: []stubRow{{"a", false}, {"b", false}, {"a", false}}}
	var got []string
	require.NoError(t, ForEachDistinct(it, func(id []byte) error {
		got = append(got, string(id))
		return nil
	}))
	// multiKey=false is a hard uniqueness guarantee: no dedup is applied,
	// exactly like planIterator.Next.
	assert.Equal(t, []string{"a", "b", "a"}, got)
}

func TestForEachDistinct_MultiKeyDedup(t *testing.T) {
	it := &stubIter{rows: []stubRow{{"a", true}, {"b", true}, {"a", true}, {"c", false}}}
	var got []string
	require.NoError(t, ForEachDistinct(it, func(id []byte) error {
		got = append(got, string(id))
		return nil
	}))
	assert.Equal(t, []string{"a", "b", "c"}, got)
}

func TestForEachDistinct_PropagatesErrors(t *testing.T) {
	srcErr := assert.AnError
	it := &stubIter{rows: []stubRow{{"a", false}}, err: srcErr}
	var n int
	err := ForEachDistinct(it, func([]byte) error { n++; return nil })
	assert.ErrorIs(t, err, srcErr, "source error must surface")
	assert.Equal(t, 1, n, "rows before the error are delivered")

	it2 := &stubIter{rows: []stubRow{{"a", false}, {"b", false}}}
	err = ForEachDistinct(it2, func([]byte) error { return assert.AnError })
	assert.ErrorIs(t, err, assert.AnError, "fn error must stop the drive and surface")
}
