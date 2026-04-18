package qplanner

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

// TestIndexFilterIter_MatchesKey covers matchesKey for: all filters match,
// one filter mismatches, and a filter whose FieldIdx is past the tuple
// (FieldBytes returns error → returns false).
func TestIndexFilterIter_MatchesKey(t *testing.T) {
	a := &anyenc.Arena{}
	// Build a two-field tuple: "hello" | 42
	var key anyenc.Tuple
	key = key.Append(a.NewString("hello"))
	key = key.Append(a.NewNumberInt(42))

	// Take the encoded bytes of the second field.
	f1Bytes, err := key.FieldBytes(1)
	require.NoError(t, err)
	f0Bytes, err := key.FieldBytes(0)
	require.NoError(t, err)

	t.Run("all_match", func(t *testing.T) {
		it := &IndexFilterIter{
			Filters: []IndexFieldFilter{
				{FieldIdx: 0, MatchValue: f0Bytes},
				{FieldIdx: 1, MatchValue: f1Bytes},
			},
		}
		assert.True(t, it.matchesKey(key))
	})
	t.Run("value_mismatch", func(t *testing.T) {
		it := &IndexFilterIter{
			Filters: []IndexFieldFilter{
				{FieldIdx: 1, MatchValue: []byte{0xff, 0xff, 0xff}}, // wrong
			},
		}
		assert.False(t, it.matchesKey(key))
	})
	t.Run("out_of_range_field", func(t *testing.T) {
		// FieldIdx=9 → FieldBytes returns error → matchesKey returns false.
		it := &IndexFilterIter{
			Filters: []IndexFieldFilter{
				{FieldIdx: 9, MatchValue: []byte{1}},
			},
		}
		assert.False(t, it.matchesKey(key))
	})
}

// TestIndexFilterIter_Next_FiltersStream feeds synthetic entries through
// IndexFilterIter and verifies that only matching keys surface.
func TestIndexFilterIter_Next_FiltersStream(t *testing.T) {
	a := &anyenc.Arena{}
	makeKey := func(s string, n int) []byte {
		var k anyenc.Tuple
		k = k.Append(a.NewString(s))
		k = k.Append(a.NewNumberInt(n))
		return []byte(k)
	}
	hits := []fakeHit{
		{key: makeKey("a", 1), docId: []byte("doc-a1")},
		{key: makeKey("b", 2), docId: []byte("doc-b2")}, // should pass
		{key: makeKey("c", 1), docId: []byte("doc-c1")},
		{key: makeKey("b", 2), docId: []byte("doc-b2-dup")}, // should pass
	}
	source := &fakeIter{hits: hits}

	// Filter: second field must equal encoded(2).
	tempKey := makeKey("b", 2)
	field1, err := anyenc.Tuple(tempKey).FieldBytes(1)
	require.NoError(t, err)

	it := &IndexFilterIter{
		Source:  source,
		Filters: []IndexFieldFilter{{FieldIdx: 1, MatchValue: field1}},
	}

	var produced []string
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		produced = append(produced, string(docId))
	}
	assert.Equal(t, []string{"doc-b2", "doc-b2-dup"}, produced)
}

// TestIndexFilterIter_Close_PropagatesAndNil_IsNoop covers Close with nil and
// non-nil Source.
func TestIndexFilterIter_Close_PropagatesAndNil_IsNoop(t *testing.T) {
	t.Run("nil_source", func(t *testing.T) {
		it := &IndexFilterIter{}
		it.Close() // must not panic
	})
	t.Run("propagates", func(t *testing.T) {
		tr := &closeTrackingIter{}
		it := &IndexFilterIter{Source: tr}
		it.Close()
		assert.Equal(t, 1, tr.closed)
	})
}

// TestIndexFilterIter_String covers single-filter vs multi-filter formatting.
func TestIndexFilterIter_String(t *testing.T) {
	src := &fakeIter{}
	single := &IndexFilterIter{Source: src, Filters: []IndexFieldFilter{{FieldIdx: 3}}}
	assert.Contains(t, single.String(), "field=3")
	multi := &IndexFilterIter{
		Source:  src,
		Filters: []IndexFieldFilter{{FieldIdx: 1}, {FieldIdx: 2}},
	}
	assert.Contains(t, multi.String(), "fields=[1 2]")
}

// TestIndexFilterIter_Next_PropagatesSourceError covers the error-propagation
// arm at index_filter_iter.go:27-29.
func TestIndexFilterIter_Next_PropagatesSourceError(t *testing.T) {
	errSource := &errIter{err: errors.New("source failure")}
	it := &IndexFilterIter{Source: errSource}
	_, _, err := it.Next()
	require.ErrorContains(t, err, "source failure")
}
