package qplanner

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

// TestLimitIter covers all branches: offset skipping, limit truncation, and
// error propagation from the source.
func TestLimitIter(t *testing.T) {
	a := &anyenc.Arena{}
	makeHit := func(id string) fakeHit {
		return fakeHit{docId: anyenc.AppendAnyValue(nil, id), doc: a.NewObject()}
	}

	t.Run("offset_skips", func(t *testing.T) {
		src := &fakeIter{hits: []fakeHit{makeHit("a"), makeHit("b"), makeHit("c")}}
		it := &LimitIter{Source: src, Offset: 2, Limit: 0}
		var seen []string
		for {
			_, docId, err := it.Next()
			require.NoError(t, err)
			if docId == nil {
				break
			}
			seen = append(seen, string(anyenc.MustParse(docId).GetStringBytes()))
		}
		assert.Equal(t, []string{"c"}, seen, "first 2 skipped by Offset")
	})
	t.Run("limit_truncates", func(t *testing.T) {
		src := &fakeIter{hits: []fakeHit{makeHit("a"), makeHit("b"), makeHit("c")}}
		it := &LimitIter{Source: src, Limit: 2}
		var got []string
		for {
			_, docId, err := it.Next()
			require.NoError(t, err)
			if docId == nil {
				break
			}
			got = append(got, string(anyenc.MustParse(docId).GetStringBytes()))
		}
		assert.Equal(t, []string{"a", "b"}, got)
	})
	t.Run("offset_plus_limit", func(t *testing.T) {
		src := &fakeIter{hits: []fakeHit{makeHit("a"), makeHit("b"), makeHit("c"), makeHit("d")}}
		it := &LimitIter{Source: src, Offset: 1, Limit: 2}
		var got []string
		for {
			_, docId, err := it.Next()
			require.NoError(t, err)
			if docId == nil {
				break
			}
			got = append(got, string(anyenc.MustParse(docId).GetStringBytes()))
		}
		assert.Equal(t, []string{"b", "c"}, got)
	})
	t.Run("source_error", func(t *testing.T) {
		it := &LimitIter{Source: &errIter{err: errors.New("boom")}}
		_, _, err := it.Next()
		require.ErrorContains(t, err, "boom")
	})
	t.Run("close_propagates_and_nil", func(t *testing.T) {
		tr := &closeTrackingIter{}
		it := &LimitIter{Source: tr}
		it.Close()
		assert.Equal(t, 1, tr.closed)

		(&LimitIter{}).Close() // nil source — no panic
	})
	t.Run("string_variants", func(t *testing.T) {
		src := &fakeIter{}
		lim := &LimitIter{Source: src, Limit: 5}
		assert.Contains(t, lim.String(), "Limit(5)")
		off := &LimitIter{Source: src, Offset: 3}
		assert.Contains(t, off.String(), "offset=3")
		both := &LimitIter{Source: src, Offset: 3, Limit: 7}
		s := both.String()
		assert.Contains(t, s, "offset=3")
		assert.Contains(t, s, "limit=7")
	})
}
