package qplanner

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
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
			_, docId, _, err := it.Next()
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
			_, docId, _, err := it.Next()
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
			_, docId, _, err := it.Next()
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
		_, _, _, err := it.Next()
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

// fakeSkipIter is a fakeIter that also implements offsetSkipper, recording how
// many rows skipOffset was asked to skip and reporting a configurable
// remainder (rows it could NOT skip). When skipOffset absorbs rows it advances
// the underlying cursor index so the subsequent Next() stream reflects the
// skip — mirroring IndexIter's cursor-level skip + handoff.
type fakeSkipIter struct {
	hits      []fakeHit
	i         int
	remainder int  // value skipOffset returns
	skipCalls int  // number of times skipOffset was invoked
	lastSkipN int  // last n passed to skipOffset
}

func (f *fakeSkipIter) Next() ([]byte, []byte, bool, error) {
	if f.i >= len(f.hits) {
		return nil, nil, false, nil
	}
	h := f.hits[f.i]
	f.i++
	return h.key, h.docId, false, nil
}
func (f *fakeSkipIter) Close()         {}
func (f *fakeSkipIter) String() string { return "fakeSkip" }

func (f *fakeSkipIter) skipOffset(n int) (int, error) {
	f.skipCalls++
	f.lastSkipN = n
	absorbed := n - f.remainder
	if absorbed < 0 {
		absorbed = 0
	}
	// Advance the cursor past the absorbed rows, as IndexIter would.
	f.i += absorbed
	if f.i > len(f.hits) {
		f.i = len(f.hits)
	}
	return f.remainder, nil
}

func limitDrain(t *testing.T, it *LimitIter) []string {
	t.Helper()
	var got []string
	for {
		_, docId, _, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, string(anyenc.MustParse(docId).GetStringBytes()))
	}
	return got
}

// TestLimitIter_FastSkip_FullyAbsorbed verifies that when the source absorbs
// the entire offset at the cursor level (remaining=0), LimitIter performs NO
// per-row skip and streams from the post-skip position.
func TestLimitIter_FastSkip_FullyAbsorbed(t *testing.T) {
	a := &anyenc.Arena{}
	mk := func(id string) fakeHit {
		return fakeHit{docId: anyenc.AppendAnyValue(nil, id), doc: a.NewObject()}
	}
	src := &fakeSkipIter{
		hits:      []fakeHit{mk("a"), mk("b"), mk("c"), mk("d"), mk("e")},
		remainder: 0, // fully absorbs the offset
	}
	it := &LimitIter{Source: src, Offset: 2, Limit: 2}

	got := limitDrain(t, it)
	assert.Equal(t, 1, src.skipCalls, "skipOffset must be called exactly once")
	assert.Equal(t, 2, src.lastSkipN, "skipOffset called with the full offset")
	assert.Equal(t, []string{"c", "d"}, got,
		"offset fully absorbed at cursor → limit window starts at row 2")
}

// TestLimitIter_FastSkip_PartialFallback verifies that when the source can
// only absorb part of the offset (remaining>0, e.g. it stopped at a multi-key
// entry), LimitIter applies the remaining offset via its per-row skip loop.
func TestLimitIter_FastSkip_PartialFallback(t *testing.T) {
	a := &anyenc.Arena{}
	mk := func(id string) fakeHit {
		return fakeHit{docId: anyenc.AppendAnyValue(nil, id), doc: a.NewObject()}
	}
	// Offset 3: source absorbs 1 (advances cursor to "b"), reports remaining=2.
	// LimitIter must then per-row skip "b","c" and start the window at "d".
	src := &fakeSkipIter{
		hits:      []fakeHit{mk("a"), mk("b"), mk("c"), mk("d"), mk("e"), mk("f")},
		remainder: 2,
	}
	it := &LimitIter{Source: src, Offset: 3, Limit: 2}

	got := limitDrain(t, it)
	assert.Equal(t, 1, src.skipCalls)
	assert.Equal(t, []string{"d", "e"}, got,
		"1 row absorbed at cursor + 2 skipped per-row = offset 3, window starts at row 3")
}

// TestLimitIter_NoFastSkip_PlainSource verifies that a source WITHOUT the
// offsetSkipper interface falls back to the original per-row skip behaviour
// unchanged (no skipOffset attempted).
func TestLimitIter_NoFastSkip_PlainSource(t *testing.T) {
	a := &anyenc.Arena{}
	mk := func(id string) fakeHit {
		return fakeHit{docId: anyenc.AppendAnyValue(nil, id), doc: a.NewObject()}
	}
	// fakeIter does NOT implement offsetSkipper.
	src := &fakeIter{hits: []fakeHit{mk("a"), mk("b"), mk("c"), mk("d")}}
	it := &LimitIter{Source: src, Offset: 1, Limit: 2}

	got := limitDrain(t, it)
	assert.Equal(t, []string{"b", "c"}, got, "plain source uses per-row offset skip")
}
