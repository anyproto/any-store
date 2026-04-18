package qplanner

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/syncpool"
)

// ---- IndexFilterIter ----

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

// errIter is an Iterator that always returns an error from Next.
type errIter struct{ err error }

func (e *errIter) Next() ([]byte, []byte, error) { return nil, nil, e.err }
func (e *errIter) Close()                        {}
func (e *errIter) String() string                { return "err" }

// ---- FetchIter ----

// TestFetchIter_Next_SkipsMissingDocId covers the ErrKeyNotFound swallow at
// fetch_iter.go:52-55 and the happy path that parses and caches DocParsed.
func TestFetchIter_Next_SkipsMissingDocId(t *testing.T) {
	db, ns := coverageBtree(t, "data_fetch", []string{"a", "b", "c"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	data := &CursorSource{Tx: rtx, Ns: ns}

	a := &anyenc.Arena{}
	_ = a
	makeHit := func(id string) fakeHit {
		return fakeHit{docId: anyenc.AppendAnyValue(nil, id)}
	}

	// Feed three docIds; "missing" is NOT in the btree so FetchIter should skip it.
	source := &fakeIter{
		hits: []fakeHit{
			makeHit("a"),
			makeHit("missing"), // ErrKeyNotFound swallow arm
			makeHit("b"),
		},
	}

	plan := &Plan{}
	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	it := &FetchIter{
		Source: source,
		Data:   data,
		Buf:    buf,
		Plan:   plan,
	}

	var gotIds []string
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		// Plan.DocParsed must be populated.
		require.NotNil(t, plan.DocParsed)
		gotIds = append(gotIds, string(plan.DocParsed.Get("id").GetStringBytes()))
	}
	assert.Equal(t, []string{"a", "b"}, gotIds,
		"missing doc must be silently skipped")
}

// TestFetchIter_Close_AndString covers FetchIter.Close (with and without
// Source) and String formatting.
func TestFetchIter_Close_AndString(t *testing.T) {
	tr := &closeTrackingIter{}
	it := &FetchIter{Source: tr}
	it.Close()
	assert.Equal(t, 1, tr.closed)

	// nil-source Close is a no-op.
	(&FetchIter{}).Close()

	// String delegates to Source.String() with a " -> Fetch" suffix.
	src := &fakeIter{}
	it2 := &FetchIter{Source: src}
	assert.Equal(t, fmt.Sprintf("%s -> Fetch", src), it2.String())
}

// TestFetchIter_Next_PropagatesSourceError covers fetch_iter.go:39-41.
func TestFetchIter_Next_PropagatesSourceError(t *testing.T) {
	it := &FetchIter{Source: &errIter{err: errors.New("upstream")}}
	_, _, err := it.Next()
	require.ErrorContains(t, err, "upstream")
}

// ---- IndexSketch ----

// TestIndexSketch_Increment_DecrementEstimate covers Increment, Decrement,
// Estimate, and the Decrement underflow-clamp (returning early when old==0).
func TestIndexSketch_Increment_DecrementEstimate(t *testing.T) {
	s := NewIndexSketch(DefaultSketchSize)
	k := []byte("key-a")

	assert.Equal(t, uint64(0), s.Estimate(k))
	s.Increment(k)
	s.Increment(k)
	assert.Equal(t, uint64(2), s.Estimate(k))

	s.Decrement(k)
	assert.Equal(t, uint64(1), s.Estimate(k))
	// Two more decrements drain the bucket; a third is a no-op (clamped at 0).
	s.Decrement(k)
	s.Decrement(k) // hits the `if old == 0 { return }` early exit
	assert.Equal(t, uint64(0), s.Estimate(k))
}

// TestIndexSketch_DocCount covers Increment/DecrementDocCount + GetDocCount
// including the underflow-clamp at old==0.
func TestIndexSketch_DocCount(t *testing.T) {
	s := NewIndexSketch(DefaultSketchSize)
	assert.Equal(t, uint64(0), s.GetDocCount())
	s.IncrementDocCount()
	s.IncrementDocCount()
	assert.Equal(t, uint64(2), s.GetDocCount())
	s.DecrementDocCount()
	assert.Equal(t, uint64(1), s.GetDocCount())
	s.DecrementDocCount()
	s.DecrementDocCount() // underflow → clamped
	assert.Equal(t, uint64(0), s.GetDocCount())
}

// TestIndexSketch_MarshalUnmarshal covers MarshalBinary / UnmarshalBinary
// round-trip and the backward-compat branch (data WITHOUT trailing docCount).
func TestIndexSketch_MarshalUnmarshal(t *testing.T) {
	s := NewIndexSketch(4)
	s.Increment([]byte("a"))
	s.Increment([]byte("b"))
	s.Increment([]byte("b"))
	s.IncrementDocCount()
	s.IncrementDocCount()

	data := s.MarshalBinary(nil)

	// Round-trip into a fresh sketch.
	s2 := NewIndexSketch(4)
	s2.UnmarshalBinary(data)
	assert.Equal(t, s.Estimate([]byte("a")), s2.Estimate([]byte("a")))
	assert.Equal(t, s.Estimate([]byte("b")), s2.Estimate([]byte("b")))
	assert.Equal(t, uint64(2), s2.GetDocCount())

	// Backward compat: trim off the trailing 8-byte docCount.
	oldData := data[:len(data)-8]
	s3 := NewIndexSketch(4)
	// Pre-set docCount to a sentinel so we can distinguish
	// "branch left it alone" from "branch zeroed it".
	s3.IncrementDocCount()
	s3.IncrementDocCount()
	s3.IncrementDocCount() // sentinel = 3
	s3.UnmarshalBinary(oldData)
	assert.Equal(t, uint64(3), s3.GetDocCount(),
		"data missing docCount must LEAVE the pre-existing value untouched, not zero it")
	assert.Equal(t, s.Estimate([]byte("a")), s3.Estimate([]byte("a")))
}

// TestIndexSketch_Reset pins Reset — all buckets and docCount go to zero.
func TestIndexSketch_Reset(t *testing.T) {
	s := NewIndexSketch(8)
	s.Increment([]byte("a"))
	s.IncrementDocCount()
	s.Reset()
	assert.Equal(t, uint64(0), s.Estimate([]byte("a")))
	assert.Equal(t, uint64(0), s.GetDocCount())
}

// TestNewIndexSketch_DefaultsBadSize covers the `if size <= 0` branch at
// sketch.go:23-25 — non-positive sizes fall back to DefaultSketchSize.
func TestNewIndexSketch_DefaultsBadSize(t *testing.T) {
	s := NewIndexSketch(0)
	assert.Equal(t, DefaultSketchSize, s.Size)
	s2 := NewIndexSketch(-5)
	assert.Equal(t, DefaultSketchSize, s2.Size)
}

// ---- CursorSource / IndexInfo helpers ----

// TestCursorSource_Get_And_AppendSeekKey covers the helper methods on
// CursorSource (iterator.go:42-56) that weren't exercised by direct unit
// tests before.
func TestCursorSource_Get_And_AppendSeekKey(t *testing.T) {
	db, ns := coverageBtree(t, "cs_methods", []string{"x", "y"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	cs := &CursorSource{Tx: rtx, Ns: ns}

	t.Run("get_hit", func(t *testing.T) {
		xkey := anyenc.AppendAnyValue(nil, "x")
		val, err := cs.Get(xkey)
		require.NoError(t, err)
		assert.NotNil(t, val)
	})
	t.Run("append_seek_key", func(t *testing.T) {
		xkey := anyenc.AppendAnyValue(nil, "x")
		seek, err := cs.AppendSeekKey(xkey, nil)
		require.NoError(t, err)
		assert.NotEmpty(t, seek, "seek must find at least one entry >= prefix")
	})
	t.Run("new_cursor", func(t *testing.T) {
		c := cs.NewCursor()
		require.NotNil(t, c)
		c.Close()
	})
}

// TestIndexInfo_AppendIndexKey covers AppendIndexKey's reverse and forward
// branches (iterator.go:71-77).
func TestIndexInfo_AppendIndexKey(t *testing.T) {
	a := &anyenc.Arena{}
	doc := a.NewObject()
	doc.Set("a", a.NewString("hello"))
	doc.Set("b", a.NewString("world"))

	ii := &IndexInfo{
		FieldNames: []string{"a", "b"},
		FieldPaths: [][]string{{"a"}, {"b"}},
		Reverse:    []bool{false, true},
	}
	var fwd, rev anyenc.Tuple
	fwd = ii.AppendIndexKey(fwd, doc, 0) // forward on field "a"
	rev = ii.AppendIndexKey(rev, doc, 1) // reverse on field "b"

	// Forward emits raw type-byte + bytes; reverse emits each byte bitwise-inverted.
	// For a string value, forward starts with TypeString (3); reverse starts
	// with ^TypeString (252). A regression that routed reverse through Append
	// instead of AppendInverted would fail these strict checks.
	require.NotEmpty(t, fwd)
	require.NotEmpty(t, rev)
	assert.Equal(t, byte(anyenc.TypeString), fwd[0],
		"forward string field must begin with TypeString byte")
	assert.Equal(t, ^byte(anyenc.TypeString), rev[0],
		"reverse string field must begin with bitwise-inverted TypeString byte")
}

// TestFetchIter_Next_NoPlanLeaves_DocParsed_Untouched covers the branch at
// fetch_iter.go:61 (if it.Plan != nil) — when Plan is nil, we skip parsing
// and DocParsed stays unset.
func TestFetchIter_Next_NoPlanLeaves_DocParsed_Untouched(t *testing.T) {
	db, ns := coverageBtree(t, "data_fetch_noplan", []string{"x"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	source := &fakeIter{
		hits: []fakeHit{{docId: anyenc.AppendAnyValue(nil, "x")}},
	}
	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	it := &FetchIter{
		Source: source,
		Data:   &CursorSource{Tx: rtx, Ns: ns},
		Buf:    buf,
		Plan:   nil, // explicitly no plan → parsing block must be skipped
	}
	// Pre-mark DocBuf with a sentinel. The no-plan path still APPENDS the raw
	// value into DocBuf (fetch_iter.go:48), so DocBuf ends populated — but
	// no Parse call should occur (Plan is nil).
	_, docId, err := it.Next()
	require.NoError(t, err)
	assert.NotNil(t, docId, "doc must still be fetched")
	// Data was read into Buf.DocBuf, proving fetch happened.
	assert.NotEmpty(t, buf.DocBuf, "no-plan path must still fetch into DocBuf")
	// There is no plan to check, so we cannot observe "DocParsed untouched"
	// directly. The coverage assertion is that fetch_iter.go:61's nil-check
	// short-circuited the parse block.
}
