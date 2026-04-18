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
		Plan:   nil, // explicitly no plan
	}
	_, docId, err := it.Next()
	require.NoError(t, err)
	assert.NotNil(t, docId, "doc must still be fetched")
}
