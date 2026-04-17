package qplanner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/query"
)

// fakeIter is a minimal Iterator used to feed canned (key, docId) hits.
// It also emulates FetchIter's contract of populating Plan.DocParsed.
type fakeIter struct {
	hits []fakeHit
	i    int
	plan *Plan
}

type fakeHit struct {
	key   []byte
	docId []byte
	doc   *anyenc.Value
}

func (f *fakeIter) Next() ([]byte, []byte, error) {
	if f.i >= len(f.hits) {
		return nil, nil, nil
	}
	h := f.hits[f.i]
	f.i++
	if f.plan != nil {
		f.plan.DocParsed = h.doc
	}
	return h.key, h.docId, nil
}

func (f *fakeIter) Close()         {}
func (f *fakeIter) String() string { return "fake" }

// encodeKey encodes a single scalar anyenc value as an index-key prefix +
// a raw docId suffix, matching the layout produced by IndexIter for a
// single-field index.
func encodeKey(val *anyenc.Value, docId string) []byte {
	out := val.MarshalTo(nil)
	return append(out, []byte(docId)...)
}

func TestCanonicalKeyDedupIter_SingleDocMultipleMatches(t *testing.T) {
	a := &anyenc.Arena{}
	arr := a.NewArray()
	arr.SetArrayItem(0, a.NewString("ai"))
	arr.SetArrayItem(1, a.NewString("theory"))
	arr.SetArrayItem(2, a.NewString("philosophy"))
	doc := a.NewObject()
	doc.Set("id", a.NewString("p1"))
	doc.Set("tags", arr)

	k1 := encodeKey(a.NewString("ai"), "p1")
	k2 := encodeKey(a.NewString("theory"), "p1")

	bs := query.Bounds{
		{Start: a.NewString("ai").MarshalTo(nil), End: a.NewString("ai").MarshalTo(nil), StartInclude: true, EndInclude: true},
		{Start: a.NewString("theory").MarshalTo(nil), End: a.NewString("theory").MarshalTo(nil), StartInclude: true, EndInclude: true},
	}

	plan := &Plan{}
	upstream := &fakeIter{
		plan: plan,
		hits: []fakeHit{
			{key: k1, docId: []byte("p1"), doc: doc},
			{key: k2, docId: []byte("p1"), doc: doc},
		},
	}

	it := &CanonicalKeyDedupIter{
		Source:    upstream,
		Plan:      plan,
		Bounds:    bs,
		FieldPath: []string{"tags"},
	}

	var got [][]byte
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, append([]byte(nil), docId...))
	}
	assert.Equal(t, [][]byte{[]byte("p1")}, got,
		"doc p1 must be emitted exactly once (at the canonical 'ai' hit)")
}

func TestCanonicalKeyDedupIter_ReverseScan(t *testing.T) {
	a := &anyenc.Arena{}
	arr := a.NewArray()
	arr.SetArrayItem(0, a.NewString("ai"))
	arr.SetArrayItem(1, a.NewString("theory"))
	doc := a.NewObject()
	doc.Set("id", a.NewString("p1"))
	doc.Set("tags", arr)

	k2 := encodeKey(a.NewString("theory"), "p1")
	k1 := encodeKey(a.NewString("ai"), "p1")

	bs := query.Bounds{
		{Start: a.NewString("ai").MarshalTo(nil), End: a.NewString("ai").MarshalTo(nil), StartInclude: true, EndInclude: true},
		{Start: a.NewString("theory").MarshalTo(nil), End: a.NewString("theory").MarshalTo(nil), StartInclude: true, EndInclude: true},
	}

	plan := &Plan{}
	// Reverse scan order: theory first, ai second.
	upstream := &fakeIter{
		plan: plan,
		hits: []fakeHit{
			{key: k2, docId: []byte("p1"), doc: doc},
			{key: k1, docId: []byte("p1"), doc: doc},
		},
	}

	it := &CanonicalKeyDedupIter{
		Source: upstream, Plan: plan, Bounds: bs,
		FieldPath: []string{"tags"}, Reverse: true,
	}

	var got [][]byte
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, append([]byte(nil), docId...))
	}
	assert.Equal(t, [][]byte{[]byte("p1")}, got,
		"reverse scan: emit at 'theory' (max), skip 'ai'")
}

func TestCanonicalKeyDedupIter_ScalarPassthrough(t *testing.T) {
	a := &anyenc.Arena{}
	doc := a.NewObject()
	doc.Set("id", a.NewString("p1"))
	doc.Set("status", a.NewString("draft")) // scalar, not array
	k := encodeKey(a.NewString("draft"), "p1")

	plan := &Plan{}
	upstream := &fakeIter{plan: plan, hits: []fakeHit{
		{key: k, docId: []byte("p1"), doc: doc},
	}}
	it := &CanonicalKeyDedupIter{
		Source: upstream, Plan: plan,
		FieldPath: []string{"status"},
	}

	_, docId, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, []byte("p1"), docId, "scalar field: always emit")

	_, docId2, err := it.Next()
	require.NoError(t, err)
	assert.Nil(t, docId2)
}

func TestCanonicalKeyDedupIter_RangeBounds(t *testing.T) {
	a := &anyenc.Arena{}
	arr := a.NewArray()
	arr.SetArrayItem(0, a.NewString("a"))
	arr.SetArrayItem(1, a.NewString("b"))
	arr.SetArrayItem(2, a.NewString("c"))
	doc := a.NewObject()
	doc.Set("id", a.NewString("p1"))
	doc.Set("tags", arr)

	// $gte "a", $lte "c" — single inclusive range hitting all three tags.
	bs := query.Bounds{{
		Start:        a.NewString("a").MarshalTo(nil),
		End:          a.NewString("c").MarshalTo(nil),
		StartInclude: true, EndInclude: true,
	}}

	plan := &Plan{}
	upstream := &fakeIter{plan: plan, hits: []fakeHit{
		{key: encodeKey(a.NewString("a"), "p1"), docId: []byte("p1"), doc: doc},
		{key: encodeKey(a.NewString("b"), "p1"), docId: []byte("p1"), doc: doc},
		{key: encodeKey(a.NewString("c"), "p1"), docId: []byte("p1"), doc: doc},
	}}
	it := &CanonicalKeyDedupIter{Source: upstream, Plan: plan, Bounds: bs, FieldPath: []string{"tags"}}

	var got []string
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, string(docId))
	}
	assert.Equal(t, []string{"p1"}, got, "range bounds: emit only at canonical 'a'")
}

func TestCanonicalKeyDedupIter_NoBounds(t *testing.T) {
	// A pure Sort("tags") with no filter produces an IndexScan with empty
	// bounds. Every array element qualifies; canonical = min of the whole array.
	a := &anyenc.Arena{}
	arr := a.NewArray()
	arr.SetArrayItem(0, a.NewString("c"))
	arr.SetArrayItem(1, a.NewString("a"))
	arr.SetArrayItem(2, a.NewString("b"))
	doc := a.NewObject()
	doc.Set("id", a.NewString("p1"))
	doc.Set("tags", arr)

	plan := &Plan{}
	// Scan order: a, b, c (index entries are sorted).
	upstream := &fakeIter{plan: plan, hits: []fakeHit{
		{key: encodeKey(a.NewString("a"), "p1"), docId: []byte("p1"), doc: doc},
		{key: encodeKey(a.NewString("b"), "p1"), docId: []byte("p1"), doc: doc},
		{key: encodeKey(a.NewString("c"), "p1"), docId: []byte("p1"), doc: doc},
	}}
	it := &CanonicalKeyDedupIter{Source: upstream, Plan: plan, FieldPath: []string{"tags"}}

	var got []string
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, string(docId))
	}
	assert.Equal(t, []string{"p1"}, got,
		"no bounds: emit only at canonical 'a' (min of whole array)")
}

func TestCanonicalKeyDedupIter_MultipleDocs(t *testing.T) {
	a := &anyenc.Arena{}

	mkDoc := func(id string, tags ...string) *anyenc.Value {
		arr := a.NewArray()
		for i, tg := range tags {
			arr.SetArrayItem(i, a.NewString(tg))
		}
		d := a.NewObject()
		d.Set("id", a.NewString(id))
		d.Set("tags", arr)
		return d
	}
	p1 := mkDoc("p1", "a", "b", "c")
	p2 := mkDoc("p2", "a")

	// Bounds {a,b,c}. p1 produces 3 hits, p2 produces 1.
	bs := query.Bounds{
		{Start: a.NewString("a").MarshalTo(nil), End: a.NewString("a").MarshalTo(nil), StartInclude: true, EndInclude: true},
		{Start: a.NewString("b").MarshalTo(nil), End: a.NewString("b").MarshalTo(nil), StartInclude: true, EndInclude: true},
		{Start: a.NewString("c").MarshalTo(nil), End: a.NewString("c").MarshalTo(nil), StartInclude: true, EndInclude: true},
	}

	// Scan order (sorted by key): a/p1, a/p2, b/p1, c/p1.
	plan := &Plan{}
	upstream := &fakeIter{plan: plan, hits: []fakeHit{
		{key: encodeKey(a.NewString("a"), "p1"), docId: []byte("p1"), doc: p1},
		{key: encodeKey(a.NewString("a"), "p2"), docId: []byte("p2"), doc: p2},
		{key: encodeKey(a.NewString("b"), "p1"), docId: []byte("p1"), doc: p1},
		{key: encodeKey(a.NewString("c"), "p1"), docId: []byte("p1"), doc: p1},
	}}
	it := &CanonicalKeyDedupIter{
		Source: upstream, Plan: plan, Bounds: bs,
		FieldPath: []string{"tags"},
	}

	var got []string
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, string(docId))
	}
	assert.Equal(t, []string{"p1", "p2"}, got)
}
