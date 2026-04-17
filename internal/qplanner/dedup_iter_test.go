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
