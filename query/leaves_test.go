package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/syncpool"
)

// Dotted paths through arrays of objects: MongoDB matcher semantics
// (docs/query-filter-contract.md item 14).
func TestKey_PathThroughArrays(t *testing.T) {
	cases := []struct {
		filter string
		doc    string
		want   bool
	}{
		{`{"a.b":1}`, `{"a":[{"b":1},{"b":2}]}`, true},
		{`{"a.b":3}`, `{"a":[{"b":1},{"b":2}]}`, false},
		{`{"a.b":1}`, `{"a":[{"b":[1,2]}]}`, true},          // leaf array, per element
		{`{"a.b":[1,2]}`, `{"a":[{"b":[1,2]}]}`, true},      // leaf array, whole
		{`{"a.b":[1,2]}`, `{"a":[{"b":1},{"b":2}]}`, false}, // not a collected array
		{`{"a.b":1}`, `{"a":[[{"b":1}]]}`, false},           // nested arrays are not traversed
		{`{"a.b.c":1}`, `{"a":[{"b":[{"c":1}]}]}`, true},    // traversal repeats per level
		// independent quantification per operator
		{`{"a.b":{"$gt":1,"$lt":2}}`, `{"a":[{"b":1},{"b":2}]}`, true},
		{`{"a.b":{"$gt":1,"$lte":1}}`, `{"a":[{"b":1},{"b":2}]}`, true},
		{`{"a.b":{"$in":[1],"$nin":[2]}}`, `{"a":[{"b":1},{"b":2}]}`, false},
		{`{"a.b":{"$in":[1],"$nin":[2]}}`, `{"a":[{"b":1},{"b":3}]}`, true},
		{`{"a.b":{"$all":[1,2]}}`, `{"a":[{"b":1},{"b":2}]}`, true},
		// negations: no leaf may match
		{`{"a.b":{"$ne":1}}`, `{"a":[{"b":1},{"b":2}]}`, false},
		{`{"a.b":{"$ne":3}}`, `{"a":[{"b":1},{"b":2}]}`, true},
		{`{"a.b":{"$nin":[2,3]}}`, `{"a":[{"b":1},{"b":2}]}`, false},
		{`{"a.b":{"$not":{"$gt":1}}}`, `{"a":[{"b":1},{"b":2}]}`, false},
		{`{"a.b":{"$not":{"$gt":5}}}`, `{"a":[{"b":1},{"b":2}]}`, true},
		{`{"$nor":[{"a.b":1}]}`, `{"a":[{"b":1},{"b":2}]}`, false},
		// null and missing leaves
		{`{"a.b":null}`, `{"a":[{"b":1},{"c":2}]}`, true},
		{`{"a.b":null}`, `{"a":[{"b":1},{"b":2}]}`, false},
		{`{"a.b":null}`, `{"a":[]}`, true},
		{`{"a.b":null}`, `{"a":[1,2]}`, true},
		{`{"a.b":null}`, `{"a":5}`, true},
		{`{"a.b":{"$ne":null}}`, `{"a":[{"b":1},{"c":2}]}`, false},
		{`{"a.b":{"$exists":true}}`, `{"a":[{"b":1},{"c":2}]}`, true},
		{`{"a.b":{"$exists":true}}`, `{"a":[{"c":2}]}`, false},
		{`{"a.b":{"$exists":false}}`, `{"a":[]}`, true},
		{`{"a.b":{"$exists":false}}`, `{"a":[{"b":null}]}`, false},
		{`{"a.b":{"$type":"null"}}`, `{"a":[{"c":1}]}`, false}, // missing is not null-typed
		{`{"a.b":{"$type":"null"}}`, `{"a":[{"b":null}]}`, true},
		// other leaf operators quantify over leaves too
		{`{"a.b":{"$size":2}}`, `{"a":[{"b":[1]},{"b":[1,2]}]}`, true},
		{`{"a.b":{"$type":"string"}}`, `{"a":[{"b":1},{"b":"x"}]}`, true},
		{`{"a.b":{"$type":"array"}}`, `{"a":[{"b":1},{"b":[]}]}`, true},
		{`{"a.b":{"$regex":"^x"}}`, `{"a":[{"b":"y"},{"b":"xz"}]}`, true},
		// numeric segments: index, and the leaf is compared whole
		{`{"a.0.b":1}`, `{"a":[{"b":1},{"b":2}]}`, true},
		{`{"a.1.b":1}`, `{"a":[{"b":1},{"b":2}]}`, false},
		{`{"a.0":1}`, `{"a":[[1,2]]}`, false},
		{`{"a.0":[1,2]}`, `{"a":[[1,2]]}`, true},
		{`{"a.0":{"$type":"number"}}`, `{"a":[[1,2]]}`, false},
		{`{"a.0":{"$size":2}}`, `{"a":[[1,2]]}`, true},
		{`{"a.0.b":1}`, `{"a":[[{"b":1}]]}`, true}, // traversal resumes after the index
		{`{"a.0":1}`, `{"a":{"0":[1,2]}}`, true},   // an object key "0" is an ordinary leaf
	}
	for _, c := range cases {
		f, err := ParseCondition(c.filter)
		require.NoError(t, err, c.filter)
		doc := anyenc.MustParseJson(c.doc)
		assert.Equal(t, c.want, f.Ok(doc, nil), "%s on %s (nil buf)", c.filter, c.doc)
		buf := &syncpool.DocBuffer{Parser: &anyenc.Parser{}}
		assert.Equal(t, c.want, f.Ok(doc, buf), "%s on %s", c.filter, c.doc)
		if rf, ok := f.(RawFilter); ok {
			// the raw path must agree or decline, never disagree
			got, handled := rf.OkRaw(doc.MarshalTo(nil), buf)
			if handled {
				assert.Equal(t, c.want, got, "raw %s on %s", c.filter, c.doc)
			}
		}
	}
}

func TestKey_PathThroughArrays_AllocFree(t *testing.T) {
	doc := anyenc.MustParseJson(`{"a":[{"b":[{"c":1},{"c":2}]},{"b":[{"c":3}]},{"x":1},5]}`)
	buf := &syncpool.DocBuffer{Parser: &anyenc.Parser{}}
	for _, js := range []string{
		`{"a.b.c":3}`,
		`{"a.b.c":{"$gt":1,"$lt":3}}`,
		`{"a.b.c":{"$ne":9}}`,
		`{"a.b.c":{"$in":[3,4]}}`,
		`{"a.b":{"$elemMatch":{"c":{"$gt":1,"$lt":3}}}}`,
		`{"a":{"$elemMatch":{"b":{"$elemMatch":{"c":3}}}}}`,
	} {
		f := MustParseCondition(js)
		require.True(t, f.Ok(doc, buf), js)
		allocs := testing.AllocsPerRun(50, func() { f.Ok(doc, buf) })
		assert.Zero(t, allocs, js)
	}
}

// Without a DocBuffer the leaf set lives on the stack.
func TestKey_PathThroughArrays_NilBufAllocFree(t *testing.T) {
	doc := anyenc.MustParseJson(`{"a":[{"b":1},{"b":2},{"b":3}]}`)
	f := MustParseCondition(`{"a.b":3}`)
	require.True(t, f.Ok(doc, nil))
	assert.Zero(t, testing.AllocsPerRun(50, func() { f.Ok(doc, nil) }))
}

// Nested Keys ($elemMatch conditions) push and pop their own leaves above the
// enclosing Key's on the shared DocBuffer stack.
func TestKey_LeafStackNesting(t *testing.T) {
	doc := anyenc.MustParseJson(`{"a":[{"b":[{"c":1},{"c":5}]},{"b":[{"c":2}]}]}`)
	buf := &syncpool.DocBuffer{Parser: &anyenc.Parser{}}
	f := MustParseCondition(`{"a.b":{"$elemMatch":{"c":{"$gt":1,"$lt":3}}}}`)
	assert.True(t, f.Ok(doc, buf))
	assert.Empty(t, buf.Leaves)
	f = MustParseCondition(`{"a.b":{"$elemMatch":{"c":{"$gt":5}}}}`)
	assert.False(t, f.Ok(doc, buf))
	assert.Empty(t, buf.Leaves)
	// the frame is cleared, not just cut: a pooled buffer pins no values
	for _, l := range buf.Leaves[:cap(buf.Leaves)] {
		assert.Nil(t, l.Value)
	}
}
