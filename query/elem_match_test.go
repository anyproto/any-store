package query

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

func TestElemMatch_Parse(t *testing.T) {
	t.Run("object form", func(t *testing.T) {
		for _, js := range []string{
			`{"a":{"$elemMatch":{"b":1}}}`,
			`{"a":{"$elemMatch":{"b":{"$gt":1},"c":2}}}`,
			`{"a":{"$elemMatch":{"$or":[{"b":1},{"c":2}]}}}`,
			`{"a":{"$elemMatch":{}}}`,
		} {
			f, err := ParseCondition(js)
			require.NoError(t, err, js)
			em, ok := f.(Key).Filter.(ElemMatch)
			require.True(t, ok, js)
			assert.False(t, em.ValueForm, js)
			assert.NotNil(t, em.Cond, js)
		}
	})
	t.Run("value form", func(t *testing.T) {
		for _, js := range []string{
			`{"a":{"$elemMatch":{"$gt":1,"$lt":3}}}`,
			`{"a":{"$elemMatch":{"$eq":1}}}`,
			`{"a":{"$elemMatch":{"$not":{"$gt":1}}}}`,
			`{"a":{"$elemMatch":{"$elemMatch":{"c":1}}}}`,
			`{"a":{"$elemMatch":{"$in":[1,2]}}}`,
		} {
			f, err := ParseCondition(js)
			require.NoError(t, err, js)
			em, ok := f.(Key).Filter.(ElemMatch)
			require.True(t, ok, js)
			assert.True(t, em.ValueForm, js)
		}
	})
	t.Run("rejections", func(t *testing.T) {
		for _, tc := range []struct{ js, path, op string }{
			{`{"a":{"$elemMatch":1}}`, "a.$elemMatch", "$elemMatch"},
			{`{"a":{"$elemMatch":{"$gt":1,"b":2}}}`, "a.$elemMatch.b", ""},
			{`{"a":{"$elemMatch":{"b":{"$sizee":1}}}}`, "a.$elemMatch.b.$sizee", "$sizee"},
			{`{"a":{"$elemMatch":{"$text":{"$search":"x"}}}}`, "a.$elemMatch", "$elemMatch"},
			{`{"a":{"$all":[{"$elemMatch":{"b":1}},2]}}`, "a.$all", "$all"},
		} {
			_, err := ParseCondition(tc.js)
			require.Error(t, err, tc.js)
			var pe *ParseError
			require.True(t, errors.As(err, &pe), tc.js)
			assert.Equal(t, tc.path, pe.Path, tc.js)
			if tc.op != "" {
				assert.Equal(t, tc.op, pe.Op, tc.js)
			}
		}
	})
	t.Run("all of elemMatch", func(t *testing.T) {
		f, err := ParseCondition(`{"a":{"$all":[{"$elemMatch":{"b":1}},{"$elemMatch":{"c":2}}]}}`)
		require.NoError(t, err)
		and, ok := f.(Key).Filter.(And)
		require.True(t, ok)
		assert.Len(t, and, 2)
		f, err = ParseCondition(`{"a":{"$all":[{"$elemMatch":{"b":1}}]}}`)
		require.NoError(t, err)
		_, ok = f.(Key).Filter.(ElemMatch)
		assert.True(t, ok)
	})
}

func TestElemMatch_Ok(t *testing.T) {
	cases := []struct {
		filter string
		doc    string
		want   bool
	}{
		// object form: one element must satisfy every predicate
		{`{"a":{"$elemMatch":{"b":1,"c":2}}}`, `{"a":[{"b":1,"c":2}]}`, true},
		{`{"a":{"$elemMatch":{"b":1,"c":2}}}`, `{"a":[{"b":1},{"c":2}]}`, false},
		{`{"a.b":1,"a.c":2}`, `{"a":[{"b":1},{"c":2}]}`, true}, // the unbound form does match
		{`{"a":{"$elemMatch":{"b":{"$gt":1,"$lt":3}}}}`, `{"a":[{"b":1},{"b":5}]}`, false},
		{`{"a.b":{"$gt":1,"$lt":3}}`, `{"a":[{"b":1},{"b":5}]}`, true},
		{`{"a":{"$elemMatch":{"b":{"$gt":1,"$lt":3}}}}`, `{"a":[{"b":[1,5,2]}]}`, true}, // leaf array inside the element
		{`{"a":{"$elemMatch":{"b.c":1}}}`, `{"a":[{"b":[{"c":1}]}]}`, true},
		{`{"a":{"$elemMatch":{"$or":[{"b":1},{"c":2}]}}}`, `{"a":[{"c":2}]}`, true},
		{`{"a":{"$elemMatch":{}}}`, `{"a":[1,{"x":1}]}`, true},
		{`{"a":{"$elemMatch":{}}}`, `{"a":[1,[{"x":1}]]}`, false}, // object elements only
		{`{"a":{"$elemMatch":{"b":{"$exists":false}}}}`, `{"a":[{"c":1}]}`, true},
		{`{"a":{"$elemMatch":{"b":1}}}`, `{"a":{"b":1}}`, false}, // not an array
		{`{"a":{"$elemMatch":{"b":1}}}`, `{}`, false},
		{`{"a":{"$elemMatch":{"b":1}}}`, `{"a":[]}`, false},
		// value form: each element as one value, no traversal
		{`{"a":{"$elemMatch":{"$gt":1,"$lt":3}}}`, `{"a":[1,2]}`, true},
		{`{"a":{"$elemMatch":{"$gt":1,"$lt":3}}}`, `{"a":[1,5]}`, false},
		{`{"a":{"$elemMatch":{"$gt":1,"$lt":3}}}`, `{"a":[[1,2],[3]]}`, false},
		{`{"a":{"$elemMatch":{"$eq":[1,2]}}}`, `{"a":[[1,2],[3]]}`, true},
		{`{"a":{"$elemMatch":{"$elemMatch":{"$eq":2}}}}`, `{"a":[[1,2],[3]]}`, true},
		{`{"a":{"$elemMatch":{"$ne":1}}}`, `{"a":[1,null]}`, true},
		{`{"a":{"$elemMatch":{"$type":"object"}}}`, `{"a":[1,{"x":1}]}`, true},
		{`{"a":{"$elemMatch":{"$type":"number"}}}`, `{"a":[[1],"x"]}`, false},
		{`{"a":{"$elemMatch":{"$regex":"^x"}}}`, `{"a":["y","xz"]}`, true},
		{`{"a":{"$elemMatch":{"$in":[1,3]}}}`, `{"a":[2,3]}`, true},
		{`{"a":{"$elemMatch":{"$exists":true}}}`, `{"a":[null]}`, true},
		{`{"a":{"$elemMatch":{"$exists":true}}}`, `{"a":[]}`, false},
		{`{"a":{"$elemMatch":{"$size":2}}}`, `{"a":[[1,2]]}`, true},
		// through a path: applies to every leaf array; a positional leaf is iterated too
		{`{"a.b":{"$elemMatch":{"$gt":1}}}`, `{"a":[{"b":[0]},{"b":[0,2]}]}`, true},
		{`{"a.0":{"$elemMatch":{"$eq":2}}}`, `{"a":[[1,2]]}`, true},
		// negation
		{`{"a":{"$not":{"$elemMatch":{"b":1}}}}`, `{"a":[{"b":1}]}`, false},
		{`{"a":{"$not":{"$elemMatch":{"b":1}}}}`, `{"a":[{"b":2}]}`, true},
		{`{"a":{"$all":[{"$elemMatch":{"b":1}},{"$elemMatch":{"c":2}}]}}`, `{"a":[{"b":1},{"c":2}]}`, true},
		{`{"a":{"$all":[{"$elemMatch":{"b":1}},{"$elemMatch":{"c":2}}]}}`, `{"a":[{"b":1}]}`, false},
	}
	for _, c := range cases {
		f, err := ParseCondition(c.filter)
		require.NoError(t, err, c.filter)
		assert.Equal(t, c.want, f.Ok(anyenc.MustParseJson(c.doc), nil), "%s on %s", c.filter, c.doc)
	}
}

func TestElemMatch_IndexBoundsAndString(t *testing.T) {
	// value form: the element-level bounds on the field itself
	f := MustParseCondition(`{"a":{"$elemMatch":{"$gt":1,"$lt":3}}}`)
	bs := f.IndexBounds("a", nil)
	require.Len(t, bs, 1)
	assert.Equal(t, anyenc.Tuple(anyenc.MustParseJson(`1`).MarshalTo(nil)), bs[0].Start) // first conjunct, wide channel
	assert.Empty(t, f.IndexBounds("a.b", nil))
	// positional path: the leaf is stored whole, no element bounds
	assert.Empty(t, MustParseCondition(`{"a.0":{"$elemMatch":{"$gt":1}}}`).IndexBounds("a.0", nil))
	// a nested $elemMatch looks inside elements the index stores whole
	assert.Empty(t, MustParseCondition(`{"a":{"$elemMatch":{"$elemMatch":{"$eq":2}}}}`).IndexBounds("a", nil))
	assert.Empty(t, MustParseCondition(`{"a":{"$elemMatch":{"$not":{"$elemMatch":{"$eq":2}}}}}`).IndexBounds("a", nil))
	// $all of object forms re-keys like the $and spelling
	all := MustParseCondition(`{"a":{"$all":[{"$elemMatch":{"b":{"$gt":1}}},{"$elemMatch":{"c":2}}]}}`)
	and := MustParseCondition(`{"$and":[{"a":{"$elemMatch":{"b":{"$gt":1}}}},{"a":{"$elemMatch":{"c":2}}}]}`)
	assert.Equal(t, and.IndexBounds("a.b", nil), all.IndexBounds("a.b", nil))
	assert.Len(t, all.IndexBounds("a.c", nil), 1)
	tbAll, _ := TightIndexBounds(all, "a.b")
	tbAnd, _ := TightIndexBounds(and, "a.b")
	assert.Equal(t, tbAnd, tbAll)
	assert.True(t, ContainsElemMatch(all))
	assert.False(t, ContainsElemMatch(MustParseCondition(`{"a.b":1}`)))

	// object form: re-keyed under the sub-field, none on the field itself
	f = MustParseCondition(`{"a":{"$elemMatch":{"b":{"$gt":1},"c":2}}}`)
	assert.Empty(t, f.IndexBounds("a", nil))
	bs = f.IndexBounds("a.b", nil)
	require.Len(t, bs, 1)
	assert.Equal(t, anyenc.Tuple(anyenc.MustParseJson(`1`).MarshalTo(nil)), bs[0].Start)
	assert.Len(t, f.IndexBounds("a.c", nil), 1)
	assert.Empty(t, f.IndexBounds("a.d", nil))
	assert.Empty(t, f.IndexBounds("ab", nil))
	assert.Empty(t, MustParseCondition(`{"a.0":{"$elemMatch":{"b":1}}}`).IndexBounds("a.0.b", nil))
	tb, empty := TightIndexBounds(f, "a.b")
	assert.False(t, empty)
	assert.Len(t, tb, 1)

	assert.Equal(t, `{"a": {"$elemMatch": {"$gt": 1}}}`, MustParseCondition(`{"a":{"$elemMatch":{"$gt":1}}}`).String())
	assert.True(t, GuaranteesPresence(MustParseCondition(`{"a":{"$elemMatch":{"b":1}}}`), "a"))
	assert.True(t, ContainsKnn(Key{Path: []string{"a"}, Filter: ElemMatch{Cond: Key{Path: []string{"v"}, Filter: &Knn{}}}}))
}
