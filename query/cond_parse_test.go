package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/syncpool"
)

type parserCase struct {
	query  string
	expect string
}

var validParserCases = []parserCase{
	{
		`{"a":1}`,
		`{"a": {"$eq": 1}}`,
	},
	{
		`{"a":1, "b":2}`,
		`{"$and":[{"a": {"$eq": 1}}, {"b": {"$eq": 2}}]}`,
	},
	{
		`{"$and":[{"a":1},{"b":2}]}`,
		`{"$and":[{"a": {"$eq": 1}}, {"b": {"$eq": 2}}]}`,
	},
	{
		`{"$or":[{"a":1},{"b":2}]}`,
		`{"$or":[{"a": {"$eq": 1}}, {"b": {"$eq": 2}}]}`,
	},
	{
		`{"$nor":[{"a":1},{"b":2}]}`,
		`{"$nor":[{"a": {"$eq": 1}}, {"b": {"$eq": 2}}]}`,
	},
	{
		`{"$or":[{"a":1},{"b":2}], "$and":[{"c":1}]}`,
		`{"$and":[{"$or":[{"a": {"$eq": 1}}, {"b": {"$eq": 2}}]}, {"c": {"$eq": 1}}]}`,
	},
	{
		`{"a": {"$lt":10, "$gt": 1}}`,
		`{"a": {"$and":[{"$lt": 10}, {"$gt": 1}]}}`,
	},
	{
		`{"a": {"$lte":10, "$gte": 1}}`,
		`{"a": {"$and":[{"$lte": 10}, {"$gte": 1}]}}`,
	},
	{
		`{"a": {"$ne": 1}}`,
		`{"a": {"$ne": 1}}`,
	},
	{
		`{"a": {"$eq": 1}}`,
		`{"a": {"$eq": 1}}`,
	},
	{
		`{"a": {"$in":[1]}}`,
		`{"a": {"$in":[1]}}`,
	},
	{
		`{"a": {"$in": [1], "$nin": [3,4], "$all": [5,6]}}`,
		`{"a": {"$and":[{"$in":[1]}, {"$nor":[{"$eq": 3}, {"$eq": 4}]}, {"$and":[{"$eq": 5}, {"$eq": 6}]}]}}`,
	},
	{
		`{"a":{"b":"c"}}`,
		`{"a": {"$eq": {"b":"c"}}}`,
	},
	{
		`{"a":[1,2,3]}`,
		`{"a": {"$eq": [1,2,3]}}`,
	},
	{
		`{"a": {"$not": {"$gt": 1.99 }}}`,
		`{"a": {"$not": {"$gt": 1.99}}}`,
	},
	{
		`{"a": {"$exists": true}}`,
		`{"a": {"$exists": true}}`,
	},
	{
		`{"a": {"$exists": "1"}}`,
		`{"a": {"$exists": true}}`,
	},
	{
		`{"a": {"$exists": 0}}`,
		`{"a": {"$not": {"$exists": true}}}`,
	},
	{
		`{"a": {"$exists": false}}`,
		`{"a": {"$not": {"$exists": true}}}`,
	},
	{
		`{"a": {"$type": 2}}`,
		`{"a": {"$type": "number"}}`,
	},
	{
		`{"a": {"$type": "number"}}`,
		`{"a": {"$type": "number"}}`,
	},
	{
		`{"a": {"$type": 11}}`,
		`{"a": {"$type": "objectId"}}`,
	},
	{
		`{"a": {"$type": "objectId"}}`,
		`{"a": {"$type": "objectId"}}`,
	},
}

var errorParserCases = []parserCase{
	{
		`{not valid json}`, ``,
	},
	{
		`[]`, ``,
	},
	{
		`{"$notExst":1}`, ``,
	},
	{
		`{"$lt":1}`, ``,
	},
	{
		`{"$and":1}`, ``,
	},
	{
		`{"$or":1}`, ``,
	},
	{
		`{"$nor":1}`, ``,
	},
	{
		`{"$and":[1]}`, ``,
	},
	{
		`{"$or":[1]}`, ``,
	},
	{
		`{"$nor":[1]}`, ``,
	},
	{
		`{"a":{"$gt":2, "b":1}}`, ``,
	},
	{
		`{"a":{"b":2, "$gt":1}}`, ``,
	},
	{
		`{"a":{"$not":1}}`, ``,
	},
	{
		`{"a":{"$not":{"a":"b"}}}`, ``,
	},
	{
		`{"a":{"$and":[{}]}}`, ``,
	},
	{
		`{"a":{"$in":1}}`, ``,
	},
	{
		`{"a":{"$nin":1}}`, ``,
	},
	{
		`{"a":{"$all":1}}`, ``,
	},
	{
		`{"a":{"$type": -1}}`, ``,
	},
	{
		`{"a":{"$type": 111}}`, ``,
	},
	{
		`{"a":{"$type": "xyz"}}`, ``,
	},
}

func TestParseCondition(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		for i, c := range validParserCases {
			f, err := ParseCondition(c.query)
			require.NoError(t, err, i)
			assert.Equal(t, c.expect, f.String(), i)
		}
	})
	t.Run("error", func(t *testing.T) {
		for i, c := range errorParserCases {
			f, err := ParseCondition(c.query)
			require.Error(t, err, i)
			assert.Nil(t, f, i)
		}
	})
}

func BenchmarkParseCondition(b *testing.B) {
	bench := func(b *testing.B, query string) {
		jq := fastjson.MustParse(query)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = ParseCondition(jq)
		}
	}
	b.Run("simple", func(b *testing.B) {
		bench(b, `{"a":"b"}`)
	})
	b.Run("middle", func(b *testing.B) {
		bench(b, `{"a": {"$in": [1, 2], "$nin": [3,4], "$all": [5,6]}}`)
	})
}

// An empty object is the condition every document satisfies — as the whole
// filter and as an element of $and/$or/$nor — and is always a real filter:
// the logical operators evaluate every element they hold, so a nil element
// would be a nil-interface call at evaluation.
func TestParseCondition_EmptyObjectMatchesAll(t *testing.T) {
	doc := anyenc.MustParseJson(`{"a":2,"b":[1,2]}`)
	for _, tc := range []struct {
		filter string
		want   bool
	}{
		{`{}`, true},
		{`{"$and":[{}]}`, true},
		{`{"$or":[{}]}`, true},
		{`{"$nor":[{}]}`, false},
		{`{"$or":[{},{"a":1}]}`, true},
		{`{"$and":[{},{"a":1}]}`, false},
		{`{"$and":[{},{"a":2}]}`, true},
		{`{"$nor":[{},{"a":1}]}`, false},
		{`{"$or":[{"$and":[{}]},{"a":5}]}`, true},
		{`{"$and":[{"$or":[{}]}],"a":2}`, true},
		{`{"b":{"$elemMatch":{}}}`, false}, // object form: no object element
	} {
		t.Run(tc.filter, func(t *testing.T) {
			f, err := ParseCondition(tc.filter)
			require.NoError(t, err)
			require.NotNil(t, f)
			assert.Equal(t, tc.want, f.Ok(doc, &syncpool.DocBuffer{}))
			assert.NotPanics(t, func() { _ = f.String() })
			assert.NotPanics(t, func() { _ = f.IndexBounds("a", nil) })
		})
	}
}

// An empty array under $and, $or or $nor is rejected at every level, alone or
// beside another key: an empty disjunction is false and an empty conjunction
// true, so a filter built from an empty list would match nothing or everything
// depending on the operator.
func TestParseCondition_EmptyLogicalArrayRejected(t *testing.T) {
	for _, tc := range []struct {
		filter, wantPath, wantOp string
	}{
		{`{"$or":[]}`, "$or", "$or"},
		{`{"$and":[]}`, "$and", "$and"},
		{`{"$nor":[]}`, "$nor", "$nor"},
		{`{"$and":[],"a":1}`, "$and", "$and"},
		{`{"$or":[],"a":1}`, "$or", "$or"},
		{`{"$nor":[],"a":1}`, "$nor", "$nor"},
		{`{"a":1,"$or":[]}`, "$or", "$or"},
		{`{"$or":[{"$and":[]},{"a":5}]}`, "$or.0.$and", "$and"},
		{`{"$and":[{"a":1},{"$or":[]}]}`, "$and.1.$or", "$or"},
		{`{"$nor":[{"$nor":[]}]}`, "$nor.0.$nor", "$nor"},
		{`{"$text":{"$search":"hello"},"$and":[]}`, "$and", "$and"},
		{`{"a":{"$elemMatch":{"$or":[]}}}`, "a.$elemMatch.$or", "$or"},
		{`{"a":{"$all":[{"$elemMatch":{"$and":[]}}]}}`, "a.$all.0.$elemMatch.$and", "$and"},
	} {
		t.Run(tc.filter, func(t *testing.T) {
			f, err := ParseCondition(tc.filter)
			require.Error(t, err)
			assert.Nil(t, f)
			var pe *ParseError
			require.ErrorAs(t, err, &pe)
			assert.Equal(t, tc.wantPath, pe.Path)
			assert.Equal(t, tc.wantOp, pe.Op)
			assert.Equal(t, tc.wantOp+" must be a non-empty array", pe.Reason)
		})
	}
}

// {"$all": []} matches no document (MongoDB's rule): not one whose array is
// empty, not one without the field. A conjunction of no equalities would be
// true everywhere, and next to the match-all {} it would let a Delete empty
// the collection.
func TestParseCondition_AllWithEmptyArrayMatchesNothing(t *testing.T) {
	// The "o" documents give the $elemMatch case object elements to test
	// the inner {"$all":[]} on.
	docs := []string{`{"tags":[1,2]}`, `{"tags":[]}`, `{"tags":null}`, `{"tags":1}`, `{}`,
		`{"o":[{"tags":[1]}]}`, `{"o":[{"tags":[]},{}]}`}
	for _, tc := range []struct {
		filter string
		want   bool
	}{
		{`{"tags":{"$all":[]}}`, false},
		{`{"$and":[{},{"tags":{"$all":[]}}]}`, false},
		{`{"$or":[{},{"tags":{"$all":[]}}]}`, true},
		{`{"$nor":[{"tags":{"$all":[]}}]}`, true},
		{`{"tags":{"$not":{"$all":[]}}}`, true},
		{`{"o":{"$elemMatch":{"tags":{"$all":[]}}}}`, false},
	} {
		t.Run(tc.filter, func(t *testing.T) {
			f, err := ParseCondition(tc.filter)
			require.NoError(t, err)
			for _, d := range docs {
				doc := anyenc.MustParseJson(d)
				assert.Equal(t, tc.want, f.Ok(doc, &syncpool.DocBuffer{}), d)
			}
		})
	}
}

// A filter that matches every document renders as {}: the parsed empty
// object (All) and a Go-built And with no conditions, alone or nested. A path
// condition over one of them renders as {"<path>": {"$nin": []}} and keeps
// its path, which the query layer validates ("_distance"). The rendering is
// what Explain shows, and it parses back to a filter that matches the same
// documents: the parser rejects null and {"$and":[]}.
func TestFilterString_MatchAllRoundTrips(t *testing.T) {
	docs := []string{`{"a":1,"b":[{"c":1}]}`, `{"a":2,"b":[]}`, `{}`}
	for _, tc := range []struct {
		name   string
		filter Filter
		want   string
	}{
		{"{}", MustParseCondition(`{}`), `{}`},
		{"$and of {}", MustParseCondition(`{"$and":[{},{"a":1}]}`), ``},
		{"$or of {}", MustParseCondition(`{"$or":[{},{"a":1}]}`), ``},
		{"$nor of {}", MustParseCondition(`{"$nor":[{}]}`), `{"$nor":[{}]}`},
		{"$elemMatch {}", MustParseCondition(`{"b":{"$elemMatch":{}}}`), `{"b": {"$elemMatch": {}}}`},
		{"And{}", And{}, `{}`},
		{"And{And{}}", And{And{}}, `{"$and":[{}]}`},
		{"And{And{}, a=1}", And{And{}, MustParseCondition(`{"a":1}`)}, ``},
		{"Key{a, And{}}", Key{Path: []string{"a"}, Filter: And{}}, `{"a": {"$nin": []}}`},
		{"Key{a, All{}}", Key{Path: []string{"a"}, Filter: All{}}, `{"a": {"$nin": []}}`},
		{"Key{b.c, Nor{}}", Key{Path: []string{"b", "c"}, Filter: Nor{}}, `{"b.c": {"$nin": []}}`},
		{"Key{a, &And{}}", Key{Path: []string{"a"}, Filter: &And{}}, `{"a": {"$nin": []}}`},
		{"$nin []", MustParseCondition(`{"a":{"$nin":[]}}`), `{"a": {"$nin": []}}`},
		{"_distance $nin []", MustParseCondition(`{"_distance":{"$nin":[]}}`), `{"_distance": {"$nin": []}}`},
		{"And{Key{a, All{}}, a=1}", And{Key{Path: []string{"a"}, Filter: All{}}, MustParseCondition(`{"a":1}`)}, ``},
		{"Nor{}", Nor{}, `{}`},
		{"Or{}", Or{}, `{"$nor":[{}]}`},
		{"Or{Or{}, a=1}", Or{Or{}, MustParseCondition(`{"a":1}`)}, ``},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.filter.String()
			if tc.want != "" {
				assert.Equal(t, tc.want, s)
			}
			back, err := ParseCondition(s)
			require.NoError(t, err, s)
			again, err := ParseCondition(back.String())
			require.NoError(t, err, back.String())
			assert.Equal(t, back.String(), again.String(), "the rendering of a parsed filter is a fixed point")
			for _, d := range docs {
				doc := anyenc.MustParseJson(d)
				assert.Equal(t, tc.filter.Ok(doc, &syncpool.DocBuffer{}), back.Ok(doc, &syncpool.DocBuffer{}), "%s on %s", s, d)
			}
		})
	}
}

// A path condition renders its path so that ParseCondition reads back the
// same field-name bytes: a field name with a quote, a backslash, a control
// byte or invalid UTF-8 parses back to the same path and selects the same
// documents. Inserted as it is, `a":1,"b` turned
// {"a\":1,\"b":{"$nin":[]}}, which matches every document, into a filter on
// a=1.
func TestKeyString_PathRoundTrips(t *testing.T) {
	for _, tc := range []struct {
		filter string
		docs   []string
	}{
		{`{"a\":1,\"b":{"$nin":[]}}`, []string{`{"a":2}`, `{"a":1}`, `{}`}},
		{`{"a\":1,\"b":2}`, []string{`{"a\":1,\"b":2}`, `{"a":1,"b":2}`, `{}`}},
		{`{"a\\b":{"$nin":[]}}`, []string{`{"a":2}`, `{}`}},
		{`{"a\\b":2}`, []string{`{"a\\b":2}`, `{"a":2}`}},
		{`{"a\u001fb.c":2}`, []string{`{"a\u001fb":{"c":2}}`, `{"ab":{"c":2}}`}},
		{"{\"a\xff\":2}", []string{"{\"a\xff\":2}", `{"a�":2}`}},
	} {
		t.Run(tc.filter, func(t *testing.T) {
			f, err := ParseCondition(tc.filter)
			require.NoError(t, err)
			s := f.String()
			back, err := ParseCondition(s)
			require.NoError(t, err, s)
			require.IsType(t, Key{}, back, s)
			assert.Equal(t, f.(Key).Path, back.(Key).Path, s)
			for _, d := range tc.docs {
				doc := anyenc.MustParseJson(d)
				assert.Equal(t, f.Ok(doc, &syncpool.DocBuffer{}), back.Ok(doc, &syncpool.DocBuffer{}), "%s on %s", s, d)
			}
			assert.True(t, back.Ok(anyenc.MustParseJson(tc.docs[0]), &syncpool.DocBuffer{}), "%s on %s", s, tc.docs[0])
		})
	}
}

// Only an And with no entries matches every value: And.Ok calls each entry,
// a nil one too.
func TestMatchesEverything_OnlyEmptyNodes(t *testing.T) {
	assert.True(t, matchesEverything(And{}))
	assert.True(t, matchesEverything(&And{}))
	assert.False(t, matchesEverything(And{nil}))
	assert.False(t, matchesEverything(&And{nil, nil}))
}
