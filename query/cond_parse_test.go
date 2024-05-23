package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"
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
		`{"a": {"$in": [1, 2]}}`,
		`{"a": {"$or":[{"$eq": 1}, {"$eq": 2}]}}`,
	},
	{
		`{"a": {"$in": [1, 2], "$nin": [3,4], "$all": [5,6]}}`,
		`{"a": {"$and":[{"$or":[{"$eq": 1}, {"$eq": 2}]}, {"$nor":[{"$eq": 3}, {"$eq": 4}]}, {"$and":[{"$eq": 5}, {"$eq": 6}]}]}}`,
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
