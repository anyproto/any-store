package aggregate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/query"
)

func TestSplitPrefix(t *testing.T) {
	t.Run("full canonical prefix", func(t *testing.T) {
		pre, rest := SplitPrefix(MustParsePipeline(`[
			{"$match": {"a": 1}},
			{"$sort": {"a": 1}},
			{"$skip": 5},
			{"$limit": 10},
			{"$group": {"_id": "$a"}}
		]`))
		assert.NotNil(t, pre.Filter)
		assert.NotNil(t, pre.Sort)
		assert.Equal(t, 5, pre.Skip)
		assert.Equal(t, 10, pre.Limit)
		require.Len(t, rest, 1)
		assert.IsType(t, GroupSpec{}, rest[0])
	})

	t.Run("contiguous matches folded into and", func(t *testing.T) {
		pre, rest := SplitPrefix(MustParsePipeline(`[
			{"$match": {"a": 1}},
			{"$match": {"b": 2}}
		]`))
		assert.IsType(t, query.And{}, pre.Filter)
		assert.Len(t, pre.Filter.(query.And), 2)
		assert.Empty(t, rest)
	})

	t.Run("match after sort stops pushdown", func(t *testing.T) {
		pre, rest := SplitPrefix(MustParsePipeline(`[
			{"$sort": {"a": 1}},
			{"$match": {"b": 2}},
			{"$limit": 3}
		]`))
		assert.Nil(t, pre.Filter)
		assert.NotNil(t, pre.Sort)
		require.Len(t, rest, 2)
		assert.IsType(t, MatchSpec{}, rest[0])
		assert.IsType(t, LimitSpec{}, rest[1])
	})

	t.Run("skip after limit stays in pipeline", func(t *testing.T) {
		pre, rest := SplitPrefix(MustParsePipeline(`[
			{"$limit": 10},
			{"$skip": 2}
		]`))
		assert.Equal(t, 10, pre.Limit)
		assert.Equal(t, 0, pre.Skip)
		require.Len(t, rest, 1)
		assert.IsType(t, SkipSpec{}, rest[0])
	})

	t.Run("blocking stage stops everything", func(t *testing.T) {
		pre, rest := SplitPrefix(MustParsePipeline(`[
			{"$unwind": "$t"},
			{"$match": {"b": 2}}
		]`))
		assert.Equal(t, Prefix{}, pre)
		assert.Len(t, rest, 2)
	})

	t.Run("empty pipeline", func(t *testing.T) {
		pre, rest := SplitPrefix(nil)
		assert.Equal(t, Prefix{}, pre)
		assert.Empty(t, rest)
	})

	t.Run("pure $expr match is residual with a full scan", func(t *testing.T) {
		pre, rest := SplitPrefix(MustParsePipeline(`[
			{"$match": {"$expr": {"$gt": ["$a", "$b"]}}},
			{"$group": {"_id": "$a"}}
		]`))
		assert.Nil(t, pre.Filter)
		require.Len(t, rest, 2)
		m := rest[0].(MatchSpec)
		assert.Nil(t, m.Filter)
		assert.Len(t, m.Exprs, 1)
		assert.IsType(t, GroupSpec{}, rest[1])
	})

	t.Run("mixed match splits: filter pushed, $expr residual", func(t *testing.T) {
		pre, rest := SplitPrefix(MustParsePipeline(`[
			{"$match": {"a": 1, "$expr": {"$gt": ["$x", "$y"]}}}
		]`))
		require.NotNil(t, pre.Filter)
		require.Len(t, rest, 1)
		m := rest[0].(MatchSpec)
		assert.Nil(t, m.Filter)
		assert.Len(t, m.Exprs, 1)
	})

	t.Run("exprs of a match chain fold into one residual", func(t *testing.T) {
		pre, rest := SplitPrefix(MustParsePipeline(`[
			{"$match": {"a": 1}},
			{"$match": {"$expr": {"$gt": ["$x", "$y"]}}},
			{"$match": {"b": 2, "$expr": {"$eq": ["$p", "$q"]}}}
		]`))
		assert.IsType(t, query.And{}, pre.Filter)
		assert.Len(t, pre.Filter.(query.And), 2)
		require.Len(t, rest, 1)
		assert.Len(t, rest[0].(MatchSpec).Exprs, 2)
	})

	t.Run("$sort still pushes past a residual $expr", func(t *testing.T) {
		pre, rest := SplitPrefix(MustParsePipeline(`[
			{"$match": {"$expr": {"$gt": ["$a", "$b"]}}},
			{"$sort": {"a": 1}}
		]`))
		assert.NotNil(t, pre.Sort)
		require.Len(t, rest, 1)
		assert.Len(t, rest[0].(MatchSpec).Exprs, 1)
	})

	t.Run("$skip/$limit stay in-pipeline after a residual $expr", func(t *testing.T) {
		// Pushed skip/limit would truncate before the residual predicate runs.
		pre, rest := SplitPrefix(MustParsePipeline(`[
			{"$match": {"$expr": {"$gt": ["$a", "$b"]}}},
			{"$sort": {"a": 1}},
			{"$skip": 2},
			{"$limit": 3}
		]`))
		assert.NotNil(t, pre.Sort)
		assert.Zero(t, pre.Skip)
		assert.Zero(t, pre.Limit)
		require.Len(t, rest, 3)
		assert.IsType(t, MatchSpec{}, rest[0])
		assert.IsType(t, SkipSpec{}, rest[1])
		assert.IsType(t, LimitSpec{}, rest[2])
	})
}

func TestExplainStages(t *testing.T) {
	out := ExplainStages(MustParsePipeline(`[
		{"$group": {"_id": "$a", "n": {"$count": {}}}},
		{"$sort": {"n": -1}},
		{"$limit": 10}
	]`))
	assert.Contains(t, out, "1. $group")
	assert.Contains(t, out, "topK 10")
	assert.Contains(t, out, "3. $limit 10")
}
