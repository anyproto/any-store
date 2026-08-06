package aggregate

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
)

func TestParsePipeline(t *testing.T) {
	t.Run("not an array", func(t *testing.T) {
		_, err := ParsePipeline(`{"$match":{}}`)
		assert.ErrorContains(t, err, "must be an array")
	})
	t.Run("stage with two keys", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$match":{}, "$limit":1}]`)
		assert.ErrorContains(t, err, "exactly one key")
	})
	t.Run("unknown stage", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$bucket":{}}]`)
		assert.ErrorContains(t, err, "unknown stage")
	})
	t.Run("stage error includes index", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$limit":1},{"$skip":-1}]`)
		assert.ErrorContains(t, err, "(at 1.$skip)")
	})
	t.Run("full pipeline", func(t *testing.T) {
		p, err := ParsePipeline(`[
			{"$match": {"a": {"$gt": 1}}},
			{"$sort": {"a": 1, "b": -1}},
			{"$skip": 5},
			{"$limit": 10},
			{"$unwind": "$tags"},
			{"$group": {"_id": "$tags", "total": {"$sum": "$n"}}},
			{"$project": {"total": 1}},
			{"$addFields": {"x": "$total"}},
			{"$count": "n"}
		]`)
		require.NoError(t, err)
		require.Len(t, p, 9)
		assert.IsType(t, MatchSpec{}, p[0])
		assert.IsType(t, SortSpec{}, p[1])
		assert.IsType(t, SkipSpec{}, p[2])
		assert.IsType(t, LimitSpec{}, p[3])
		assert.IsType(t, UnwindSpec{}, p[4])
		assert.IsType(t, GroupSpec{}, p[5])
		assert.IsType(t, ProjectSpec{}, p[6])
		assert.IsType(t, AddFieldsSpec{}, p[7])
		assert.IsType(t, CountSpec{}, p[8])
	})
	t.Run("pipeline passthrough", func(t *testing.T) {
		p := Pipeline{LimitSpec{N: 1}}
		p2, err := ParsePipeline(p)
		require.NoError(t, err)
		assert.Equal(t, p, p2)
	})
}

func TestParseMatchExpr(t *testing.T) {
	t.Run("pure expr", func(t *testing.T) {
		p := MustParsePipeline(`[{"$match": {"$expr": {"$gt": ["$a", "$b"]}}}]`)
		s := p[0].(MatchSpec)
		assert.Nil(t, s.Filter)
		require.Len(t, s.Exprs, 1)
		assert.Equal(t, `$match {"$expr":{$gt:[$a,$b]}}`, s.String())
	})
	t.Run("mixed spec splits", func(t *testing.T) {
		p := MustParsePipeline(`[{"$match": {"a": 1, "$expr": {"$gt": ["$x", "$y"]}}}]`)
		s := p[0].(MatchSpec)
		require.NotNil(t, s.Filter)
		require.Len(t, s.Exprs, 1)
		assert.Contains(t, s.String(), `"a"`)
		assert.Contains(t, s.String(), `{"$expr":{$gt:[$x,$y]}}`)
		assert.Contains(t, s.String(), `"$and"`)
	})
	t.Run("nested in top-level $and", func(t *testing.T) {
		p := MustParsePipeline(`[{"$match": {"$and": [
			{"a": 1},
			{"$expr": {"$gt": ["$x", "$y"]}},
			{"b": 2, "$expr": {"$eq": ["$p", "$q"]}}
		]}}]`)
		s := p[0].(MatchSpec)
		require.NotNil(t, s.Filter)
		require.Len(t, s.Exprs, 2)
		assert.Equal(t, `{$gt:[$x,$y]}`, s.Exprs[0].String())
		assert.Equal(t, `{$eq:[$p,$q]}`, s.Exprs[1].String())
		fs := s.Filter.String()
		assert.Contains(t, fs, `"a"`)
		assert.Contains(t, fs, `"b"`)
	})
	t.Run("$and of only exprs leaves no filter", func(t *testing.T) {
		p := MustParsePipeline(`[{"$match": {"$and": [
			{"$expr": {"$gt": ["$x", 1]}},
			{"$expr": {"$lt": ["$x", 9]}}
		]}}]`)
		s := p[0].(MatchSpec)
		assert.Nil(t, s.Filter)
		assert.Len(t, s.Exprs, 2)
	})
	t.Run("rejected under $or and $nor", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$match": {"$or": [{"a": 1}, {"$expr": {"$gt": ["$x", "$y"]}}]}}]`)
		assert.ErrorContains(t, err, "$expr is not supported under $or")
		assert.ErrorContains(t, err, "(at 0.$match.$or)")
		_, err = ParsePipeline(`[{"$match": {"$nor": [{"$expr": true}]}}]`)
		assert.ErrorContains(t, err, "$expr is not supported under $nor")
		// Nested one level down inside $and still reports the $or placement.
		_, err = ParsePipeline(`[{"$match": {"$and": [{"$or": [{"$expr": true}]}]}}]`)
		assert.ErrorContains(t, err, "$expr is not supported under $or")
		assert.ErrorContains(t, err, "(at 0.$match.$and.0.$or)")
	})
	t.Run("field-level $expr stays a filter error", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$match": {"a": {"$expr": 1}}}]`)
		assert.ErrorContains(t, err, "unknown operator: $expr")
		assert.ErrorIs(t, err, query.ErrUnknownOperator)
	})
	t.Run("$expr key deep inside an equality value is data", func(t *testing.T) {
		p := MustParsePipeline(`[{"$match": {"a": {"b": {"$expr": 1}}}}]`)
		s := p[0].(MatchSpec)
		assert.NotNil(t, s.Filter)
		assert.Empty(t, s.Exprs)
	})
	t.Run("expression parse error located", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$match": {"$expr": {"$bogus": 1}}}]`)
		assert.ErrorContains(t, err, "unsupported expression operator: $bogus")
		assert.ErrorContains(t, err, "(at 0.$match.$expr.$bogus)")
		assert.ErrorIs(t, err, query.ErrUnknownOperator)
	})
	t.Run("stripped filter error still structured", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$match": {"$expr": true, "a": {"$sizee": 1}}}]`)
		assert.ErrorContains(t, err, "unknown operator: $sizee")
		assert.ErrorIs(t, err, query.ErrUnknownOperator)
	})
	t.Run("filter error keeps original $and indices", func(t *testing.T) {
		// The $expr-only element 0 is stripped out before the filter parse;
		// the error must still point at element 1 of the user's spec.
		_, err := ParsePipeline(`[{"$match": {"$and": [{"$expr": true}, {"b": {"$sizee": 1}}]}}]`)
		assert.ErrorContains(t, err, "unknown operator: $sizee")
		assert.ErrorContains(t, err, "(at 0.$match.$and.1.b.$sizee)")
	})
	t.Run("nested $and filter error keeps original indices", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$match": {"$and": [
			{"$expr": true},
			{"$and": [{"$expr": true}, {"c": {"$bogus": 1}}]}
		]}}]`)
		assert.ErrorContains(t, err, "unknown operator: $bogus")
		assert.ErrorContains(t, err, "(at 0.$match.$and.1.$and.1.c.$bogus)")
	})
}

func TestParseSortStage(t *testing.T) {
	t.Run("directions", func(t *testing.T) {
		p := MustParsePipeline(`[{"$sort": {"a.b": 1, "c": -1}}]`)
		s := p[0].(SortSpec)
		require.Len(t, s.Fields, 2)
		assert.Equal(t, []string{"a", "b"}, s.Fields[0].Path)
		assert.False(t, s.Fields[0].Reverse)
		assert.True(t, s.Fields[1].Reverse)
	})
	t.Run("invalid direction", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$sort": {"a": 2}}]`)
		assert.ErrorContains(t, err, "must be 1 or -1")
		_, err = ParsePipeline(`[{"$sort": {"a": "asc"}}]`)
		assert.ErrorContains(t, err, "must be 1 or -1")
	})
	t.Run("empty", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$sort": {}}]`)
		assert.ErrorContains(t, err, "at least one field")
	})
}

func TestParseSkipLimitCount(t *testing.T) {
	_, err := ParsePipeline(`[{"$skip": -1}]`)
	assert.ErrorContains(t, err, "non-negative")
	_, err = ParsePipeline(`[{"$limit": 0}]`)
	assert.ErrorContains(t, err, "positive")
	_, err = ParsePipeline(`[{"$count": ""}]`)
	assert.ErrorContains(t, err, "empty field name")
	_, err = ParsePipeline(`[{"$count": "$x"}]`)
	assert.ErrorContains(t, err, "must not start with $")
	_, err = ParsePipeline(`[{"$count": 5}]`)
	assert.ErrorContains(t, err, "must be a string")
}

func TestParseProject(t *testing.T) {
	t.Run("include and expression", func(t *testing.T) {
		p := MustParsePipeline(`[{"$project": {"a": 1, "b": true, "c": "$x.y", "d": "lit"}}]`)
		s := p[0].(ProjectSpec)
		require.Len(t, s.Fields, 4)
		assert.IsType(t, &FieldRefExpr{}, s.Fields[0].Expr)
		assert.Equal(t, []string{"a"}, s.Fields[0].Expr.(*FieldRefExpr).Path)
		assert.IsType(t, &FieldRefExpr{}, s.Fields[1].Expr)
		assert.Equal(t, []string{"x", "y"}, s.Fields[2].Expr.(*FieldRefExpr).Path)
		assert.IsType(t, &LiteralExpr{}, s.Fields[3].Expr)
	})
	t.Run("id can be projected", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$project": {"id": 1}}]`)
		assert.NoError(t, err)
	})
	t.Run("exclusion rejected", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$project": {"a": 0}}]`)
		assert.ErrorContains(t, err, "exclusion")
		_, err = ParsePipeline(`[{"$project": {"a": false}}]`)
		assert.ErrorContains(t, err, "exclusion")
	})
	t.Run("dotted output rejected", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$project": {"a.b": 1}}]`)
		assert.ErrorContains(t, err, "dotted")
	})
	t.Run("addFields literal numbers stay literal", func(t *testing.T) {
		p := MustParsePipeline(`[{"$addFields": {"a": 1}}]`)
		s := p[0].(AddFieldsSpec)
		assert.IsType(t, &LiteralExpr{}, s.Fields[0].Expr)
	})
	t.Run("set alias", func(t *testing.T) {
		p := MustParsePipeline(`[{"$set": {"a": 1}}]`)
		assert.IsType(t, AddFieldsSpec{}, p[0])
	})
}

func TestParseUnwind(t *testing.T) {
	t.Run("string form", func(t *testing.T) {
		p := MustParsePipeline(`[{"$unwind": "$a.b"}]`)
		s := p[0].(UnwindSpec)
		assert.Equal(t, []string{"a", "b"}, s.Path)
		assert.False(t, s.PreserveNullAndEmptyArrays)
	})
	t.Run("object form", func(t *testing.T) {
		p := MustParsePipeline(`[{"$unwind": {"path": "$a", "preserveNullAndEmptyArrays": true}}]`)
		s := p[0].(UnwindSpec)
		assert.Equal(t, []string{"a"}, s.Path)
		assert.True(t, s.PreserveNullAndEmptyArrays)
	})
	t.Run("errors", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$unwind": "a"}]`)
		assert.ErrorContains(t, err, "must start with $")
		_, err = ParsePipeline(`[{"$unwind": "$"}]`)
		assert.ErrorContains(t, err, "must start with $")
		_, err = ParsePipeline(`[{"$unwind": {"path": "$a", "bogus": 1}}]`)
		assert.ErrorContains(t, err, "unknown $unwind option")
		_, err = ParsePipeline(`[{"$unwind": 5}]`)
		assert.ErrorContains(t, err, "must be a string or an object")
	})
}

func TestParseGroup(t *testing.T) {
	t.Run("_id maps to key", func(t *testing.T) {
		p := MustParsePipeline(`[{"$group": {"_id": "$cat", "n": {"$count": {}}, "s": {"$sum": "$v"}}}]`)
		s := p[0].(GroupSpec)
		assert.IsType(t, &FieldRefExpr{}, s.Key)
		require.Len(t, s.Accums, 2)
		assert.Equal(t, AccumCount, s.Accums[0].Op)
		assert.Nil(t, s.Accums[0].Arg)
		assert.Equal(t, AccumSum, s.Accums[1].Op)
		assert.NotNil(t, s.Accums[1].Arg)
	})
	t.Run("id spelling accepted", func(t *testing.T) {
		p := MustParsePipeline(`[{"$group": {"id": null}}]`)
		s := p[0].(GroupSpec)
		assert.IsType(t, &LiteralExpr{}, s.Key)
	})
	t.Run("compound key", func(t *testing.T) {
		p := MustParsePipeline(`[{"$group": {"_id": {"a": "$x", "b": "$y"}}}]`)
		s := p[0].(GroupSpec)
		assert.IsType(t, &ObjectExpr{}, s.Key)
	})
	t.Run("both id spellings rejected", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$group": {"_id": "$a", "id": "$b"}}]`)
		assert.ErrorContains(t, err, "both id and _id")
	})
	t.Run("missing key", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$group": {"n": {"$sum": 1}}}]`)
		assert.ErrorContains(t, err, "requires an id")
	})
	t.Run("all accumulators", func(t *testing.T) {
		p := MustParsePipeline(`[{"$group": {"_id": null,
			"a": {"$sum": "$v"}, "b": {"$avg": "$v"}, "c": {"$min": "$v"},
			"d": {"$max": "$v"}, "e": {"$count": {}}, "f": {"$first": "$v"},
			"g": {"$last": "$v"}, "h": {"$push": "$v"}, "i": {"$addToSet": "$v"}}}]`)
		s := p[0].(GroupSpec)
		require.Len(t, s.Accums, 9)
		ops := make([]AccumOp, 0, 9)
		for _, a := range s.Accums {
			ops = append(ops, a.Op)
		}
		assert.Equal(t, []AccumOp{AccumSum, AccumAvg, AccumMin, AccumMax, AccumCount,
			AccumFirst, AccumLast, AccumPush, AccumAddToSet}, ops)
	})
	t.Run("errors", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$group": {"_id": null, "n": {"$bogus": 1}}}]`)
		assert.ErrorContains(t, err, "unknown accumulator")
		_, err = ParsePipeline(`[{"$group": {"_id": null, "n": 1}}]`)
		assert.ErrorContains(t, err, "must be an accumulator object")
		_, err = ParsePipeline(`[{"$group": {"_id": null, "n": {"$count": {"x":1}}}}]`)
		assert.ErrorContains(t, err, "empty object")
		_, err = ParsePipeline(`[{"$group": {"_id": null, "a.b": {"$sum": 1}}}]`)
		assert.ErrorContains(t, err, "dotted")
		_, err = ParsePipeline(`[{"$group": {"_id": null, "n": {"$sum": "$a", "$avg": "$a"}}}]`)
		assert.ErrorContains(t, err, "must be an accumulator object")
	})
}

func TestParseLookup(t *testing.T) {
	t.Run("full form", func(t *testing.T) {
		p := MustParsePipeline(`[{"$lookup": {"from": "objects", "localField": "links.to", "foreignField": "id", "as": "linked"}}]`)
		s := p[0].(LookupSpec)
		assert.Equal(t, "objects", s.From)
		assert.Equal(t, "links.to", s.LocalField)
		assert.Equal(t, []string{"links", "to"}, s.LocalPath)
		assert.Equal(t, "linked", s.As)
		assert.Equal(t, `$lookup {"from":"objects","localField":"links.to","foreignField":"id","as":"linked"}`, s.String())
	})
	t.Run("self form", func(t *testing.T) {
		// from and foreignField are optional: self-join on "id" is implied.
		p := MustParsePipeline(`[{"$lookup": {"localField": "ref", "as": "refDoc"}}]`)
		s := p[0].(LookupSpec)
		assert.Empty(t, s.From)
		assert.Equal(t, `$lookup {"localField":"ref","foreignField":"id","as":"refDoc"}`, s.String())
	})
	t.Run("string round-trip", func(t *testing.T) {
		for _, j := range []string{
			`{"localField": "ref", "as": "d"}`,
			`{"from": "c", "localField": "a.b", "foreignField": "id", "as": "out"}`,
		} {
			s := MustParsePipeline(`[{"$lookup": ` + j + `}]`)[0].String()
			obj, ok := strings.CutPrefix(s, "$lookup ")
			require.True(t, ok, s)
			again := MustParsePipeline(`[{"$lookup": ` + obj + `}]`)[0].String()
			assert.Equal(t, s, again)
		}
	})
	t.Run("foreignField not id", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$lookup": {"localField": "a", "foreignField": "ref", "as": "d"}}]`)
		assert.ErrorContains(t, err, "only primary-key self-joins")
		var pe *query.ParseError
		require.ErrorAs(t, err, &pe)
		assert.Equal(t, "0.$lookup.foreignField", pe.Path)
	})
	t.Run("missing as", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$lookup": {"localField": "a"}}]`)
		assert.ErrorContains(t, err, "requires as")
	})
	t.Run("missing localField", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$lookup": {"as": "d"}}]`)
		assert.ErrorContains(t, err, "requires localField")
	})
	t.Run("pipeline form rejected", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$lookup": {"from": "c", "let": {"x": "$a"}, "pipeline": [], "as": "d"}}]`)
		assert.ErrorContains(t, err, "pipeline-form $lookup is not supported")
	})
	t.Run("unknown option", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$lookup": {"localField": "a", "as": "d", "bogus": 1}}]`)
		assert.ErrorContains(t, err, "unknown $lookup option")
	})
	t.Run("invalid as", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$lookup": {"localField": "a", "as": "$d"}}]`)
		assert.ErrorContains(t, err, "must not start with $")
		_, err = ParsePipeline(`[{"$lookup": {"localField": "a", "as": "d.e"}}]`)
		assert.ErrorContains(t, err, "dotted")
	})
	t.Run("not an object", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$lookup": "x"}]`)
		assert.ErrorContains(t, err, "must be an object")
	})
}

func TestParseFacet(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		p := MustParsePipeline(`[{"$facet": {
			"count": [{"$count": "n"}],
			"top": [{"$match": {"v": {"$gte": 5}}}, {"$sort": {"v": -1}}, {"$limit": 2}]
		}}]`)
		s := p[0].(FacetSpec)
		assert.Equal(t, []string{"count", "top"}, s.Names)
		require.Len(t, s.Pipelines, 2)
		assert.Len(t, s.Pipelines[0], 1)
		assert.Len(t, s.Pipelines[1], 3)
	})
	t.Run("string", func(t *testing.T) {
		p := MustParsePipeline(`[{"$facet": {"a": [{"$count": "n"}], "b": [{"$skip": 1}, {"$limit": 2}]}}]`)
		assert.Equal(t, `$facet {"a":[$count "n"],"b":[$skip 1, $limit 2]}`, p[0].String())
	})
	t.Run("empty facet object", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$facet": {}}]`)
		assert.ErrorContains(t, err, "requires at least one facet")
	})
	t.Run("not an object", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$facet": []}]`)
		assert.ErrorContains(t, err, "$facet must be an object")
	})
	t.Run("empty sub-pipeline", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$facet": {"a": []}}]`)
		assert.ErrorContains(t, err, "facet pipeline must not be empty")
	})
	t.Run("sub-pipeline not an array", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$facet": {"a": {"$count": "n"}}}]`)
		assert.ErrorContains(t, err, "facet must be a pipeline array")
	})
	t.Run("nested facet rejected", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$facet": {"a": [{"$facet": {"b": [{"$count": "n"}]}}]}}]`)
		assert.ErrorContains(t, err, "$facet cannot be nested")
		var pe *query.ParseError
		require.ErrorAs(t, err, &pe)
		assert.Equal(t, "0.$facet.a.0", pe.Path)
	})
	t.Run("duplicate facet name", func(t *testing.T) {
		// JSON input cannot express duplicate keys (the JSON parser keeps the
		// last one); splice a second copy at the anyenc encoding level, like
		// TestDuplicateStructuredParams.
		base := anyenc.MustParseJson(`{"a":[{"$count":"n"}]}`).MarshalTo(nil)
		kv := anyenc.MustParseJson(`{"a":[{"$skip":1}]}`).MarshalTo(nil)
		v, perr := (&anyenc.Parser{}).Parse(append(base[:len(base)-1], kv[1:]...))
		require.NoError(t, perr)
		_, err := parseFacet(v)
		assert.ErrorContains(t, err, "duplicate facet name: a")
		var pe *query.ParseError
		require.ErrorAs(t, err, &pe)
		assert.Equal(t, "a", pe.Path)
	})
	t.Run("bad facet name", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$facet": {"$a": [{"$count": "n"}]}}]`)
		assert.ErrorContains(t, err, "must not start with $")
		_, err = ParsePipeline(`[{"$facet": {"a.b": [{"$count": "n"}]}}]`)
		assert.ErrorContains(t, err, "dotted")
	})
	t.Run("sub-stage error path", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$facet": {"b": [{"$skip": 0}, {"$limit": 0}]}}]`)
		assert.ErrorContains(t, err, "$limit must be a positive integer")
		var pe *query.ParseError
		require.ErrorAs(t, err, &pe)
		assert.Equal(t, "0.$facet.b.1.$limit", pe.Path)
		assert.Equal(t, "pipeline", pe.Source)
	})
}

func TestParseExpr(t *testing.T) {
	a := &anyenc.Arena{}
	doc := anyenc.MustParseJson(`{"a": {"b": 7}, "s": "str", "arr": [1,2]}`)

	eval := func(t *testing.T, exprJson string) *anyenc.Value {
		e, err := ParseExpr(anyenc.MustParseJson(exprJson))
		require.NoError(t, err)
		v, err := e.Eval(a, doc)
		require.NoError(t, err)
		return v
	}

	t.Run("field ref", func(t *testing.T) {
		assert.Equal(t, float64(7), eval(t, `"$a.b"`).GetFloat64())
	})
	t.Run("missing field ref", func(t *testing.T) {
		assert.Nil(t, eval(t, `"$nope.x"`))
	})
	t.Run("plain string literal", func(t *testing.T) {
		assert.Equal(t, "hello", string(eval(t, `"hello"`).GetStringBytes()))
	})
	t.Run("$literal escape", func(t *testing.T) {
		assert.Equal(t, "$a.b", string(eval(t, `{"$literal": "$a.b"}`).GetStringBytes()))
	})
	t.Run("document expression", func(t *testing.T) {
		v := eval(t, `{"x": "$a.b", "y": "$nope"}`)
		assert.Equal(t, float64(7), v.GetFloat64("x"))
		assert.Nil(t, v.Get("y")) // missing omitted
	})
	t.Run("array expression", func(t *testing.T) {
		v := eval(t, `["$a.b", "$nope", 3]`)
		arr := v.GetArray()
		require.Len(t, arr, 3)
		assert.Equal(t, float64(7), arr[0].GetFloat64())
		assert.Equal(t, anyenc.TypeNull, arr[1].Type()) // missing → null
		assert.Equal(t, float64(3), arr[2].GetFloat64())
	})
	t.Run("unsupported operator", func(t *testing.T) {
		_, err := ParseExpr(anyenc.MustParseJson(`{"$toUpper": "$a"}`))
		assert.ErrorContains(t, err, "unsupported expression operator")
	})
	t.Run("mixed operator and fields", func(t *testing.T) {
		_, err := ParseExpr(anyenc.MustParseJson(`{"$literal": 1, "x": 2}`))
		assert.ErrorContains(t, err, "cannot be mixed")
	})
	t.Run("variables rejected", func(t *testing.T) {
		_, err := ParseExpr(anyenc.MustParseJson(`"$$ROOT"`))
		assert.ErrorContains(t, err, "variables are not supported")
	})
	t.Run("empty field ref", func(t *testing.T) {
		_, err := ParseExpr(anyenc.MustParseJson(`"$"`))
		assert.ErrorContains(t, err, "empty field reference")
	})
}

func TestParseExprOperators(t *testing.T) {
	t.Run("operator kinds", func(t *testing.T) {
		for _, tc := range []struct {
			json string
			want Expr
		}{
			{`{"$add": ["$a", 1]}`, &ArithExpr{}},
			{`{"$subtract": ["$a", "$b"]}`, &ArithExpr{}},
			{`{"$multiply": [2, 3]}`, &ArithExpr{}},
			{`{"$divide": ["$a", 2]}`, &ArithExpr{}},
			{`{"$abs": ["$a"]}`, &AbsExpr{}},
			{`{"$round": ["$a", 2]}`, &RoundExpr{}},
			{`{"$concat": ["$a", "-"]}`, &ConcatExpr{}},
			{`{"$replaceOne": {"input": "$a", "find": "x", "replacement": "y"}}`, &ReplaceOneExpr{}},
			{`{"$cond": ["$a", 1, 2]}`, &CondExpr{}},
			{`{"$cond": {"if": "$a", "then": 1, "else": 2}}`, &CondExpr{}},
			{`{"$switch": {"branches": [{"case": "$a", "then": 1}]}}`, &SwitchExpr{}},
			{`{"$ifNull": ["$a", 0]}`, &IfNullExpr{}},
			{`{"$eq": ["$a", 1]}`, &CompareExpr{}},
			{`{"$cmp": ["$a", "$b"]}`, &CompareExpr{}},
			{`{"$dateAdd": {"startDate": "$a", "unit": "day", "amount": 1}}`, &DateAddExpr{}},
			{`{"$dateDiff": {"startDate": "$a", "endDate": "$b", "unit": "month"}}`, &DateDiffExpr{}},
			{`{"$dateTrunc": {"date": "$a", "unit": "week"}}`, &DateTruncExpr{}},
			{`{"$year": "$a"}`, &DatePartExpr{}},
			{`{"$week": {"date": "$a", "timezone": "+02:00"}}`, &DatePartExpr{}},
		} {
			e, err := ParseExpr(anyenc.MustParseJson(tc.json))
			require.NoError(t, err, tc.json)
			assert.IsType(t, tc.want, e, tc.json)
		}
	})
	t.Run("single non-array operand shorthand", func(t *testing.T) {
		for _, j := range []string{`{"$abs": "$a"}`, `{"$round": "$a"}`, `{"$add": "$a"}`, `{"$concat": "$a"}`} {
			_, err := ParseExpr(anyenc.MustParseJson(j))
			assert.NoError(t, err, j)
		}
	})
	t.Run("strings", func(t *testing.T) {
		for _, tc := range []struct{ json, want string }{
			{`{"$add": ["$a", {"$multiply": ["$b", 2]}, 1]}`, `{$add:[$a,{$multiply:[$b,2]},1]}`},
			{`{"$subtract": ["$a", "$b"]}`, `{$subtract:[$a,$b]}`},
			{`{"$divide": ["$a", 2]}`, `{$divide:[$a,2]}`},
			{`{"$abs": "$a"}`, `{$abs:[$a]}`},
			{`{"$round": "$a"}`, `{$round:[$a]}`},
			{`{"$round": ["$a", -1]}`, `{$round:[$a,-1]}`},
			{`{"$concat": ["$a", "-", "$b"]}`, `{$concat:[$a,"-",$b]}`},
			{`{"$replaceOne": {"replacement": "", "find": "any://", "input": "$rel"}}`,
				`{$replaceOne:{input:$rel,find:"any://",replacement:""}}`},
			// Both $cond spellings render the canonical array form.
			{`{"$cond": [{"$lt": ["$a", 1]}, "$a", "$b"]}`, `{$cond:[{$lt:[$a,1]},$a,$b]}`},
			{`{"$cond": {"if": "$a", "then": 1, "else": 2}}`, `{$cond:[$a,1,2]}`},
			{`{"$switch": {"branches": [{"case": "$a", "then": 1}], "default": 0}}`,
				`{$switch:{branches:[{case:$a,then:1}],default:0}}`},
			{`{"$switch": {"branches": [{"case": true, "then": 1}, {"case": false, "then": 2}]}}`,
				`{$switch:{branches:[{case:true,then:1},{case:false,then:2}]}}`},
			{`{"$ifNull": ["$a", "$b", 0]}`, `{$ifNull:[$a,$b,0]}`},
			{`{"$ne": ["$a", null]}`, `{$ne:[$a,null]}`},
			{`{"$cmp": ["$a", "$b"]}`, `{$cmp:[$a,$b]}`},
			{`{"$dateAdd": {"startDate": "$d", "unit": "day", "amount": 5}}`,
				`{$dateAdd:{startDate:$d,unit:"day",amount:5}}`},
			// Spec order does not matter: String renders the canonical order.
			{`{"$dateAdd": {"timezone": "Europe/Berlin", "amount": 1, "unit": "month", "startDate": "$d"}}`,
				`{$dateAdd:{startDate:$d,unit:"month",amount:1,timezone:"Europe/Berlin"}}`},
			{`{"$dateDiff": {"startDate": "$a", "endDate": "$b", "unit": "week", "startOfWeek": "Monday"}}`,
				`{$dateDiff:{startDate:$a,endDate:$b,unit:"week",startOfWeek:"Monday"}}`},
			{`{"$dateTrunc": {"date": "$d", "unit": "day", "binSize": 14, "timezone": "+02:00"}}`,
				`{$dateTrunc:{date:$d,unit:"day",binSize:14,timezone:"+02:00"}}`},
			{`{"$dateTrunc": {"date": "$d", "unit": "hour"}}`, `{$dateTrunc:{date:$d,unit:"hour"}}`},
			{`{"$year": "$d"}`, `{$year:[$d]}`},
			{`{"$week": {"date": "$d", "timezone": "-05:00"}}`, `{$week:{date:$d,timezone:"-05:00"}}`},
			{`{"$week": {"date": "$d"}}`, `{$week:[$d]}`},
		} {
			e, err := ParseExpr(anyenc.MustParseJson(tc.json))
			require.NoError(t, err, tc.json)
			assert.Equal(t, tc.want, e.String(), tc.json)
		}
	})
}

// TestDuplicateStructuredParams pins duplicate-key rejection in $cond object
// form and $switch branches. JSON input cannot express duplicates (the JSON
// parser keeps the last key), but raw anyenc objects can — splice a second
// copy of a key at the encoding level (object = tag, kvs, EOS terminator).
func TestDuplicateStructuredParams(t *testing.T) {
	splice := func(t *testing.T, base, extra string) *anyenc.Value {
		t.Helper()
		m := anyenc.MustParseJson(base).MarshalTo(nil)
		kv := anyenc.MustParseJson(extra).MarshalTo(nil)
		v, err := (&anyenc.Parser{}).Parse(append(m[:len(m)-1], kv[1:]...))
		require.NoError(t, err)
		return v
	}
	t.Run("$cond object form", func(t *testing.T) {
		_, err := parseCond(splice(t, `{"if":true,"then":1,"else":2}`, `{"if":false}`))
		assert.ErrorContains(t, err, "duplicate $cond parameter: if")
	})
	t.Run("$switch branch", func(t *testing.T) {
		_, _, err := parseSwitchBranch(splice(t, `{"case":true,"then":1}`, `{"then":2}`))
		assert.ErrorContains(t, err, "duplicate $switch branch parameter: then")
	})
	t.Run("date operator object", func(t *testing.T) {
		_, err := parseDateAdd(splice(t, `{"startDate":"$d","unit":"day","amount":1}`, `{"unit":"hour"}`))
		assert.ErrorContains(t, err, "duplicate $dateAdd parameter: unit")
	})
	t.Run("$replaceOne", func(t *testing.T) {
		_, err := parseReplaceOne(splice(t, `{"input":"a","find":"b","replacement":"c"}`, `{"find":"d"}`))
		assert.ErrorContains(t, err, "duplicate $replaceOne parameter: find")
	})
}

// TestParseReplaceOne covers the object-form contract: all three parameters
// required, unknown keys rejected, the array spelling is not accepted.
func TestParseReplaceOne(t *testing.T) {
	bad := func(t *testing.T, exprJson, contains string) {
		t.Helper()
		_, err := ParseExpr(anyenc.MustParseJson(exprJson))
		assert.ErrorContains(t, err, contains, exprJson)
	}

	t.Run("missing parameters", func(t *testing.T) {
		bad(t, `{"$replaceOne": {"find": "a", "replacement": "b"}}`, "$replaceOne requires 'input', 'find' and 'replacement'")
		bad(t, `{"$replaceOne": {"input": "$a", "replacement": "b"}}`, "$replaceOne requires")
		bad(t, `{"$replaceOne": {"input": "$a", "find": "a"}}`, "$replaceOne requires")
		bad(t, `{"$replaceOne": {}}`, "$replaceOne requires")
	})
	t.Run("unknown parameter", func(t *testing.T) {
		bad(t, `{"$replaceOne": {"input": "$a", "find": "a", "replacement": "b", "bogus": 1}}`, "unknown $replaceOne parameter: bogus")
	})
	t.Run("array form rejected", func(t *testing.T) {
		bad(t, `{"$replaceOne": ["$a", "x", "y"]}`, "requires an object")
	})
	t.Run("nested parse error located", func(t *testing.T) {
		_, err := ParseExpr(anyenc.MustParseJson(`{"$replaceOne": {"input": "$a", "find": {"$bogus": 1}, "replacement": ""}}`))
		require.Error(t, err)
		var pe *query.ParseError
		require.ErrorAs(t, err, &pe)
		assert.Equal(t, "$replaceOne.find.$bogus", pe.Path)
	})
}

// TestParseDateOps covers the parse-time-literal contract of the date
// operators beyond the structured-error table: unit/timezone/startOfWeek and
// binSize must be literals of the right shape.
func TestParseDateOps(t *testing.T) {
	bad := func(t *testing.T, exprJson, contains string) {
		t.Helper()
		_, err := ParseExpr(anyenc.MustParseJson(exprJson))
		assert.ErrorContains(t, err, contains, exprJson)
	}
	good := func(t *testing.T, exprJson string) {
		t.Helper()
		_, err := ParseExpr(anyenc.MustParseJson(exprJson))
		assert.NoError(t, err, exprJson)
	}

	t.Run("binSize must be a positive integer", func(t *testing.T) {
		bad(t, `{"$dateTrunc": {"date": "$d", "unit": "day", "binSize": 1.5}}`, "positive integer")
		bad(t, `{"$dateTrunc": {"date": "$d", "unit": "day", "binSize": -2}}`, "positive integer")
		bad(t, `{"$dateTrunc": {"date": "$d", "unit": "day", "binSize": "2"}}`, "positive integer")
	})
	t.Run("unit must be a literal string", func(t *testing.T) {
		bad(t, `{"$dateAdd": {"startDate": "$d", "unit": 3, "amount": 1}}`, "literal string")
		bad(t, `{"$dateAdd": {"startDate": "$d", "unit": {"$concat": ["da", "y"]}, "amount": 1}}`, "literal string")
		bad(t, `{"$dateAdd": {"startDate": "$d", "unit": "$u", "amount": 1}}`, "literal string")
	})
	t.Run("timezone forms", func(t *testing.T) {
		good(t, `{"$dateAdd": {"startDate": "$d", "unit": "day", "amount": 1, "timezone": "+02"}}`)
		good(t, `{"$dateAdd": {"startDate": "$d", "unit": "day", "amount": 1, "timezone": "-0500"}}`)
		good(t, `{"$dateAdd": {"startDate": "$d", "unit": "day", "amount": 1, "timezone": "+14:00"}}`)
		good(t, `{"$dateAdd": {"startDate": "$d", "unit": "day", "amount": 1, "timezone": "UTC"}}`)
		bad(t, `{"$dateAdd": {"startDate": "$d", "unit": "day", "amount": 1, "timezone": "+2:00"}}`, "unknown timezone")
		bad(t, `{"$dateAdd": {"startDate": "$d", "unit": "day", "amount": 1, "timezone": "+25:00"}}`, "unknown timezone")
		bad(t, `{"$dateAdd": {"startDate": "$d", "unit": "day", "amount": 1, "timezone": ""}}`, "unknown timezone")
	})
	t.Run("startOfWeek is case-insensitive", func(t *testing.T) {
		good(t, `{"$dateDiff": {"startDate": "$a", "endDate": "$b", "unit": "week", "startOfWeek": "SUNDAY"}}`)
		good(t, `{"$dateTrunc": {"date": "$d", "unit": "week", "startOfWeek": "Friday"}}`)
	})
	t.Run("missing required parameters", func(t *testing.T) {
		bad(t, `{"$dateDiff": {"startDate": "$a", "unit": "day"}}`, "$dateDiff requires 'startDate', 'endDate' and 'unit'")
		bad(t, `{"$dateTrunc": {"date": "$d"}}`, "$dateTrunc requires 'date' and 'unit'")
	})
	t.Run("$year/$week object form validates keys", func(t *testing.T) {
		bad(t, `{"$year": {"Date": "$d"}}`, "unknown $year parameter: Date")
		bad(t, `{"$year": {"timezone": "+01:00"}}`, "$year requires 'date'")
		bad(t, `{"$week": {}}`, "$week requires 'date'")
		bad(t, `{"$week": {"date": "$d", "startOfWeek": "monday"}}`, "unknown $week parameter: startOfWeek")
		good(t, `{"$week": {"$literal": 1}}`) // $-keyed objects stay expressions
	})
}

func TestLiteralDetachment(t *testing.T) {
	// Literals must not alias the input value's memory.
	p := &anyenc.Parser{}
	src, err := p.Parse(anyenc.MustParseJson(`{"x": "the-literal-value"}`).MarshalTo(nil))
	require.NoError(t, err)

	e, err := ParseExpr(src.Get("x"))
	require.NoError(t, err)

	// Reuse the parser, invalidating src.
	_, err = p.Parse(anyenc.MustParseJson(`{"y": "overwritten-0123456789"}`).MarshalTo(nil))
	require.NoError(t, err)

	a := &anyenc.Arena{}
	v, err := e.Eval(a, anyenc.MustParseJson(`{}`))
	require.NoError(t, err)
	assert.Equal(t, "the-literal-value", string(v.GetStringBytes()))
}

func TestSpecStrings(t *testing.T) {
	p := MustParsePipeline(`[
		{"$match": {"a": 1}},
		{"$sort": {"a": 1, "b": -1}},
		{"$skip": 2},
		{"$limit": 3},
		{"$unwind": "$t"},
		{"$group": {"_id": "$t", "n": {"$count": {}}}},
		{"$project": {"n": 1}},
		{"$count": "total"}
	]`)
	for _, spec := range p {
		assert.NotEmpty(t, spec.String())
	}
	assert.Equal(t, `$sort {"a":1,"b":-1}`, p[1].String())
	assert.Equal(t, `$unwind "$t"`, p[4].String())
}

// TestPipelineParseError pins the structured rejection contract for the
// pipeline grammar: every rejection is a *query.ParseError with Source
// "pipeline" whose Path's leading segment is the stage index — including
// filter faults inside $match ("1.$match.a.$gt").
func TestPipelineParseError(t *testing.T) {
	for _, tc := range []struct {
		name       string
		json       string
		wantPath   string
		wantOp     string
		wantReason string // substring of Reason
		wantIs     error  // finer class sentinel, nil if none
	}{
		{
			name:       "pipeline not an array",
			json:       `{"$match":{}}`,
			wantPath:   "",
			wantReason: "pipeline must be an array of stages",
		},
		{
			name:       "stage not an object",
			json:       `[1]`,
			wantPath:   "0",
			wantReason: "stage must be an object",
		},
		{
			name:       "stage with two keys",
			json:       `[{"$match":{},"$limit":1}]`,
			wantPath:   "0",
			wantReason: "exactly one key",
		},
		{
			name:     "unknown stage",
			json:     `[{"$bucket":{}}]`,
			wantPath: "0.$bucket", wantOp: "$bucket",
			wantReason: "unknown stage: $bucket", wantIs: query.ErrUnknownOperator,
		},
		{
			name:     "$lookup foreignField not the pk",
			json:     `[{"$lookup":{"localField":"a","foreignField":"ref","as":"d"}}]`,
			wantPath: "0.$lookup.foreignField", wantOp: "$lookup",
			wantReason: "only primary-key self-joins are supported",
		},
		{
			name:     "bad filter in a later $match names the stage index",
			json:     `[{"$limit":1},{"$match":{"a":{"$foo":1}}}]`,
			wantPath: "1.$match.a.$foo", wantOp: "$foo",
			wantReason: "unknown operator", wantIs: query.ErrUnknownOperator,
		},
		{
			name:     "$sort bad direction",
			json:     `[{"$sort":{"a":2}}]`,
			wantPath: "0.$sort.a", wantOp: "$sort",
			wantReason: "must be 1 or -1",
		},
		{
			name:     "$skip negative",
			json:     `[{"$limit":1},{"$skip":-1}]`,
			wantPath: "1.$skip", wantOp: "$skip",
			wantReason: "$skip must be a non-negative integer",
		},
		{
			name:     "$count operator-like field name",
			json:     `[{"$count":"$x"}]`,
			wantPath: "0.$count", wantOp: "$count",
			wantReason: "must not start with $",
		},
		{
			name:     "$project exclusion",
			json:     `[{"$project":{"a":0}}]`,
			wantPath: "0.$project.a", wantOp: "$project",
			wantReason: "exclusion",
		},
		{
			name:     "unsupported expression operator",
			json:     `[{"$project":{"a":{"$toUpper":"$x"}}}]`,
			wantPath: "0.$project.a.$toUpper", wantOp: "$toUpper",
			wantReason: "unsupported expression operator: $toUpper", wantIs: query.ErrUnknownOperator,
		},
		{
			name:     "$subtract wrong arity",
			json:     `[{"$project":{"a":{"$subtract":[1]}}}]`,
			wantPath: "0.$project.a.$subtract", wantOp: "$subtract",
			wantReason: "$subtract requires exactly 2 operands, got 1",
		},
		{
			name:     "$round too many operands",
			json:     `[{"$project":{"a":{"$round":[1,2,3]}}}]`,
			wantPath: "0.$project.a.$round", wantOp: "$round",
			wantReason: "$round requires 1 to 2 operands, got 3",
		},
		{
			name:     "$abs single-operand shorthand rejects two operands",
			json:     `[{"$project":{"a":{"$abs":[1,2]}}}]`,
			wantPath: "0.$project.a.$abs", wantOp: "$abs",
			wantReason: "$abs requires exactly 1 operand, got 2",
		},
		{
			name:     "$cond array wrong arity",
			json:     `[{"$project":{"a":{"$cond":[true,1]}}}]`,
			wantPath: "0.$project.a.$cond", wantOp: "$cond",
			wantReason: "$cond requires exactly 3 operands, got 2",
		},
		{
			name:     "$cond object form missing else",
			json:     `[{"$project":{"a":{"$cond":{"if":true,"then":1}}}}]`,
			wantPath: "0.$project.a.$cond", wantOp: "$cond",
			wantReason: "$cond object form requires 'if', 'then' and 'else'",
		},
		{
			name:     "$cond object form unknown parameter",
			json:     `[{"$project":{"a":{"$cond":{"if":true,"then":1,"elze":2}}}}]`,
			wantPath: "0.$project.a.$cond.elze", wantOp: "$cond",
			wantReason: "unknown $cond parameter: elze",
		},
		{
			name:     "$switch non-object operand",
			json:     `[{"$project":{"a":{"$switch":[1]}}}]`,
			wantPath: "0.$project.a.$switch", wantOp: "$switch",
			wantReason: "$switch requires an object with a 'branches' array",
		},
		{
			name:     "$switch empty branches",
			json:     `[{"$project":{"a":{"$switch":{"branches":[]}}}}]`,
			wantPath: "0.$project.a.$switch", wantOp: "$switch",
			wantReason: "$switch requires at least one branch",
		},
		{
			name:     "$switch branch missing then",
			json:     `[{"$project":{"a":{"$switch":{"branches":[{"case":true,"then":1},{"case":false}]}}}}]`,
			wantPath: "0.$project.a.$switch.branches.1", wantOp: "$switch",
			wantReason: "$switch branch requires 'case' and 'then'",
		},
		{
			name:     "$switch branch unknown parameter",
			json:     `[{"$project":{"a":{"$switch":{"branches":[{"case":true,"themn":1}]}}}}]`,
			wantPath: "0.$project.a.$switch.branches.0.themn", wantOp: "$switch",
			wantReason: "unknown $switch branch parameter: themn",
		},
		{
			name:     "$ifNull one operand",
			json:     `[{"$project":{"a":{"$ifNull":["$x"]}}}]`,
			wantPath: "0.$project.a.$ifNull", wantOp: "$ifNull",
			wantReason: "$ifNull requires at least 2 operands, got 1",
		},
		{
			name:     "$eq wrong arity",
			json:     `[{"$project":{"a":{"$eq":[1,2,3]}}}]`,
			wantPath: "0.$project.a.$eq", wantOp: "$eq",
			wantReason: "$eq requires exactly 2 operands, got 3",
		},
		{
			name:     "$dateAdd unknown unit",
			json:     `[{"$project":{"a":{"$dateAdd":{"startDate":"$d","unit":"fortnight","amount":1}}}}]`,
			wantPath: "0.$project.a.$dateAdd.unit", wantOp: "$dateAdd",
			wantReason: "unknown $dateAdd unit: fortnight",
		},
		{
			name:     "$dateAdd unknown timezone",
			json:     `[{"$project":{"a":{"$dateAdd":{"startDate":"$d","unit":"day","amount":1,"timezone":"Mars/Olympus"}}}}]`,
			wantPath: "0.$project.a.$dateAdd.timezone", wantOp: "$dateAdd",
			wantReason: "unknown timezone: Mars/Olympus",
		},
		{
			name:     "$dateAdd non-literal timezone",
			json:     `[{"$project":{"a":{"$dateAdd":{"startDate":"$d","unit":"day","amount":1,"timezone":"$tz"}}}}]`,
			wantPath: "0.$project.a.$dateAdd.timezone", wantOp: "$dateAdd",
			wantReason: "$dateAdd 'timezone' must be a literal string",
		},
		{
			name:     "$dateAdd missing amount",
			json:     `[{"$project":{"a":{"$dateAdd":{"startDate":"$d","unit":"day"}}}}]`,
			wantPath: "0.$project.a.$dateAdd", wantOp: "$dateAdd",
			wantReason: "$dateAdd requires 'startDate', 'unit' and 'amount'",
		},
		{
			name:     "$dateAdd unknown parameter",
			json:     `[{"$project":{"a":{"$dateAdd":{"startDate":"$d","unit":"day","amount":1,"bogus":1}}}}]`,
			wantPath: "0.$project.a.$dateAdd.bogus", wantOp: "$dateAdd",
			wantReason: "unknown $dateAdd parameter: bogus",
		},
		{
			name:     "$dateDiff non-object operand",
			json:     `[{"$project":{"a":{"$dateDiff":["$a","$b"]}}}]`,
			wantPath: "0.$project.a.$dateDiff", wantOp: "$dateDiff",
			wantReason: "$dateDiff requires an object",
		},
		{
			name:     "$dateDiff bad startOfWeek",
			json:     `[{"$project":{"a":{"$dateDiff":{"startDate":"$a","endDate":"$b","unit":"week","startOfWeek":"caturday"}}}}]`,
			wantPath: "0.$project.a.$dateDiff.startOfWeek", wantOp: "$dateDiff",
			wantReason: "unknown $dateDiff startOfWeek: caturday",
		},
		{
			name:     "$dateTrunc binSize zero",
			json:     `[{"$project":{"a":{"$dateTrunc":{"date":"$d","unit":"day","binSize":0}}}}]`,
			wantPath: "0.$project.a.$dateTrunc.binSize", wantOp: "$dateTrunc",
			wantReason: "$dateTrunc 'binSize' must be a positive integer",
		},
		{
			name:     "$year object form unknown parameter",
			json:     `[{"$project":{"a":{"$year":{"date":"$d","startOfWeek":"monday"}}}}]`,
			wantPath: "0.$project.a.$year.startOfWeek", wantOp: "$year",
			wantReason: "unknown $year parameter: startOfWeek",
		},
		{
			name:       "$dateAdd startDate expression error",
			json:       `[{"$project":{"a":{"$dateAdd":{"startDate":"$$d","unit":"day","amount":1}}}}]`,
			wantPath:   "0.$project.a.$dateAdd.startDate",
			wantReason: "variables are not supported",
		},
		{
			name:       "operator operand error names the element index",
			json:       `[{"$project":{"a":{"$add":["$x","$$y"]}}}]`,
			wantPath:   "0.$project.a.$add.1",
			wantReason: "variables are not supported",
		},
		{
			name:       "shorthand operand error has no index segment",
			json:       `[{"$project":{"a":{"$abs":"$$x"}}}]`,
			wantPath:   "0.$project.a.$abs",
			wantReason: "variables are not supported",
		},
		{
			name:       "variable reference in expression",
			json:       `[{"$project":{"a":"$$now"}}]`,
			wantPath:   "0.$project.a",
			wantReason: "variables are not supported",
		},
		{
			name:       "expression error inside an array names the element index",
			json:       `[{"$project":{"a":["$x","$$y"]}}]`,
			wantPath:   "0.$project.a.1",
			wantReason: "variables are not supported",
		},
		{
			// An option miss inside a known stage — like an unknown $text
			// field in the filter grammar, deliberately not ErrUnknownOperator.
			name:     "unknown $unwind option",
			json:     `[{"$unwind":{"path":"$a","x":1}}]`,
			wantPath: "0.$unwind.x", wantOp: "$unwind",
			wantReason: "unknown $unwind option: x",
		},
		{
			name:     "unknown accumulator",
			json:     `[{"$group":{"id":"$a","total":{"$summ":"$n"}}}]`,
			wantPath: "0.$group.total.$summ", wantOp: "$summ",
			wantReason: "unknown accumulator: $summ", wantIs: query.ErrUnknownOperator,
		},
		{
			name:     "$count accumulator with a non-empty argument",
			json:     `[{"$group":{"id":"$a","n":{"$count":1}}}]`,
			wantPath: "0.$group.n.$count", wantOp: "$count",
			wantReason: "$count takes an empty object {}",
		},
		{
			name:       "$group key expression error",
			json:       `[{"$group":{"id":"$$v"}}]`,
			wantPath:   "0.$group.id",
			wantReason: "variables are not supported",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := ParsePipeline(tc.json)
			require.Error(t, err)
			assert.Nil(t, p)

			var pe *query.ParseError
			require.True(t, errors.As(err, &pe), "not a *query.ParseError: %v", err)
			assert.Equal(t, "pipeline", pe.Source)
			assert.Equal(t, tc.wantPath, pe.Path)
			assert.Equal(t, tc.wantOp, pe.Op)
			assert.Contains(t, pe.Reason, tc.wantReason)
			assert.Contains(t, err.Error(), "parse pipeline: ")
			if pe.Path != "" {
				assert.Contains(t, err.Error(), "(at "+pe.Path+")")
			}

			if tc.wantIs != nil {
				assert.True(t, errors.Is(err, tc.wantIs), "want errors.Is(%v)", tc.wantIs)
			} else {
				assert.False(t, errors.Is(err, query.ErrUnknownOperator))
			}
		})
	}
}

// TestStages and TestAccumulators pin the exported vocabularies — same
// snapshot contract as query's TestOperators: adding or removing an entry
// must update them, which is the moment to update advertising consumers too.
func TestStages(t *testing.T) {
	stages := Stages()
	assert.Equal(t, []string{
		"$addFields", "$count", "$facet", "$group", "$limit", "$lookup",
		"$match", "$merge", "$out", "$project", "$set", "$skip", "$sort",
		"$unwind",
	}, stages)

	// The slice is a fresh copy: mutating it must not poison later calls.
	stages[0] = "$corrupted"
	assert.Equal(t, "$addFields", Stages()[0])
}

func TestAccumulators(t *testing.T) {
	accums := Accumulators()
	assert.Equal(t, []string{
		"$addToSet", "$avg", "$count", "$first", "$last", "$max", "$min",
		"$push", "$sum",
	}, accums)

	accums[0] = "$corrupted"
	assert.Equal(t, "$addToSet", Accumulators()[0])
}

func TestParseOut(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		p := MustParsePipeline(`[{"$out": "target"}]`)
		require.Len(t, p, 1)
		assert.Equal(t, OutSpec{Coll: "target"}, p[0])
		assert.Equal(t, `$out "target"`, p[0].String())
	})
	t.Run("db-qualified form rejected", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$out": {"db": "d", "coll": "c"}}]`)
		assert.ErrorContains(t, err, "db-qualified form is not supported")
	})
	t.Run("empty name", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$out": ""}]`)
		assert.ErrorContains(t, err, "must not be empty")
	})
	t.Run("must be last", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$out": "t"}, {"$limit": 1}]`)
		assert.ErrorContains(t, err, "$out must be the last pipeline stage")
		assert.ErrorContains(t, err, "(at 0.$out)")
	})
	t.Run("rejected inside facet", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$facet": {"f": [{"$out": "t"}]}}]`)
		assert.ErrorContains(t, err, "$out is not allowed inside $facet")
	})
}

func TestParseMerge(t *testing.T) {
	t.Run("string shorthand", func(t *testing.T) {
		p := MustParsePipeline(`[{"$merge": "target"}]`)
		require.Len(t, p, 1)
		assert.Equal(t, MergeSpec{Into: "target"}, p[0])
		// Zero values are the defaults.
		assert.Equal(t,
			`$merge {"into":"target","on":"id","whenMatched":"merge","whenNotMatched":"insert"}`,
			p[0].String())
	})
	t.Run("full object", func(t *testing.T) {
		p := MustParsePipeline(`[{"$merge": {
			"into": "t", "on": "id",
			"whenMatched": "keepExisting", "whenNotMatched": "discard"
		}}]`)
		assert.Equal(t, MergeSpec{
			Into:           "t",
			WhenMatched:    MergeMatchedKeepExisting,
			WhenNotMatched: MergeNotMatchedDiscard,
		}, p[0])
	})
	t.Run("all enum values", func(t *testing.T) {
		for name, want := range map[string]MergeWhenMatched{
			"replace": MergeMatchedReplace, "keepExisting": MergeMatchedKeepExisting,
			"merge": MergeMatchedMerge, "fail": MergeMatchedFail,
		} {
			p := MustParsePipeline(`[{"$merge": {"into": "t", "whenMatched": "` + name + `"}}]`)
			assert.Equal(t, want, p[0].(MergeSpec).WhenMatched, name)
		}
		for name, want := range map[string]MergeWhenNotMatched{
			"insert": MergeNotMatchedInsert, "discard": MergeNotMatchedDiscard,
			"fail": MergeNotMatchedFail,
		} {
			p := MustParsePipeline(`[{"$merge": {"into": "t", "whenNotMatched": "` + name + `"}}]`)
			assert.Equal(t, want, p[0].(MergeSpec).WhenNotMatched, name)
		}
	})
	t.Run("string round-trip", func(t *testing.T) {
		src := `[{"$merge": {"into": "t", "whenMatched": "replace", "whenNotMatched": "fail"}}]`
		p := MustParsePipeline(src)
		p2 := MustParsePipeline(`[{"$merge": ` + strings.TrimPrefix(p[0].String(), "$merge ") + `}]`)
		assert.Equal(t, p[0], p2[0])
	})
	t.Run("into required", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$merge": {"whenMatched": "merge"}}]`)
		assert.ErrorContains(t, err, "$merge requires into")
	})
	t.Run("db-qualified into rejected", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$merge": {"into": {"db": "d", "coll": "c"}}}]`)
		assert.ErrorContains(t, err, "db-qualified form is not supported")
	})
	t.Run("on must be id", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$merge": {"into": "t", "on": "sku"}}]`)
		assert.ErrorContains(t, err, `on must be "id"`)
		_, err = ParsePipeline(`[{"$merge": {"into": "t", "on": ["id"]}}]`)
		assert.ErrorContains(t, err, `on must be "id"`)
	})
	t.Run("bad whenMatched", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$merge": {"into": "t", "whenMatched": "upsert"}}]`)
		assert.ErrorContains(t, err, "whenMatched must be one of")
	})
	t.Run("pipeline-form whenMatched rejected", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$merge": {"into": "t", "whenMatched": [{"$addFields": {"a": 1}}]}}]`)
		assert.ErrorContains(t, err, "pipeline-form whenMatched is not supported")
		_, err = ParsePipeline(`[{"$merge": {"into": "t", "let": {"new": "$$ROOT"}}}]`)
		assert.ErrorContains(t, err, "not supported")
	})
	t.Run("bad whenNotMatched", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$merge": {"into": "t", "whenNotMatched": "replace"}}]`)
		assert.ErrorContains(t, err, "whenNotMatched must be one of")
	})
	t.Run("unknown option", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$merge": {"into": "t", "nope": 1}}]`)
		assert.ErrorContains(t, err, "unknown $merge option")
	})
	t.Run("must be last", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$match": {}}, {"$merge": "t"}, {"$limit": 1}]`)
		assert.ErrorContains(t, err, "$merge must be the last pipeline stage")
		assert.ErrorContains(t, err, "(at 1.$merge)")
	})
	t.Run("last is fine", func(t *testing.T) {
		p := MustParsePipeline(`[{"$group": {"_id": "$c"}}, {"$merge": "t"}]`)
		require.Len(t, p, 2)
		sink, rest := CutSink(p)
		assert.Equal(t, MergeSpec{Into: "t"}, sink)
		require.Len(t, rest, 1)
	})
	t.Run("rejected inside facet", func(t *testing.T) {
		_, err := ParsePipeline(`[{"$facet": {"f": [{"$limit": 1}, {"$merge": "t"}]}}]`)
		assert.ErrorContains(t, err, "$merge is not allowed inside $facet")
	})
}

func TestCutSink(t *testing.T) {
	t.Run("no sink", func(t *testing.T) {
		p := MustParsePipeline(`[{"$limit": 1}]`)
		sink, rest := CutSink(p)
		assert.Nil(t, sink)
		assert.Equal(t, p, rest)
	})
	t.Run("empty", func(t *testing.T) {
		sink, rest := CutSink(nil)
		assert.Nil(t, sink)
		assert.Nil(t, rest)
	})
	t.Run("out", func(t *testing.T) {
		p := MustParsePipeline(`[{"$out": "t"}]`)
		sink, rest := CutSink(p)
		assert.Equal(t, OutSpec{Coll: "t"}, sink)
		assert.Len(t, rest, 0)
	})
}
