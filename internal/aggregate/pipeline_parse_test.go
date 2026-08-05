package aggregate

import (
	"errors"
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
		_, err := ParsePipeline(`[{"$lookup":{}}]`)
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
		_, err := ParseExpr(anyenc.MustParseJson(`{"$add": [1, 2]}`))
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
			name: "pipeline not an array",
			json: `{"$match":{}}`,
			wantPath:   "",
			wantReason: "pipeline must be an array of stages",
		},
		{
			name: "stage not an object",
			json: `[1]`,
			wantPath:   "0",
			wantReason: "stage must be an object",
		},
		{
			name: "stage with two keys",
			json: `[{"$match":{},"$limit":1}]`,
			wantPath:   "0",
			wantReason: "exactly one key",
		},
		{
			name: "unknown stage",
			json: `[{"$lookup":{}}]`,
			wantPath: "0.$lookup", wantOp: "$lookup",
			wantReason: "unknown stage: $lookup", wantIs: query.ErrUnknownOperator,
		},
		{
			name: "bad filter in a later $match names the stage index",
			json: `[{"$limit":1},{"$match":{"a":{"$foo":1}}}]`,
			wantPath: "1.$match.a.$foo", wantOp: "$foo",
			wantReason: "unknown operator", wantIs: query.ErrUnknownOperator,
		},
		{
			name: "$sort bad direction",
			json: `[{"$sort":{"a":2}}]`,
			wantPath: "0.$sort.a", wantOp: "$sort",
			wantReason: "must be 1 or -1",
		},
		{
			name: "$skip negative",
			json: `[{"$limit":1},{"$skip":-1}]`,
			wantPath: "1.$skip", wantOp: "$skip",
			wantReason: "$skip must be a non-negative integer",
		},
		{
			name: "$count operator-like field name",
			json: `[{"$count":"$x"}]`,
			wantPath: "0.$count", wantOp: "$count",
			wantReason: "must not start with $",
		},
		{
			name: "$project exclusion",
			json: `[{"$project":{"a":0}}]`,
			wantPath: "0.$project.a", wantOp: "$project",
			wantReason: "exclusion",
		},
		{
			name: "unsupported expression operator",
			json: `[{"$project":{"a":{"$add":[1,2]}}}]`,
			wantPath: "0.$project.a.$add", wantOp: "$add",
			wantReason: "unsupported expression operator: $add", wantIs: query.ErrUnknownOperator,
		},
		{
			name: "variable reference in expression",
			json: `[{"$project":{"a":"$$now"}}]`,
			wantPath:   "0.$project.a",
			wantReason: "variables are not supported",
		},
		{
			name: "expression error inside an array names the element index",
			json: `[{"$project":{"a":["$x","$$y"]}}]`,
			wantPath:   "0.$project.a.1",
			wantReason: "variables are not supported",
		},
		{
			// An option miss inside a known stage — like an unknown $text
			// field in the filter grammar, deliberately not ErrUnknownOperator.
			name: "unknown $unwind option",
			json: `[{"$unwind":{"path":"$a","x":1}}]`,
			wantPath: "0.$unwind.x", wantOp: "$unwind",
			wantReason: "unknown $unwind option: x",
		},
		{
			name: "unknown accumulator",
			json: `[{"$group":{"id":"$a","total":{"$summ":"$n"}}}]`,
			wantPath: "0.$group.total.$summ", wantOp: "$summ",
			wantReason: "unknown accumulator: $summ", wantIs: query.ErrUnknownOperator,
		},
		{
			name: "$count accumulator with a non-empty argument",
			json: `[{"$group":{"id":"$a","n":{"$count":1}}}]`,
			wantPath: "0.$group.n.$count", wantOp: "$count",
			wantReason: "$count takes an empty object {}",
		},
		{
			name: "$group key expression error",
			json: `[{"$group":{"id":"$$v"}}]`,
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
		"$addFields", "$count", "$group", "$limit", "$match", "$project",
		"$set", "$skip", "$sort", "$unwind",
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
