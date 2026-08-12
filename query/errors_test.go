package query

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"
)

// TestParseError pins the structured rejection contract: every parse-rejection
// class comes back as a *ParseError whose Path locates the failing key inside
// the filter, whose Op names the operator at fault, and whose Reason is a
// self-contained human message — so a caller exposing the filter grammar to
// untrusted input can errors.As one type and build a precise bad-request
// instead of string-matching parser messages.
func TestParseError(t *testing.T) {
	for _, tc := range []struct {
		name       string
		json       string
		wantPath   string
		wantOp     string
		wantReason string // substring of Reason
		wantIs     error  // finer class sentinel, nil if none
	}{
		{
			name: "unknown operator on a field",
			json: `{"tags":{"$sizee":1}}`,
			wantPath: "tags.$sizee", wantOp: "$sizee",
			wantReason: "unknown operator: $sizee", wantIs: ErrUnknownOperator,
		},
		{
			name: "unknown operator at top level",
			json: `{"$contains":"x"}`,
			wantPath: "$contains", wantOp: "$contains",
			wantReason: "unknown operator", wantIs: ErrUnknownOperator,
		},
		{
			// $expr is an aggregation-only operator: internal/aggregate
			// intercepts it in $match before this parser runs. A plain Find
			// filter rejects it as a vocabulary miss.
			name: "aggregation $expr rejected in a plain filter",
			json: `{"$expr":{"$gt":["$a","$b"]}}`,
			wantPath: "$expr", wantOp: "$expr",
			wantReason: "unknown operator: $expr", wantIs: ErrUnknownOperator,
		},
		{
			name: "unknown operator under $and names the element index",
			json: `{"$and":[{"a":1},{"b":{"$foo":2}}]}`,
			wantPath: "$and.1.b.$foo", wantOp: "$foo",
			wantReason: "unknown operator", wantIs: ErrUnknownOperator,
		},
		{
			name: "unknown operator under $not",
			json: `{"a":{"$not":{"$contains":1}}}`,
			wantPath: "a.$not.$contains", wantOp: "$contains",
			wantReason: "unknown operator", wantIs: ErrUnknownOperator,
		},
		{
			name: "$and operand not an array",
			json: `{"$and":{}}`,
			wantPath: "$and", wantOp: "$and",
			wantReason: "$and must be an array",
		},
		{
			name: "$or operand not an array",
			json: `{"$or":1}`,
			wantPath: "$or", wantOp: "$or",
			wantReason: "$or must be an array",
		},
		{
			name: "$nor operand not an array",
			json: `{"$nor":1}`,
			wantPath: "$nor", wantOp: "$nor",
			wantReason: "$nor must be an array",
		},
		{
			name: "$and element not an object",
			json: `{"$and":[1]}`,
			wantPath: "$and.0",
			wantReason: "query filter must be an object",
		},
		{
			name: "field-level operator at top level",
			json: `{"$eq":1}`,
			wantPath: "$eq", wantOp: "$eq",
			wantReason: "operator $eq is not valid at the top level",
		},
		{
			name: "top-level operator in field position",
			json: `{"a":{"$and":[]}}`,
			wantPath: "a.$and", wantOp: "$and",
			wantReason: "operator $and is not valid in a field condition",
		},
		{
			name: "value key after operators",
			json: `{"a":{"$gt":1,"b":2}}`,
			wantPath: "a.b",
			wantReason: "mixed operators and values",
		},
		{
			name: "operator after value keys",
			json: `{"a":{"b":2,"$gt":1}}`,
			wantPath: "a.$gt", wantOp: "$gt",
			wantReason: "mixed operators and values",
		},
		{
			name: "$in operand not an array",
			json: `{"a":{"$in":1}}`,
			wantPath: "a.$in", wantOp: "$in",
			wantReason: "$in must be an array",
		},
		{
			name: "$nin operand not an array",
			json: `{"a":{"$nin":1}}`,
			wantPath: "a.$nin", wantOp: "$nin",
			wantReason: "$nin must be an array",
		},
		{
			name: "$all operand not an array",
			json: `{"a":{"$all":"x"}}`,
			wantPath: "a.$all", wantOp: "$all",
			wantReason: "$all must be an array",
		},
		{
			name: "$regex pattern does not compile",
			json: `{"a":{"$regex":"["}}`,
			wantPath: "a.$regex", wantOp: "$regex",
			wantReason: "invalid regular expression",
		},
		{
			name: "$regex operand not a string",
			json: `{"a":{"$regex":1}}`,
			wantPath: "a.$regex", wantOp: "$regex",
			wantReason: "$regex must be a string",
		},
		{
			name: "$options without a sibling $regex",
			json: `{"a":{"$options":"i"}}`,
			wantPath: "a.$options", wantOp: "$options",
			wantReason: "$options requires a $regex",
		},
		{
			name: "$options operand not a string",
			json: `{"a":{"$regex":"x","$options":1}}`,
			wantPath: "a.$options", wantOp: "$options",
			wantReason: "$options must be a string",
		},
		{
			name: "$options unsupported flag",
			json: `{"a":{"$regex":"x","$options":"ix"}}`,
			wantPath: "a.$options", wantOp: "$options",
			wantReason: "unsupported $options flag 'x'",
		},
		{
			name: "$options at top level",
			json: `{"$options":"i"}`,
			wantPath: "$options", wantOp: "$options",
			wantReason: "operator $options is not valid at the top level",
		},
		{
			name: "$size operand not an integer",
			json: `{"a":{"$size":"x"}}`,
			wantPath: "a.$size", wantOp: "$size",
			wantReason: "$size must be an integer",
		},
		{
			name: "$type out of range",
			json: `{"a":{"$type":111}}`,
			wantPath: "a.$type", wantOp: "$type",
			wantReason: "unexpected type: 111",
		},
		{
			name: "$type unknown name",
			json: `{"a":{"$type":"xyz"}}`,
			wantPath: "a.$type", wantOp: "$type",
			wantReason: "unexpected type: xyz",
		},
		{
			name: "$not without operators",
			json: `{"a":{"$not":{}}}`,
			wantPath: "a.$not", wantOp: "$not",
			wantReason: "no operators found for $not",
		},
		{
			name: "$not operand not an object",
			json: `{"a":{"$not":5}}`,
			wantPath: "a.$not",
			wantReason: "expected an object of operators",
		},
		{
			name: "$text operand not an object",
			json: `{"$text":"hi"}`,
			wantPath: "$text", wantOp: "$text",
			wantReason: "$text must be an object",
		},
		{
			name: "$text without $search",
			json: `{"$text":{}}`,
			wantPath: "$text", wantOp: "$text",
			wantReason: "$text requires $search",
		},
		{
			name: "$text unknown field",
			json: `{"$text":{"$search":"x","$bogus":1}}`,
			wantPath: "$text.$bogus", wantOp: "$text",
			wantReason: "unknown $text field: $bogus",
		},
		{
			name: "$text $search not a string",
			json: `{"$text":{"$search":1}}`,
			wantPath: "$text.$search", wantOp: "$text",
			wantReason: "$search must be a string",
		},
		{
			name: "$text $require entry not a string",
			json: `{"$text":{"$search":"x","$require":[1]}}`,
			wantPath: "$text.$require", wantOp: "$text",
			wantReason: "$require entries must be strings",
		},
		{
			name: "$knn bad $k",
			json: `{"v":{"$knn":{"$query":[1],"$k":0}}}`,
			wantPath: "v.$knn.$k", wantOp: "$knn",
			wantReason: "$k must be an integer",
		},
		{
			name: "$knn without $query",
			json: `{"v":{"$knn":{"$k":5}}}`,
			wantPath: "v.$knn", wantOp: "$knn",
			wantReason: "$knn requires $query",
		},
		{
			name: "$knn mixed with another operator",
			json: `{"v":{"$knn":{"$query":[1],"$k":10},"$exists":true}}`,
			wantPath: "v", wantOp: "$knn",
			wantReason: "$knn must be the only operator on its field",
		},
		{
			name: "$knn under $not",
			json: `{"v":{"$not":{"$knn":{"$query":[1],"$k":10}}}}`,
			wantPath: "v.$not", wantOp: "$knn",
			wantReason: "$knn is not allowed under $not",
		},
		{
			// {"$vector":[…]} decodes into a vector VALUE before the parser
			// runs, so this is the JSON spelling of an ordering op against a
			// vector operand.
			name: "ordering operator against a vector",
			json: `{"a":{"$gt":{"$vector":[1,2]}}}`,
			wantPath: "a.$gt", wantOp: "$gt",
			wantReason: "not orderable", wantIs: ErrVectorNotOrderable,
		},
		{
			name: "filter document not an object",
			json: `[1,2]`,
			wantPath: "",
			wantReason: "query filter must be an object",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, err := ParseCondition(tc.json)
			require.Error(t, err)
			assert.Nil(t, f)

			var pe *ParseError
			require.True(t, errors.As(err, &pe), "not a *ParseError: %v", err)
			assert.Equal(t, tc.wantPath, pe.Path)
			assert.Equal(t, tc.wantOp, pe.Op)
			assert.Contains(t, pe.Reason, tc.wantReason)

			// Error() embeds Reason verbatim and appends the location.
			assert.Contains(t, err.Error(), pe.Reason)
			if pe.Path != "" {
				assert.Contains(t, err.Error(), "(at "+pe.Path+")")
			}

			if tc.wantIs != nil {
				assert.True(t, errors.Is(err, tc.wantIs), "want errors.Is(%v)", tc.wantIs)
			} else {
				// A known operator misused is a distinct fault from an
				// unrecognized token; only true vocabulary misses carry the
				// ErrUnknownOperator class.
				assert.False(t, errors.Is(err, ErrUnknownOperator))
			}
		})
	}
}

// Every rejection the filter grammar produces is structured: the whole legacy
// error-case table must come back as *ParseError. Cases that fail before the
// grammar (invalid JSON) are the JSON layer's business and are skipped.
func TestParseConditionErrorsAreStructured(t *testing.T) {
	for i, c := range errorParserCases {
		if _, jerr := fastjson.Parse(c.query); jerr != nil {
			continue
		}
		_, err := ParseCondition(c.query)
		require.Error(t, err, "case %d: %s", i, c.query)
		var pe *ParseError
		assert.True(t, errors.As(err, &pe), "case %d: %s -> %T: %v", i, c.query, err, err)
	}
}

// TestParseModifierError pins the structured rejection contract for the
// modifier grammar: same shape as filter rejections (*ParseError), with
// Source "modifier" and Path locating the failing key inside the modifier
// document.
func TestParseModifierError(t *testing.T) {
	for _, tc := range []struct {
		name       string
		json       string
		wantPath   string
		wantOp     string
		wantReason string // substring of Reason
		wantIs     error  // finer class sentinel, nil if none
	}{
		{
			name: "unknown modifier",
			json: `{"a":"b"}`,
			wantPath: "a", wantOp: "a",
			wantReason: "unknown modifier: a", wantIs: ErrUnknownOperator,
		},
		{
			name: "unknown $-modifier",
			json: `{"$sett":{"a":1}}`,
			wantPath: "$sett", wantOp: "$sett",
			wantReason: "unknown modifier: $sett", wantIs: ErrUnknownOperator,
		},
		{
			name: "modifier document not an object",
			json: `[]`,
			wantPath:   "",
			wantReason: "modifier must be an object",
		},
		{
			name: "empty modifier",
			json: `{}`,
			wantPath:   "",
			wantReason: "empty modifier",
		},
		{
			name: "modifier operand not an object",
			json: `{"$set":1}`,
			wantPath: "$set", wantOp: "$set",
			wantReason: "$set must be an object of field paths",
		},
		{
			// A '$'-key where a field path belongs is a position fault, not a
			// vocabulary miss — deliberately not ErrUnknownOperator.
			name: "operator in field-path position",
			json: `{"$set":{"$a":1}}`,
			wantPath: "$set.$a", wantOp: "$a",
			wantReason: "unexpected operator $a",
		},
		{
			name: "$inc operand not numeric",
			json: `{"$inc":{"count":"x"}}`,
			wantPath: "$inc.count", wantOp: "$inc",
			wantReason: "$inc requires a numeric value",
		},
		{
			name: "$rename target not a string",
			json: `{"$rename":{"a":1}}`,
			wantPath: "$rename.a", wantOp: "$rename",
			wantReason: "$rename requires a string target field path",
		},
		{
			name: "$pop argument out of range",
			json: `{"$pop":{"arr":2}}`,
			wantPath: "$pop.arr", wantOp: "$pop",
			wantReason: "$pop must be 1 (last) or -1 (first)",
		},
		{
			name: "$pullAll operand not an array",
			json: `{"$pullAll":{"arr":1}}`,
			wantPath: "$pullAll.arr", wantOp: "$pullAll",
			wantReason: "$pullAll must be an array",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, err := ParseModifier(tc.json)
			require.Error(t, err)
			assert.Nil(t, m)

			var pe *ParseError
			require.True(t, errors.As(err, &pe), "not a *ParseError: %v", err)
			assert.Equal(t, "modifier", pe.Source)
			assert.Equal(t, tc.wantPath, pe.Path)
			assert.Equal(t, tc.wantOp, pe.Op)
			assert.Contains(t, pe.Reason, tc.wantReason)
			assert.Contains(t, err.Error(), "parse modifier: ")

			if tc.wantIs != nil {
				assert.True(t, errors.Is(err, tc.wantIs), "want errors.Is(%v)", tc.wantIs)
			} else {
				assert.False(t, errors.Is(err, ErrUnknownOperator))
			}
		})
	}
}

// TestModifierOperators pins the exported modifier vocabulary — same snapshot
// contract as TestOperators.
func TestModifierOperators(t *testing.T) {
	ops := ModifierOperators()
	assert.Equal(t, []string{
		"$addToSet", "$inc", "$pop", "$pull", "$pullAll", "$push",
		"$rename", "$set", "$unset",
	}, ops)

	// The slice is a fresh copy: mutating it must not poison later calls.
	ops[0] = "$corrupted"
	assert.Equal(t, "$addToSet", ModifierOperators()[0])
}

// TestOperators pins the exported vocabulary: exactly the operators the parser
// recognizes, sorted, and returned as a fresh copy. The snapshot is
// intentional — adding or removing an operator must update it, which is the
// moment to also update whatever consumers advertise.
func TestOperators(t *testing.T) {
	ops := Operators()
	assert.Equal(t, []string{
		"$all", "$and", "$eq", "$exists", "$gt", "$gte", "$in", "$knn",
		"$lt", "$lte", "$ne", "$nin", "$nor", "$not", "$options", "$or",
		"$regex", "$size", "$text", "$type",
	}, ops)

	// Every advertised operator is recognized by the parser, each as a
	// distinct Operator value — Operators() and isOperator cannot drift.
	seen := make(map[Operator]bool, len(ops))
	for _, name := range ops {
		ok, op, err := isOperator([]byte(name))
		require.NoError(t, err, name)
		assert.True(t, ok, name)
		assert.False(t, seen[op], "two names map to one Operator: %s", name)
		seen[op] = true
	}

	// The slice is a fresh copy: mutating it must not poison later calls.
	ops[0] = "$corrupted"
	assert.Equal(t, "$all", Operators()[0])
}
