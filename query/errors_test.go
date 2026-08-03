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

// TestOperators pins the exported vocabulary: exactly the operators the parser
// recognizes, sorted, and returned as a fresh copy. The snapshot is
// intentional — adding or removing an operator must update it, which is the
// moment to also update whatever consumers advertise.
func TestOperators(t *testing.T) {
	ops := Operators()
	assert.Equal(t, []string{
		"$all", "$and", "$eq", "$exists", "$gt", "$gte", "$in", "$knn",
		"$lt", "$lte", "$ne", "$nin", "$nor", "$not", "$or", "$regex",
		"$size", "$text", "$type",
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
