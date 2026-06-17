package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTextSearch(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []TextClause
	}{
		{"empty", "", nil},
		{"spaces", "   \t ", nil},
		{
			"bare words (should)",
			"east tokyo",
			[]TextClause{{Raw: "east", Op: TextShould}, {Raw: "tokyo", Op: TextShould}},
		},
		{
			// Inline +/- are NOT operators anymore — they are ordinary token text
			// (the analyzer drops the punctuation later). No false detection.
			"leading +/- are literal, not operators",
			"+east -crash tokyo",
			[]TextClause{
				{Raw: "+east", Op: TextShould},
				{Raw: "-crash", Op: TextShould},
				{Raw: "tokyo", Op: TextShould},
			},
		},
		{
			"negative number is a plain should term",
			"-9 degrees",
			[]TextClause{{Raw: "-9", Op: TextShould}, {Raw: "degrees", Op: TextShould}},
		},
		{
			"phrase",
			`"east tokyo" crash`,
			[]TextClause{
				{Raw: "east tokyo", Phrase: true, Op: TextShould},
				{Raw: "crash", Op: TextShould},
			},
		},
		{
			"prefix",
			"pre* east",
			[]TextClause{{Raw: "pre", Prefix: true, Op: TextShould}, {Raw: "east", Op: TextShould}},
		},
		{
			"star inside phrase is literal (not a prefix clause)",
			`"east tokyo*"`,
			[]TextClause{{Raw: "east tokyo*", Phrase: true, Op: TextShould}},
		},
		{
			"unterminated phrase consumes the rest",
			`"east tokyo`,
			[]TextClause{{Raw: "east tokyo", Phrase: true, Op: TextShould}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, ParseTextSearch(tc.in))
		})
	}
}

func TestParseText_RequireExclude(t *testing.T) {
	f := MustParseCondition(map[string]any{
		"$text": map[string]any{
			"$search":          "east tokyo",
			"$defaultOperator": "and",
			"$require":         []any{"critical", `"east side"`},
			"$exclude":         "draft",
		},
	})
	txt, ok := f.(Text)
	require.True(t, ok)
	assert.True(t, txt.DefaultAnd)
	assert.Equal(t, []TextClause{
		{Raw: "east", Op: TextShould},
		{Raw: "tokyo", Op: TextShould},
		{Raw: "critical", Op: TextMust},
		{Raw: "east side", Phrase: true, Op: TextMust},
		{Raw: "draft", Op: TextMustNot},
	}, txt.Clauses)
}

func TestParseText_ExcludeArray(t *testing.T) {
	f := MustParseCondition(map[string]any{
		"$text": map[string]any{"$search": "x", "$exclude": []any{"a", "b*"}},
	})
	txt := f.(Text)
	assert.Equal(t, []TextClause{
		{Raw: "x", Op: TextShould},
		{Raw: "a", Op: TextMustNot},
		{Raw: "b", Prefix: true, Op: TextMustNot},
	}, txt.Clauses)
}
