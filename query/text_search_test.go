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
		{
			"phrase-prefix: star after the closing quote",
			`"east tok"* crash`,
			[]TextClause{
				{Raw: "east tok", Phrase: true, Prefix: true, Op: TextShould},
				{Raw: "crash", Op: TextShould},
			},
		},
		{
			"anchored word, phrase, prefix and phrase-prefix",
			`^east ^"east tokyo" ^tok* ^"east tok"*`,
			[]TextClause{
				{Raw: "east", Anchored: true, Op: TextShould},
				{Raw: "east tokyo", Phrase: true, Anchored: true, Op: TextShould},
				{Raw: "tok", Prefix: true, Anchored: true, Op: TextShould},
				{Raw: "east tok", Phrase: true, Prefix: true, Anchored: true, Op: TextShould},
			},
		},
		{
			"detached ^ and * are dropped",
			`^ east * ^* "" ^"" tokyo`,
			[]TextClause{{Raw: "east", Op: TextShould}, {Raw: "tokyo", Op: TextShould}},
		},
		{
			"multiple trailing stars collapse",
			`"east tok"** pre**`,
			[]TextClause{
				{Raw: "east tok", Phrase: true, Prefix: true, Op: TextShould},
				{Raw: "pre", Prefix: true, Op: TextShould},
			},
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

func TestParseText_Fields(t *testing.T) {
	f := MustParseCondition(map[string]any{
		"$text": map[string]any{"$search": "east", "$fields": "title"},
	})
	txt, ok := f.(Text)
	require.True(t, ok)
	assert.Equal(t, []string{"title"}, txt.Fields)
	assert.Equal(t, `{"$text":{"$search":"east","$fields":["title"]}}`, txt.String())

	f = MustParseCondition(map[string]any{
		"$text": map[string]any{"$search": "east", "$fields": []any{"title", "body"}},
	})
	assert.Equal(t, []string{"title", "body"}, f.(Text).Fields)

	for _, bad := range []any{1, []any{"title", 2}, "", []any{""}} {
		_, err := ParseCondition(map[string]any{"$text": map[string]any{"$search": "east", "$fields": bad}})
		assert.Error(t, err, "%v", bad)
	}
	// Anchors and phrase-prefixes keep their flags under $require / $exclude.
	f = MustParseCondition(map[string]any{
		"$text": map[string]any{"$search": "x", "$require": "^east", "$exclude": []any{`"east tok"*`}},
	})
	assert.Equal(t, []TextClause{
		{Raw: "x", Op: TextShould},
		{Raw: "east", Anchored: true, Op: TextMust},
		{Raw: "east tok", Phrase: true, Prefix: true, Op: TextMustNot},
	}, f.(Text).Clauses)
}
