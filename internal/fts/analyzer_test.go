package fts

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// terms extracts just the term strings, dropping positions, for readability.
func terms(toks []Token) []string {
	out := make([]string, len(toks))
	for i, t := range toks {
		out[i] = t.Term
	}
	return out
}

func TestAnalyze_Latin(t *testing.T) {
	got := Analyze("The quick, brown FOX!")
	assert.Equal(t, []string{"the", "quick", "brown", "fox"}, terms(got))
}

func TestAnalyze_Positions(t *testing.T) {
	got := Analyze("alpha beta gamma")
	require.Len(t, got, 3)
	assert.Equal(t, Token{"alpha", 0}, got[0])
	assert.Equal(t, Token{"beta", 1}, got[1])
	assert.Equal(t, Token{"gamma", 2}, got[2])
}

func TestAnalyze_DropsPunctuationAndWhitespace(t *testing.T) {
	got := Analyze("  hello --- world ...  ")
	// Punctuation/whitespace tokens are skipped and positions stay contiguous.
	assert.Equal(t, []string{"hello", "world"}, terms(got))
	assert.Equal(t, uint32(0), got[0].Pos)
	assert.Equal(t, uint32(1), got[1].Pos)
}

func TestAnalyze_Cyrillic(t *testing.T) {
	got := Analyze("Привет Мир")
	assert.Equal(t, []string{"привет", "мир"}, terms(got))
}

func TestAnalyze_CaseFold(t *testing.T) {
	// Full case folding, not ToLower: ß -> ss, full-width -> ascii via NFKC.
	assert.Equal(t, []string{"strasse"}, terms(Analyze("Straße")))
	assert.Equal(t, []string{"apple"}, terms(Analyze("Ａｐｐｌｅ")))
}

func TestAnalyze_NFKCComposedEqualsDecomposed(t *testing.T) {
	composed := Analyze("café")              // é = U+00E9
	decomposed := Analyze("café")      // e + combining acute
	assert.Equal(t, terms(composed), terms(decomposed))
	assert.Equal(t, []string{"café"}, terms(composed))
}

func TestAnalyze_Digits(t *testing.T) {
	got := Analyze("version 2 build 1024")
	assert.Equal(t, []string{"version", "2", "build", "1024"}, terms(got))
}

func TestAnalyze_CJKBigrams(t *testing.T) {
	// 東京都 -> overlapping bigrams 東京, 京都 at consecutive positions.
	got := Analyze("東京都")
	require.Len(t, got, 2)
	assert.Equal(t, Token{"東京", 0}, got[0])
	assert.Equal(t, Token{"京都", 1}, got[1])
}

func TestAnalyze_CJKSingleChar(t *testing.T) {
	// A lone ideograph has no bigram; emit it as a unigram.
	got := Analyze("水")
	assert.Equal(t, []string{"水"}, terms(got))
}

func TestAnalyze_MixedScripts(t *testing.T) {
	// Latin + CJK run + Latin. The CJK run bigrams sit between the latin words
	// with contiguous positions; the run boundary is the latin words.
	got := Analyze("hello 東京都 world")
	assert.Equal(t, []string{"hello", "東京", "京都", "world"}, terms(got))
	// positions are a single increasing sequence across scripts
	for i := range got {
		assert.Equal(t, uint32(i), got[i].Pos, "token %d", i)
	}
}

func TestAnalyze_CJKRunsSeparatedByPunctuation(t *testing.T) {
	// Two separate runs must not bridge a bigram across the punctuation.
	got := Analyze("東京・大阪")
	assert.Equal(t, []string{"東京", "大阪"}, terms(got))
}

func TestAnalyze_Empty(t *testing.T) {
	assert.Empty(t, Analyze(""))
	assert.Empty(t, Analyze("   \t\n  "))
	assert.Empty(t, Analyze("--- ... !!!"))
}

func TestAnalyzer_ReuseMatchesOneShot(t *testing.T) {
	a := NewAnalyzer()
	inputs := []string{"Hello World", "東京都", "Straße café", "Привет 水 test"}
	for _, in := range inputs {
		reused := a.Append(nil, in)
		oneShot := Analyze(in)
		assert.Equal(t, oneShot, reused, "input %q", in)
	}
}

func TestAnalyzer_AppendAccumulates(t *testing.T) {
	a := NewAnalyzer()
	var toks []Token
	toks = a.Append(toks, "alpha")
	toks = a.Append(toks, "beta")
	// Append does not renumber across calls; each call restarts positions at 0.
	// Callers that index multiple fields handle field offsets themselves.
	assert.Equal(t, []string{"alpha", "beta"}, terms(toks))
}
