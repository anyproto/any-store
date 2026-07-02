// Package fts implements the lexical full-text-search layer of any-store.
//
// The analysis pipeline turns a raw text field into a stream of positioned
// terms that the inverted index stores. It is deliberately small and
// dependency-light:
//
//	NFKC normalize  ->  Unicode case-fold  ->  UAX#29 word split  ->  CJK bigram
//
//   - NFKC (golang.org/x/text/unicode/norm) collapses full-width forms,
//     compatibility ligatures and combining-mark spelling differences so that
//     text typed on different platforms/IMEs matches.
//   - Case folding (golang.org/x/text/cases) is the locale-independent,
//     search-oriented equivalent of lower-casing (handles ß->ss, Turkish I,
//     final sigma, etc.). It is NOT plain strings.ToLower.
//   - UAX#29 word boundaries (github.com/rivo/uniseg) split spaced scripts
//     (Latin, Cyrillic, Greek, ...) into words. UAX#29 emits each CJK
//     ideograph/kana/Hangul syllable as its own token.
//   - CJK runs are re-assembled into overlapping bigrams ("東京都" -> "東京",
//     "京都"), which is the standard space-free-script strategy: a phrase query
//     over the bigrams recovers the original substring. A lone CJK character is
//     emitted as a unigram.
//
// Stemming and diacritic-folding are intentionally NOT done here (see DESIGN.md):
// any-store is a generic, multi-lingual document store, so we cannot assume a
// per-field language, and prefix search covers most of what stemming would.
package fts

import (
	"unicode"

	"github.com/rivo/uniseg"
	"golang.org/x/text/cases"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

// Token is a single analyzed term together with its ordinal position in the
// field (0-based). Position counts emitted terms, not bytes or runes, and is
// what phrase / CJK-bigram adjacency checks compare.
type Token struct {
	Term string
	Pos  uint32
}

// Analyzer turns text into tokens. It reuses internal transformer state across
// calls, so a single Analyzer is cheap to call repeatedly but is NOT safe for
// concurrent use — give each goroutine its own (or pool them).
type Analyzer struct {
	fold      cases.Caser
	normalize transform.Transformer
	// scratch buffers reused across Append calls
	normBuf []byte
	cjkRun  []rune
}

// NewAnalyzer returns a ready-to-use Analyzer.
func NewAnalyzer() *Analyzer {
	return &Analyzer{
		fold:      cases.Fold(),
		normalize: norm.NFKC,
	}
}

// Analyze is a convenience wrapper that allocates a one-shot Analyzer. Hot paths
// should hold a reusable Analyzer and call Append instead.
func Analyze(text string) []Token {
	return NewAnalyzer().Append(nil, text)
}

// Append analyzes text and appends the resulting tokens to dst, returning the
// extended slice. dst may be nil.
func (a *Analyzer) Append(dst []Token, text string) []Token {
	normalized := a.prepare(text)

	var pos uint32
	a.cjkRun = a.cjkRun[:0]

	state := -1
	for len(normalized) > 0 {
		var word string
		word, normalized, state = uniseg.FirstWordInString(normalized, state)

		// Buffer CJK words so we can emit overlapping bigrams across the run.
		// UAX#29 emits each Han ideograph and each Hiragana char as its own word,
		// but keeps Katakana runs and Hangul syllable words together (Katakana ×
		// Katakana and ALetter × ALetter don't break) — so a CJK word here is any
		// word consisting solely of CJK runes, not just a single rune. Appending
		// whole words to one run also keeps bigrams flowing across script
		// boundaries inside a run (e.g. 東京タワー = 東 + 京 + タワー).
		if allCJK(word) {
			for _, r := range word {
				a.cjkRun = append(a.cjkRun, r)
			}
			continue
		}

		// Non-CJK boundary: flush any pending CJK run first, then handle word.
		dst, pos = a.flushCJK(dst, pos)

		if isIndexable(word) {
			dst = append(dst, Token{Term: word, Pos: pos})
			pos++
		}
	}
	dst, _ = a.flushCJK(dst, pos)
	return dst
}

// prepare applies NFKC normalization + case folding, reusing a.normBuf.
func (a *Analyzer) prepare(text string) string {
	a.normalize.Reset()
	a.fold.Reset()
	// NFKC then fold. Both via transform into the reused buffer.
	var err error
	a.normBuf, _, err = transform.Append(a.normalize, a.normBuf[:0], []byte(text))
	if err != nil {
		// Transformers only fail on malformed encoding; fall back to the raw
		// bytes so we still index something rather than dropping the field.
		a.normBuf = append(a.normBuf[:0], text...)
	}
	folded, _, err := transform.Append(a.fold, nil, a.normBuf)
	if err != nil {
		return string(a.normBuf)
	}
	return string(folded)
}

// flushCJK emits overlapping bigrams (or a lone unigram) for the buffered CJK
// run, advancing pos, then clears the run.
func (a *Analyzer) flushCJK(dst []Token, pos uint32) ([]Token, uint32) {
	run := a.cjkRun
	switch {
	case len(run) == 0:
		return dst, pos
	case len(run) == 1:
		dst = append(dst, Token{Term: string(run), Pos: pos})
		pos++
	default:
		// Overlapping bigrams: run[i],run[i+1]. Each bigram's position is the
		// position of its leading character, so adjacency (pos+1) holds.
		for i := 0; i+1 < len(run); i++ {
			dst = append(dst, Token{Term: string(run[i : i+2]), Pos: pos})
			pos++
		}
	}
	a.cjkRun = a.cjkRun[:0]
	return dst, pos
}

// allCJK reports whether word is non-empty and consists solely of space-free-
// script (CJK) runes.
func allCJK(word string) bool {
	n := 0
	for _, r := range word {
		if !isCJK(r) {
			return false
		}
		n++
	}
	return n > 0
}

// isIndexable reports whether a UAX#29 word should be indexed: it must contain
// at least one letter or digit (drops pure punctuation/whitespace tokens).
func isIndexable(word string) bool {
	for _, r := range word {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return true
		}
	}
	return false
}

// isCJK reports whether r is a space-free script that should be bigram-indexed:
// Han, Hiragana, Katakana, or Hangul — plus the script-Common marks that occur
// inside such words (ー prolonged sound mark, 々ゝゞヽヾ iteration marks), which
// would otherwise split a run mid-word (タワー, 人々). The katakana middle dot
// ・ (U+30FB) is deliberately absent: it separates words.
func isCJK(r rune) bool {
	switch r {
	case 'ー', '々', 'ゝ', 'ゞ', 'ヽ', 'ヾ':
		return true
	}
	switch {
	case unicode.Is(unicode.Han, r),
		unicode.Is(unicode.Hiragana, r),
		unicode.Is(unicode.Katakana, r),
		unicode.Is(unicode.Hangul, r):
		return true
	}
	return false
}
