package anystore

import "github.com/anyproto/any-store/v2/internal/fts"

// fulltext_export.go exposes a minimal test-support surface so external test
// harnesses (any-store-tests) can build an exact BM25 oracle with the same
// analyzer and parameters as the engine. Not intended for application use.

// FtsAnalyzeTerms runs the full-text analyzer pipeline (NFKC + case folding +
// UAX#29 word boundaries + CJK bigrams) over text and returns the resulting
// terms in token order.
func FtsAnalyzeTerms(text string) []string {
	tokens := fts.Analyze(text)
	terms := make([]string, len(tokens))
	for i, tok := range tokens {
		terms[i] = tok.Term
	}
	return terms
}

// FtsBM25Params returns the BM25 k1 and b parameters used by $text scoring.
func FtsBM25Params() (k1, b float64) {
	return bm25K1, bm25B
}
