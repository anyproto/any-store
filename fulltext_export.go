package anystore

import "github.com/anyproto/any-store/v2/internal/fts"

// fulltext_export.go exposes the full-text analyzer and scoring parameters so
// an application can split text exactly as the index does (query building,
// highlighting, an external BM25 oracle in tests).

// FtsAnalyzeTerms runs the full-text analyzer pipeline (NFKC + case folding +
// UAX#29 word boundaries + CJK bigrams) over text and returns the resulting
// terms in token order — the terms a $text clause over the same text would
// match. An application builds search-as-you-type queries with it: split the
// typed string, mark the last term a prefix, quote the rest as a phrase.
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
