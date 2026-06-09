package anystore

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/fts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Axis A: math correctness — brute-force BM25 oracle --------------------
//
// The oracle runs the SAME analyzer (fts.Analyze) and the SAME BM25 formula and
// tie-break (score desc, then IntDocID = insertion order asc) as the engine, in
// plain in-memory float64 math. Asserting Engine.TopK == Oracle.TopK isolates
// the postings/chunk/accumulator path: it proves the data structures and
// aggregation are correct, independent of tokenization quality.

type bruteDoc struct {
	id  string
	tf  map[string]int
	dl  int
	idx int // 1-based IntDocID (insertion order among indexable docs)
}

type bruteBM25 struct {
	docs        []*bruteDoc
	df          map[string]int
	totalTokens int
}

func newBruteBM25() *bruteBM25 { return &bruteBM25{df: map[string]int{}} }

func (b *bruteBM25) add(id, text string) {
	toks := fts.Analyze(text)
	if len(toks) == 0 {
		return // matches the engine: empty docs are not indexed / get no IntDocID
	}
	tf := make(map[string]int, len(toks))
	for _, t := range toks {
		tf[t.Term]++
	}
	for term := range tf {
		b.df[term]++
	}
	b.totalTokens += len(toks)
	b.docs = append(b.docs, &bruteDoc{id: id, tf: tf, dl: len(toks), idx: len(b.docs) + 1})
}

func (b *bruteBM25) topK(query string, k int) []string {
	n := len(b.docs)
	if n == 0 {
		return nil
	}
	avgdl := float64(b.totalTokens) / float64(n)
	if avgdl == 0 {
		avgdl = 1
	}
	var qterms []string
	for _, t := range fts.Analyze(query) {
		if !slices.Contains(qterms, t.Term) {
			qterms = append(qterms, t.Term)
		}
	}
	type sc struct {
		idx   int
		id    string
		score float64
	}
	scores := make(map[int]float64)
	// Iterate terms in the SAME order the engine does so the per-doc float64
	// summation order (and thus the exact result) is identical.
	for _, term := range qterms {
		df := b.df[term]
		if df == 0 {
			continue
		}
		idf := math.Log(1 + (float64(n)-float64(df)+0.5)/(float64(df)+0.5))
		for _, d := range b.docs {
			tf := float64(d.tf[term])
			if tf == 0 {
				continue
			}
			denom := tf + bm25K1*(1-bm25B+bm25B*float64(d.dl)/avgdl)
			scores[d.idx] += idf * (tf * (bm25K1 + 1)) / denom
		}
	}
	ranked := make([]sc, 0, len(scores))
	for _, d := range b.docs {
		if s, ok := scores[d.idx]; ok {
			ranked = append(ranked, sc{d.idx, d.id, s})
		}
	}
	slices.SortFunc(ranked, func(a, b sc) int {
		switch {
		case a.score > b.score:
			return -1
		case a.score < b.score:
			return 1
		case a.idx < b.idx:
			return -1
		case a.idx > b.idx:
			return 1
		default:
			return 0
		}
	})
	out := make([]string, 0, k)
	for i := 0; i < len(ranked) && i < k; i++ {
		out = append(out, ranked[i].id)
	}
	return out
}

// qualityVocab is a pool with skewed (zipf-ish) frequencies so documents share
// terms and BM25 has real ranking decisions to make.
var qualityVocab = []string{
	"the", "the", "the", "a", "a", "of", "and", "to", "in", // common (stop-word-like)
	"aircraft", "aircraft", "flight", "flight", "engine", "crash", "crash",
	"landing", "runway", "pilot", "weather", "altitude", "fuel", "fire",
	"mountain", "ocean", "passengers", "crew", "survivors", "wreckage",
	"investigation", "report", "cause", "failure", "mechanical", "storm",
	"london", "paris", "tokyo", "berlin", "moscow", "boeing", "airbus",
	"douglas", "lockheed", "turbulence", "collision", "emergency", "rescue",
}

func qualityCorpus(rng *rand.Rand, ndocs int) []struct{ id, text string } {
	docs := make([]struct{ id, text string }, ndocs)
	for i := 0; i < ndocs; i++ {
		wlen := 12 + rng.Intn(60) // varied lengths so length-norm matters
		var sb strings.Builder
		for w := 0; w < wlen; w++ {
			if w > 0 {
				sb.WriteByte(' ')
			}
			sb.WriteString(qualityVocab[rng.Intn(len(qualityVocab))])
		}
		docs[i] = struct{ id, text string }{fmt.Sprintf("d%d", i), sb.String()}
	}
	return docs
}

func TestFtsRankingMatchesBruteForce(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	docs := qualityCorpus(rng, 200)

	fx := newFixture(t)
	defer fx.finish()
	coll, err := fx.CreateCollection(ctx, "q")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Kind: IndexKindFulltext, Fields: []string{"text"}}))

	oracle := newBruteBM25()
	for _, d := range docs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%q,"text":%q}`, d.id, d.text))))
		oracle.add(d.id, d.text)
	}

	const k = 10
	mism := 0
	for q := 0; q < 100; q++ {
		// random 1-3 term queries from the vocab
		qlen := 1 + rng.Intn(3)
		var terms []string
		for j := 0; j < qlen; j++ {
			terms = append(terms, qualityVocab[rng.Intn(len(qualityVocab))])
		}
		query := strings.Join(terms, " ")
		want := oracle.topK(query, k)
		got := ftsSearchIDs(t, coll, query, func(qr Query) Query { return qr.Limit(k) })
		if !slices.Equal(want, got) {
			mism++
			if mism <= 3 {
				t.Errorf("query %q\n  oracle: %v\n  engine: %v", query, want, got)
			}
		}
	}
	assert.Zero(t, mism, "engine ranking must match the brute-force BM25 oracle exactly")
}

// ---- Axis B: relevance — synthetic known-item queries ---------------------

// queryStopwords is a small set used ONLY to reject trivial all-stop-word
// synthetic queries (the index itself keeps stop words).
var queryStopwords = func() map[string]bool {
	m := map[string]bool{}
	for _, w := range []string{"the", "a", "an", "of", "and", "to", "in", "is", "it", "on", "for", "with", "as", "at", "by", "or"} {
		m[w] = true
	}
	return m
}()

// knownItemQuery extracts a contiguous 2–4 term span from a document's analyzed
// tokens (a fragment a user might remember). Returns ok=false for docs too short
// or spans that are entirely stop words. When maxDF > 0, the span must contain
// at least one reasonably DISTINCTIVE term (df <= maxDF) — simulating that users
// recall and search distinctive fragments, not generic boilerplate (the
// expert's TF-IDF-weighted sampling).
func knownItemQuery(rng *rand.Rand, text string, df map[string]int, maxDF int) (string, bool) {
	toks := fts.Analyze(text)
	if len(toks) < 2 {
		return "", false
	}
	span := 2 + rng.Intn(3)
	if span > len(toks) {
		span = len(toks)
	}
	start := rng.Intn(len(toks) - span + 1)
	allStop, distinctive := true, false
	var sb strings.Builder
	for i := 0; i < span; i++ {
		if i > 0 {
			sb.WriteByte(' ')
		}
		term := toks[start+i].Term
		sb.WriteString(term)
		if !queryStopwords[term] {
			allStop = false
		}
		if maxDF > 0 && df[term] > 0 && df[term] <= maxDF {
			distinctive = true
		}
	}
	if allStop {
		return "", false
	}
	if maxDF > 0 && !distinctive {
		return "", false
	}
	return sb.String(), true
}

// TestFtsRelevanceKnownItem measures MRR and Recall@5 with synthetic known-item
// queries. It uses the real air-crash corpus when FTS_DATASET is set (reporting
// real quality numbers), otherwise a deterministic synthetic corpus. The
// assertions are loose lower bounds to catch ranking regressions without flaking.
func TestFtsRelevanceKnownItem(t *testing.T) {
	type doc struct{ id, text string }
	var docs []doc

	dir := os.Getenv("FTS_DATASET")
	if dir == "" {
		dir = "../any-store-vector/aircrashdataset"
	}
	if files, err := filepath.Glob(filepath.Join(dir, "*.md")); err == nil && len(files) > 0 {
		for _, f := range files {
			raw, e := os.ReadFile(f)
			if e != nil {
				continue
			}
			id, body := parseFrontmatter(string(raw))
			if id == "" {
				id = filepath.Base(f)
			}
			docs = append(docs, doc{id, body})
		}
		t.Logf("relevance corpus: %d real docs", len(docs))
	} else {
		rng := rand.New(rand.NewSource(7))
		for _, d := range qualityCorpus(rng, 1000) {
			docs = append(docs, doc{d.id, d.text})
		}
		t.Logf("relevance corpus: %d synthetic docs (set FTS_DATASET for real)", len(docs))
	}

	fx := newFixture(t)
	defer fx.finish()
	coll, err := fx.CreateCollection(ctx, "rel")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Kind: IndexKindFulltext, Fields: []string{"text"}}))
	batch := make([]*anyenc.Value, len(docs))
	for i, d := range docs {
		batch[i] = anyenc.MustParseJson(fmt.Sprintf(`{"id":%q,"text":%q}`, d.id, d.text))
	}
	require.NoError(t, coll.Insert(ctx, batch...))

	// Document frequencies over the corpus, for distinctive-span selection.
	df := map[string]int{}
	for _, d := range docs {
		seen := map[string]bool{}
		for _, tk := range fts.Analyze(d.text) {
			if !seen[tk.Term] {
				seen[tk.Term] = true
				df[tk.Term]++
			}
		}
	}
	// "distinctive" = a term appearing in at most ~5% of docs.
	maxDF := len(docs) / 20
	if maxDF < 1 {
		maxDF = 1
	}

	// measure runs `want` known-item queries and returns (MRR, Recall@5).
	measure := func(seed int64, distinctiveOnly bool) (float64, float64, int) {
		rng := rand.New(rand.NewSource(seed))
		const want = 500
		var mrrSum float64
		var recall5, asked, tries int
		for asked < want && tries < want*50 {
			tries++
			d := docs[rng.Intn(len(docs))]
			dfMax := 0
			if distinctiveOnly {
				dfMax = maxDF
			}
			query, ok := knownItemQuery(rng, d.text, df, dfMax)
			if !ok {
				continue
			}
			asked++
			results := ftsSearchIDs(t, coll, query, func(qr Query) Query { return qr.Limit(10) })
			rank := slices.Index(results, d.id) + 1 // 0 if absent
			if rank > 0 {
				mrrSum += 1.0 / float64(rank)
				if rank <= 5 {
					recall5++
				}
			}
		}
		return mrrSum / float64(asked), float64(recall5) / float64(asked), asked
	}

	mrrR, recR, nR := measure(99, false)
	mrrD, recD, nD := measure(99, true)
	t.Logf("random-span    (n=%d):  MRR=%.4f  Recall@5=%.4f  (pessimistic — generic fragments)", nR, mrrR, recR)
	t.Logf("distinctive    (n=%d):  MRR=%.4f  Recall@5=%.4f  (realistic known-item)", nD, mrrD, recD)

	// Loose regression guards (baselines measured on this corpus). A real ranking
	// regression — broken tokenizer, wrong BM25 — drops these far below.
	assert.GreaterOrEqual(t, mrrR, 0.30, "random-span MRR regressed")
	assert.GreaterOrEqual(t, mrrD, 0.70, "distinctive MRR regressed")
	assert.GreaterOrEqual(t, recD, 0.80, "distinctive Recall@5 regressed")
}
