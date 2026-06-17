package anystore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"sync"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/fts"
	"github.com/anyproto/any-store/v2/internal/qplanner"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
)

// fulltext_search.go implements the read/query side: BM25 ranking over the
// postings (read path) and the iterator that drives a $text query. See
// docs/fts/DESIGN.md.

// BM25 parameters (Robertson/Sparck-Jones defaults).
const (
	bm25K1 = 1.2
	bm25B  = 0.75
)

// ErrNoFulltextIndex is returned when a $text query runs against a collection
// that has no full-text index.
var ErrNoFulltextIndex = errors.New("any-store: collection has no full-text index for $text")

// searchCandidates runs a bag-of-words BM25 query over the index and returns a
// stream of matched documents ordered by descending score (ties broken by
// ascending IntDocID for determinism) — the source the FtsIter pulls from.
//
// The accumulation phase visits every matching posting (alloc-free: pooled
// flat accumulator + pooled rank scratch), but per-candidate materialization
// (reverse-map lookup + docId copy) happens lazily in the stream, so a
// Limit-cut query allocates O(consumed), not O(matched). The stream's Close
// returns the pooled state; a nil stream means no matches.
//
// Semantics: the $text predicate is parsed into clauses — should clauses from
// $search ("quoted phrases", prefix* terms, plain terms; OR by default, AND when
// $defaultOperator:"and"), required clauses from $require, and excluded clauses
// from $exclude. (Boolean role comes from the typed sub-field, not inline +/- in
// the string — see query.ParseTextSearch.) Each clause's text is analyzed with
// the same pipeline as indexing. Positive clauses contribute BM25 to a
// per-document accumulator; required clauses also set a bit in the doc's
// requiredMask (a doc must satisfy every required clause to survive); excluded
// clauses tombstone matching docs. A bare word that analyzes to several tokens
// (notably any CJK run, e.g. "東京都" → "東京","京都") becomes an implicit phrase
// matched by positional adjacency — this is what makes CJK substring search
// correct. A pure bag-of-words OR query reduces exactly to the v1 behavior (same
// term scan order, same float64 summation), so BM25 ranking is unchanged.
func (fx *ftsIndex) searchCandidates(tx *btree.ReadTx, text query.Text) (qplanner.FtsCandidateStream, error) {
	clauses := text.ParsedClauses()
	if len(clauses) == 0 {
		return nil, nil
	}

	n, err := ftsGetUint(tx, fx.nsMeta, ftsMetaCount)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	totalTokens, err := ftsGetUint(tx, fx.nsMeta, ftsMetaTokens)
	if err != nil {
		return nil, err
	}
	avgdl := float64(totalTokens) / float64(n)
	if avgdl == 0 {
		avgdl = 1
	}

	// Per-query score accumulator: a flat open-addressing IntDocID->score table
	// with O(1) generation reset, pooled across queries (see ftsScoreAcc). It
	// replaces a per-query map[uint64]float64 that would grow/rehash as a common
	// term accumulates thousands of docs. On success its ownership transfers to
	// the returned stream (released in Close); every error path puts it back.
	acc := ftsScoreAccPool.Get().(*ftsScoreAcc)
	acc.reset()

	e := ftsExec{tx: tx, fx: fx, acc: acc, n: float64(n), avgdl: avgdl, az: fts.NewAnalyzer()}
	if err = e.run(clauses, text.DefaultAnd); err != nil {
		ftsScoreAccPool.Put(acc)
		return nil, err
	}

	// Rank in the accumulator's pooled scratch: sort by score desc / docID asc.
	// appendTo applies the required-mask gate and drops tombstoned docs.
	acc.scratch = acc.appendTo(acc.scratch[:0], e.requiredAll)
	if len(acc.scratch) == 0 {
		ftsScoreAccPool.Put(acc)
		return nil, nil
	}
	slices.SortFunc(acc.scratch, func(a, b scoredDoc) int {
		if a.score > b.score {
			return -1
		}
		if a.score < b.score {
			return 1
		}
		if a.docID < b.docID {
			return -1
		}
		if a.docID > b.docID {
			return 1
		}
		return 0
	})

	return &ftsCandidateStream{tx: tx, fx: fx, acc: acc}, nil
}

// ftsPrefixMaxExpansions caps how many vocabulary completions a single prefix
// (foo*) clause expands to — the top-M by document frequency. It bounds the work
// (and the accumulator growth) of a broad prefix like "a*"; truncation is logged
// by run so a silent cap never reads as full coverage.
const ftsPrefixMaxExpansions = 50

// ftsExec carries the per-query scoring state shared across clauses.
type ftsExec struct {
	tx          *btree.ReadTx
	fx          *ftsIndex
	acc         *ftsScoreAcc
	n           float64 // corpus doc count
	avgdl       float64
	az          *fts.Analyzer
	dlBuf       []byte // reusable docinfo value buffer (one per query)
	tokBuf      []fts.Token
	requiredAll uint64 // OR of every allocated required-clause bit
	nextBit     uint   // next required-clause bit index
}

// bm25Contribution is the per-term BM25 score contribution. The expression and
// evaluation order match the v1 inline form exactly, so pure-OR ranking is
// bit-for-bit unchanged.
func bm25Contribution(idf, tf, dl, avgdl float64) float64 {
	denom := tf + bm25K1*(1-bm25B+bm25B*dl/avgdl)
	return idf * (tf * (bm25K1 + 1)) / denom
}

// allocReqBit hands out the next required-clause bit, or 0 once 64 are exhausted
// (the clause then degrades to ungated OR rather than corrupting the mask).
func (e *ftsExec) allocReqBit() uint64 {
	if e.nextBit >= 64 {
		return 0
	}
	b := uint64(1) << e.nextBit
	e.nextBit++
	e.requiredAll |= b
	return b
}

// analyze tokenizes a clause's raw text into its term list (reusing the buffer).
func (e *ftsExec) analyze(raw string) []string {
	e.tokBuf = e.az.Append(e.tokBuf[:0], raw)
	terms := make([]string, len(e.tokBuf))
	for i := range e.tokBuf {
		terms[i] = e.tokBuf[i].Term
	}
	return terms
}

// run compiles and executes the clauses. Positive clauses (should/must) are
// scored first — plain terms in first-seen order, so a pure-OR query keeps the
// exact v1 summation order — then phrases and prefixes; negated clauses run last
// (tombstones only affect docs already scored).
func (e *ftsExec) run(clauses []query.TextClause, defaultAnd bool) error {
	// Ordered, deduped positive plain terms (preserves v1 scan order + parity).
	var plainOrder []string
	plainBit := map[string]uint64{}
	type group struct {
		terms  []string // >1 ⇒ phrase
		stem   string   // prefix clause stem (Prefix only)
		prefix bool
		reqBit uint64
	}
	var posGroups, negGroups []group

	for _, c := range clauses {
		terms := e.analyze(c.Raw)
		if len(terms) == 0 {
			continue
		}
		required := c.Op == query.TextMust || (c.Op == query.TextShould && defaultAnd)
		negated := c.Op == query.TextMustNot

		switch {
		case c.Prefix && len(terms) == 1 && !c.Phrase:
			g := group{stem: terms[0], prefix: true}
			if negated {
				negGroups = append(negGroups, g)
			} else {
				if required {
					g.reqBit = e.allocReqBit()
				}
				posGroups = append(posGroups, g)
			}
		case len(terms) == 1 && !c.Phrase:
			term := terms[0]
			if negated {
				negGroups = append(negGroups, group{terms: terms})
				continue
			}
			var reqBit uint64
			if required {
				reqBit = e.allocReqBit()
			}
			// Dedup a repeated positive term: score once, union its bits.
			if _, seen := plainBit[term]; seen {
				plainBit[term] |= reqBit
			} else {
				plainBit[term] = reqBit
				plainOrder = append(plainOrder, term)
			}
		default:
			// Multi-token: explicit phrase, or an implicit phrase (CJK / joined word).
			g := group{terms: terms}
			if negated {
				negGroups = append(negGroups, g)
			} else {
				if required {
					g.reqBit = e.allocReqBit()
				}
				posGroups = append(posGroups, g)
			}
		}
	}

	// 1) Plain positive terms, in first-seen order (v1 parity).
	for _, term := range plainOrder {
		if err := e.scanTerm(term, plainBit[term], false); err != nil {
			return err
		}
	}
	// 2) Positive phrases and prefixes.
	for _, g := range posGroups {
		if err := e.scoreGroup(g.terms, g.stem, g.prefix, g.reqBit, false); err != nil {
			return err
		}
	}
	// 3) Negated clauses (tombstone only docs already present).
	for _, g := range negGroups {
		if err := e.scoreGroup(g.terms, g.stem, g.prefix, 0, true); err != nil {
			return err
		}
	}
	return nil
}

// scoreGroup dispatches a phrase or prefix group to its scorer (tomb selects the
// tombstone variant for a negated clause).
func (e *ftsExec) scoreGroup(terms []string, stem string, prefix bool, reqBit uint64, tomb bool) error {
	if prefix {
		return e.scorePrefix(stem, reqBit, tomb)
	}
	if len(terms) == 1 {
		return e.scanTerm(terms[0], reqBit, tomb)
	}
	return e.scorePhrase(terms, reqBit, tomb)
}

// scanTerm scores (or, if tomb, tombstones) every posting of a single term.
func (e *ftsExec) scanTerm(term string, reqBit uint64, tomb bool) error {
	df, err := ftsGetUint(e.tx, e.fx.nsVocab, []byte(term))
	if err != nil {
		return err
	}
	if df == 0 {
		return nil
	}
	idf := math.Log(1 + (e.n-float64(df)+0.5)/(float64(df)+0.5))

	cur := e.tx.NewCursor(e.fx.nsPost)
	defer cur.Close()
	prefix := postingsTermPrefix(nil, term)
	if err = cur.Seek(prefix); err != nil {
		return err
	}
	for cur.Valid() {
		key, kerr := cur.Key()
		if kerr != nil {
			return kerr
		}
		if len(key) < len(prefix) || string(key[:len(prefix)]) != string(prefix) {
			break
		}
		val, verr := cur.Value()
		if verr != nil {
			return verr
		}
		it, ierr := fts.NewChunkIterator(val)
		if ierr != nil {
			return ierr
		}
		for it.Next() {
			docID := it.DocID()
			if tomb {
				e.acc.markTomb(docID)
				continue
			}
			tf := float64(it.TF())
			dl, derr := e.docLen(docID)
			if derr != nil {
				return derr
			}
			e.acc.addScore(docID, bm25Contribution(idf, tf, float64(dl), e.avgdl), reqBit)
		}
		if it.Err() != nil {
			return it.Err()
		}
		if err = cur.Next(); err != nil {
			return err
		}
	}
	return nil
}

// scorePrefix expands stem against the vocabulary (top-M completions by df) and
// scores/tombstones each completion as a plain term sharing the clause's bit.
func (e *ftsExec) scorePrefix(stem string, reqBit uint64, tomb bool) error {
	terms, err := e.expandPrefix(stem)
	if err != nil {
		return err
	}
	for _, t := range terms {
		if err = e.scanTerm(t, reqBit, tomb); err != nil {
			return err
		}
	}
	return nil
}

// expandPrefix returns up to ftsPrefixMaxExpansions vocabulary terms with the
// given byte prefix, the most frequent (highest df) first.
func (e *ftsExec) expandPrefix(stem string) ([]string, error) {
	if stem == "" {
		return nil, nil
	}
	type td struct {
		term string
		df   uint64
	}
	var matches []td
	cur := e.tx.NewCursor(e.fx.nsVocab)
	defer cur.Close()
	p := []byte(stem)
	if err := cur.Seek(p); err != nil {
		return nil, err
	}
	for cur.Valid() {
		key, err := cur.Key()
		if err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(key, p) {
			break
		}
		val, err := cur.Value()
		if err != nil {
			return nil, err
		}
		df, _ := binary.Uvarint(val)
		matches = append(matches, td{string(key), df})
		if err = cur.Next(); err != nil {
			return nil, err
		}
	}
	slices.SortFunc(matches, func(a, b td) int {
		if a.df != b.df {
			if a.df > b.df {
				return -1
			}
			return 1
		}
		if a.term < b.term {
			return -1
		}
		if a.term > b.term {
			return 1
		}
		return 0
	})
	if len(matches) > ftsPrefixMaxExpansions {
		matches = matches[:ftsPrefixMaxExpansions]
	}
	out := make([]string, len(matches))
	for i := range matches {
		out[i] = matches[i].term
	}
	return out, nil
}

// scorePhrase finds documents where the phrase terms occur at consecutive
// positions and contributes (or tombstones) a synthetic-term BM25 score: the
// term frequency is the number of adjacency-confirmed occurrences and the IDF is
// the sum of the constituent terms' IDFs (the standard phrase-IDF substitute).
func (e *ftsExec) scorePhrase(terms []string, reqBit uint64, tomb bool) error {
	// Per-term IDF; if any term is absent (df==0) the phrase cannot occur.
	idfSum := 0.0
	for _, t := range terms {
		df, err := ftsGetUint(e.tx, e.fx.nsVocab, []byte(t))
		if err != nil {
			return err
		}
		if df == 0 {
			return nil
		}
		idfSum += math.Log(1 + (e.n-float64(df)+0.5)/(float64(df)+0.5))
	}

	streams := make([]*termStream, len(terms))
	for i, t := range terms {
		streams[i] = newTermStream(e.tx, e.fx.nsPost, t)
	}
	defer func() {
		for _, s := range streams {
			s.close()
		}
	}()

	posLists := make([][]uint32, len(terms))
	for {
		// All streams must be live to have a common document.
		live := true
		for _, s := range streams {
			if s.err != nil {
				return s.err
			}
			if !s.valid {
				live = false
				break
			}
		}
		if !live {
			break
		}
		// Target = max current docID; seek every stream up to it.
		target := streams[0].docID
		for _, s := range streams[1:] {
			if s.docID > target {
				target = s.docID
			}
		}
		allMatch := true
		exhausted := false
		for _, s := range streams {
			s.seek(target)
			if s.err != nil {
				return s.err
			}
			if !s.valid {
				exhausted = true
				break
			}
			if s.docID != target {
				allMatch = false
			}
		}
		if exhausted {
			break
		}
		if !allMatch {
			continue // some stream advanced past target; recompute max
		}
		// Common document: decode positions and count adjacency.
		for i, s := range streams {
			posLists[i] = s.positions(posLists[i][:0])
			if s.err != nil {
				return s.err
			}
		}
		if cnt := countAdjacent(posLists); cnt > 0 {
			if tomb {
				e.acc.markTomb(target)
			} else {
				dl, derr := e.docLen(target)
				if derr != nil {
					return derr
				}
				e.acc.addScore(target, bm25Contribution(idfSum, float64(cnt), float64(dl), e.avgdl), reqBit)
			}
		}
		for _, s := range streams {
			s.next()
		}
	}
	return nil
}

// countAdjacent counts positions b at which the phrase occurs: posLists[0] holds
// b, posLists[1] holds b+1, … posLists[k] holds b+k. Each list is ascending.
func countAdjacent(posLists [][]uint32) uint32 {
	var cnt uint32
	for _, b := range posLists[0] {
		ok := true
		for i := 1; i < len(posLists); i++ {
			if _, found := slices.BinarySearch(posLists[i], b+uint32(i)); !found {
				ok = false
				break
			}
		}
		if ok {
			cnt++
		}
	}
	return cnt
}

// docLen returns a document's token length, reusing the per-query value buffer.
func (e *ftsExec) docLen(docID uint64) (uint32, error) {
	dl, buf, err := ftsDocLenBuf(e.tx, e.fx.nsDocinfo, docID, e.dlBuf)
	e.dlBuf = buf
	return dl, err
}

// ftsCandidateStream lazily materializes the ranked (IntDocID, score) pairs in
// acc.scratch into (docId, score) candidates as the planner pulls them: one
// reverse-map lookup + one copy into a reusable buffer per consumed candidate.
// It owns the pooled accumulator until Close.
type ftsCandidateStream struct {
	tx     *btree.ReadTx
	fx     *ftsIndex
	acc    *ftsScoreAcc
	idx    int
	mapKey []byte
	idBuf  []byte
}

// Next returns the next candidate's docId (valid until the following Next
// call) and score.
func (s *ftsCandidateStream) Next() (docId []byte, score float64, ok bool, err error) {
	for s.idx < len(s.acc.scratch) {
		r := s.acc.scratch[s.idx]
		s.idx++
		s.mapKey = ftsMapReverseKey(s.mapKey, r.docID)
		idBuf, gerr := s.tx.AppendValue(s.fx.nsMap, s.mapKey, s.idBuf[:0])
		if gerr != nil {
			if errors.Is(gerr, btree.ErrKeyNotFound) {
				continue // stale posting (doc removed) — skip
			}
			return nil, 0, false, gerr
		}
		s.idBuf = idBuf
		return s.idBuf, r.score, true, nil
	}
	return nil, 0, false, nil
}

func (s *ftsCandidateStream) Close() {
	if s.acc != nil {
		ftsScoreAccPool.Put(s.acc)
		s.acc = nil
	}
}

// scoredDoc is one accumulated (IntDocID, BM25 score) pair.
type scoredDoc struct {
	docID uint64
	score float64
}

// ftsScoreAcc is a flat open-addressing accumulator from a dense IntDocID to a
// summed BM25 score, with O(1) generation reset. It is the FTS read-path analog
// of internal/vivf's u32fmap (which replaced a per-query map[uint32]float32):
// IntDocIDs are dense, so linear probing over a power-of-two table with a
// splitmix64 mix beats the Go map's hashing, and reset bumps a tag instead of
// clearing. Pooled across queries and pointer-free (no GC scan cost).
type ftsScoreAcc struct {
	key  []uint64
	val  []float64
	seen []uint32 // slot occupied iff seen[i] == gen
	req  []uint64 // bitmask of required clauses this doc satisfied (AND semantics)
	tomb []bool   // doc excluded by a negated (-term) clause
	gen  uint32
	mask uint32
	n    int

	// scratch is the pooled rank buffer the accumulated pairs are sorted in;
	// it lives here so the per-query []scoredDoc is reused across queries.
	scratch []scoredDoc
}

var ftsScoreAccPool = sync.Pool{New: func() any { return &ftsScoreAcc{} }}

// hashU64 is the splitmix64 finalizer.
func hashU64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// reset clears the accumulator in O(1) after the first call (bumps the generation).
func (m *ftsScoreAcc) reset() {
	if m.key == nil {
		const init = 1024 // power of two; grows on demand
		m.key = make([]uint64, init)
		m.val = make([]float64, init)
		m.seen = make([]uint32, init)
		m.req = make([]uint64, init)
		m.tomb = make([]bool, init)
		m.mask = init - 1
		m.gen = 1
		m.n = 0
		return
	}
	m.n = 0
	m.gen++
	if m.gen == 0 { // wrapped: stale tags could false-hit, so clear once
		for i := range m.seen {
			m.seen[i] = 0
		}
		m.gen = 1
	}
}

// addScore sums d into key's accumulated score, inserting the key on first
// sight, and ORs reqBit into the doc's satisfied-required-clauses mask. A plain
// (non-required) contribution passes reqBit == 0.
func (m *ftsScoreAcc) addScore(key uint64, d float64, reqBit uint64) {
	pos := uint32(hashU64(key)) & m.mask
	for m.seen[pos] == m.gen {
		if m.key[pos] == key {
			m.val[pos] += d
			m.req[pos] |= reqBit
			return
		}
		pos = (pos + 1) & m.mask
	}
	if m.n >= int(m.mask-m.mask/4) { // keep load < 0.75
		m.grow()
		pos = uint32(hashU64(key)) & m.mask
		for m.seen[pos] == m.gen {
			pos = (pos + 1) & m.mask
		}
	}
	m.seen[pos] = m.gen
	m.key[pos] = key
	m.val[pos] = d
	m.req[pos] = reqBit
	m.tomb[pos] = false
	m.n++
}

// markTomb excludes key from the result if it is present in the accumulator (a
// negated clause). A key not already scored by a positive clause is ignored —
// negation only removes, it never introduces documents.
func (m *ftsScoreAcc) markTomb(key uint64) {
	pos := uint32(hashU64(key)) & m.mask
	for m.seen[pos] == m.gen {
		if m.key[pos] == key {
			m.tomb[pos] = true
			return
		}
		pos = (pos + 1) & m.mask
	}
}

// appendTo appends the live (docID, score) pairs to dst, dropping tombstoned
// docs and — when requiredAll != 0 — any doc that did not satisfy every required
// clause (its mask must cover requiredAll).
func (m *ftsScoreAcc) appendTo(dst []scoredDoc, requiredAll uint64) []scoredDoc {
	for i := range m.key {
		if m.seen[i] != m.gen || m.tomb[i] {
			continue
		}
		if requiredAll != 0 && m.req[i]&requiredAll != requiredAll {
			continue
		}
		dst = append(dst, scoredDoc{m.key[i], m.val[i]})
	}
	return dst
}

func (m *ftsScoreAcc) grow() {
	size := len(m.key) * 2
	nkey := make([]uint64, size)
	nval := make([]float64, size)
	nseen := make([]uint32, size)
	nreq := make([]uint64, size)
	ntomb := make([]bool, size)
	nmask := uint32(size - 1)
	g := m.gen
	for i := range m.key {
		if m.seen[i] != g {
			continue
		}
		p := uint32(hashU64(m.key[i])) & nmask
		for nseen[p] == g {
			p = (p + 1) & nmask
		}
		nseen[p] = g
		nkey[p] = m.key[i]
		nval[p] = m.val[i]
		nreq[p] = m.req[i]
		ntomb[p] = m.tomb[i]
	}
	m.key, m.val, m.seen, m.req, m.tomb, m.mask = nkey, nval, nseen, nreq, ntomb, nmask
}

// computeStats gathers the storage + corpus statistics of the full-text index:
// the five namespaces' on-disk sizes plus the maintained counters (doc count,
// total tokens) and the distinct-term count (vocab namespace entry count).
func (fx *ftsIndex) computeStats(tx *btree.ReadTx, pageSize int) (FtsIndexStats, error) {
	bytesOf := func(ns *btree.Namespace) (int, int, error) {
		sz, err := tx.NamespaceSize(ns)
		if err != nil {
			return 0, 0, err
		}
		return sz.TotalPages() * pageSize, sz.Entries, nil
	}

	postBytes, _, err := bytesOf(fx.nsPost)
	if err != nil {
		return FtsIndexStats{}, err
	}
	vocabBytes, vocabEntries, err := bytesOf(fx.nsVocab)
	if err != nil {
		return FtsIndexStats{}, err
	}
	mapBytes, _, err := bytesOf(fx.nsMap)
	if err != nil {
		return FtsIndexStats{}, err
	}
	infoBytes, _, err := bytesOf(fx.nsDocinfo)
	if err != nil {
		return FtsIndexStats{}, err
	}
	metaBytes, _, err := bytesOf(fx.nsMeta)
	if err != nil {
		return FtsIndexStats{}, err
	}

	n, err := ftsGetUint(tx, fx.nsMeta, ftsMetaCount)
	if err != nil {
		return FtsIndexStats{}, err
	}
	totalTokens, err := ftsGetUint(tx, fx.nsMeta, ftsMetaTokens)
	if err != nil {
		return FtsIndexStats{}, err
	}

	fs := FtsIndexStats{
		Name:          fx.info.Name,
		Fields:        append([]string(nil), fx.info.Fields...),
		DocCount:      int(n),
		VocabSize:     vocabEntries,
		TotalTokens:   int(totalTokens),
		PostingsBytes: postBytes,
		VocabBytes:    vocabBytes,
		DocmapBytes:   mapBytes,
		DocinfoBytes:  infoBytes,
		MetaBytes:     metaBytes,
	}
	fs.SizeBytes = postBytes + vocabBytes + mapBytes + infoBytes + metaBytes
	if n > 0 {
		fs.AvgDocLen = float64(totalTokens) / float64(n)
	}
	return fs, nil
}

// postingsTermPrefix builds the key prefix shared by all chunks of a term:
// uvarint(len(term)) | term. (postingsKey appends the 8-byte chunkID.)
func postingsTermPrefix(dst []byte, term string) []byte {
	dst = binary.AppendUvarint(dst[:0], uint64(len(term)))
	return append(dst, term...)
}

func ftsGetUint(tx *btree.ReadTx, ns *btree.Namespace, key []byte) (uint64, error) {
	v, err := tx.Get(ns, key)
	if err != nil {
		if errors.Is(err, btree.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}
	n, _ := binary.Uvarint(v)
	return n, nil
}

func ftsDocLen(tx *btree.ReadTx, ns *btree.Namespace, docID uint64) (uint32, error) {
	dl, _, err := ftsDocLenBuf(tx, ns, docID, nil)
	return dl, err
}

// ftsDocLenBuf is ftsDocLen with a caller-owned value buffer: the BM25
// accumulation loop calls it once per posting, and tx.Get's nil-buffer
// AppendValue would allocate every time.
func ftsDocLenBuf(tx *btree.ReadTx, ns *btree.Namespace, docID uint64, buf []byte) (uint32, []byte, error) {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], docID)
	v, err := tx.AppendValue(ns, k[:], buf[:0])
	if err != nil {
		if errors.Is(err, btree.ErrKeyNotFound) {
			return 0, buf, nil
		}
		return 0, buf, err
	}
	n, _ := binary.Uvarint(v)
	return uint32(n), v, nil
}

// detectFtsQuery inspects the query's condition for a $text clause. If present,
// it returns an FtsQuerySpec (whose search closure runs BM25 over the
// collection's full-text index) and the residual filter (everything except the
// $text clause). If absent, it returns (nil, q.cond, nil). Mirrors
// detectVectorQuery. An error is returned for invalid placement or a missing
// full-text index.
func (q *collQuery) detectFtsQuery() (*qplanner.FtsQuerySpec, query.Filter, error) {
	text, hasText, err := findTextFilter(q.cond)
	if err != nil {
		return nil, nil, err
	}
	if !hasText {
		return nil, q.cond, nil
	}
	fxs := q.c.loadFtsIndexes()
	if len(fxs) == 0 {
		return nil, nil, ErrNoFulltextIndex
	}
	fx := fxs[0]
	spec := &qplanner.FtsQuerySpec{
		Ordered:    true, // searchCandidates returns score-descending
		NeedScores: true, // cleared by ftsScanPlan for Count/Update/Delete
		Search: func(tx *btree.ReadTx) (qplanner.FtsCandidateStream, error) {
			return fx.searchCandidates(tx, text)
		},
	}
	return spec, ftsResidualFilter(q.cond), nil
}

// ftsScanPlan builds the docId-producing plan for the non-Iter operations
// (Count/Update/Delete/Explain) when the query is a $text query, or returns
// ok=false to let the caller take its normal CBO path. It keeps those operations
// correct for $text (the BM25 source drives; the residual filter runs
// downstream) instead of falling through to a match-everything full scan.
func (q *collQuery) ftsScanPlan(btx *btree.ReadTx, buf *syncpool.DocBuffer) (plan *qplanner.Plan, ok bool, err error) {
	spec, residual, derr := q.detectFtsQuery()
	if derr != nil {
		return nil, false, derr
	}
	if spec == nil {
		return nil, false, nil
	}
	spec.NeedScores = false // Count/Update/Delete never read Score()
	plan = qplanner.BuildPlan(&qplanner.PlanParams{
		Tx:     btx,
		DataNs: q.c.ns,
		Filter: residual,
		Sorter: ftsSorter(q.sort),
		Limit:  int(q.limit),
		Offset: int(q.offset),
		Buf:    buf,
		Fts:    spec,
	})
	return plan, true, nil
}

// ftsSorter maps the query's sort to the planner's sorter for a $text query:
// the default (nil) and the relevance projection {$meta:"textScore"} both yield
// the FtsIter's intrinsic score-descending order (nil → no SortIter); any other
// sort is a real field that needs a SortIter.
func ftsSorter(s query.Sort) query.Sort {
	if s == nil || query.IsTextScoreSort(s) {
		return nil
	}
	return s
}

// findTextFilter locates the $text predicate in a parsed filter. v1 supports a
// $text only at the top level or directly inside an $and (AND context); a $text
// nested under $or/$nor/$not is rejected. Returns (text, found, error).
func findTextFilter(f query.Filter) (query.Text, bool, error) {
	switch ft := f.(type) {
	case query.Text:
		return ft, true, nil
	case query.And:
		return findTextInAnd(ft)
	case *query.And:
		return findTextInAnd(*ft)
	case query.Key:
		if _, ok, _ := findTextFilter(ft.Filter); ok {
			return query.Text{}, false, errFtsBadPlacement
		}
	case query.Or, query.Nor, query.Not:
		if containsText(f) {
			return query.Text{}, false, errFtsBadPlacement
		}
	}
	return query.Text{}, false, nil
}

func findTextInAnd(and query.And) (query.Text, bool, error) {
	var found query.Text
	var ok bool
	for _, sub := range and {
		t, has, err := findTextFilter(sub)
		if err != nil {
			return query.Text{}, false, err
		}
		if has {
			if ok {
				return query.Text{}, false, errFtsMultiple
			}
			found, ok = t, true
		}
	}
	return found, ok, nil
}

var (
	errFtsBadPlacement = errors.New("any-store: $text must be at the top level or inside $and (not under $or/$nor/$not)")
	errFtsMultiple     = errors.New("any-store: only one $text expression is supported per query")
)

// containsText reports whether any node in the filter tree is a $text predicate.
func containsText(f query.Filter) bool {
	switch ft := f.(type) {
	case query.Text:
		return true
	case query.And:
		return anyContainsText(ft)
	case *query.And:
		return anyContainsText(*ft)
	case query.Or:
		return anyContainsText(query.And(ft))
	case query.Nor:
		return anyContainsText(query.And(ft))
	case query.Not:
		return containsText(ft.Filter)
	case query.Key:
		return containsText(ft.Filter)
	}
	return false
}

func anyContainsText(fs []query.Filter) bool {
	for _, f := range fs {
		if containsText(f) {
			return true
		}
	}
	return false
}

// ftsResidualFilter returns the filter with the $text clause removed — the
// predicate the planner's FilterIter applies to the BM25-ranked candidates. A
// query that is just $text leaves no residual (query.All).
func ftsResidualFilter(f query.Filter) query.Filter {
	switch ft := f.(type) {
	case query.Text:
		return query.All{}
	case query.And:
		return ftsResidualFromAnd(ft)
	case *query.And:
		return ftsResidualFromAnd(*ft)
	}
	return f
}

func ftsResidualFromAnd(and query.And) query.Filter {
	var rest query.And
	for _, sub := range and {
		if _, isText := sub.(query.Text); !isText {
			rest = append(rest, sub)
		}
	}
	switch len(rest) {
	case 0:
		return query.All{}
	case 1:
		return rest[0]
	default:
		return rest
	}
}
