package anystore

import (
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
// v1 semantics: the query is analyzed with the same pipeline as indexing and
// treated as a bag of terms (OR); each matching document's score is the sum of
// the per-term BM25 contributions. CJK bigrams participate as ordinary terms
// (positional phrase matching is a documented v2 refinement).
func (fx *ftsIndex) searchCandidates(tx *btree.ReadTx, queryStr string) (qplanner.FtsCandidateStream, error) {
	terms := fts.Analyze(queryStr)
	if len(terms) == 0 {
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

	var key, val []byte
	var dlBuf []byte       // reusable docinfo value buffer (tx.Get would alloc per posting)
	var doneTerms []string // distinct query terms already scanned (few)
	for _, tok := range terms {
		term := tok.Term
		if slices.Contains(doneTerms, term) {
			continue // repeated query word contributes once
		}
		doneTerms = append(doneTerms, term)

		df, derr := ftsGetUint(tx, fx.nsVocab, []byte(term))
		if derr != nil {
			ftsScoreAccPool.Put(acc)
			return nil, derr
		}
		if df == 0 {
			continue
		}
		idf := math.Log(1 + (float64(n)-float64(df)+0.5)/(float64(df)+0.5))

		// Scan every chunk of this term.
		cur := tx.NewCursor(fx.nsPost)
		prefix := postingsTermPrefix(nil, term)
		if serr := cur.Seek(prefix); serr != nil {
			cur.Close()
			ftsScoreAccPool.Put(acc)
			return nil, serr
		}
		var scanErr error
		for cur.Valid() {
			key, scanErr = cur.Key()
			if scanErr != nil {
				break
			}
			if len(key) < len(prefix) || string(key[:len(prefix)]) != string(prefix) {
				break
			}
			val, scanErr = cur.Value()
			if scanErr != nil {
				break
			}
			r, rerr := fts.NewChunkReader(val)
			if rerr != nil {
				scanErr = rerr
				break
			}
			for r.Next() {
				docID := r.DocID()
				tf := float64(r.TF())
				var dl uint32
				var e error
				dl, dlBuf, e = ftsDocLenBuf(tx, fx.nsDocinfo, docID, dlBuf)
				if e != nil {
					scanErr = e
					break
				}
				denom := tf + bm25K1*(1-bm25B+bm25B*float64(dl)/avgdl)
				acc.add(docID, idf*(tf*(bm25K1+1))/denom)
			}
			if scanErr == nil {
				scanErr = r.Err()
			}
			if scanErr != nil {
				break
			}
			if scanErr = cur.Next(); scanErr != nil {
				break
			}
		}
		cur.Close()
		if scanErr != nil {
			ftsScoreAccPool.Put(acc)
			return nil, scanErr
		}
	}

	if acc.n == 0 {
		ftsScoreAccPool.Put(acc)
		return nil, nil
	}

	// Rank in the accumulator's pooled scratch: sort by score desc / docID asc.
	acc.scratch = acc.appendTo(acc.scratch[:0])
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

// add sums d into key's accumulated score, inserting the key on first sight.
func (m *ftsScoreAcc) add(key uint64, d float64) {
	pos := uint32(hashU64(key)) & m.mask
	for m.seen[pos] == m.gen {
		if m.key[pos] == key {
			m.val[pos] += d
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
	m.n++
}

// appendTo appends every live (docID, score) pair to dst.
func (m *ftsScoreAcc) appendTo(dst []scoredDoc) []scoredDoc {
	for i := range m.key {
		if m.seen[i] == m.gen {
			dst = append(dst, scoredDoc{m.key[i], m.val[i]})
		}
	}
	return dst
}

func (m *ftsScoreAcc) grow() {
	size := len(m.key) * 2
	nkey := make([]uint64, size)
	nval := make([]float64, size)
	nseen := make([]uint32, size)
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
	}
	m.key, m.val, m.seen, m.mask = nkey, nval, nseen, nmask
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
	search := text.Search
	spec := &qplanner.FtsQuerySpec{
		Ordered:    true, // searchCandidates returns score-descending
		NeedScores: true, // cleared by ftsScanPlan for Count/Update/Delete
		Search: func(tx *btree.ReadTx) (qplanner.FtsCandidateStream, error) {
			return fx.searchCandidates(tx, search)
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
