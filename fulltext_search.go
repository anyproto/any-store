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

// ErrFulltextUnsupportedOp is returned for operations that do not yet support
// $text (Update/Delete/Explain in v1).
var ErrFulltextUnsupportedOp = errors.New("any-store: $text is only supported in Find().Iter()/Count()")

// ftsHit is a scored search result: the document's data-namespace key (its
// encoded string id) and BM25 score.
type ftsHit struct {
	idKey []byte
	score float64
}

// search runs a bag-of-words BM25 query over the index and returns hits sorted
// by descending score (ties broken by ascending IntDocID for determinism).
//
// v1 semantics: the query is analyzed with the same pipeline as indexing and
// treated as a bag of terms (OR); each matching document's score is the sum of
// the per-term BM25 contributions. CJK bigrams participate as ordinary terms
// (positional phrase matching is a documented v2 refinement).
func (fx *ftsIndex) search(tx *btree.ReadTx, queryStr string) ([]ftsHit, error) {
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
	// term accumulates thousands of docs.
	acc := ftsScoreAccPool.Get().(*ftsScoreAcc)
	acc.reset()
	defer ftsScoreAccPool.Put(acc)

	var key, val []byte
	var doneTerms []string // distinct query terms already scanned (few)
	for _, tok := range terms {
		term := tok.Term
		if slices.Contains(doneTerms, term) {
			continue // repeated query word contributes once
		}
		doneTerms = append(doneTerms, term)

		df, derr := ftsGetUint(tx, fx.nsVocab, []byte(term))
		if derr != nil {
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
				dl, e := ftsDocLen(tx, fx.nsDocinfo, docID)
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
			return nil, scanErr
		}
	}

	if acc.n == 0 {
		return nil, nil
	}

	// Materialize live (docID, score) pairs and sort by score desc / docID asc.
	ranked := acc.appendTo(nil)
	slices.SortFunc(ranked, func(a, b scoredDoc) int {
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

	// NOTE: do not reuse `key` here — it still aliases btree page memory from the
	// last cur.Key() above; writing into it would corrupt the page cache.
	hits := make([]ftsHit, 0, len(ranked))
	var mapKey []byte
	for _, r := range ranked {
		mapKey = ftsMapReverseKey(mapKey, r.docID)
		idBytes, gerr := tx.Get(fx.nsMap, mapKey)
		if gerr != nil {
			if errors.Is(gerr, btree.ErrKeyNotFound) {
				continue // stale posting (doc removed) — skip
			}
			return nil, gerr
		}
		hits = append(hits, ftsHit{idKey: slices.Clone(idBytes), score: r.score})
	}
	return hits, nil
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
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], docID)
	v, err := tx.Get(ns, k[:])
	if err != nil {
		if errors.Is(err, btree.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}
	n, _ := binary.Uvarint(v)
	return uint32(n), nil
}

// ftsScanIter is the planner iterator that drives a $text query: it yields the
// pre-ranked document ids (data-namespace keys) in descending score order. It
// satisfies qplanner.Iterator so it can sit beneath FilterIter / LimitIter.
type ftsScanIter struct {
	hits []ftsHit
	pos  int
}

func (it *ftsScanIter) Next() (key []byte, docId []byte, multiKey bool, err error) {
	if it.pos >= len(it.hits) {
		return nil, nil, false, nil
	}
	h := it.hits[it.pos]
	it.pos++
	return h.idKey, h.idKey, false, nil
}

func (it *ftsScanIter) Close() {}

func (it *ftsScanIter) String() string { return "FtsScan" }

var _ qplanner.Iterator = (*ftsScanIter)(nil)

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

// condHasResidual reports whether the filter constrains anything beyond the
// $text predicate itself (i.e. whether a residual FilterIter is needed).
func condHasResidual(f query.Filter) bool {
	switch ft := f.(type) {
	case query.Text:
		return false
	case query.And:
		for _, sub := range ft {
			if condHasResidual(sub) {
				return true
			}
		}
		return false
	case *query.And:
		for _, sub := range *ft {
			if condHasResidual(sub) {
				return true
			}
		}
		return false
	case nil, query.All:
		return false
	}
	return true
}
