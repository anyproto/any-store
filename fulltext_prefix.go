package anystore

import (
	"math/bits"
	"sync"

	"github.com/anyproto/any-store/v2/internal/fts"
)

// fulltext_prefix.go scores prefix clauses — foo*, "foo bar"*, ^foo* — as ONE
// term whose postings are the merged postings of every vocabulary completion
// of the stem (what SQLite FTS5 does with a prefix token). Per document the
// completions' term frequencies add up, and the IDF comes from the number of
// documents holding any completion, so a rare completion never outweighs a
// common one and a document is scored once per clause, not once per matching
// completion. The merge needs the completions' postings walked in full, which
// is why a prefix clause has no probe form (planStats marks it DriverOnly).

// scorePrefix scores (or tombstones) a prefix group: a bare stem as the merged
// term, a phrase-prefix or an anchored stem through the positional variant.
func (e *ftsExec) scorePrefix(g ftsClauseGroup, reqBit uint64, tomb bool) error {
	comps, err := e.expandPrefix(g.stem)
	if err != nil || len(comps) == 0 {
		return err
	}
	if len(g.terms) > 0 || g.anchored {
		return e.scorePhrasePrefix(g, comps, reqBit, tomb)
	}
	if tomb {
		// Exclusion needs no merge: every in-scope posting of every completion
		// tombstones its document.
		return e.walkCompletions(comps, func(it fts.ChunkIterator, docID uint64) error {
			if _, ok := e.postingTF(it); ok {
				e.acc.markTomb(docID)
			}
			return nil
		})
	}
	return e.scorePrefixUnion(comps, reqBit)
}

// scorePrefixUnion scores a bare prefix clause: one pass over the completions'
// postings sums the per-field term frequencies per document into a dense
// table, then every document in the table is scored once with the merged IDF.
func (e *ftsExec) scorePrefixUnion(comps []ftsCompletion, reqBit uint64) error {
	tab := ftsGetDocTable()
	defer ftsPutDocTable(tab)
	nf := e.nFields
	tfs := tab.aux[:0] // per slot: nf per-field TF sums
	err := e.walkCompletions(comps, func(it fts.ChunkIterator, docID uint64) error {
		mask := it.FieldMask() & e.scope
		if mask == 0 {
			return nil
		}
		slot, inserted := tab.insert(docID)
		if inserted {
			for i := 0; i < nf; i++ {
				tfs = append(tfs, 0)
			}
		}
		row := tfs[slot*nf : (slot+1)*nf]
		for m := mask; m != 0; m &= m - 1 {
			f := bits.TrailingZeros64(m)
			row[f] += it.FieldTF(f)
		}
		return nil
	})
	tab.aux = tfs
	if err != nil {
		return err
	}
	n := tab.len()
	if n == 0 {
		return nil
	}
	idf := e.idf(uint64(n))
	for slot := 0; slot < n; slot++ {
		docID := tab.ids[slot]
		tf := e.fieldsTF(tfs[slot*nf : (slot+1)*nf])
		dl, derr := e.docLen(docID)
		if derr != nil {
			return derr
		}
		e.acc.addScore(docID, e.bm25(idf, tf, float64(dl)), reqBit)
	}
	return nil
}

// scorePhrasePrefix scores "t1 … tk"* (k ≥ 1 fixed terms) and ^stem* (k == 0,
// anchored): a document matches where the fixed phrase occurs (anchored: at
// position 0) and a completion of the stem follows it immediately. The fixed
// phrase's occurrence ends come from the positional merge; the completions'
// postings are then walked once and each posting of a candidate document is
// checked against those ends — the cost is the fixed merge plus the
// completions' posting lists, never occurrences × completions. Scored as one
// synthetic term: TF = confirmed occurrences, IDF = Σ idf(fixed terms) + the
// merged completions' IDF.
func (e *ftsExec) scorePhrasePrefix(g ftsClauseGroup, comps []ftsCompletion, reqBit uint64, tomb bool) error {
	k := len(g.terms)
	idfSum, alive, err := e.phraseIDF(g.terms)
	if err != nil || !alive {
		return err
	}
	cand := ftsGetDocTable()
	defer ftsPutDocTable(cand)
	union := ftsGetDocTable()
	defer ftsPutDocTable(union)

	// next[endOff[slot]:endOff[slot+1]] are the positions a completion must
	// occupy for candidate slot (the fixed occurrence's end + 1), ascending;
	// cnt[slot] the confirmed occurrences. All three live in the pooled tables'
	// scratch so a phrase-prefix query allocates nothing per call.
	next := union.aux[:0]
	endOff := cand.aux2[:0]
	cnt := cand.aux[:0]
	if k > 0 {
		err = e.phraseMerge(g.terms, func(docID uint64, posLists [][]uint32) error {
			start := len(next)
			if g.anchored {
				if anchoredAt(posLists) {
					next = append(next, uint32(k))
				}
			} else {
				for _, b := range posLists[0] {
					ok := true
					for i := 1; i < k; i++ {
						if _, found := searchU32(posLists[i], b+uint32(i)); !found {
							ok = false
							break
						}
					}
					if ok {
						next = append(next, b+uint32(k))
					}
				}
			}
			if len(next) == start {
				return nil
			}
			cand.insert(docID)
			endOff = append(endOff, uint32(start))
			cnt = append(cnt, 0)
			return nil
		})
		if err != nil {
			union.aux, cand.aux, cand.aux2 = next, cnt, endOff
			return err
		}
		endOff = append(endOff, uint32(len(next)))
	}

	err = e.walkCompletions(comps, func(it fts.ChunkIterator, docID uint64) error {
		if it.FieldMask()&e.scope == 0 {
			return nil
		}
		union.insert(docID)
		if k == 0 {
			// ^stem*: the completion is the document's first token.
			e.posOut = e.positionsOf(it, e.posOut[:0])
			if len(e.posOut) > 0 && e.posOut[0] == 0 {
				slot, inserted := cand.insert(docID)
				if inserted {
					cnt = append(cnt, 0)
				}
				cnt[slot] = 1
			}
			return nil
		}
		slot, ok := cand.lookup(docID)
		if !ok {
			return nil
		}
		e.posOut = e.positionsOf(it, e.posOut[:0])
		cnt[slot] += countCommon(next[endOff[slot]:endOff[slot+1]], e.posOut)
		return nil
	})
	union.aux, cand.aux, cand.aux2 = next, cnt, endOff
	if err != nil {
		return err
	}
	if union.len() == 0 {
		return nil
	}
	idf := idfSum + e.idf(uint64(union.len()))
	for slot := 0; slot < cand.len(); slot++ {
		c := cnt[slot]
		if c == 0 {
			continue
		}
		docID := cand.ids[slot]
		if tomb {
			e.acc.markTomb(docID)
			continue
		}
		dl, derr := e.docLen(docID)
		if derr != nil {
			return derr
		}
		e.acc.addScore(docID, e.bm25(idf, float64(c), float64(dl)), reqBit)
	}
	return nil
}

// walkCompletions streams every posting of every completion, in completion
// (vocabulary) order, through visit.
func (e *ftsExec) walkCompletions(comps []ftsCompletion, visit func(it fts.ChunkIterator, docID uint64) error) error {
	for _, c := range comps {
		if err := e.walkTerm(c.term, visit); err != nil {
			return err
		}
	}
	return nil
}

// countCommon counts the values present in both ascending lists.
func countCommon(a, b []uint32) (n uint32) {
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
		default:
			n++
			i++
			j++
		}
	}
	return n
}

// searchU32 is a binary search in an ascending list.
func searchU32(l []uint32, v uint32) (int, bool) {
	lo, hi := 0, len(l)
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if l[m] < v {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo, lo < len(l) && l[lo] == v
}

// ftsDocTable maps IntDocIDs to dense slots 0..n-1 in insertion order, so a
// prefix clause's per-document side tables (summed per-field term frequencies,
// occurrence ends, match counts) are plain slices indexed by slot. Same layout
// as ftsScoreAcc — open addressing over a power-of-two table, splitmix64 mix,
// linear probing, generation reset — pointer-free and pooled across queries.
type ftsDocTable struct {
	key  []uint64
	slot []uint32
	seen []uint32 // bucket occupied iff seen[i] == gen
	ids  []uint64 // docID by slot (insertion order)
	gen  uint32
	mask uint32
	// aux and aux2 are per-slot scratch the scorers lay their side tables in
	// (TF sums, occurrence ends, counts); pooled and bounded with the table.
	aux  []uint32
	aux2 []uint32
}

var ftsDocTablePool = sync.Pool{New: func() any { return &ftsDocTable{} }}

func ftsGetDocTable() *ftsDocTable {
	t := ftsDocTablePool.Get().(*ftsDocTable)
	t.reset()
	return t
}

// ftsPutDocTable returns a table to the pool, dropping its arrays first if they
// grew past ftsAccMaxPooled (the accumulator's bound, for the same reason).
func ftsPutDocTable(t *ftsDocTable) {
	if cap(t.key) > ftsAccMaxPooled {
		t.key, t.slot, t.seen, t.ids = nil, nil, nil, nil
		t.mask, t.gen = 0, 0
	}
	if cap(t.aux) > ftsAccMaxPooled {
		t.aux = nil
	}
	if cap(t.aux2) > ftsAccMaxPooled {
		t.aux2 = nil
	}
	ftsDocTablePool.Put(t)
}

func (t *ftsDocTable) len() int { return len(t.ids) }

// reset empties the table in O(1) after the first call (bumps the generation).
func (t *ftsDocTable) reset() {
	t.ids = t.ids[:0]
	if t.key == nil {
		const init = 1024
		t.key = make([]uint64, init)
		t.slot = make([]uint32, init)
		t.seen = make([]uint32, init)
		t.mask = init - 1
		t.gen = 1
		return
	}
	t.gen++
	if t.gen == 0 {
		for i := range t.seen {
			t.seen[i] = 0
		}
		t.gen = 1
	}
}

// lookup returns doc's slot.
func (t *ftsDocTable) lookup(doc uint64) (slot int, ok bool) {
	pos := uint32(hashU64(doc)) & t.mask
	for t.seen[pos] == t.gen {
		if t.key[pos] == doc {
			return int(t.slot[pos]), true
		}
		pos = (pos + 1) & t.mask
	}
	return 0, false
}

// insert returns doc's slot, allocating the next one on first sight.
func (t *ftsDocTable) insert(doc uint64) (slot int, inserted bool) {
	pos := uint32(hashU64(doc)) & t.mask
	for t.seen[pos] == t.gen {
		if t.key[pos] == doc {
			return int(t.slot[pos]), false
		}
		pos = (pos + 1) & t.mask
	}
	if len(t.ids) >= int(t.mask-t.mask/4) { // keep load < 0.75
		t.grow()
		pos = uint32(hashU64(doc)) & t.mask
		for t.seen[pos] == t.gen {
			pos = (pos + 1) & t.mask
		}
	}
	slot = len(t.ids)
	t.ids = append(t.ids, doc)
	t.seen[pos] = t.gen
	t.key[pos] = doc
	t.slot[pos] = uint32(slot)
	return slot, true
}

func (t *ftsDocTable) grow() {
	size := len(t.key) * 2
	nkey := make([]uint64, size)
	nslot := make([]uint32, size)
	nseen := make([]uint32, size)
	nmask := uint32(size - 1)
	g := t.gen
	for i := range t.key {
		if t.seen[i] != g {
			continue
		}
		p := uint32(hashU64(t.key[i])) & nmask
		for nseen[p] == g {
			p = (p + 1) & nmask
		}
		nseen[p] = g
		nkey[p] = t.key[i]
		nslot[p] = t.slot[i]
	}
	t.key, t.slot, t.seen, t.mask = nkey, nslot, nseen, nmask
}
