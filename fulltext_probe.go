package anystore

import (
	"errors"
	"math"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/fts"
	"github.com/anyproto/any-store/v2/internal/qplanner"
	"github.com/anyproto/any-store/v2/query"
)

// fulltext_probe.go implements the probe (predicate) form of a $text query:
// given ONE document id, decide whether the compiled $text predicate matches it
// and compute its BM25 score — without walking any posting list. Each probe
// costs a docmap point-get, a docinfo point-get, and one postings-chunk
// point-get per query term. The planner uses this to drive a $text query from
// a selective index or a primary-key restriction instead of the posting lists.
//
// PARITY CONTRACT: for any document, the prober must produce EXACTLY the same
// match verdict and BIT-IDENTICAL score as the driver scan (searchCandidates).
// Both sides share ftsExec.compileClauses (clause order, required bits) and
// ftsExec.bm25 (formula and float evaluation order), and this file adds each
// doc's per-clause contributions in the same order the driver's scans would
// have: plain terms in first-seen order, then groups in clause order (each
// prefix expansion in expansion order). A bounded write verb selects rows by
// (score desc, IntDocID asc), so an epsilon here is wrong rows, not noise —
// the differential test in fulltext_probe_test.go pins this.

// probeTerm is one plain term (or prefix expansion) with its precomputed IDF.
type probeTerm struct {
	term   string
	idf    float64
	df     uint64
	reqBit uint64
}

// probeGroup is one compiled phrase or prefix clause, resolved for probing:
// a prefix is already expanded to its vocabulary completions.
type probeGroup struct {
	phrase bool
	terms  []probeTerm // phrase: constituents (idf unused); prefix: expansions
	idfSum float64     // phrase: summed constituent IDF
	dead   bool        // some phrase constituent has df == 0 → can't match
	reqBit uint64
}

// ftsProber evaluates the compiled $text predicate against single documents.
// Not safe for concurrent use; one per query execution.
type ftsProber struct {
	e  ftsExec
	fx *ftsIndex
	tx *btree.ReadTx

	plain       []probeTerm
	groups      []probeGroup
	negs        []probeGroup
	requiredAll uint64
	// hasPositive is false when no positive clause can match anything (all
	// df == 0 or the query analyzed to nothing): every probe is then a miss,
	// matching the driver's empty candidate stream.
	hasPositive bool

	keyBuf    []byte
	valBuf    []byte
	fwdBuf    []byte
	mapValBuf []byte
	posBufs   [][]uint32
	posBufB   [][]uint32

	// per-Probe scoring state (fields, not closures: Probe runs once per
	// candidate in the planner's inner loop and must not allocate).
	curScore    float64
	curMask     uint64
	curMatched  bool
	curDl       float64
	curDlLoaded bool
}

// curDocLen returns the probed document's token length, loading it once.
func (p *ftsProber) curDocLen(intId uint64) (float64, error) {
	if !p.curDlLoaded {
		dl, err := p.e.docLen(intId)
		if err != nil {
			return 0, err
		}
		p.curDl = float64(dl)
		p.curDlLoaded = true
	}
	return p.curDl, nil
}

// addContribution mirrors the driver's addScore for the probed document.
func (p *ftsProber) addContribution(intId uint64, idf, tf float64, reqBit uint64) error {
	d, err := p.curDocLen(intId)
	if err != nil {
		return err
	}
	p.curScore += p.e.bm25(idf, tf, d)
	p.curMask |= reqBit
	p.curMatched = true
	return nil
}

// newFtsProber compiles the $text predicate for probing on the given snapshot.
// Corpus stats (N, avgdl) and prefix expansions are resolved against tx, so a
// prober and a driver scan on the same snapshot see identical vocabularies.
func (fx *ftsIndex) newFtsProber(tx *btree.ReadTx, text query.Text) (*ftsProber, error) {
	clauses := text.ParsedClauses()
	p := &ftsProber{fx: fx, tx: tx}

	n, err := ftsGetUint(tx, fx.nsMeta, ftsMetaCount)
	if err != nil {
		return nil, err
	}
	totalTokens, err := ftsGetUint(tx, fx.nsMeta, ftsMetaTokens)
	if err != nil {
		return nil, err
	}
	avgdl := float64(totalTokens) / float64(n)
	if avgdl == 0 || math.IsNaN(avgdl) {
		avgdl = 1
	}
	k1, b := ftsResolveBM25(fx.info.Fulltext)
	weights, weighted := ftsResolveWeights(fx.info.Fulltext, fx.info.Fields)
	p.e = ftsExec{
		tx: tx, fx: fx, n: float64(n), avgdl: avgdl, az: fts.NewAnalyzer(),
		k1: k1, b: b, nFields: fx.nFields, weights: weights, weighted: weighted,
	}
	if n == 0 || len(clauses) == 0 {
		return p, nil // hasPositive stays false: probes never match
	}

	cq := p.e.compileClauses(clauses, text.DefaultAnd)
	p.requiredAll = cq.requiredAll

	termOf := func(term string, reqBit uint64) (probeTerm, error) {
		df, derr := ftsGetUint(tx, fx.nsVocab, []byte(term))
		if derr != nil {
			return probeTerm{}, derr
		}
		pt := probeTerm{term: term, df: df, reqBit: reqBit}
		if df > 0 {
			pt.idf = math.Log(1 + (p.e.n-float64(df)+0.5)/(float64(df)+0.5))
		}
		return pt, nil
	}

	for _, term := range cq.plainOrder {
		pt, terr := termOf(term, cq.plainBit[term])
		if terr != nil {
			return nil, terr
		}
		p.plain = append(p.plain, pt)
		if pt.df > 0 {
			p.hasPositive = true
		}
	}
	buildGroup := func(g ftsClauseGroup) (probeGroup, error) {
		pg := probeGroup{reqBit: g.reqBit}
		if g.prefix {
			// Same expansion set and order as the driver's scorePrefix.
			terms, xerr := p.e.expandPrefix(g.stem)
			if xerr != nil {
				return pg, xerr
			}
			for _, t := range terms {
				pt, terr := termOf(t, g.reqBit)
				if terr != nil {
					return pg, terr
				}
				pg.terms = append(pg.terms, pt)
			}
			pg.dead = len(pg.terms) == 0
			return pg, nil
		}
		// Mirror scoreGroup's dispatch exactly: only a MULTI-token group is a
		// phrase; a quoted clause that analyzes to one token is scored by the
		// driver as a plain scanTerm (weighted TF), so the prober must route
		// it through the per-term path too.
		pg.phrase = len(g.terms) > 1
		for _, t := range g.terms {
			pt, terr := termOf(t, 0)
			if terr != nil {
				return pg, terr
			}
			if pt.df == 0 && pg.phrase {
				pg.dead = true // driver's scorePhrase: any absent term → no matches
			}
			pg.idfSum += pt.idf
			pg.terms = append(pg.terms, pt)
		}
		if !pg.phrase {
			dead := true
			for _, pt := range pg.terms {
				if pt.df > 0 {
					dead = false
				}
			}
			pg.dead = dead
		}
		return pg, nil
	}
	for _, g := range cq.posGroups {
		pg, gerr := buildGroup(g)
		if gerr != nil {
			return nil, gerr
		}
		p.groups = append(p.groups, pg)
		if !pg.dead && len(pg.terms) > 0 {
			p.hasPositive = true
		}
	}
	for _, g := range cq.negGroups {
		pg, gerr := buildGroup(g)
		if gerr != nil {
			return nil, gerr
		}
		p.negs = append(p.negs, pg)
	}
	return p, nil
}

// termTF returns the term's frequency in document intId (the weighted
// pseudo-frequency under BM25F, matching scanTerm's weighted branch) and
// whether a posting for the document EXISTS. found is the driver's semantic
// trigger: scanTerm scores/tombstones/sets required bits for every posting it
// visits regardless of the computed tf (a zero-weight field yields tf == 0
// with found == true), so callers must gate on found, never on tf > 0.
// Positions, when dst is non-nil, are decoded into *dst for phrase adjacency.
func (p *ftsProber) termTF(term string, intId uint64, dst *[]uint32) (tf float64, found bool, err error) {
	p.keyBuf = postingsKey(p.keyBuf, term, fts.ChunkID(intId))
	p.valBuf, err = p.tx.AppendValue(p.fx.nsPost, p.keyBuf, p.valBuf[:0])
	if err != nil {
		if errors.Is(err, btree.ErrKeyNotFound) {
			return 0, false, nil
		}
		return 0, false, err
	}
	it, rerr := p.e.chunkReader(p.valBuf)
	if rerr != nil {
		return 0, false, ftsMapFormatErr(rerr)
	}
	for it.Next() {
		docID := it.DocID()
		if docID > intId {
			break
		}
		if docID != intId {
			continue
		}
		if p.e.weighted {
			for f := 0; f < p.e.nFields; f++ {
				if w := p.e.weights[f]; w != 0 {
					if ftf := it.FieldTF(f); ftf != 0 {
						tf += w * float64(ftf)
					}
				}
			}
		} else {
			tf = float64(it.TF())
		}
		if dst != nil {
			*dst = it.AppendPositions((*dst)[:0])
		}
		if it.Err() != nil {
			return 0, false, it.Err()
		}
		return tf, true, nil
	}
	return 0, false, it.Err()
}

// phrasePresent reports the phrase's adjacency-confirmed occurrence count in
// intId (0 when any constituent is absent or no adjacent run exists).
func (p *ftsProber) phraseCount(g *probeGroup, intId uint64) (uint32, error) {
	for len(p.posBufs) < len(g.terms) {
		p.posBufs = append(p.posBufs, nil)
	}
	lists := p.posBufB[:0]
	for i, t := range g.terms {
		_, found, err := p.termTF(t.term, intId, &p.posBufs[i])
		if err != nil {
			return 0, err
		}
		// Presence, not tf: the driver's scorePhrase matches on positions
		// alone and never consults the (possibly zero-weighted) tf.
		if !found {
			p.posBufB = lists
			return 0, nil
		}
		lists = append(lists, p.posBufs[i])
	}
	p.posBufB = lists
	return countAdjacent(lists), nil
}

// Probe evaluates the $text predicate for one document. ok reports a match;
// score is the document's BM25 score and intId its stable IntDocID (the
// driver-order tie-break key). docId is the document's data-namespace key.
func (p *ftsProber) Probe(docId []byte) (score float64, intId uint64, ok bool, err error) {
	if !p.hasPositive {
		return 0, 0, false, nil
	}
	p.fwdBuf = ftsMapForwardKey(p.fwdBuf, docId)
	var found bool
	intId, found, p.mapValBuf, err = ftsGetUintOkBuf(p.tx, p.fx.nsMap, p.fwdBuf, p.mapValBuf)
	if err != nil || !found {
		return 0, 0, false, err
	}
	p.curScore, p.curMask, p.curMatched, p.curDlLoaded = 0, 0, false, false

	// Positive contributions, in the driver's exact order: plain terms
	// (first-seen order), then groups (each prefix expansion in order).
	for i := range p.plain {
		pt := &p.plain[i]
		if pt.df == 0 {
			continue
		}
		tf, hit, terr := p.termTF(pt.term, intId, nil)
		if terr != nil {
			return 0, 0, false, terr
		}
		if hit {
			// Presence-gated like scanTerm: a zero-weight-field posting still
			// enters the doc (score += 0) and sets the required bit.
			if err = p.addContribution(intId, pt.idf, tf, pt.reqBit); err != nil {
				return 0, 0, false, err
			}
		}
	}
	for gi := range p.groups {
		g := &p.groups[gi]
		if g.dead {
			continue
		}
		if g.phrase {
			cnt, cerr := p.phraseCount(g, intId)
			if cerr != nil {
				return 0, 0, false, cerr
			}
			if cnt > 0 {
				if err = p.addContribution(intId, g.idfSum, float64(cnt), g.reqBit); err != nil {
					return 0, 0, false, err
				}
			}
			continue
		}
		for ti := range g.terms {
			pt := &g.terms[ti]
			if pt.df == 0 {
				continue
			}
			tf, hit, terr := p.termTF(pt.term, intId, nil)
			if terr != nil {
				return 0, 0, false, terr
			}
			if hit {
				if err = p.addContribution(intId, pt.idf, tf, g.reqBit); err != nil {
					return 0, 0, false, err
				}
			}
		}
	}
	if !p.curMatched {
		return 0, 0, false, nil
	}

	// Negated clauses: any match excludes the doc (the driver's tombstone).
	for gi := range p.negs {
		g := &p.negs[gi]
		if g.dead {
			continue
		}
		if g.phrase {
			cnt, cerr := p.phraseCount(g, intId)
			if cerr != nil {
				return 0, 0, false, cerr
			}
			if cnt > 0 {
				return 0, 0, false, nil
			}
			continue
		}
		for ti := range g.terms {
			pt := &g.terms[ti]
			if pt.df == 0 {
				continue
			}
			_, hit, terr := p.termTF(pt.term, intId, nil)
			if terr != nil {
				return 0, 0, false, terr
			}
			if hit {
				return 0, 0, false, nil
			}
		}
	}

	// Required-clause gate (the driver's appendTo mask check).
	if p.requiredAll != 0 && p.curMask&p.requiredAll != p.requiredAll {
		return 0, 0, false, nil
	}
	return p.curScore, intId, true, nil
}

func (p *ftsProber) Close() {}

// ftsPhraseMatchFraction estimates what fraction of documents containing every
// phrase constituent also contain them adjacently. Coarse by design: it prices
// the plan-time yield estimate, never correctness.
const ftsPhraseMatchFraction = 0.25

// planStats compiles the predicate and reads the per-term document frequencies
// (one vocab point-get per term; a prefix pays one bounded vocab scan — the
// same reads the chosen plan repeats at execution). SumDF and ProbeTerms are
// exact; only EstMatches is an estimate.
func (fx *ftsIndex) planStats(tx *btree.ReadTx, text query.Text) (st qplanner.FtsPlanStats, err error) {
	clauses := text.ParsedClauses()
	if len(clauses) == 0 {
		return st, nil
	}
	n, err := ftsGetUint(tx, fx.nsMeta, ftsMetaCount)
	if err != nil || n == 0 {
		return st, err
	}
	st.CorpusDocs = float64(n)
	e := ftsExec{tx: tx, fx: fx, az: fts.NewAnalyzer(), nFields: fx.nFields}
	cq := e.compileClauses(clauses, text.DefaultAnd)

	df := func(term string) (float64, error) {
		v, derr := ftsGetUint(tx, fx.nsVocab, []byte(term))
		return float64(v), derr
	}

	// Per-clause match estimates: OR-combined for should clauses, intersected
	// (independence) for required ones.
	orSum := 0.0  // Σ df over non-required positive clauses
	andSel := 1.0 // ∏ (df_i / N) over required clauses
	hasAnd := false

	account := func(clauseDF float64, required bool) {
		if required {
			hasAnd = true
			andSel *= min(clauseDF/float64(n), 1.0)
		} else {
			orSum += clauseDF
		}
	}

	for _, term := range cq.plainOrder {
		d, derr := df(term)
		if derr != nil {
			return st, derr
		}
		st.SumDF += d
		st.ProbeTerms++
		account(d, cq.plainBit[term] != 0)
	}
	groupStats := func(g ftsClauseGroup, positive bool) error {
		if g.prefix {
			terms, xerr := e.expandPrefix(g.stem)
			if xerr != nil {
				return xerr
			}
			var sum float64
			for _, t := range terms {
				d, derr := df(t)
				if derr != nil {
					return derr
				}
				sum += d
				st.SumDF += d
				st.ProbeTerms++
			}
			if positive {
				account(sum, g.reqBit != 0)
			}
			return nil
		}
		// Phrase: the zig-zag advances every stream at most min-df times.
		minDF := math.MaxFloat64
		for _, t := range g.terms {
			d, derr := df(t)
			if derr != nil {
				return derr
			}
			minDF = min(minDF, d)
			st.ProbeTerms++
		}
		if minDF == math.MaxFloat64 {
			minDF = 0
		}
		st.SumDF += minDF * float64(len(g.terms))
		if positive {
			account(minDF*ftsPhraseMatchFraction, g.reqBit != 0)
		}
		return nil
	}
	for _, g := range cq.posGroups {
		if gerr := groupStats(g, true); gerr != nil {
			return st, gerr
		}
	}
	for _, g := range cq.negGroups {
		if gerr := groupStats(g, false); gerr != nil {
			return st, gerr
		}
	}

	est := min(orSum, float64(n))
	if hasAnd {
		// Required clauses bound the yield multiplicatively; a pure-AND query
		// has no OR contribution.
		andEst := float64(n) * andSel
		if orSum > 0 {
			est = min(est, andEst)
		} else {
			est = andEst
		}
	}
	st.EstMatches = max(est, 1)
	st.Valid = true
	return st, nil
}
