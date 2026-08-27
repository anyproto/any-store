package qplanner

import (
	"fmt"
	"slices"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/query"
)

// fts_probe_iter.go holds the probe form of a $text query: instead of the
// posting lists driving the query (FtsIter), another access path — a
// primary-key restriction or a secondary index — enumerates candidate docIds
// and each candidate is verified against the text index individually. The
// per-candidate cost is a few point-gets instead of a page fetch, and the
// posting lists are never walked. Plan selection between the two forms is the
// CBO's cost comparison in buildTextPlan.

// FtsProber verifies the $text predicate for single documents. Implemented by
// the anystore layer (it owns the fts namespaces); the contract mirrors
// FtsSearchFunc. Probe must return the same verdict and a bit-identical score
// to what the driver scan would produce for the same document on the same
// snapshot — intId is the document's stable IntDocID, the driver's tie-break
// key for equal scores.
type FtsProber interface {
	Probe(docId []byte) (score float64, intId uint64, ok bool, err error)
	Close()
}

// FtsProbeFunc opens a prober on the given read tx. A nil FtsProbeFunc on the
// spec means the probe form is unavailable and only the driver plan is legal.
type FtsProbeFunc func(tx *btree.ReadTx) (FtsProber, error)

// FtsStatsFunc computes the predicate's plan-time stats on the plan's read tx.
type FtsStatsFunc func(tx *btree.ReadTx) (FtsPlanStats, error)

// FtsPlanStats are the plan-time cost inputs of the $text predicate, filled by
// the anystore layer from the index's exact per-term document frequencies.
type FtsPlanStats struct {
	// SumDF is the posting count the driver scan must accumulate.
	SumDF float64
	// ProbeTerms is the number of postings-chunk point-gets one probe costs.
	ProbeTerms float64
	// EstMatches estimates how many documents match the predicate.
	EstMatches float64
	// CorpusDocs is the fts corpus document count (meta N) — the TotalDocs
	// fallback when no secondary-index sketch published one.
	CorpusDocs float64
	// Valid gates cost-based probe selection; false (unit tests, stats read
	// failure) keeps the driver-only behavior.
	Valid bool
}

// FtsProbeIter filters an upstream candidate stream through the $text
// predicate.
//
// Two modes:
//   - streaming (Rank == false): each upstream row is probed as it is pulled;
//     misses are dropped, hits pass through unchanged (key and multiKey
//     preserved, so downstream dedup stages keep working). Upstream order is
//     preserved — used when a real-field sort (or no ordering need) makes the
//     relevance order irrelevant.
//   - rank (Rank == true): the upstream is drained (deduped by docId), every
//     distinct candidate probed, and the matches re-emitted in the driver's
//     exact order — (score desc, IntDocID asc) — so a relevance-ordered probe
//     plan is row- and order-identical to the FtsIter plan.
//
// Matched scores are recorded in Plan.Scores keyed by docId; the sidecar is
// created unconditionally for probe plans because the downstream
// ScoreInjectIter reads it to decorate fetched documents with _score.
type FtsProbeIter struct {
	Spec   *FtsQuerySpec
	Source Iterator
	Tx     *btree.ReadTx
	Plan   *Plan
	Rank   bool

	prober FtsProber
	inited bool

	// rank state: docId bytes live in arena, entries reference spans.
	entries []probeEntry
	arena   []byte
	idx     int
}

type probeEntry struct {
	off   uint32
	ln    uint32
	intId uint64
	score float64
}

func (it *FtsProbeIter) init() error {
	it.inited = true
	prober, err := it.Spec.Probe(it.Tx)
	if err != nil {
		return err
	}
	it.prober = prober
	if it.Plan != nil && it.Plan.Scores == nil {
		it.Plan.Scores = &FloatSidecar{}
	}
	if !it.Rank {
		return nil
	}
	// Rank mode: drain, probe distinct candidates, order like the driver.
	var dedup DocDedup
	for {
		_, docId, mk, serr := it.Source.Next()
		if serr != nil {
			return serr
		}
		if docId == nil {
			break
		}
		if !dedup.Accept(docId, mk) {
			continue
		}
		score, intId, ok, perr := it.prober.Probe(docId)
		if perr != nil {
			return perr
		}
		if !ok {
			continue
		}
		off := uint32(len(it.arena))
		it.arena = append(it.arena, docId...)
		it.entries = append(it.entries, probeEntry{off: off, ln: uint32(len(docId)), intId: intId, score: score})
	}
	slices.SortFunc(it.entries, func(a, b probeEntry) int {
		if a.score > b.score {
			return -1
		}
		if a.score < b.score {
			return 1
		}
		if a.intId < b.intId {
			return -1
		}
		if a.intId > b.intId {
			return 1
		}
		return 0
	})
	return nil
}

func (it *FtsProbeIter) Next() (key []byte, docId []byte, multiKey bool, err error) {
	if !it.inited {
		if err = it.init(); err != nil {
			return nil, nil, false, err
		}
	}
	if it.Rank {
		if it.idx >= len(it.entries) {
			return nil, nil, false, nil
		}
		e := it.entries[it.idx]
		it.idx++
		id := it.arena[e.off : e.off+e.ln]
		if it.Plan != nil {
			it.Plan.Scores.Set(id, e.score)
		}
		return id, id, false, nil
	}
	for {
		key, docId, multiKey, err = it.Source.Next()
		if err != nil || docId == nil {
			return nil, nil, false, err
		}
		score, _, ok, perr := it.prober.Probe(docId)
		if perr != nil {
			return nil, nil, false, perr
		}
		if !ok {
			continue
		}
		if it.Plan != nil {
			it.Plan.Scores.Set(docId, score)
		}
		return key, docId, multiKey, nil
	}
}

func (it *FtsProbeIter) Close() {
	if it.prober != nil {
		it.prober.Close()
		it.prober = nil
	}
	if it.Source != nil {
		it.Source.Close()
	}
}

func (it *FtsProbeIter) String() string {
	if it.Rank {
		return fmt.Sprintf("%s -> FtsProbe(rank)", it.Source)
	}
	return fmt.Sprintf("%s -> FtsProbe", it.Source)
}

// ScoreInjectIter decorates fetched documents with the _score virtual field
// from Plan.Scores, so the residual filter and Doc() see the same document
// shape a driver (FtsIter) plan produces. Sits between FetchIter and
// FilterIter in probe plans.
type ScoreInjectIter struct {
	Source Iterator
	Plan   *Plan

	arena anyenc.Arena
}

func (it *ScoreInjectIter) Next() (key []byte, docId []byte, multiKey bool, err error) {
	key, docId, multiKey, err = it.Source.Next()
	if err != nil || docId == nil {
		return nil, nil, false, err
	}
	if it.Plan != nil && it.Plan.DocParsed != nil && it.Plan.Scores != nil {
		if s, ok := it.Plan.Scores.Get(docId); ok {
			injectScore(&it.arena, it.Plan.DocParsed, s)
		}
	}
	return key, docId, multiKey, nil
}

func (it *ScoreInjectIter) Close() {
	if it.Source != nil {
		it.Source.Close()
	}
}

func (it *ScoreInjectIter) String() string {
	return fmt.Sprintf("%s -> ScoreInject", it.Source)
}

// IdBoundsIter is the leaf of a primary-key probe plan: it emits the fixed
// primary-key bounds as candidate docIds, in bound order, without touching any
// namespace. The bounds over-approximate the pk predicate (SortAndMerge'd
// point set), and non-existent ids cost the downstream probe a single failed
// docmap point-get — the residual FilterIter still decides every row.
// Requires AllBoundsFixed(Bounds).
type IdBoundsIter struct {
	Bounds query.Bounds
	i      int
}

func (it *IdBoundsIter) Next() (key []byte, docId []byte, multiKey bool, err error) {
	if it.i >= len(it.Bounds) {
		return nil, nil, false, nil
	}
	id := it.Bounds[it.i].Start
	it.i++
	return id, id, false, nil
}

func (it *IdBoundsIter) Close() {}

func (it *IdBoundsIter) String() string {
	return fmt.Sprintf("IdBounds(%d)", len(it.Bounds))
}
