package qplanner

import (
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/syncpool"
)

// ScoreField is the virtual field name under which a full-text query exposes the
// per-document BM25 relevance score. It is injected into the in-flight parsed
// document so the existing FilterIter/SortIter can read it like any field, and
// is surfaced on the public iterator via Score(). It is the FTS analog of
// DistanceField for vector queries.
const ScoreField = "_score"

// FtsCandidate is one ranked full-text result handed to the planner by the
// search closure: the document's data-namespace key and its BM25 score.
type FtsCandidate struct {
	DocId []byte
	Score float64
}

// FtsSearchFunc runs the full-text search on the given read tx and returns the
// matched documents ranked by score. Supplied by the anystore layer so qplanner
// stays free of any fts/analyzer dependency (mirrors VectorSearchFunc).
type FtsSearchFunc func(tx *btree.ReadTx) ([]FtsCandidate, error)

// FtsQuerySpec directs BuildPlan to build a full-text-search plan. When present
// it is the SOLE source — all range-index/CBO selection is bypassed (a $text
// predicate must drive the query).
type FtsQuerySpec struct {
	Search FtsSearchFunc
	// Ordered is true when Search returns candidates already sorted by score
	// descending (always the case for BM25). The planner can then skip the
	// SortIter for the default relevance order and stream straight to LimitIter;
	// an explicit sort on a real field still inserts a SortIter.
	Ordered bool
}

// FtsIter is the source iterator for a full-text query. On first Next it runs
// the search, then streams candidates: for each it fetches+parses the document,
// injects the _score virtual field, and records the score in Plan.Scores (so a
// post-sort re-fetch and the public Score() still work). Mirrors VectorIter.
type FtsIter struct {
	Spec *FtsQuerySpec
	Data *CursorSource
	Buf  *syncpool.DocBuffer
	Plan *Plan

	arena      anyenc.Arena
	candidates []FtsCandidate
	idx        int
	inited     bool
}

func (it *FtsIter) Next() (key []byte, docId []byte, multiKey bool, err error) {
	if !it.inited {
		it.inited = true
		it.candidates, err = it.Spec.Search(it.Data.Tx)
		if err != nil {
			return nil, nil, false, err
		}
		if it.Plan != nil && it.Plan.Scores == nil {
			it.Plan.Scores = make(map[string]float64, len(it.candidates))
		}
		for _, c := range it.candidates {
			if it.Plan != nil {
				it.Plan.Scores[string(c.DocId)] = c.Score
			}
		}
	}

	for it.idx < len(it.candidates) {
		c := it.candidates[it.idx]
		it.idx++

		it.Buf.DocBuf, err = it.Data.AppendValue(c.DocId, it.Buf.DocBuf[:0])
		if err != nil {
			if err == btree.ErrKeyNotFound {
				continue // doc gone since the search snapshot — skip
			}
			return nil, nil, false, err
		}
		doc, perr := it.Buf.Parser.ParseOwned(it.Buf.DocBuf)
		if perr != nil {
			return nil, nil, false, perr
		}
		if it.Plan != nil {
			injectScore(&it.arena, doc, c.Score)
			it.Plan.DocParsed = doc
		}
		return c.DocId, c.DocId, false, nil
	}
	return nil, nil, false, nil
}

// injectScore sets the _score virtual field on the in-flight document. The arena
// is reset each call: the previous candidate's doc has already been consumed
// downstream by the time the next candidate is read (SortIter copies its sort
// key out; FilterIter evaluates synchronously).
func injectScore(arena *anyenc.Arena, doc *anyenc.Value, score float64) {
	arena.Reset()
	doc.Set(ScoreField, arena.NewNumberFloat64(score))
}

func (it *FtsIter) Close() {}

func (it *FtsIter) String() string { return "FtsSearch" }
