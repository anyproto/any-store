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

// FtsCandidateStream yields ranked full-text candidates one at a time, in
// descending-score order. The returned DocId is only valid until the next
// Next call (consumers copy what they retain), which lets the search keep all
// per-candidate state in pooled buffers: a query's allocation cost is
// O(candidates consumed), not O(documents matched). Close releases the pooled
// search state and must be called exactly once.
type FtsCandidateStream interface {
	Next() (docId []byte, score float64, ok bool, err error)
	Close()
}

// FtsSearchFunc runs the full-text search on the given read tx and returns a
// stream of matched documents ranked by score. Supplied by the anystore layer
// so qplanner stays free of any fts/analyzer dependency (mirrors
// VectorSearchFunc). A nil stream means no matches.
type FtsSearchFunc func(tx *btree.ReadTx) (FtsCandidateStream, error)

// FtsQuerySpec directs BuildPlan to build a full-text-search plan. When present
// it is the SOLE source — all range-index/CBO selection is bypassed (a $text
// predicate must drive the query).
type FtsQuerySpec struct {
	Search FtsSearchFunc
	// IndexName is the driving full-text index, reported by Explain.
	IndexName string
	// Ordered is true when Search streams candidates already sorted by score
	// descending (always the case for BM25). The planner can then skip the
	// SortIter for the default relevance order and stream straight to LimitIter;
	// an explicit sort on a real field still inserts a SortIter.
	Ordered bool
	// NeedScores is true when the per-document score sidecar must be kept for
	// the public iterator's Score(). Count/Update/Delete plans never read
	// scores, so they skip the sidecar entirely.
	NeedScores bool
}

// FtsIter is the source iterator for a full-text query. On first Next it runs
// the search, then streams candidates: for each it fetches+parses the document,
// injects the _score virtual field, and records the score in Plan.Scores (so a
// post-sort re-fetch and the public Score() still work). Mirrors VectorIter.
// Because candidates stream lazily, a Limit-cut relevance query only ever pays
// materialization cost for the candidates it consumes.
type FtsIter struct {
	Spec *FtsQuerySpec
	Data *CursorSource
	Buf  *syncpool.DocBuffer
	Plan *Plan

	arena  anyenc.Arena
	stream FtsCandidateStream
	inited bool
}

func (it *FtsIter) Next() (key []byte, docId []byte, multiKey bool, err error) {
	if !it.inited {
		it.inited = true
		it.stream, err = it.Spec.Search(it.Data.Tx)
		if err != nil {
			return nil, nil, false, err
		}
		if it.stream != nil && it.Plan != nil && it.Plan.Scores == nil && it.Spec.NeedScores {
			it.Plan.Scores = &FloatSidecar{}
		}
	}
	if it.stream == nil {
		return nil, nil, false, nil
	}

	for {
		cDocId, score, ok, serr := it.stream.Next()
		if serr != nil {
			return nil, nil, false, serr
		}
		if !ok {
			return nil, nil, false, nil
		}

		it.Buf.DocBuf, err = it.Data.AppendValue(cDocId, it.Buf.DocBuf[:0])
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
			if it.Plan.Scores != nil {
				it.Plan.Scores.Set(cDocId, score)
			}
			injectScore(&it.arena, doc, score)
			it.Plan.DocParsed = doc
		}
		return cDocId, cDocId, false, nil
	}
}

// injectScore sets the _score virtual field on the in-flight document. The arena
// is reset each call: the previous candidate's doc has already been consumed
// downstream by the time the next candidate is read (SortIter copies its sort
// key out; FilterIter evaluates synchronously).
func injectScore(arena *anyenc.Arena, doc *anyenc.Value, score float64) {
	arena.Reset()
	doc.Set(ScoreField, arena.NewNumberFloat64(score))
}

func (it *FtsIter) Close() {
	if it.stream != nil {
		it.stream.Close()
		it.stream = nil
	}
}

func (it *FtsIter) String() string { return "FtsSearch" }
