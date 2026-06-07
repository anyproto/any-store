package qplanner

import (
	"fmt"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/syncpool"
)

// DistanceField is the virtual field name under which a vector query exposes the
// per-document distance. It is injected into the in-flight parsed document so
// the existing FilterIter/SortIter can read it like any field, and is surfaced
// on the public iterator via Distance().
const DistanceField = "_distance"

// VectorCandidate is one ANN result handed to the planner by the search closure.
type VectorCandidate struct {
	DocId    []byte
	Distance float32
}

// VectorSearchFunc runs the ANN search on the given read tx and returns up to ef
// candidates. Supplied by the anystore layer so qplanner stays free of any
// vindex dependency.
type VectorSearchFunc func(tx *btree.ReadTx, query []float32, ef int) ([]VectorCandidate, error)

// VectorQuerySpec directs BuildPlan to build a vector-search plan. When present
// it is the SOLE source — all range-index/CBO selection is bypassed.
type VectorQuerySpec struct {
	Query  []float32
	Ef     int
	Search VectorSearchFunc
	// Ordered is true when Search returns candidates already sorted closest-first
	// (the ANN path). The planner can then skip the SortIter for the default
	// distance-ascending order. Brute-force search leaves it false (its candidates
	// come back in document order), so a distance SortIter is always applied.
	Ordered bool
}

// VectorIter is the source iterator for a vector query. On first Next it runs
// the ANN search, then streams candidates: for each it fetches+parses the
// document, injects the _distance virtual field, and records the distance in
// Plan.Distances (so a post-sort re-fetch and the public Distance() still work).
type VectorIter struct {
	Spec *VectorQuerySpec
	Data *CursorSource
	Buf  *syncpool.DocBuffer
	Plan *Plan

	arena      anyenc.Arena
	candidates []VectorCandidate
	idx        int
	inited     bool
}

func (it *VectorIter) Next() (key []byte, docId []byte, multiKey bool, err error) {
	if !it.inited {
		it.inited = true
		it.candidates, err = it.Spec.Search(it.Data.Tx, it.Spec.Query, it.Spec.Ef)
		if err != nil {
			return nil, nil, false, err
		}
		if it.Plan != nil && it.Plan.Distances == nil {
			it.Plan.Distances = make(map[string]float32, len(it.candidates))
		}
		for _, c := range it.candidates {
			if it.Plan != nil {
				it.Plan.Distances[string(c.DocId)] = c.Distance
			}
		}
	}

	for it.idx < len(it.candidates) {
		c := it.candidates[it.idx]
		it.idx++

		it.Buf.DocBuf, err = it.Data.AppendValue(c.DocId, it.Buf.DocBuf[:0])
		if err != nil {
			if err == btree.ErrKeyNotFound {
				continue // doc gone since the index entry — skip
			}
			return nil, nil, false, err
		}
		doc, perr := it.Buf.Parser.ParseOwned(it.Buf.DocBuf)
		if perr != nil {
			return nil, nil, false, perr
		}
		if it.Plan != nil {
			injectDistance(&it.arena, doc, c.Distance)
			it.Plan.DocParsed = doc
		}
		return c.DocId, c.DocId, false, nil
	}
	return nil, nil, false, nil
}

// injectDistance sets the _distance virtual field on the in-flight document. The
// arena is reset each call: the previous candidate's doc has already been
// consumed downstream (SortIter copies the sort key into its own arena, and
// FilterIter evaluates synchronously) by the time the next candidate is read.
func injectDistance(arena *anyenc.Arena, doc *anyenc.Value, dist float32) {
	arena.Reset()
	doc.Set(DistanceField, arena.NewNumberFloat64(float64(dist)))
}

func (it *VectorIter) Close() {}

func (it *VectorIter) String() string {
	return fmt.Sprintf("VectorSearch(ef=%d)", it.Spec.Ef)
}
