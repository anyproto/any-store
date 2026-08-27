package qplanner

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
)

// knn_plan.go is the cost-based plan selection for $knn queries.
//
// The clause's semantics are "the K nearest documents among those passing the
// residual filter". Two enforcement forms exist:
//
//   - DRIVER (approximate): the ANN index returns ef candidates in distance
//     order, the residual filters them, and the k-cut keeps the first K
//     survivors. Recall degrades as the residual gets more selective — the ef
//     candidates may contain few (or none) of the true filtered neighbours.
//   - PROBE (exact): a bounded access path (fixed pk bounds, a bounded
//     secondary index) enumerates the filter candidates, each one's stored
//     vector is scored directly (same kernel and vector source as the
//     brute-force backend), and the true K nearest survivors are kept.
//
// The probe form is exact — under a selective filter it is both faster AND
// more correct than post-filtering an ANN result, so the cost model prefers
// it whenever the candidate count is small. The k-cut, user sort, and
// pagination stack identically on both forms ("k selects, Sort orders, Limit
// paginates").

type knnCandidate struct {
	name string
	cost float64
	est  float64
	idx  *CBOIndex // nil for driver and pk-ids plans
	kind knnPlanKind
}

type knnPlanKind int

const (
	knnPlanDriver knnPlanKind = iota
	knnPlanProbeIds
	knnPlanProbeSeek
)

// buildKnnPlan enumerates the legal $knn plans, costs them, and builds the
// cheapest. Without a probe form (no DistFromDoc) or without any bounded
// access path it degenerates to the ANN driver plan.
func buildKnnPlan(params *PlanParams) *Plan {
	spec := params.Vector
	totalDocs := float64(params.TotalDocs)
	if totalDocs < 1 {
		totalDocs = 1
	}
	needFilter := params.Filter != nil && !isAllFilter(params.Filter)

	pResidual := calculateSelectivity(params.Filter, params.Indexes, totalDocs, params.FieldBounds)
	idFixed := len(params.IDBounds) > 0 && AllBoundsFixed(params.IDBounds)

	// Per-candidate exact scoring: vector extraction is zero-copy from the
	// already-parsed doc, so the kernel cost scales with the dimension.
	scoreCost := CostVecScoreBase + float64(len(spec.Query))*CostVecScoreDim

	var cands []knnCandidate

	// ---- ANN driver (always legal) -------------------------------------
	{
		ef := float64(spec.Ef)
		if ef < 1 {
			ef = 1
		}
		var searchCost, stream float64
		if spec.BruteDriver {
			// Brute-force scans and scores the whole collection; with a
			// residual it returns every doc ranked (topK=0) and the k-cut
			// consumes candidates until K survivors, without one exactly K.
			searchCost = totalDocs * (CostSeqRead + CostKnnBruteDoc + scoreCost)
			stream = totalDocs
			if !needFilter {
				stream = float64(spec.K)
			}
		} else {
			perCand := spec.SearchCostPerCand
			if perCand <= 0 {
				perCand = CostKnnSearchPerCand
			}
			searchCost = ef * perCand
			stream = ef
		}
		// The k-cut stops consumption after K residual survivors.
		consumed := stream
		if needFilter {
			if need := float64(spec.K) / max(pResidual, 0.0001); need < consumed {
				consumed = need
			}
		} else if float64(spec.K) < consumed {
			consumed = float64(spec.K)
		}
		cands = append(cands, knnCandidate{
			name: "KnnSearch", cost: searchCost + consumed*(CostDocFetch+CostFilter),
			est: min(consumed, float64(spec.K)), kind: knnPlanDriver,
		})
	}

	if spec.DistFromDoc != nil {
		finish := func(e float64) float64 {
			kept := min(e, float64(spec.K))
			return e*(CostDocFetch+CostFilter+scoreCost) + sortCost(kept)
		}
		if idFixed {
			e := float64(len(params.IDBounds))
			cands = append(cands, knnCandidate{
				name: "KnnProbeIds", cost: finish(e), est: min(e, float64(spec.K)),
				kind: knnPlanProbeIds,
			})
		}
		var fieldSelBuf [8]fieldSelEntry
		fieldSel := collectFieldSelectivity(params, totalDocs, fieldSelBuf[:0])
		for i := range params.Indexes {
			idx := &params.Indexes[i]
			if len(idx.Bounds) == 0 {
				continue
			}
			if !sparseIndexComplete(idx, params.Filter) {
				continue
			}
			e := estimateIndexDocsWithFieldSel(idx, totalDocs, fieldSel)
			if e < 1 {
				e = 1
			}
			nSeeks := float64(len(idx.Bounds))
			if nSeeks < 1 {
				nSeeks = 1
			}
			cands = append(cands, knnCandidate{
				name: "KnnProbeSeek(" + idx.Info.Name + ")", idx: idx,
				cost: nSeeks*CostIndexSeek + e*CostSeqRead + finish(e),
				est:  min(e, float64(spec.K)), kind: knnPlanProbeSeek,
			})
		}
	}

	// Hint boosts: the vector index name boosts the driver, the pk field the
	// pk probe, a secondary index name its probe.
	if len(params.IndexHints) > 0 {
		pk := params.PrimaryKey
		if pk == "" {
			pk = "id"
		}
		for ci := range cands {
			c := &cands[ci]
			hintName := spec.IndexName
			switch {
			case c.idx != nil:
				hintName = c.idx.Info.Name
			case c.kind == knnPlanProbeIds:
				hintName = pk
			}
			for _, h := range params.IndexHints {
				if h.IndexName == hintName {
					c.cost -= float64(h.Boost)
				}
			}
		}
	}

	best := &cands[0]
	for ci := 1; ci < len(cands); ci++ {
		if cands[ci].cost < best.cost {
			best = &cands[ci]
		}
	}

	var plan *Plan
	if best.kind == knnPlanDriver {
		plan = buildVectorPlan(params)
	} else {
		plan = buildKnnProbePlan(params, best)
	}
	plan.Cost = best.cost
	if !params.CountOnly {
		explainCands := make([]CandidatePlan, 0, len(cands))
		for ci := range cands {
			c := cands[ci]
			explainCands = append(explainCands, CandidatePlan{Name: c.name, Cost: c.cost, EstRows: c.est})
		}
		slices.SortFunc(explainCands, func(a, b CandidatePlan) int {
			return cmp.Compare(a.Cost, b.Cost)
		})
		plan.Explain = ExplainInfo{
			TotalDocs:   int(totalDocs),
			Selectivity: pResidual,
			Candidates:  explainCands,
			ChosenIndex: plan.IndexName,
		}
	}
	return plan
}

// buildKnnProbePlan assembles the exact pre-filter chain:
//
//	driver -> [dedup] -> Fetch -> VectorScore(residual, k-cut) -> [Sort] -> [Limit]
//
// VectorScoreIter owns the residual (it must run over _distance-decorated
// documents, exactly like the driver plan's FilterIter) and the k-cut.
func buildKnnProbePlan(params *PlanParams, cand *knnCandidate) *Plan {
	dataCS := &CursorSource{Tx: params.Tx, Ns: params.DataNs}
	indexName := params.Vector.IndexName

	var root Iterator
	if cand.kind == knnPlanProbeIds {
		root = &IdBoundsIter{Bounds: params.IDBounds}
	} else {
		idx := cand.idx
		indexName = idx.Info.Name
		if len(idx.Bounds) > 0 {
			idx.Bounds = AdjustBoundsForNonUnique(idx.Bounds)
		}
		if idx.Info.Unique && idx.fullKeyPointBound() {
			// CoverIter takes the UN-finalized bounds — it seeks the raw
			// prefix itself; the reverse-tail pad would double-pad and
			// MergeOverlappingBounds would collapse distinct point probes
			// (see buildIndexSeekChain).
			root = &CoverIter{
				Source:  &CursorSource{Tx: params.Tx, Ns: idx.Info.Ns},
				IdxInfo: idx.Info,
				Bounds:  idx.Bounds,
			}
			if len(idx.Bounds) > 1 {
				root = &DocDedupIter{Source: root}
			}
		} else {
			finalizeIndexBounds(idx)
			root = &IndexIter{
				Source:       &CursorSource{Tx: params.Tx, Ns: idx.Info.Ns},
				IdxInfo:      idx.Info,
				Bounds:       idx.Bounds,
				PointLookup:  idx.PointLookup,
				FullKeyBound: idx.fullKeyPointBound(),
				ScalarProven: idx.ScalarProven,
			}
			if !idx.ScalarProven {
				root = &DocDedupIter{Source: root}
			}
		}
	}

	root = &FetchIter{Source: root, Data: dataCS, Buf: params.Buf}
	root = &VectorScoreIter{
		Spec:   params.Vector,
		Source: root,
		Data:   dataCS,
		Buf:    params.Buf,
		Filter: params.Filter,
	}
	if params.Sorter != nil {
		root = &SortIter{
			Source: root,
			Data:   dataCS,
			Sorter: params.Sorter,
			Buf:    params.Buf,
			TopK:   sortTopK(params),
		}
	}
	if params.Limit > 0 || params.Offset > 0 {
		root = &LimitIter{Source: root, Limit: params.Limit, Offset: params.Offset}
	}

	plan := &Plan{Root: root, Name: cand.name, IndexName: indexName}
	setPlanRef(root, plan)
	return plan
}

// VectorScoreIter is the exact form of the $knn source: it drains an upstream
// document stream (FetchIter beneath — Plan.DocParsed is the current doc),
// scores each distinct document's stored vector against the query, evaluates
// the residual filter over the _distance-decorated document, and keeps the K
// nearest survivors — the clause's exact semantics. Emission is in the same
// total (distance asc, docId asc) order the ANN backends guarantee, and each
// emitted row's document is re-fetched and re-decorated so downstream stages
// (a user SortIter reading _distance, Doc()) see the driver plan's shape.
type VectorScoreIter struct {
	Spec   *VectorQuerySpec
	Source Iterator
	Data   *CursorSource
	Buf    *syncpool.DocBuffer
	Filter query.Filter
	Plan   *Plan

	inited bool
	kept   []scoredVecCand
	arena  []byte
	idx    int

	injArena anyenc.Arena
}

type scoredVecCand struct {
	off  uint32
	ln   uint32
	dist float32
}

func (it *VectorScoreIter) less(a, b scoredVecCand) bool {
	if a.dist != b.dist {
		return a.dist < b.dist
	}
	return bytes.Compare(it.arena[a.off:a.off+a.ln], it.arena[b.off:b.off+b.ln]) < 0
}

func (it *VectorScoreIter) init() error {
	it.inited = true
	if it.Spec.CheckTx != nil {
		if err := it.Spec.CheckTx(it.Data.Tx); err != nil {
			return err
		}
	}
	k := it.Spec.K
	var dedup DocDedup
	for {
		_, docId, mk, err := it.Source.Next()
		if err != nil {
			return err
		}
		if docId == nil {
			break
		}
		if !dedup.Accept(docId, mk) {
			continue
		}
		doc := docOf(it.Plan)
		if doc == nil {
			continue // defensive: the chain always has a FetchIter beneath
		}
		dist, ok := it.Spec.DistFromDoc(doc)
		if !ok {
			continue // no valid vector at the field — never an ANN candidate
		}
		// The residual runs over the decorated document, mirroring the driver
		// chain's FilterIter position (before the k-cut).
		injectDistance(&it.injArena, doc, dist)
		if it.Filter != nil && !it.Filter.Ok(doc, it.Buf) {
			continue
		}
		nc := scoredVecCand{off: uint32(len(it.arena)), ln: uint32(len(docId)), dist: dist}
		if k > 0 && len(it.kept) == k {
			// Max-heap replacement: on a tie the incumbent wins only when its
			// docId is smaller — the total order decides, not arrival.
			it.arena = append(it.arena, docId...)
			if it.less(nc, it.kept[0]) {
				it.kept[0] = nc
				it.siftDown(0)
			} else {
				it.arena = it.arena[:nc.off] // rejected: reclaim the id bytes
			}
			continue
		}
		it.arena = append(it.arena, docId...)
		it.kept = append(it.kept, nc)
		if k > 0 {
			it.siftUp(len(it.kept) - 1)
		}
	}
	slices.SortFunc(it.kept, func(a, b scoredVecCand) int {
		if it.less(a, b) {
			return -1
		}
		if it.less(b, a) {
			return 1
		}
		return 0
	})
	return nil
}

// siftUp/siftDown maintain a max-heap under less (root = worst kept).
func (it *VectorScoreIter) siftUp(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !it.less(it.kept[parent], it.kept[i]) {
			return
		}
		it.kept[parent], it.kept[i] = it.kept[i], it.kept[parent]
		i = parent
	}
}

func (it *VectorScoreIter) siftDown(i int) {
	n := len(it.kept)
	for {
		l, r := 2*i+1, 2*i+2
		worst := i
		if l < n && it.less(it.kept[worst], it.kept[l]) {
			worst = l
		}
		if r < n && it.less(it.kept[worst], it.kept[r]) {
			worst = r
		}
		if worst == i {
			return
		}
		it.kept[i], it.kept[worst] = it.kept[worst], it.kept[i]
		i = worst
	}
}

func docOf(plan *Plan) *anyenc.Value {
	if plan == nil {
		return nil
	}
	return plan.DocParsed
}

func (it *VectorScoreIter) Next() (key []byte, docId []byte, multiKey bool, err error) {
	if !it.inited {
		if err = it.init(); err != nil {
			return nil, nil, false, err
		}
	}
	for it.idx < len(it.kept) {
		c := it.kept[it.idx]
		it.idx++
		id := it.arena[c.off : c.off+c.ln]

		// Re-fetch + re-decorate: downstream stages must see the same
		// document shape the driver plan streams past its k-cut.
		it.Buf.DocBuf, err = it.Data.AppendValue(id, it.Buf.DocBuf[:0])
		if err != nil {
			return nil, nil, false, err
		}
		doc, perr := it.Buf.Parser.ParseOwned(it.Buf.DocBuf)
		if perr != nil {
			return nil, nil, false, perr
		}
		if it.Plan != nil {
			if it.Spec.NeedDistances {
				if it.Plan.Distances == nil {
					it.Plan.Distances = &FloatSidecar{}
				}
				it.Plan.Distances.Set(id, float64(c.dist))
			}
			injectDistance(&it.injArena, doc, c.dist)
			it.Plan.DocParsed = doc
		}
		return id, id, false, nil
	}
	return nil, nil, false, nil
}

func (it *VectorScoreIter) Close() {
	if it.Source != nil {
		it.Source.Close()
	}
}

func (it *VectorScoreIter) String() string {
	return fmt.Sprintf("%s -> VectorScore(k=%d)", it.Source, it.Spec.K)
}
