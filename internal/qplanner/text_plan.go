package qplanner

import (
	"cmp"
	"fmt"
	"slices"
)

// text_plan.go is the cost-based plan selection for $text queries. The $text
// predicate can be enforced two ways:
//
//   - DRIVER: FtsIter walks the posting lists and streams candidates in
//     relevance order; everything else is a residual filter. Cost is O(Σ df) —
//     the length of the involved posting lists — regardless of how selective
//     the rest of the query is.
//   - PROBE: another access path (fixed primary-key bounds, or a bounded /
//     sort-covering secondary index) enumerates candidates and each one is
//     verified against the text index individually (FtsProbeIter). Cost scales
//     with the candidate count, not the posting lists.
//
// The two forms select exactly the same documents (the prober is
// score-bit-identical to the driver by contract), so choosing between them is
// a pure cost decision fed by exact per-term document frequencies
// (FtsPlanStats) and the same sketch/interpolation estimates the ordinary CBO
// uses. No shape rules: a query where the restriction is broad prices the
// probe form out naturally.
//
// Ordering: when the query wants relevance order (no real-field sort) and the
// order is observable (Iter, or a bounded write selecting rows by it), a probe
// plan runs the FtsProbeIter in rank mode, reproducing the driver's exact
// (score desc, IntDocID asc) order — bounded writes then modify identical rows
// under either plan. When order is irrelevant (Count, unbounded writes) or an
// explicit sort reorders anyway, the probe streams.

// textCandidate is one enumerated $text plan alternative.
type textCandidate struct {
	name    string
	cost    float64
	estRows float64
	idx     *CBOIndex // nil for driver and pk-ids plans
	kind    textPlanKind
}

type textPlanKind int

const (
	textPlanDriver textPlanKind = iota
	textPlanProbeIds
	textPlanProbeSeek
	textPlanProbeScan
)

// buildTextPlan enumerates the legal $text plans, costs them, and builds the
// cheapest. With no usable probe form (no Probe func, no stats, no candidate
// access paths) it degenerates to the driver plan — the pre-CBO behavior.
func buildTextPlan(params *PlanParams) *Plan {
	spec := params.Fts
	totalDocs := float64(params.TotalDocs)
	if spec.Stats.CorpusDocs > totalDocs {
		totalDocs = spec.Stats.CorpusDocs
	}
	if totalDocs < 1 {
		totalDocs = 1
	}
	needSort := params.Sorter != nil
	needFilter := params.Filter != nil && !isAllFilter(params.Filter)

	canProbe := spec.Probe != nil && spec.Stats.Valid
	stats := spec.Stats

	// selText: fraction of the collection matching the $text predicate.
	selText := stats.EstMatches / totalDocs
	if selText <= 0 {
		selText = 0.0001
	}
	if selText > 1 {
		selText = 1
	}

	// pResidual: combined selectivity of the residual (non-text) predicates,
	// from the same estimator the ordinary CBO uses.
	pResidual := calculateSelectivity(params.Filter, params.Indexes, totalDocs, params.FieldBounds)

	// Fixed primary-key bounds bound both the driver's gate and the pk probe.
	idFixed := len(params.IDBounds) > 0 && AllBoundsFixed(params.IDBounds)

	// idScope: fraction of the collection the pk bounds admit (1 when
	// unbounded or non-fixed — the gate still applies at runtime, but the
	// cost model has no estimate for a pk range here).
	idScope := 1.0
	if idFixed {
		idScope = float64(len(params.IDBounds)) / totalDocs
		if idScope > 1 {
			idScope = 1
		}
	}
	// pOther: residual selectivity excluding the pk restriction (candidates
	// that already satisfy the pk bounds still face the rest of the residual).
	pOther := pResidual
	if idScope > 0 && idScope < 1 {
		pOther = pResidual / idScope
	}
	if pOther > 1 {
		pOther = 1
	}
	if pOther <= 0 {
		pOther = 0.0001
	}

	limitN := float64(params.Limit + params.Offset)

	// Rank mode: relevance order must be reproduced by a probe plan — the
	// order is observable (public iterator, or a bounded write selecting rows
	// by it). Count and unbounded writes are set-invariant and stream.
	rankMode := params.Sorter == nil && !params.CountOnly &&
		(params.Limit > 0 || params.Offset > 0 || spec.NeedScores)

	probeDocCost := CostFtsProbeDoc + stats.ProbeTerms*CostFtsProbeTerm

	var cands []textCandidate

	// ---- Driver plan (always legal) ------------------------------------
	{
		accCost := stats.SumDF * CostFtsPosting
		gated := stats.EstMatches * idScope // candidates surviving the id gate
		if gated < 1 {
			gated = 1
		}
		consumed := gated
		if params.Sorter == nil && !params.CountOnly && params.Limit > 0 {
			// Relevance order streams; the limit cuts materialization.
			if need := limitN / pOther; need < consumed {
				consumed = need
			}
		}
		cost := accCost + consumed*(CostDocFetch+CostFilter)
		if needSort {
			yield := gated * pOther
			cost += sortCost(yield) + yield*CostMaterialize
		}
		cands = append(cands, textCandidate{
			name: "FtsSearch", cost: cost, estRows: gated, kind: textPlanDriver,
		})
	}

	if canProbe {
		// probeCost returns the cost of probing e candidates and finishing
		// the query from the survivors. fetchAll forces charging a fetch for
		// every match (no limit cut available).
		finishCost := func(e float64, orderedOut bool) float64 {
			matches := e * selText
			if matches < 1 {
				matches = 1
			}
			fetchN := matches
			if params.CountOnly && !needFilter {
				fetchN = 0
			} else if orderedOut && params.Limit > 0 {
				// Output already in final order: the limit stops the fetches.
				if need := limitN / pOther; need < fetchN {
					fetchN = need
				}
			}
			cost := e*probeDocCost + fetchN*(CostDocFetch+CostFilter)
			if needSort {
				cost += sortCost(matches*pOther) + matches*pOther*CostMaterialize
			}
			return cost
		}

		// ---- Probe from fixed pk bounds --------------------------------
		if idFixed {
			e := float64(len(params.IDBounds))
			cands = append(cands, textCandidate{
				name: "FtsProbeIds", cost: finishCost(e, rankMode), estRows: e * selText,
				kind: textPlanProbeIds,
			})
		}

		// ---- Probe from bounded secondary indexes ----------------------
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
			access := nSeeks*CostIndexSeek + e*CostSeqRead
			orderedOut := rankMode || (needSort && idx.ExactSort)
			cands = append(cands, textCandidate{
				name: "FtsProbeSeek(" + idx.Info.Name + ")", idx: idx,
				cost:    access + finishCost(e, orderedOut),
				estRows: e * selText, kind: textPlanProbeSeek,
			})
		}

		// ---- Probe along a sort-covering index scan --------------------
		// The scan streams in final order, so with a LIMIT only
		// (L+O)/(pass rate) entries are visited and probed.
		if needSort {
			for i := range params.Indexes {
				idx := &params.Indexes[i]
				if !idx.ExactSort {
					continue
				}
				if !sparseIndexComplete(idx, params.Filter) {
					continue
				}
				idxSel := selectivityForIndex(idx, totalDocs)
				scanSel := pResidual / idxSel
				if scanSel > 1 {
					scanSel = 1
				}
				if scanSel <= 0 {
					scanSel = 0.0001
				}
				scanPop := totalDocs
				if len(idx.Bounds) > 0 {
					scanPop = totalDocs * idxSel
					if scanPop < 1 {
						scanPop = 1
					}
				}
				s := scanPop
				if params.Limit > 0 {
					s = limitN / (scanSel * selText)
					if s > scanPop {
						s = scanPop
					}
					if s < 1 {
						s = 1
					}
				}
				cost := s*(CostIndexSeek+probeDocCost) + s*selText*(CostDocFetch+CostFilter)
				cands = append(cands, textCandidate{
					name: "FtsProbeScan(" + idx.Info.Name + ")", idx: idx,
					cost: cost, estRows: s * selText * scanSel, kind: textPlanProbeScan,
				})
			}
		}
	}

	// Hint boosts, by index name: the fts index name boosts the driver, a
	// secondary index name its probe plans, and the primary-key field name
	// the pk probe plan.
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
			case c.kind == textPlanProbeIds:
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

	// ---- Build the chosen chain ----------------------------------------
	var plan *Plan
	switch best.kind {
	case textPlanDriver:
		plan = buildFtsPlan(params)
	default:
		plan = buildTextProbePlan(params, best, rankMode)
	}

	plan.Cost = best.cost
	if !params.CountOnly {
		explainCands := make([]CandidatePlan, 0, len(cands))
		for ci := range cands {
			c := cands[ci]
			explainCands = append(explainCands, CandidatePlan{
				Name: c.name, Cost: c.cost, EstRows: c.estRows,
			})
		}
		slices.SortFunc(explainCands, func(a, b CandidatePlan) int {
			return cmp.Compare(a.Cost, b.Cost)
		})
		plan.Explain = ExplainInfo{
			TotalDocs:   int(totalDocs),
			Selectivity: selText * pResidual,
			Candidates:  explainCands,
			ChosenIndex: plan.IndexName,
		}
	}
	return plan
}

// collectFieldSelectivity mirrors BuildPlan's per-field selectivity pass
// (single-field point-lookup indexes with sketches).
func collectFieldSelectivity(params *PlanParams, totalDocs float64, buf []fieldSelEntry) []fieldSelEntry {
	for i := range params.Indexes {
		idx := &params.Indexes[i]
		if len(idx.Info.FieldNames) == 1 && idx.PointLookup && len(idx.Bounds) > 0 {
			var est float64
			if idx.Info.Unique && !idx.Info.Sparse {
				est = float64(len(idx.Bounds))
			} else if idx.Sketch != nil {
				est = float64(idx.Sketch.Estimate(0, idx.Bounds[0].Start))
			}
			if est > 0 && len(buf) < cap(buf) {
				buf = append(buf, fieldSelEntry{field: idx.Info.FieldNames[0], sel: est / totalDocs})
			}
		}
	}
	return buf
}

// buildTextProbePlan assembles the chosen probe chain:
//
//	rank:   driver -> FtsProbe(rank) -> Fetch -> ScoreInject -> [Filter] -> [Limit]
//	stream: driver -> [dedup] -> FtsProbe -> Fetch -> ScoreInject -> [Filter]
//	        -> [CanonicalDedup] -> [Sort] -> [Limit]
//
// CountOnly with a driver-covered residual skips everything after the probe
// (no document is ever fetched).
func buildTextProbePlan(params *PlanParams, cand *textCandidate, rankMode bool) *Plan {
	needFilter := params.Filter != nil && !isAllFilter(params.Filter)
	needSort := params.Sorter != nil
	dataCS := &CursorSource{Tx: params.Tx, Ns: params.DataNs}

	var root Iterator
	var canonicalWrap func(Iterator) Iterator
	indexName := params.Fts.IndexName

	countCovered := false

	switch cand.kind {
	case textPlanProbeIds:
		root = &IdBoundsIter{Bounds: params.IDBounds}
		// The pk bounds exactly represent a single-predicate pk-only residual;
		// then Count needs no fetch (mirrors indexCoversFilter's conditions).
		pk := params.PrimaryKey
		if pk == "" {
			pk = "id"
		}
		hasFields := false
		countCovered = !needFilter ||
			(filterFieldsCoveredBy(params.Filter, []string{pk}, &hasFields) && hasFields &&
				countFilterFieldPreds(params.Filter, pk) <= 1)
	default:
		idx := cand.idx
		indexName = idx.Info.Name
		if len(idx.Bounds) > 0 {
			idx.Bounds = AdjustBoundsForNonUnique(idx.Bounds)
		}
		reverse := shouldReverse(params.Sorter, idx)

		if idx.Info.Unique && idx.fullKeyPointBound() {
			// CoverIter must get the UN-finalized bounds: it seeks the raw
			// prefix and applies HasExactFieldPrefix itself, so the reverse-
			// tail pad would double-pad the seek, and MergeOverlappingBounds
			// would collapse distinct point probes (see buildIndexSeekChain).
			root = &CoverIter{
				Source:  &CursorSource{Tx: params.Tx, Ns: idx.Info.Ns},
				IdxInfo: idx.Info,
				Bounds:  idx.Bounds,
			}
			if len(idx.Bounds) > 1 {
				root = &DocDedupIter{Source: root}
			}
		} else {
			dedupB := finalizeIndexBounds(idx)
			root = &IndexIter{
				Source:       &CursorSource{Tx: params.Tx, Ns: idx.Info.Ns},
				IdxInfo:      idx.Info,
				Bounds:       idx.Bounds,
				Reverse:      reverse,
				PointLookup:  idx.PointLookup,
				FullKeyBound: idx.fullKeyPointBound(),
				ScalarProven: idx.ScalarProven,
			}
			// Compound multikey dedup below the probe (same placement as the
			// ordinary chains); single-field dups keep their canonical dedup
			// above the filter in stream mode, and rank mode dedups
			// internally.
			if len(idx.Info.FieldPaths) > 1 && !idx.ScalarProven {
				root = &DocDedupIter{Source: root}
			}
			if len(idx.Info.FieldPaths) == 1 && !rankMode {
				fieldReverse := len(idx.Info.Reverse) > 0 && idx.Info.Reverse[0]
				canonicalWrap = func(it Iterator) Iterator {
					return &CanonicalKeyDedupIter{
						Source:       it,
						Bounds:       dedupB,
						FieldPath:    idx.Info.FieldPaths[0],
						Reverse:      reverse,
						FieldReverse: fieldReverse,
					}
				}
			}
		}
		countCovered = !needFilter ||
			(idx.PointLookup && indexCoversFilter(idx, params.Filter))
	}

	probe := &FtsProbeIter{
		Spec:   params.Fts,
		Source: root,
		Tx:     params.Tx,
		Rank:   rankMode && !params.CountOnly,
	}
	// The rank materialization can be bounded by the result window (like
	// SortIter's heap) ONLY when no downstream stage rejects rows — a
	// residual filter after the probe would turn the bound into row loss.
	if !needFilter {
		probe.TopK = sortTopK(params)
	}
	root = probe

	if params.CountOnly && countCovered {
		// Count of the probed stream: distinct docIds, no fetches at all.
		// The Limit/Offset wrap below still applies — countPlanRoot's
		// LimitIter.CountDistinct is what windows the distinct count.
	} else {
		root = &FetchIter{Source: root, Data: dataCS, Buf: params.Buf}
		// _score decoration is only needed when someone reads it: the public
		// iterator (NeedScores) or a residual referencing the virtual field
		// (the query layer forces NeedScores on for that).
		if params.Fts.NeedScores {
			root = &ScoreInjectIter{Source: root}
		}
		if needFilter {
			root = &FilterIter{Source: root, Data: dataCS, Filter: params.Filter, Buf: params.Buf}
		}
		if canonicalWrap != nil {
			root = canonicalWrap(root)
		}
		if needSort && (cand.idx == nil || !cand.idx.ExactSort) {
			root = &SortIter{
				Source: root,
				Data:   dataCS,
				Sorter: params.Sorter,
				Buf:    params.Buf,
				TopK:   sortTopK(params),
			}
		}
	}
	if params.Limit > 0 || params.Offset > 0 {
		root = &LimitIter{Source: root, Limit: params.Limit, Offset: params.Offset}
	}

	plan := &Plan{Root: root, Name: cand.name, IndexName: indexName}
	setPlanRef(root, plan)
	return plan
}

// textPlanName is used by tests to assert plan choice via Plan.Name.
func (k textPlanKind) String() string {
	switch k {
	case textPlanDriver:
		return "FtsSearch"
	case textPlanProbeIds:
		return "FtsProbeIds"
	case textPlanProbeSeek:
		return "FtsProbeSeek"
	case textPlanProbeScan:
		return "FtsProbeScan"
	}
	return fmt.Sprintf("textPlanKind(%d)", int(k))
}
