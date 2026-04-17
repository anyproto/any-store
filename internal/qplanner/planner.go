package qplanner

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/btree"
	"github.com/anyproto/any-store/query"
	"github.com/anyproto/any-store/syncpool"
)

// fieldSelEntry holds per-field selectivity from single-field index sketches.
type fieldSelEntry struct {
	field string
	sel   float64
}

// CandidatePlan represents one plan alternative considered by the CBO.
type CandidatePlan struct {
	Name    string        // "FullScan", "IndexSeek(a)", "IndexScan(a)"
	Cost    float64       // computed cost
	EstRows float64       // estimated rows scanned/fetched
	details func() string // lazy one-line cost formula (only evaluated by ExplainString)
}

// Details returns the cost formula string, computing it lazily on first call.
func (c *CandidatePlan) Details() string {
	if c.details != nil {
		return c.details()
	}
	return ""
}

// ExplainInfo holds rich explain metadata collected during BuildPlan.
type ExplainInfo struct {
	TotalDocs   int
	Selectivity float64
	Candidates  []CandidatePlan // all plans considered, sorted by cost
	ChosenIndex string
}

// Plan holds the root iterator of a query execution plan.
type Plan struct {
	Root      Iterator
	DocParsed *anyenc.Value // set by FilterIter/FetchIter/FullScanIter after parsing

	// CBO metadata (for Explain)
	Name      string  // "FullScan", "IndexSeek", "IndexScan"
	Cost      float64 // computed cost
	IndexName string  // name of the index used (empty for full scan)

	Explain ExplainInfo // rich explain data
}

// Close releases all cursor resources held by the plan's iterator chain.
func (p *Plan) Close() {
	if p.Root != nil {
		p.Root.Close()
	}
}

// String returns a human-readable description of the full plan chain.
func (p *Plan) String() string {
	if p.Root == nil {
		return "NoPlan"
	}
	return p.Root.String()
}

// ExplainString returns a multi-line human-readable explain output with
// cost breakdown and all candidate plans considered.
func (p *Plan) ExplainString() string {
	var sb strings.Builder

	// Header line
	sb.WriteString(fmt.Sprintf("Plan: %s", p.Name))
	if p.IndexName != "" {
		sb.WriteString(fmt.Sprintf("  Index: %s", p.IndexName))
	}
	sb.WriteString(fmt.Sprintf("  Cost: %.1f\n", p.Cost))

	// Selectivity info
	info := &p.Explain
	estRows := float64(info.TotalDocs) * info.Selectivity
	sb.WriteString(fmt.Sprintf("  Selectivity: %.2f (%.0f of %d docs)\n", info.Selectivity, estRows, info.TotalDocs))

	// Iterator chain
	sb.WriteString(fmt.Sprintf("  Iterator: %s\n", p.String()))

	// Chosen candidate's cost details
	for i := range info.Candidates {
		c := &info.Candidates[i]
		if c.Cost == p.Cost && strings.Contains(c.Name, p.Name) {
			if d := c.Details(); d != "" {
				sb.WriteString(fmt.Sprintf("  Cost breakdown: %s\n", d))
			}
			break
		}
	}

	// Candidates list
	if len(info.Candidates) > 0 {
		sb.WriteString("Candidates:\n")
		for i, c := range info.Candidates {
			chosen := ""
			if c.Cost == p.Cost && strings.Contains(c.Name, p.Name) {
				chosen = "  [chosen]"
			}
			sb.WriteString(fmt.Sprintf("  %d. %-25s cost=%-10.1f est_rows=%.0f%s\n",
				i+1, c.Name, c.Cost, c.EstRows, chosen))
		}
	}

	return sb.String()
}

// formatFullScanDetails returns a cost formula string for a full scan plan.
func formatFullScanDetails(totalDocs, estimatedYield float64, needSort, idBoundsSeek bool) string {
	perDocCost := CostDocFetch
	label := "fetch"
	if totalDocs > 500 && !idBoundsSeek {
		perDocCost = CostSeqRead
		label = "seq"
	}
	s := fmt.Sprintf("%.0f×%s(%.1f) + %.0f×filter(%.1f)", totalDocs, label, perDocCost, totalDocs, CostFilter)
	if needSort {
		s += fmt.Sprintf(" + sort(%.0f)=%.1f", estimatedYield, sortCost(estimatedYield))
	}
	base := (totalDocs * perDocCost) + (totalDocs * CostFilter)
	if needSort {
		base += sortCost(estimatedYield)
	}
	s += fmt.Sprintf(" = %.1f", base)
	return s
}

// formatSeekDetails returns a cost formula string for an index seek plan.
func formatSeekDetails(nSeeks, estRows, fetchCost, seekSortCost float64) string {
	s := fmt.Sprintf("%.0f×seek(%.1f) + %.0f×fetch(%.1f) + %.0f×filter(%.1f)",
		nSeeks, CostIndexSeek, estRows, fetchCost, estRows, CostFilter)
	if seekSortCost > 0 {
		s += fmt.Sprintf(" + sort=%.1f", seekSortCost)
	}
	total := (nSeeks * CostIndexSeek) + (estRows * fetchCost) + (estRows * CostFilter) + seekSortCost
	s += fmt.Sprintf(" = %.1f", total)
	return s
}

// formatScanDetails returns a cost formula string for an index scan plan.
func formatScanDetails(scanRows, fetchCost float64, hasLimit bool) string {
	s := fmt.Sprintf("%.0f×seek(%.1f) + %.0f×fetch(%.1f) + %.0f×filter(%.1f)",
		scanRows, CostIndexSeek, scanRows, fetchCost, scanRows, CostFilter)
	if hasLimit {
		s += " [limit-optimized]"
	}
	total := (scanRows * CostIndexSeek) + (scanRows * fetchCost) + (scanRows * CostFilter)
	s += fmt.Sprintf(" = %.1f", total)
	return s
}

// PlanParams holds all the parameters needed to build a query plan.
type PlanParams struct {
	Tx       *btree.ReadTx
	DataNs   *btree.Namespace
	Filter   query.Filter
	Sorter   query.Sort
	IDBounds query.Bounds
	Limit    int
	Offset   int
	Buf      *syncpool.DocBuffer

	// CBO parameters
	TotalDocs  int
	Indexes    []CBOIndex
	IndexHints []IndexHintParam

	// CountOnly signals that the caller only needs a count of matching documents.
	// When true and the chosen index covers all filter fields, FetchIter and
	// FilterIter are skipped (covering index count optimization).
	CountOnly bool

	// FieldBounds is an optional pre-computed bounds result.
	// When set, calculateSelectivity uses cached bounds instead of calling
	// filter.IndexBounds repeatedly (avoids ~N redundant filter tree traversals).
	FieldBounds *BoundsResult
}

// IndexHintParam mirrors the public IndexHint type.
type IndexHintParam struct {
	IndexName string
	Boost     int
}

// CBOIndex represents an index candidate for the CBO planner.
type CBOIndex struct {
	Info    *IndexInfo
	Sketch  *IndexSketch
	Bounds  query.Bounds
	Reverse []bool // per-field reverse flags

	// BoundFields is the number of index fields covered by the bound chain.
	// Sketch estimates are only valid when BoundFields == len(Info.FieldNames).
	BoundFields int

	// PointLookup is true when ALL original bounds are equality (Start == End),
	// before AdjustBoundsForNonUnique modifies End. This allows correct sketch estimation.
	PointLookup bool

	// Sort coverage analysis
	ExactSort   bool
	PartialSort bool
}

// BuildPlan constructs an iterator chain using the Cost-Based Optimizer.
// It evaluates full scan, index seek, and index scan plans, then picks the cheapest.
func BuildPlan(params *PlanParams) *Plan {
	needSort := params.Sorter != nil
	needFilter := params.Filter != nil && !isAllFilter(params.Filter)
	totalDocs := float64(params.TotalDocs)
	if totalDocs < 1 {
		totalDocs = 1
	}

	// Calculate combined selectivity for all filter predicates
	pTotal := calculateSelectivity(params.Filter, params.Indexes, totalDocs, params.FieldBounds)

	estimatedYield := totalDocs * pTotal

	// Collect all candidate plans for explain output (skip when CountOnly to reduce allocations)
	collectExplain := !params.CountOnly
	var candidates []CandidatePlan
	if collectExplain {
		candidates = make([]CandidatePlan, 0, len(params.Indexes)+1)
	}

	// ---- Plan A: Full Collection Scan ----
	// When idBounds are present with point lookups, FullScan only reads those specific docs.
	fullScanDocs := totalDocs
	idBoundsSeek := false
	if len(params.IDBounds) > 0 && AllBoundsFixed(params.IDBounds) {
		fullScanDocs = float64(len(params.IDBounds))
		idBoundsSeek = true
		if fullScanDocs < estimatedYield {
			estimatedYield = fullScanDocs
		}
	}
	// FullScan naturally reads in ID order, so sorting by "id" is free.
	fullScanNeedSort := needSort
	if needSort {
		fields := params.Sorter.Fields()
		if len(fields) == 1 && fields[0].Field == "id" {
			fullScanNeedSort = false
		}
	}
	// When FullScan provides id-order (no sort needed) and there's a LIMIT,
	// we only need to scan enough docs to find `limit` matching rows.
	fullScanEffective := fullScanDocs
	if !fullScanNeedSort && (params.Limit > 0 || params.Offset > 0) {
		needed := float64(params.Limit + params.Offset)
		if pTotal > 0 && pTotal < 1.0 {
			// Scan needed/selectivity docs on average to find enough matches
			needed = needed / pTotal
		}
		if needed < fullScanDocs {
			fullScanEffective = needed
		}
	}
	fullScanCost := computeFullScanCost(fullScanEffective, estimatedYield, fullScanNeedSort, idBoundsSeek)

	if collectExplain {
		fse := fullScanEffective
		candidates = append(candidates, CandidatePlan{
			Name:    "FullScan",
			Cost:    fullScanCost,
			EstRows: fse,
			details: func() string { return formatFullScanDetails(fse, estimatedYield, fullScanNeedSort, idBoundsSeek) },
		})
	}

	bestPlanName := "FullScan"
	bestCost := fullScanCost
	var bestIndex *CBOIndex

	// Build hint lookup (skip allocation when no hints)
	var hintBoosts map[string]int
	if len(params.IndexHints) > 0 {
		hintBoosts = make(map[string]int, len(params.IndexHints))
		for _, h := range params.IndexHints {
			hintBoosts[h.IndexName] = h.Boost
		}
	}

	// Compute per-field selectivity from single-field indexes with sketches.
	// This allows compound indexes with partial bounds to get accurate estimates.
	// Use inline array to avoid map allocation for typical queries.
	var fieldSelBuf [8]fieldSelEntry
	nFieldSel := 0
	for i := range params.Indexes {
		idx := &params.Indexes[i]
		if len(idx.Info.FieldNames) == 1 && idx.PointLookup && idx.Sketch != nil && len(idx.Bounds) > 0 {
			est := float64(idx.Sketch.Estimate(idx.Bounds[0].Start))
			if est > 0 && nFieldSel < len(fieldSelBuf) {
				fieldSelBuf[nFieldSel] = fieldSelEntry{
					field: idx.Info.FieldNames[0],
					sel:   est / totalDocs,
				}
				nFieldSel++
			}
		}
	}
	var fieldSelectivity []fieldSelEntry
	if nFieldSel > 0 {
		fieldSelectivity = fieldSelBuf[:nFieldSel]
	}

	// ---- Plan B: Index Seek (Filtering Priority) ----
	for i := range params.Indexes {
		idx := &params.Indexes[i]
		if len(idx.Bounds) == 0 {
			continue
		}

		// Estimate docs matching this index seek
		e := estimateIndexDocsWithFieldSel(idx, totalDocs, fieldSelectivity)
		if e < 1 {
			e = 1
		}

		// Remaining filter selectivity after index seek
		idxSel := selectivityForIndex(idx, totalDocs)
		filteredYield := e * (pTotal / idxSel)
		if filteredYield < 1 {
			filteredYield = 1
		}
		if filteredYield > e {
			filteredYield = e
		}

		// Index seek cost: B-tree seeks (one per bound) + fetch only matching docs + evaluate filter
		// The key advantage is that e << totalDocs for selective queries
		fetchCost := indexFetchCost(totalDocs)
		nSeeks := float64(len(idx.Bounds))
		if nSeeks < 1 {
			nSeeks = 1
		}
		seekCost := (nSeeks * CostIndexSeek) + (e * fetchCost) + (e * CostFilter)

		// Covering count: when only counting and this index covers the filter with
		// equality bounds, no document fetch or filter evaluation is needed.
		// Cost is just the index traversal (sequential reads through the index).
		isCovering := params.CountOnly && idx.PointLookup && indexCoversFilter(idx, params.Filter)
		if isCovering {
			seekCost = (nSeeks * CostIndexSeek) + (e * CostSeqRead)
		}

		// When the index covers the sort and we have a LIMIT, we only need to
		// scan limit/scanSel docs through the index (same logic as Plan C).
		if needSort && idx.ExactSort && params.Limit > 0 && !isCovering {
			scanSel := pTotal / idxSel
			if scanSel > 1.0 {
				scanSel = 1.0
			}
			if scanSel <= 0 {
				scanSel = 0.0001
			}
			s := float64(params.Limit+params.Offset) / scanSel
			if s > e {
				s = e
			}
			if s < 1 {
				s = 1
			}
			seekCost = (nSeeks * CostIndexSeek) + (s * fetchCost) + (s * CostFilter)
		}

		seekSortCost := 0.0
		if needSort && !idx.ExactSort {
			seekSortCost = sortCost(filteredYield)
			seekCost += seekSortCost
		}
		// No sort cost if the index also covers the sort

		// Apply index hint boost (negative cost adjustment)
		if boost, ok := hintBoosts[idx.Info.Name]; ok {
			seekCost -= float64(boost)
		}

		if collectExplain {
			seekNS, seekE, seekFetchCost, seekSC := nSeeks, e, fetchCost, seekSortCost
			candidates = append(candidates, CandidatePlan{
				Name:    "IndexSeek(" + idx.Info.Name + ")",
				Cost:    seekCost,
				EstRows: e,
				details: func() string { return formatSeekDetails(seekNS, seekE, seekFetchCost, seekSC) },
			})
		}

		isBetter := seekCost < bestCost
		if !isBetter && seekCost == bestCost {
			// Tie-breaking: prefer index seek over full scan, unique over non-unique
			isBetter = bestPlanName == "FullScan" ||
				(bestPlanName == "IndexSeek" && idx.Info.Unique && bestIndex != nil && !bestIndex.Info.Unique)
		}
		if isBetter {
			bestCost = seekCost
			bestPlanName = "IndexSeek"
			bestIndex = idx
		}
	}

	// ---- Plan C: Index Scan (Sorting Priority) ----
	// Consider when an index covers the sort order, with or without LIMIT.
	if needSort {
		for i := range params.Indexes {
			idx := &params.Indexes[i]
			if !idx.ExactSort {
				continue
			}

			fetchCost := indexFetchCost(totalDocs)

			// When the index has bounds (e.g. compound index with equality prefix),
			// the scan only visits entries matching those bounds. The selectivity
			// of the remaining filter within the bounded range is higher.
			idxSel := selectivityForIndex(idx, totalDocs)
			// Effective selectivity within the index range
			scanSel := pTotal / idxSel
			if scanSel > 1.0 {
				scanSel = 1.0
			}
			if scanSel <= 0 {
				scanSel = 0.0001
			}
			// Population to scan: bounded by index selectivity
			scanPopulation := totalDocs
			if len(idx.Bounds) > 0 {
				scanPopulation = totalDocs * idxSel
				if scanPopulation < 1 {
					scanPopulation = 1
				}
			}

			// Check if non-bound index fields cover filter conditions.
			// When they do, IndexFilterIter can check values from the key tuple
			// and only fetch documents for matching entries.
			coverFilters := coveringFilterFields(idx, params.FieldBounds)
			coverSel := coveringFilterSelectivity(coverFilters, idx, fieldSelectivity)

			var scanCost float64
			var scanRows float64
			if params.Limit > 0 {
				// With LIMIT: expected docs to scan = LIMIT / scanSel, capped at scanPopulation
				s := float64(params.Limit+params.Offset) / scanSel
				if s > scanPopulation {
					s = scanPopulation
				}
				if s < 1 {
					s = 1
				}
				scanRows = s
				if len(coverFilters) > 0 {
					// Covering filter: sequential index reads + fetch only matching docs
					scanCost = (s * CostSeqRead) + (s * coverSel * fetchCost) + (s * CostFilter)
				} else {
					scanCost = (s * CostIndexSeek) + (s * fetchCost) + (s * CostFilter)
				}
			} else {
				// Without LIMIT: scan all docs in the index range (no sort penalty)
				scanRows = scanPopulation
				if len(coverFilters) > 0 {
					// Covering filter: sequential index reads + fetch only matching docs
					scanCost = (scanPopulation * CostSeqRead) + (scanPopulation * coverSel * fetchCost) + (scanPopulation * CostFilter)
				} else {
					scanCost = (scanPopulation * CostIndexSeek) + (scanPopulation * fetchCost) + (scanPopulation * CostFilter)
				}
			}
			// No sort penalty since index provides order

			// Apply index hint boost
			if boost, ok := hintBoosts[idx.Info.Name]; ok {
				scanCost -= float64(boost)
			}

			if collectExplain {
				scanSR, scanFC, scanHL := scanRows, fetchCost, params.Limit > 0
				candidates = append(candidates, CandidatePlan{
					Name:    "IndexScan(" + idx.Info.Name + ")",
					Cost:    scanCost,
					EstRows: scanRows,
					details: func() string { return formatScanDetails(scanSR, scanFC, scanHL) },
				})
			}

			if scanCost < bestCost {
				bestCost = scanCost
				bestPlanName = "IndexScan"
				bestIndex = idx
			}
		}
	}

	// Build the iterator chain for the chosen plan
	var root Iterator
	indexName := ""

	switch bestPlanName {
	case "IndexSeek":
		root = buildIndexSeekChain(params, bestIndex, needFilter, needSort)
		indexName = bestIndex.Info.Name
	case "IndexScan":
		root = buildIndexScanChain(params, bestIndex, needFilter)
		indexName = bestIndex.Info.Name
	default: // "FullScan"
		root = buildFullScanChain(params, needFilter, needSort)
	}

	// Apply limit/offset
	if params.Limit > 0 || params.Offset > 0 {
		offset := params.Offset
		if fsi, ok := root.(*FullScanIter); ok && fsi.Offset > 0 {
			offset = 0 // FullScanIter handles offset via cursor-level batch skip
		}
		root = &LimitIter{
			Source: root,
			Limit:  params.Limit,
			Offset: offset,
		}
	}

	// Sort candidates by cost ascending (skip when not collecting explain)
	if collectExplain {
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].Cost < candidates[j].Cost
		})
	}

	plan := &Plan{
		Root:      root,
		Name:      bestPlanName,
		Cost:      bestCost,
		IndexName: indexName,
		Explain: ExplainInfo{
			TotalDocs:   params.TotalDocs,
			Selectivity: pTotal,
			Candidates:  candidates,
			ChosenIndex: indexName,
		},
	}

	// Wire plan reference into FilterIter/FetchIter instances for doc value caching
	setPlanRef(root, plan)

	return plan
}

// computeFullScanCost computes the cost for a full collection scan.
// For collections above the sequential-read threshold, cursor reads are much cheaper
// than random B-tree point lookups, so we use CostSeqRead instead of CostDocFetch.
// For small collections, B-tree depth is shallow and both access patterns
// have similar cost, so we use CostDocFetch (preserving original behavior).
// When idBoundsSeek is true, the scan does random point lookups (not sequential),
// so CostDocFetch is used regardless of collection size.
func computeFullScanCost(totalDocs, estimatedYield float64, needSort, idBoundsSeek bool) float64 {
	perDocCost := CostDocFetch
	if totalDocs > 500 && !idBoundsSeek {
		perDocCost = CostSeqRead
	}
	cost := (totalDocs * perDocCost) + (totalDocs * CostFilter)
	if needSort {
		cost += sortCost(estimatedYield)
	}
	return cost
}

// indexFetchCost returns the per-doc cost for random B-tree point lookups.
// Random lookups cost more than sequential reads, but internal B-tree nodes
// are typically cached, so the penalty is moderate regardless of collection size.
func indexFetchCost(totalDocs float64) float64 {
	return CostDocFetch
}

// sortCost computes the sort cost using n*log2(n)*CostSortSwap.
func sortCost(n float64) float64 {
	if n <= 1 {
		return 0
	}
	return n * math.Log2(n) * CostSortSwap
}

// calculateSelectivity computes the combined selectivity for all filter predicates.
func calculateSelectivity(filter query.Filter, indexes []CBOIndex, totalDocs float64, br *BoundsResult) float64 {
	if filter == nil || isAllFilter(filter) {
		return 1.0
	}

	pTotal := 1.0
	// Use a small inline slice to track used fields (avoids map allocation for typical queries)
	var usedFields [8]string
	nUsed := 0

	// For each index, check if any of its fields have bounds in the filter
	for i := range indexes {
		idx := &indexes[i]
		for fi, fieldName := range idx.Info.FieldNames {
			// Check if field already processed (linear scan is faster than map for ≤8 fields)
			alreadyUsed := false
			for j := 0; j < nUsed; j++ {
				if usedFields[j] == fieldName {
					alreadyUsed = true
					break
				}
			}
			if alreadyUsed {
				continue
			}
			var bounds query.Bounds
			var isEquality bool
			if br != nil {
				var fixed, found bool
				bounds, fixed, found = br.Lookup(fieldName)
				if !found || len(bounds) == 0 {
					continue
				}
				isEquality = fixed
			} else {
				bounds = filter.IndexBounds(fieldName, nil)
				if len(bounds) == 0 {
					continue
				}
				isEquality = AllBoundsFixed(bounds)
			}
			if nUsed < len(usedFields) {
				usedFields[nUsed] = fieldName
				nUsed++
			}

			if isEquality && idx.Sketch != nil && fi == 0 && len(idx.Info.FieldNames) == 1 {
				// Use sketch estimate for single-field equality predicates
				est := idx.Sketch.Estimate(bounds[0].Start)
				p := float64(est) / totalDocs
				if p > 1.0 {
					p = 1.0
				}
				if p <= 0 {
					p = 0.0001
				}
				pTotal *= p
			} else if isEquality {
				// Equality on a field but can't use sketch directly (compound index)
				// Use a more selective estimate than default range
				pTotal *= DefaultRangeSelectivity
			} else {
				// Range predicate: use default selectivity
				pTotal *= DefaultRangeSelectivity
			}
		}
	}

	// If no index fields matched the filter, it might have predicates on non-indexed fields
	// Still use default range selectivity for those
	if nUsed == 0 && !isAllFilter(filter) {
		pTotal = DefaultRangeSelectivity
	}

	// Clamp
	if pTotal <= 0 {
		pTotal = 0.0001
	}
	if pTotal > 1.0 {
		pTotal = 1.0
	}

	return pTotal
}

// selectivityForIndex returns the selectivity contribution of a specific index.
func selectivityForIndex(idx *CBOIndex, totalDocs float64) float64 {
	if len(idx.Bounds) == 0 {
		return 1.0
	}

	// Only use sketch when bounds cover ALL index fields
	if idx.PointLookup && idx.Sketch != nil && idx.BoundFields == len(idx.Info.FieldNames) {
		est := idx.Sketch.Estimate(idx.Bounds[0].Start)
		p := float64(est) / totalDocs
		if p <= 0 {
			p = 0.0001
		}
		if p > 1.0 {
			p = 1.0
		}
		return p
	}

	return DefaultRangeSelectivity
}

// estimateIndexDocsWithFieldSel estimates the number of documents an index seek will return,
// using per-field selectivity from single-field indexes for compound indexes with partial bounds.
func estimateIndexDocsWithFieldSel(idx *CBOIndex, totalDocs float64, fieldSel []fieldSelEntry) float64 {
	if len(idx.Bounds) == 0 {
		return totalDocs
	}

	// Best case: sketch covers ALL index fields (exact match)
	if idx.PointLookup && idx.Sketch != nil && idx.BoundFields == len(idx.Info.FieldNames) {
		var total float64
		for _, b := range idx.Bounds {
			total += float64(idx.Sketch.Estimate(b.Start))
		}
		return total
	}

	// Fallback for partial bounds: use per-field selectivity from single-field indexes
	if idx.BoundFields > 0 && len(fieldSel) > 0 {
		sel := 1.0
		hasFieldSel := false
		for fi := range idx.BoundFields {
			if fi < len(idx.Info.FieldNames) {
				fname := idx.Info.FieldNames[fi]
				found := false
				for j := range fieldSel {
					if fieldSel[j].field == fname {
						sel *= fieldSel[j].sel
						hasFieldSel = true
						found = true
						break
					}
				}
				if !found {
					sel *= DefaultRangeSelectivity
				}
			}
		}
		if hasFieldSel {
			return totalDocs * sel
		}
	}

	// Fallback: multiply DefaultRangeSelectivity per bound field
	sel := 1.0
	for range idx.BoundFields {
		sel *= DefaultRangeSelectivity
	}
	return totalDocs * sel
}

// buildFullScanChain constructs the iterator chain for a full collection scan.
func buildFullScanChain(params *PlanParams, needFilter, needSort bool) Iterator {
	var root Iterator

	idSorted := false
	if needSort {
		fields := params.Sorter.Fields()
		if len(fields) == 1 && fields[0].Field == "id" {
			idSorted = true
			fsi := &FullScanIter{
				Source: &CursorSource{
					Tx: params.Tx,
					Ns: params.DataNs,
				},
				Filter:   params.Filter,
				IDBounds: params.IDBounds,
				Buf:      params.Buf,
				Reverse:  fields[0].Reverse,
			}
			// Absorb offset into cursor-level batch skip when no filter.
			if !needFilter && params.Offset > 0 {
				fsi.Offset = params.Offset
			}
			root = fsi
		}
	}

	if root == nil {
		root = &FullScanIter{
			Source: &CursorSource{
				Tx: params.Tx,
				Ns: params.DataNs,
			},
			Filter:   params.Filter,
			IDBounds: params.IDBounds,
			Buf:      params.Buf,
		}
	}

	if needSort && !idSorted {
		root = &SortIter{
			Source: root,
			Data: &CursorSource{
				Tx: params.Tx,
				Ns: params.DataNs,
			},
			Sorter: params.Sorter,
			Buf:    params.Buf,
			TopK:   params.Limit + params.Offset,
		}
	}

	return root
}

// seekBatch batches common iterator allocations for an index seek plan
// into a single heap allocation instead of 5 separate ones.
type seekBatch struct {
	indexCS    CursorSource
	dataCS     CursorSource
	indexIter  IndexIter
	fetchIter  FetchIter
	filterIter FilterIter
}

// buildIndexSeekChain constructs the iterator chain for an index seek plan.
func buildIndexSeekChain(params *PlanParams, idx *CBOIndex, needFilter, needSort bool) Iterator {
	// Adjust End bounds so IndexIter range scans capture all key suffixes.
	// For non-unique indexes, keys include a docId suffix after the index fields.
	// For unique indexes with partial prefix bounds (BoundFields < len(FieldNames)),
	// keys include trailing field values beyond the bound prefix.
	// In both cases, appending 0xff to End extends the range to cover all suffixes.
	needBoundsAdjust := len(idx.Bounds) > 0 &&
		(!idx.Info.Unique || idx.BoundFields < len(idx.Info.FieldNames))
	if needBoundsAdjust {
		idx.Bounds = AdjustBoundsForNonUnique(idx.Bounds)
	}

	// Determine reverse scan direction
	reverse := shouldReverse(params.Sorter, idx)

	// Check for unique index point lookup (CoverIter shortcut).
	// Only safe when ALL index fields are covered by equality bounds;
	// a partial prefix (BoundFields < len(FieldNames)) can match multiple
	// entries with different trailing fields, so a range scan is needed.
	if idx.Info.Unique && idx.PointLookup && idx.BoundFields == len(idx.Info.FieldNames) {
		var root Iterator = &CoverIter{
			Source: &CursorSource{
				Tx: params.Tx,
				Ns: idx.Info.Ns,
			},
			IdxInfo: idx.Info,
			Bounds:  idx.Bounds,
		}

		if needFilter {
			root = &FilterIter{
				Source: root,
				Data: &CursorSource{
					Tx: params.Tx,
					Ns: params.DataNs,
				},
				Filter: params.Filter,
				Buf:    params.Buf,
			}
		}

		if needSort {
			root = &SortIter{
				Source: root,
				Data: &CursorSource{
					Tx: params.Tx,
					Ns: params.DataNs,
				},
				Sorter: params.Sorter,
				Buf:    params.Buf,
				TopK:   params.Limit + params.Offset,
			}
		}

		return root
	}

	// Use batched allocation for the common case: IndexIter + FetchIter + FilterIter.
	// This replaces 5 separate heap allocations with 1.
	b := &seekBatch{}
	b.indexCS = CursorSource{Tx: params.Tx, Ns: idx.Info.Ns}
	b.indexIter = IndexIter{
		Source:  &b.indexCS,
		IdxInfo: idx.Info,
		Bounds:  idx.Bounds,
		Reverse: reverse,
	}

	var root Iterator = &b.indexIter

	// Covering index count: when only counting and the index has exact equality
	// bounds (PointLookup) that cover all filter fields, skip FetchIter and FilterIter.
	// Equality bounds are precise: each index entry in the range matches the filter.
	// Range/exclusive bounds are NOT safe because non-unique index key suffixes
	// can cause incorrect inclusive/exclusive comparisons.
	if params.CountOnly && idx.PointLookup && indexCoversFilter(idx, params.Filter) {
		return root
	}

	// Index verification for count queries: instead of fetching documents to
	// check uncovered filter fields, verify each docId against single-field
	// indexes for those fields. This avoids expensive document fetches.
	if params.CountOnly && idx.PointLookup && params.FieldBounds != nil {
		if verifyRoot := buildVerifyChain(params, idx, root); verifyRoot != nil {
			return verifyRoot
		}
	}

	// Fetch documents by docId — share CursorSource between FetchIter and FilterIter
	b.dataCS = CursorSource{Tx: params.Tx, Ns: params.DataNs}
	b.fetchIter = FetchIter{
		Source: root,
		Data:   &b.dataCS,
		Buf:    params.Buf,
	}
	root = &b.fetchIter

	if needFilter {
		b.filterIter = FilterIter{
			Source: root,
			Data:   &b.dataCS, // FilterIter uses Plan.DocParsed from upstream FetchIter
			Filter: params.Filter,
			Buf:    params.Buf,
		}
		root = &b.filterIter
	}

	// Dedup wrap for multi-key safety.
	// Single-field indexes use canonical-key dedup (O(1) memory, streaming).
	// Compound indexes use the hash-set fallback because canonical-key
	// selection across compound tuples is non-trivial and deliberately
	// out of scope here — see docs/plans/2026-04-17-multikey-index-dedup.md.
	// Both branches wrap even when Bounds is empty: a bound-less
	// IndexScan (pure Sort("tags")) still produces one hit per array
	// element of every doc.
	switch {
	case len(idx.Info.FieldPaths) == 1:
		root = &CanonicalKeyDedupIter{
			Source:    root,
			Bounds:    idx.Bounds,
			FieldPath: idx.Info.FieldPaths[0],
			Reverse:   reverse,
		}
	case len(idx.Info.FieldPaths) > 1:
		root = &SeenSetDedupIter{Source: root}
	}

	if needSort && !idx.ExactSort {
		root = &SortIter{
			Source: root,
			Data: &CursorSource{
				Tx: params.Tx,
				Ns: params.DataNs,
			},
			Sorter:          params.Sorter,
			Buf:             params.Buf,
			TopK:            params.Limit + params.Offset,
			PartiallySorted: idx.PartialSort,
		}
	}

	return root
}

// buildIndexScanChain constructs the iterator chain for an index scan plan
// (scan index in sort order, filter, stop at limit — no in-memory sort needed).
func buildIndexScanChain(params *PlanParams, idx *CBOIndex, needFilter bool) Iterator {
	// Adjust bounds for non-unique indexes (deferred from buildCBOIndexesInto).
	if !idx.Info.Unique && len(idx.Bounds) > 0 {
		idx.Bounds = AdjustBoundsForNonUnique(idx.Bounds)
	}
	reverse := shouldReverse(params.Sorter, idx)

	var root Iterator = &IndexIter{
		Source: &CursorSource{
			Tx: params.Tx,
			Ns: idx.Info.Ns,
		},
		IdxInfo: idx.Info,
		Bounds:  idx.Bounds, // may be nil for full index scan
		Reverse: reverse,
	}

	// Insert IndexFilterIter when compound index fields cover filter conditions.
	// This filters non-matching entries using the index key tuple before fetching docs.
	coverFilters := coveringFilterFields(idx, params.FieldBounds)
	if len(coverFilters) > 0 {
		root = &IndexFilterIter{
			Source:  root,
			Filters: coverFilters,
		}
	}

	// Fetch documents by docId — share CursorSource between FetchIter and FilterIter
	scanDataSrc := &CursorSource{
		Tx: params.Tx,
		Ns: params.DataNs,
	}
	root = &FetchIter{
		Source: root,
		Data:   scanDataSrc,
		Buf:    params.Buf,
	}

	if needFilter {
		root = &FilterIter{
			Source: root,
			Data:   scanDataSrc,
			Filter: params.Filter,
			Buf:    params.Buf,
		}
	}

	// Dedup wrap — see buildIndexSeekChain for rationale.
	switch {
	case len(idx.Info.FieldPaths) == 1:
		root = &CanonicalKeyDedupIter{
			Source:    root,
			Bounds:    idx.Bounds,
			FieldPath: idx.Info.FieldPaths[0],
			Reverse:   reverse,
		}
	case len(idx.Info.FieldPaths) > 1:
		root = &SeenSetDedupIter{Source: root}
	}

	// No sort needed — index provides the order
	return root
}

// shouldReverse determines if an index scan should go in reverse direction
// based on the sort spec and index field ordering.
func shouldReverse(sorter query.Sort, idx *CBOIndex) bool {
	if sorter == nil {
		return false
	}
	fields := sorter.Fields()
	if len(fields) == 0 {
		return false
	}
	// Scan direction controls final output direction for index iteration.
	// Use requested sort direction directly for stable behavior with reverse indexes.
	_ = idx
	return fields[0].Reverse
}

// setPlanRef walks the iterator chain and sets the Plan reference on FilterIter/FetchIter/FullScanIter nodes.
// It stops at SortIter because SortIter collects all docs first, making cached values stale.
func setPlanRef(it Iterator, plan *Plan) {
	switch v := it.(type) {
	case *FilterIter:
		v.Plan = plan
		setPlanRef(v.Source, plan)
	case *FetchIter:
		v.Plan = plan
		setPlanRef(v.Source, plan)
	case *FullScanIter:
		v.Plan = plan
		// don't recurse — FullScanIter is a leaf
	case *SortIter:
		v.Plan = plan
		setPlanRef(v.Source, plan)
	case *IndexFilterIter:
		setPlanRef(v.Source, plan)
	case *LimitIter:
		setPlanRef(v.Source, plan)
	case *CanonicalKeyDedupIter:
		v.Plan = plan
		setPlanRef(v.Source, plan)
	case *SeenSetDedupIter:
		setPlanRef(v.Source, plan)
	}
}

// idBoundsPreferred returns true when id bounds are specific enough that
// direct data-namespace lookups + in-memory sort is cheaper than a full index scan.
func idBoundsPreferred(idBounds query.Bounds) bool {
	if len(idBounds) == 0 {
		return false
	}
	return AllBoundsFixed(idBounds)
}

// AllBoundsFixed returns true if all bounds have Start == End (point lookups).
func AllBoundsFixed(bounds query.Bounds) bool {
	for _, b := range bounds {
		if len(b.Start) == 0 || !bytes.Equal(b.Start, b.End) {
			return false
		}
	}
	return true
}

// indexCoversFilter returns true if the index's fields cover all fields
// referenced by the filter. When true, index bounds alone are sufficient
// to determine which documents match, and no data fetch is needed for Count().
// Zero-allocation: uses inline field matching instead of maps or slices.
func indexCoversFilter(idx *CBOIndex, filter query.Filter) bool {
	if filter == nil || len(idx.Bounds) == 0 {
		return false
	}
	// Walk the filter tree checking each field against the index's field names.
	// Returns false if any filter field is not in the index, or if the filter
	// contains complex nodes (Or, Not, Nor) where field extraction isn't reliable.
	hasFields := false
	ok := filterFieldsCoveredBy(filter, idx.Info.FieldNames, &hasFields)
	return ok && hasFields
}

// filterFieldsCoveredBy walks the filter tree and checks that every referenced
// field name is present in idxFields. Zero-allocation.
func filterFieldsCoveredBy(f query.Filter, idxFields []string, hasFields *bool) bool {
	switch ft := f.(type) {
	case query.Key:
		name := strings.Join(ft.Path, ".")
		for _, idxF := range idxFields {
			if idxF == name {
				*hasFields = true
				return true
			}
		}
		return false
	case query.And:
		for _, sub := range ft {
			if !filterFieldsCoveredBy(sub, idxFields, hasFields) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// collectUncoveredFilterFields walks the filter tree and returns field names
// not present in coveredFields. Returns nil if the filter contains complex
// nodes (Or, Not, Nor) that can't be reliably field-analyzed.
func collectUncoveredFilterFields(f query.Filter, coveredFields []string) []string {
	switch ft := f.(type) {
	case query.Key:
		name := strings.Join(ft.Path, ".")
		for _, cf := range coveredFields {
			if cf == name {
				return []string{} // covered
			}
		}
		return []string{name}
	case query.And:
		var result []string
		for _, sub := range ft {
			fields := collectUncoveredFilterFields(sub, coveredFields)
			if fields == nil {
				return nil
			}
			result = append(result, fields...)
		}
		return result
	case *query.And:
		var result []string
		for _, sub := range *ft {
			fields := collectUncoveredFilterFields(sub, coveredFields)
			if fields == nil {
				return nil
			}
			result = append(result, fields...)
		}
		return result
	default:
		return nil
	}
}

// buildVerifyChain constructs a chain of VerifyIter instances for count queries
// where the chosen index doesn't cover all filter fields. For each uncovered
// field with an equality bound, it finds a matching single-field non-unique
// index and verifies docIds against it instead of fetching full documents.
// Returns nil if verification is not possible.
func buildVerifyChain(params *PlanParams, idx *CBOIndex, root Iterator) Iterator {
	uncovered := collectUncoveredFilterFields(params.Filter, idx.Info.FieldNames[:idx.BoundFields])
	if len(uncovered) == 0 {
		return nil
	}

	current := root
	for _, field := range uncovered {
		bounds, fixed, found := params.FieldBounds.Lookup(field)
		if !found || !fixed || len(bounds) != 1 {
			return nil
		}

		// Find a non-unique single-field index for this field
		var verifyNs *btree.Namespace
		for i := range params.Indexes {
			info := params.Indexes[i].Info
			if !info.Unique && len(info.FieldNames) == 1 && info.FieldNames[0] == field {
				verifyNs = info.Ns
				break
			}
		}
		if verifyNs == nil {
			return nil
		}

		current = &VerifyIter{
			Source:   current,
			Tx:       params.Tx,
			VerifyNs: verifyNs,
			Prefix:   bounds[0].Start,
		}
	}

	return current
}

// isAllFilter returns true if the filter matches everything.
func isAllFilter(f query.Filter) bool {
	_, ok := f.(query.All)
	return ok
}

// BoundsResult stores IndexBounds results for all unique fields, computed once per query.
// All bounds live in one slice; FieldBounds entries point into it by index.
type BoundsResult struct {
	Bounds    []query.Bound // flat slice of all bounds across all fields
	Fields    []FieldBounds // per-field metadata pointing into Bounds
	boundsBuf [8]query.Bound
	fieldsBuf [8]FieldBounds
}

// FieldBounds holds pre-computed bounds for a single filter field.
type FieldBounds struct {
	Field string
	Start int  // start index into BoundsResult.Bounds
	Count int  // number of bounds for this field
	Fixed bool // all bounds are equality (Start == End)
}

// Build computes bounds for all unique fields across the given indexes.
func (br *BoundsResult) Build(indexInfos []*IndexInfo, filter query.Filter) {
	br.Bounds = br.boundsBuf[:0]
	br.Fields = br.fieldsBuf[:0]
	for _, info := range indexInfos {
		for _, field := range info.FieldNames {
			// Check if already computed
			found := false
			for j := range br.Fields {
				if br.Fields[j].Field == field {
					found = true
					break
				}
			}
			if found {
				continue
			}
			start := len(br.Bounds)
			bs := filter.IndexBounds(field, nil)
			br.Bounds = append(br.Bounds, bs...)
			count := len(bs)
			allFixed := true
			for _, b := range br.Bounds[start:] {
				if len(b.Start) == 0 || !bytes.Equal(b.Start, b.End) {
					allFixed = false
					break
				}
			}
			br.Fields = append(br.Fields, FieldBounds{
				Field: field,
				Start: start,
				Count: count,
				Fixed: allFixed,
			})
		}
	}
}

// Lookup returns the bounds for a field name.
func (br *BoundsResult) Lookup(field string) (bounds query.Bounds, fixed bool, found bool) {
	for i := range br.Fields {
		if br.Fields[i].Field == field {
			s := br.Fields[i].Start
			return br.Bounds[s : s+br.Fields[i].Count], br.Fields[i].Fixed, true
		}
	}
	return nil, false, false
}

// FieldCount returns the number of unique filter fields.
func (br *BoundsResult) FieldCount() int {
	return len(br.Fields)
}

// AllFixed returns true if all fields have equality (fixed point) bounds.
func (br *BoundsResult) AllFixed() bool {
	if len(br.Fields) == 0 {
		return false
	}
	for i := range br.Fields {
		if !br.Fields[i].Fixed {
			return false
		}
	}
	return true
}

// ComputeIndexBounds computes combined tuple bounds for an index
// using pre-computed per-field bounds from BoundsResult.
func ComputeIndexBounds(idx *IndexInfo, br *BoundsResult) (query.Bounds, int) {
	type fieldBound struct {
		bounds query.Bounds
		fixed  bool
	}

	var chainBuf [4]fieldBound // stack-allocated for typical compound indexes
	chain := chainBuf[:0]
	for _, field := range idx.FieldNames {
		fb, fixed, found := br.Lookup(field)
		if !found || len(fb) == 0 {
			break
		}
		if len(chain) < len(chainBuf) {
			chain = append(chain, fieldBound{bounds: fb, fixed: fixed})
		}
		if !fixed {
			break
		}
	}

	if len(chain) == 0 {
		return nil, 0
	}

	chainLen := len(chain)

	// Single-field index: return cached bounds directly (no copy needed)
	if len(chain) == 1 {
		return chain[0].bounds, chainLen
	}

	// Compound index: build combined tuple bounds using arena to avoid per-tuple heap allocs.
	// Each sub-slice reserves 1 extra cap byte so AdjustBoundsForNonUnique can append 0xff in-place.
	var arenaBuf [256]byte
	arena := arenaBuf[:0]

	var resultBuf [4]query.Bound
	result := query.Bounds(resultBuf[:0])
	for _, b := range chain[0].bounds {
		result = append(result, b)
	}

	for i := 1; i < len(chain); i++ {
		if !chain[i-1].fixed {
			break
		}
		var extBuf [4]query.Bound
		extended := query.Bounds(extBuf[:0])
		for _, prev := range result {
			for _, cur := range chain[i].bounds {
				eb := query.Bound{
					StartInclude: cur.StartInclude,
					EndInclude:   cur.EndInclude,
				}
				if len(cur.Start) > 0 {
					off := len(arena)
					arena = append(arena, prev.Start...)
					arena = append(arena, cur.Start...)
					n := len(arena) - off
					arena = append(arena, 0)
					eb.Start = anyenc.Tuple(arena[off : off+n : off+n+1])
				} else {
					off := len(arena)
					arena = append(arena, prev.Start...)
					n := len(arena) - off
					arena = append(arena, 0)
					eb.Start = anyenc.Tuple(arena[off : off+n : off+n+1])
					eb.StartInclude = true
				}
				if len(cur.End) > 0 {
					off := len(arena)
					arena = append(arena, prev.End...)
					arena = append(arena, cur.End...)
					n := len(arena) - off
					arena = append(arena, 0)
					eb.End = anyenc.Tuple(arena[off : off+n : off+n+1])
				} else {
					off := len(arena)
					arena = append(arena, prev.End...)
					arena = append(arena, 0xff)
					n := len(arena) - off
					arena = append(arena, 0)
					eb.End = anyenc.Tuple(arena[off : off+n : off+n+1])
					eb.EndInclude = true
				}
				extended = append(extended, eb)
			}
		}
		result = extended
	}

	return result, chainLen
}

// AdjustBoundsForNonUnique adjusts End bounds in-place for non-unique indexes
// by appending 0xff to capture all docId suffixes.
func AdjustBoundsForNonUnique(bounds query.Bounds) query.Bounds {
	for i := range bounds {
		if len(bounds[i].End) > 0 && bounds[i].EndInclude {
			bounds[i].End = append(bounds[i].End, 0xff)
		}
	}
	return bounds
}

// IndexSortMatch checks if an index covers the sort fields.
// equalityPrefix is the number of leading index fields pinned by equality filters;
// these can be skipped when matching sort fields since they're constant within a range.
func IndexSortMatch(idx *IndexInfo, sortFields []query.SortField, equalityPrefix int) (exactSort, partialSort bool) {
	if len(sortFields) == 0 || len(idx.FieldNames) == 0 {
		return false, false
	}
	matchAt := func(start int) int {
		if start < 0 || start >= len(idx.FieldNames) {
			return 0
		}
		match := 0
		modeSet := false
		sameMode := false // true: idx dir == sort dir, false: idx dir != sort dir
		for si, sf := range sortFields {
			ii := start + si
			if ii >= len(idx.FieldNames) {
				break
			}
			if idx.FieldNames[ii] != sf.Field {
				break
			}
			idxRev := false
			if ii < len(idx.Reverse) {
				idxRev = idx.Reverse[ii]
			}
			curSame := idxRev == sf.Reverse
			if !modeSet {
				sameMode = curSame
				modeSet = true
			} else if curSame != sameMode {
				break
			}
			match++
		}
		return match
	}

	bestMatch := matchAt(0)
	if equalityPrefix > 0 && equalityPrefix < len(idx.FieldNames) {
		if m := matchAt(equalityPrefix); m > bestMatch {
			bestMatch = m
		}
	}

	if bestMatch == 0 {
		return false, false
	}
	if bestMatch == len(sortFields) {
		return true, false
	}
	return false, true
}

// coveringFilterFields identifies non-bound index fields that have equality
// filter conditions, allowing IndexFilterIter to check them from the key tuple.
func coveringFilterFields(idx *CBOIndex, fieldBounds *BoundsResult) []IndexFieldFilter {
	if fieldBounds == nil || len(idx.Info.FieldNames) <= idx.BoundFields {
		return nil
	}

	var filters []IndexFieldFilter
	for fi := idx.BoundFields; fi < len(idx.Info.FieldNames); fi++ {
		fieldName := idx.Info.FieldNames[fi]
		bounds, fixed, found := fieldBounds.Lookup(fieldName)
		if !found || !fixed || len(bounds) != 1 {
			continue
		}

		matchValue := bounds[0].Start
		// For reverse fields, invert the match value to compare against stored bytes
		if fi < len(idx.Info.Reverse) && idx.Info.Reverse[fi] {
			inv := make([]byte, len(matchValue))
			for j, b := range matchValue {
				inv[j] = ^b
			}
			matchValue = inv
		}

		filters = append(filters, IndexFieldFilter{
			FieldIdx:   fi,
			MatchValue: matchValue,
		})
	}

	return filters
}

// coveringFilterSelectivity returns the combined selectivity of the fields
// that will be checked by IndexFilterIter. Used to adjust CBO cost: only
// entries passing the index-level filter need a document fetch.
func coveringFilterSelectivity(filters []IndexFieldFilter, idx *CBOIndex, fieldSel []fieldSelEntry) float64 {
	if len(filters) == 0 {
		return 1.0
	}
	sel := 1.0
	for _, f := range filters {
		fieldName := idx.Info.FieldNames[f.FieldIdx]
		found := false
		for j := range fieldSel {
			if fieldSel[j].field == fieldName {
				sel *= fieldSel[j].sel
				found = true
				break
			}
		}
		if !found {
			sel *= DefaultRangeSelectivity
		}
	}
	return sel
}
