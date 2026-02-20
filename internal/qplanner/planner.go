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

// CandidatePlan represents one plan alternative considered by the CBO.
type CandidatePlan struct {
	Name    string  // "FullScan", "IndexSeek(a)", "IndexScan(a)"
	Cost    float64 // computed cost
	EstRows float64 // estimated rows scanned/fetched
	Details string  // one-line cost formula
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
	for _, c := range info.Candidates {
		if c.Cost == p.Cost && strings.Contains(c.Name, p.Name) {
			if c.Details != "" {
				sb.WriteString(fmt.Sprintf("  Cost breakdown: %s\n", c.Details))
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
func formatFullScanDetails(totalDocs, estimatedYield float64, needSort bool) string {
	s := fmt.Sprintf("%.0f×fetch(%.1f) + %.0f×filter(%.1f)", totalDocs, CostDocFetch, totalDocs, CostFilter)
	if needSort {
		s += fmt.Sprintf(" + sort(%.0f)=%.1f", estimatedYield, sortCost(estimatedYield))
	}
	base := (totalDocs * CostDocFetch) + (totalDocs * CostFilter)
	if needSort {
		base += sortCost(estimatedYield)
	}
	s += fmt.Sprintf(" = %.1f", base)
	return s
}

// formatSeekDetails returns a cost formula string for an index seek plan.
func formatSeekDetails(estRows, seekSortCost float64) string {
	s := fmt.Sprintf("seek(%.1f) + %.0f×fetch(%.1f) + %.0f×filter(%.1f)",
		CostIndexSeek, estRows, CostDocFetch, estRows, CostFilter)
	if seekSortCost > 0 {
		s += fmt.Sprintf(" + sort=%.1f", seekSortCost)
	}
	total := (1 * CostIndexSeek) + (estRows * CostDocFetch) + (estRows * CostFilter) + seekSortCost
	s += fmt.Sprintf(" = %.1f", total)
	return s
}

// formatScanDetails returns a cost formula string for an index scan plan.
func formatScanDetails(scanRows float64, hasLimit bool) string {
	s := fmt.Sprintf("%.0f×seek(%.1f) + %.0f×fetch(%.1f) + %.0f×filter(%.1f)",
		scanRows, CostIndexSeek, scanRows, CostDocFetch, scanRows, CostFilter)
	if hasLimit {
		s += " [limit-optimized]"
	}
	total := (scanRows * CostIndexSeek) + (scanRows * CostDocFetch) + (scanRows * CostFilter)
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

	// PointLookup is true when ALL original bounds are equality (Start == End),
	// before AdjustBoundsForNonUnique modifies End. This allows correct sketch estimation.
	PointLookup bool

	// BoundFields is the number of index fields covered by the bound chain.
	// Sketch estimates are only valid when BoundFields == len(Info.FieldNames).
	BoundFields int

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
	pTotal := calculateSelectivity(params.Filter, params.Indexes, totalDocs)

	estimatedYield := totalDocs * pTotal

	// Collect all candidate plans for explain output
	var candidates []CandidatePlan

	// ---- Plan A: Full Collection Scan ----
	// When idBounds are present with point lookups, FullScan only reads those specific docs.
	fullScanDocs := totalDocs
	if len(params.IDBounds) > 0 && AllBoundsFixed(params.IDBounds) {
		fullScanDocs = float64(len(params.IDBounds))
		if fullScanDocs < estimatedYield {
			estimatedYield = fullScanDocs
		}
	}
	fullScanCost := computeFullScanCost(fullScanDocs, estimatedYield, needSort)

	candidates = append(candidates, CandidatePlan{
		Name:    "FullScan",
		Cost:    fullScanCost,
		EstRows: fullScanDocs,
		Details: formatFullScanDetails(fullScanDocs, estimatedYield, needSort),
	})

	bestPlanName := "FullScan"
	bestCost := fullScanCost
	var bestIndex *CBOIndex


	// Build hint lookup
	hintBoosts := make(map[string]int)
	for _, h := range params.IndexHints {
		hintBoosts[h.IndexName] = h.Boost
	}

	// Compute per-field selectivity from single-field indexes with sketches.
	// This allows compound indexes with partial bounds to get accurate estimates.
	fieldSelectivity := make(map[string]float64)
	for i := range params.Indexes {
		idx := &params.Indexes[i]
		if len(idx.Info.FieldNames) == 1 && idx.PointLookup && idx.Sketch != nil && len(idx.Bounds) > 0 {
			est := float64(idx.Sketch.Estimate(idx.Bounds[0].Start))
			if est > 0 {
				fieldSelectivity[idx.Info.FieldNames[0]] = est / totalDocs
			}
		}
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

		// Index seek cost: B-tree seek + fetch only matching docs + evaluate filter
		// The key advantage is that e << totalDocs for selective queries
		seekCost := (1 * CostIndexSeek) + (e * CostDocFetch) + (e * CostFilter)
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

		candidates = append(candidates, CandidatePlan{
			Name:    fmt.Sprintf("IndexSeek(%s)", idx.Info.Name),
			Cost:    seekCost,
			EstRows: e,
			Details: formatSeekDetails(e, seekSortCost),
		})

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

			var scanCost float64
			if params.Limit > 0 {
				// With LIMIT: expected docs to scan = LIMIT / P_total, capped at TotalDocs
				s := float64(params.Limit+params.Offset) / pTotal
				if s > totalDocs {
					s = totalDocs
				}
				if s < 1 {
					s = 1
				}
				scanCost = (s * CostIndexSeek) + (s * CostDocFetch) + (s * CostFilter)
			} else {
				// Without LIMIT: scan all docs via index (no sort penalty)
				scanCost = (totalDocs * CostIndexSeek) + (totalDocs * CostDocFetch) + (totalDocs * CostFilter)
			}
			// No sort penalty since index provides order

			// Apply index hint boost
			if boost, ok := hintBoosts[idx.Info.Name]; ok {
				scanCost -= float64(boost)
			}

			var scanRows float64
			if params.Limit > 0 {
				scanRows = float64(params.Limit+params.Offset) / pTotal
				if scanRows > totalDocs {
					scanRows = totalDocs
				}
			} else {
				scanRows = totalDocs
			}
			candidates = append(candidates, CandidatePlan{
				Name:    fmt.Sprintf("IndexScan(%s)", idx.Info.Name),
				Cost:    scanCost,
				EstRows: scanRows,
				Details: formatScanDetails(scanRows, params.Limit > 0),
			})

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
		root = &LimitIter{
			Source: root,
			Limit:  params.Limit,
			Offset: params.Offset,
		}
	}

	// Sort candidates by cost ascending
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Cost < candidates[j].Cost
	})

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
func computeFullScanCost(totalDocs, estimatedYield float64, needSort bool) float64 {
	cost := (totalDocs * CostDocFetch) + (totalDocs * CostFilter)
	if needSort {
		cost += sortCost(estimatedYield)
	}
	return cost
}

// sortCost computes the sort cost using n*log2(n)*CostSortSwap.
func sortCost(n float64) float64 {
	if n <= 1 {
		return 0
	}
	return n * math.Log2(n) * CostSortSwap
}

// calculateSelectivity computes the combined selectivity for all filter predicates.
func calculateSelectivity(filter query.Filter, indexes []CBOIndex, totalDocs float64) float64 {
	if filter == nil || isAllFilter(filter) {
		return 1.0
	}

	pTotal := 1.0
	usedFields := make(map[string]bool)

	// For each index, check if any of its fields have bounds in the filter
	for i := range indexes {
		idx := &indexes[i]
		for fi, fieldName := range idx.Info.FieldNames {
			if usedFields[fieldName] {
				continue
			}
			bounds := filter.IndexBounds(fieldName, nil)
			if len(bounds) == 0 {
				continue
			}
			usedFields[fieldName] = true

			// Check if this is an equality (point) bound
			isEquality := AllBoundsFixed(bounds)

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
	if len(usedFields) == 0 && !isAllFilter(filter) {
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
func estimateIndexDocsWithFieldSel(idx *CBOIndex, totalDocs float64, fieldSel map[string]float64) float64 {
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
				if fs, ok := fieldSel[idx.Info.FieldNames[fi]]; ok {
					sel *= fs
					hasFieldSel = true
				} else {
					sel *= DefaultRangeSelectivity
				}
			}
		}
		if hasFieldSel {
			return totalDocs * sel
		}
	}

	return totalDocs * DefaultRangeSelectivity
}

// buildFullScanChain constructs the iterator chain for a full collection scan.
func buildFullScanChain(params *PlanParams, needFilter, needSort bool) Iterator {
	var root Iterator

	idSorted := false
	if needSort && !needFilter {
		fields := params.Sorter.Fields()
		if len(fields) == 1 && fields[0].Field == "id" {
			idSorted = true
			root = &FullScanIter{
				Source: &CursorSource{
					Tx: params.Tx,
					Ns: params.DataNs,
				},
				IDBounds: params.IDBounds,
				Buf:      params.Buf,
				Reverse:  fields[0].Reverse,
			}
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
		}
	}

	return root
}

// buildIndexSeekChain constructs the iterator chain for an index seek plan.
func buildIndexSeekChain(params *PlanParams, idx *CBOIndex, needFilter, needSort bool) Iterator {
	// Determine reverse scan direction
	reverse := shouldReverse(params.Sorter, idx)

	// Check for unique index point lookup (CoverIter shortcut)
	if idx.Info.Unique && AllBoundsFixed(idx.Bounds) {
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
			}
		}

		return root
	}

	var root Iterator = &IndexIter{
		Source: &CursorSource{
			Tx: params.Tx,
			Ns: idx.Info.Ns,
		},
		IdxInfo: idx.Info,
		Bounds:  idx.Bounds,
		Reverse: reverse,
	}

	// Fetch documents by docId
	root = &FetchIter{
		Source: root,
		Data: &CursorSource{
			Tx: params.Tx,
			Ns: params.DataNs,
		},
		Buf: params.Buf,
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

	if needSort && !idx.ExactSort {
		root = &SortIter{
			Source: root,
			Data: &CursorSource{
				Tx: params.Tx,
				Ns: params.DataNs,
			},
			Sorter:    params.Sorter,
			Buf:       params.Buf,
			PartiallySorted: idx.PartialSort,
		}
	}

	return root
}

// buildIndexScanChain constructs the iterator chain for an index scan plan
// (scan index in sort order, filter, stop at limit — no in-memory sort needed).
func buildIndexScanChain(params *PlanParams, idx *CBOIndex, needFilter bool) Iterator {
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

	// Fetch documents by docId
	root = &FetchIter{
		Source: root,
		Data: &CursorSource{
			Tx: params.Tx,
			Ns: params.DataNs,
		},
		Buf: params.Buf,
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
	if len(fields) == 0 || len(idx.Reverse) == 0 {
		return false
	}
	// If the first sort field's direction differs from the index's first field direction,
	// we need to reverse the scan.
	return fields[0].Reverse != idx.Reverse[0]
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
	case *LimitIter:
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

// isAllFilter returns true if the filter matches everything.
func isAllFilter(f query.Filter) bool {
	_, ok := f.(query.All)
	return ok
}

// ComputeIndexBounds computes combined tuple bounds for an index (exported for use from query.go).
func ComputeIndexBounds(idx *IndexInfo, cond query.Filter) (query.Bounds, int) {
	type fieldBound struct {
		bounds query.Bounds
		fixed  bool
	}

	var chain []fieldBound
	for _, field := range idx.FieldNames {
		fb := cond.IndexBounds(field, nil)
		if len(fb) == 0 {
			break
		}
		allFixed := true
		for _, b := range fb {
			if len(b.Start) == 0 || !bytes.Equal(b.Start, b.End) {
				allFixed = false
				break
			}
		}
		chain = append(chain, fieldBound{bounds: fb, fixed: allFixed})
		if !allFixed {
			break
		}
	}

	if len(chain) == 0 {
		return nil, 0
	}

	chainLen := len(chain)

	var result query.Bounds
	for _, b := range chain[0].bounds {
		result = append(result, b)
	}

	for i := 1; i < len(chain); i++ {
		if !chain[i-1].fixed {
			break
		}
		var extended query.Bounds
		for _, prev := range result {
			for _, cur := range chain[i].bounds {
				eb := query.Bound{
					StartInclude: cur.StartInclude,
					EndInclude:   cur.EndInclude,
				}
				if len(cur.Start) > 0 {
					eb.Start = append(append(anyenc.Tuple(nil), prev.Start...), cur.Start...)
				} else {
					eb.Start = append(anyenc.Tuple(nil), prev.Start...)
					eb.StartInclude = true
				}
				if len(cur.End) > 0 {
					eb.End = append(append(anyenc.Tuple(nil), prev.End...), cur.End...)
				} else {
					eb.End = append(append(anyenc.Tuple(nil), prev.End...), 0xff)
					eb.EndInclude = true
				}
				extended = append(extended, eb)
			}
		}
		result = extended
	}

	return result, chainLen
}

// AdjustBoundsForNonUnique adjusts End bounds for non-unique indexes by appending 0xff
// to capture all docId suffixes.
func AdjustBoundsForNonUnique(bounds query.Bounds) query.Bounds {
	adjusted := make(query.Bounds, len(bounds))
	for i, b := range bounds {
		adjusted[i] = b
		if len(b.End) > 0 && b.EndInclude {
			adjusted[i].End = append(append(anyenc.Tuple(nil), b.End...), 0xff)
			adjusted[i].EndInclude = true
		}
	}
	return adjusted
}

// IndexSortMatch checks if an index covers the sort fields.
// equalityPrefix is the number of leading index fields pinned by equality filters;
// these can be skipped when matching sort fields since they're constant within a range.
func IndexSortMatch(idx *IndexInfo, sortFields []query.SortField, equalityPrefix int) (exactSort, partialSort bool) {
	if len(sortFields) == 0 || len(idx.FieldNames) == 0 {
		return false, false
	}

	bestMatch := 0

	// Try 1: match sort fields starting from the beginning of the index
	match := 0
	for i, sf := range sortFields {
		if i >= len(idx.FieldNames) {
			break
		}
		if idx.FieldNames[i] != sf.Field {
			break
		}
		match++
	}
	if match > bestMatch {
		bestMatch = match
	}

	// Try 2: match sort fields starting after the equality prefix
	if equalityPrefix > 0 && equalityPrefix < len(idx.FieldNames) {
		match = 0
		for si, sf := range sortFields {
			ii := equalityPrefix + si
			if ii >= len(idx.FieldNames) {
				break
			}
			if idx.FieldNames[ii] != sf.Field {
				break
			}
			match++
		}
		if match > bestMatch {
			bestMatch = match
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

