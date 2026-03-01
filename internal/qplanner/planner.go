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
	Name    string  // "FullScan", "IndexSeek(a)", "IndexScan(a)"
	Cost    float64 // computed cost
	EstRows float64 // estimated rows scanned/fetched
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
func formatFullScanDetails(totalDocs, estimatedYield float64, needSort bool) string {
	perDocCost := CostDocFetch
	label := "fetch"
	if totalDocs > 500 {
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
func formatSeekDetails(estRows, fetchCost, seekSortCost float64) string {
	s := fmt.Sprintf("seek(%.1f) + %.0f×fetch(%.1f) + %.0f×filter(%.1f)",
		CostIndexSeek, estRows, fetchCost, estRows, CostFilter)
	if seekSortCost > 0 {
		s += fmt.Sprintf(" + sort=%.1f", seekSortCost)
	}
	total := (1 * CostIndexSeek) + (estRows * fetchCost) + (estRows * CostFilter) + seekSortCost
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

	// FBCache is an optional pre-computed field bounds cache.
	// When set, calculateSelectivity uses cached bounds instead of calling
	// filter.IndexBounds repeatedly (avoids ~N redundant filter tree traversals).
	FBCache *FieldBoundsCache
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
	pTotal := calculateSelectivity(params.Filter, params.Indexes, totalDocs, params.FBCache)

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
	if len(params.IDBounds) > 0 && AllBoundsFixed(params.IDBounds) {
		fullScanDocs = float64(len(params.IDBounds))
		if fullScanDocs < estimatedYield {
			estimatedYield = fullScanDocs
		}
	}
	fullScanCost := computeFullScanCost(fullScanDocs, estimatedYield, needSort)

	if collectExplain {
		candidates = append(candidates, CandidatePlan{
			Name:    "FullScan",
			Cost:    fullScanCost,
			EstRows: fullScanDocs,
			details: func() string { return formatFullScanDetails(fullScanDocs, estimatedYield, needSort) },
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

		// Index seek cost: B-tree seek + fetch only matching docs + evaluate filter
		// The key advantage is that e << totalDocs for selective queries
		fetchCost := indexFetchCost(totalDocs)
		seekCost := (1 * CostIndexSeek) + (e * fetchCost) + (e * CostFilter)

		// Covering count: when only counting and this index covers the filter with
		// equality bounds, no document fetch or filter evaluation is needed.
		// Cost is just the index traversal (sequential reads through the index).
		isCovering := params.CountOnly && idx.PointLookup && indexCoversFilter(idx, params.Filter)
		if isCovering {
			seekCost = (1 * CostIndexSeek) + (e * CostSeqRead)
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
			seekE, seekFetchCost, seekSC := e, fetchCost, seekSortCost
			candidates = append(candidates, CandidatePlan{
				Name:    "IndexSeek(" + idx.Info.Name + ")",
				Cost:    seekCost,
				EstRows: e,
				details: func() string { return formatSeekDetails(seekE, seekFetchCost, seekSC) },
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
				scanCost = (s * CostIndexSeek) + (s * fetchCost) + (s * CostFilter)
			} else {
				// Without LIMIT: scan all docs via index (no sort penalty)
				scanCost = (totalDocs * CostIndexSeek) + (totalDocs * fetchCost) + (totalDocs * CostFilter)
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
		root = &LimitIter{
			Source: root,
			Limit:  params.Limit,
			Offset: params.Offset,
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
func computeFullScanCost(totalDocs, estimatedYield float64, needSort bool) float64 {
	perDocCost := CostDocFetch
	if totalDocs > 500 {
		perDocCost = CostSeqRead
	}
	cost := (totalDocs * perDocCost) + (totalDocs * CostFilter)
	if needSort {
		cost += sortCost(estimatedYield)
	}
	return cost
}

// indexFetchCost returns the per-doc cost for random B-tree point lookups.
// For large collections (>500 docs), deeper B-trees make each random lookup
// more expensive due to additional tree level traversals (index + data trees).
func indexFetchCost(totalDocs float64) float64 {
	if totalDocs <= 500 {
		return CostDocFetch
	}
	return CostDocFetch * math.Log10(totalDocs)
}

// sortCost computes the sort cost using n*log2(n)*CostSortSwap.
func sortCost(n float64) float64 {
	if n <= 1 {
		return 0
	}
	return n * math.Log2(n) * CostSortSwap
}

// calculateSelectivity computes the combined selectivity for all filter predicates.
func calculateSelectivity(filter query.Filter, indexes []CBOIndex, totalDocs float64, fbCache *FieldBoundsCache) float64 {
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
			if fbCache != nil {
				var fixed, found bool
				bounds, fixed, found = fbCache.Lookup(fieldName)
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
	// Adjust bounds for non-unique indexes (deferred from buildCBOIndexesInto
	// to avoid allocation overhead for indexes that aren't chosen by CBO).
	if !idx.Info.Unique && len(idx.Bounds) > 0 {
		idx.Bounds = AdjustBoundsForNonUnique(idx.Bounds)
	}

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
	if params.CountOnly && idx.PointLookup && params.FBCache != nil {
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
		var name string
		if ft.FullPath != "" {
			name = ft.FullPath
		} else if len(ft.Path) == 1 {
			name = ft.Path[0]
		} else {
			name = strings.Join(ft.Path, ".")
		}
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
		name := ft.FullPath
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
		bounds, fixed, found := params.FBCache.Lookup(field)
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

// FieldBoundsCache pre-computes IndexBounds for all unique field names,
// avoiding repeated filter tree traversals. Use with ComputeIndexBoundsFromCache.
type FieldBoundsCache struct {
	entries   [8]fieldBoundsEntry // inline storage for typical queries (≤8 fields)
	n         int
	arenaBuf  [128]byte    // inline arena for compound tuple construction
	arena     []byte       // current arena slice (grows from arenaBuf)
	boundsBuf    [4]query.Bound // inline buffer for per-field bounds (avoids Bounds.Append allocs)
	boundsN      int            // number of bounds used in boundsBuf
	compBoundBuf [2]query.Bound // inline buffer for compound index combined bounds
	compBoundN   int            // number of compound bounds used
}

type fieldBoundsEntry struct {
	field  string
	bounds query.Bounds
	fixed  bool // all bounds are equality (Start == End)
}

// allocTuple allocates a tuple from the arena, returning a sub-slice with 1 byte
// of extra capacity. The extra byte allows AdjustBoundsForNonUnique to append
// 0xff without triggering a heap allocation.
func (c *FieldBoundsCache) allocTuple(parts ...[]byte) anyenc.Tuple {
	total := 0
	for _, p := range parts {
		total += len(p)
	}
	if c.arena == nil {
		c.arena = c.arenaBuf[:0]
	}
	off := len(c.arena)
	for _, p := range parts {
		c.arena = append(c.arena, p...)
	}
	// Reserve 1 extra byte so AdjustBoundsForNonUnique can append 0xff in-place
	c.arena = append(c.arena, 0)
	return c.arena[off : off+total : off+total+1]
}

// Build populates the cache by calling filter.IndexBounds once per unique field
// across all index infos.
func (c *FieldBoundsCache) Build(indexInfos []*IndexInfo, filter query.Filter) {
	c.n = 0
	c.arena = nil
	c.boundsN = 0
	c.compBoundN = 0
	for _, info := range indexInfos {
		for _, field := range info.FieldNames {
			// Check if already cached
			found := false
			for j := 0; j < c.n; j++ {
				if c.entries[j].field == field {
					found = true
					break
				}
			}
			if found || c.n >= len(c.entries) {
				continue
			}
			// Use inline boundsBuf to avoid allocation in Bounds.Append.
			// Pass a pre-allocated slice with capacity so append reuses it.
			var fb query.Bounds
			if c.boundsN < len(c.boundsBuf) {
				fb = filter.IndexBounds(field, c.boundsBuf[c.boundsN:c.boundsN:c.boundsN+1])
				if len(fb) > 0 {
					c.boundsN += len(fb)
				}
			} else {
				fb = filter.IndexBounds(field, nil)
			}
			allFixed := true
			for _, b := range fb {
				if len(b.Start) == 0 || !bytes.Equal(b.Start, b.End) {
					allFixed = false
					break
				}
			}
			c.entries[c.n] = fieldBoundsEntry{field: field, bounds: fb, fixed: allFixed}
			c.n++
		}
	}
}

// Lookup returns the cached bounds for a field name, or nil if not found.
func (c *FieldBoundsCache) Lookup(field string) (query.Bounds, bool, bool) {
	for i := 0; i < c.n; i++ {
		if c.entries[i].field == field {
			return c.entries[i].bounds, c.entries[i].fixed, true
		}
	}
	return nil, false, false
}

// FieldCount returns the number of unique filter fields in the cache.
func (c *FieldBoundsCache) FieldCount() int {
	return c.n
}

// AllFixed returns true if all cached fields have equality (fixed point) bounds.
func (c *FieldBoundsCache) AllFixed() bool {
	for i := 0; i < c.n; i++ {
		if !c.entries[i].fixed {
			return false
		}
	}
	return c.n > 0
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

// ComputeIndexBoundsFromCache computes combined tuple bounds using pre-cached field bounds.
// This avoids repeated filter.IndexBounds calls when processing multiple indexes.
// Compound tuple byte slices are allocated from the cache's arena to reduce heap allocations.
func ComputeIndexBoundsFromCache(idx *IndexInfo, cache *FieldBoundsCache) (query.Bounds, int) {
	type fieldBound struct {
		bounds query.Bounds
		fixed  bool
	}

	var chainBuf [4]fieldBound // stack-allocated for typical compound indexes
	chain := chainBuf[:0]
	for _, field := range idx.FieldNames {
		fb, fixed, found := cache.Lookup(field)
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

	// Compound index: build combined tuple bounds using arena for byte allocations
	var resultBuf [1]query.Bound // stack alloc for single-bound case (most common)
	result := resultBuf[:0]
	for _, b := range chain[0].bounds {
		result = append(result, b)
	}

	var extBuf [1]query.Bound // stack alloc for extended bounds
	for i := 1; i < len(chain); i++ {
		if !chain[i-1].fixed {
			break
		}
		extended := extBuf[:0]
		for _, prev := range result {
			for _, cur := range chain[i].bounds {
				eb := query.Bound{
					StartInclude: cur.StartInclude,
					EndInclude:   cur.EndInclude,
				}
				if len(cur.Start) > 0 {
					eb.Start = cache.allocTuple(prev.Start, cur.Start)
				} else {
					eb.Start = cache.allocTuple(prev.Start)
					eb.StartInclude = true
				}
				if len(cur.End) > 0 {
					eb.End = cache.allocTuple(prev.End, cur.End)
				} else {
					eb.End = cache.allocTuple(prev.End, []byte{0xff})
					eb.EndInclude = true
				}
				extended = append(extended, eb)
			}
		}
		result = extended
	}

	// Use cache's inline compound bounds buffer if space is available
	if cache.compBoundN+len(result) <= len(cache.compBoundBuf) {
		off := cache.compBoundN
		copy(cache.compBoundBuf[off:], result)
		cache.compBoundN += len(result)
		return cache.compBoundBuf[off : off+len(result) : off+len(result)], chainLen
	}
	// Fallback: heap-allocate when inline buffer is full
	heapResult := make(query.Bounds, len(result))
	copy(heapResult, result)
	return heapResult, chainLen
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

