package qplanner

import (
	"bytes"
	"cmp"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
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

	// DocRaw is the current row's decoded (s2-decompressed) document bytes,
	// set by a RawForSort FullScanIter instead of parsing. Consumed — and
	// cleared — by SortIter's raw sort-key path. Valid only until the next
	// row's decode: the bytes live in the parser's decode buffer.
	DocRaw []byte

	// Distances is the per-document ANN distance sidecar for a vector query
	// (docId bytes -> distance). Populated by VectorIter; consumed by the
	// public iterator's Distance(). nil for non-vector plans.
	Distances *FloatSidecar

	// Scores is the per-document BM25 relevance sidecar for a full-text query
	// (docId bytes -> score). Populated by FtsIter; consumed by the public
	// iterator's Score(). nil for non-fts plans.
	Scores *FloatSidecar

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

	// PrimaryKey is the collection's primary-key field. Empty ⇒ "id". A single
	// Sort on this field needs no SortIter because a full scan already yields
	// primary-key order.
	PrimaryKey string

	Limit  int
	Offset int
	Buf    *syncpool.DocBuffer

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

	// Vector, when set, makes this a vector (ANN) query: BuildPlan builds a
	// VectorIter source and ignores all other indexes. Filter (the residual,
	// minus the vector clause) and Sorter still apply as downstream stages.
	Vector *VectorQuerySpec

	// Fts, when set, makes this a full-text query: BuildPlan builds an FtsIter
	// source and ignores all other indexes. Filter (the residual, minus the
	// $text clause) and Sorter still apply as downstream stages.
	Fts *FtsQuerySpec
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

	// Ns is the index's B-tree namespace, used by BuildPlan to estimate range
	// selectivity via page interpolation (RangeFraction) at plan time. May be nil
	// in unit tests, in which case the planner falls back to DefaultRangeSelectivity.
	Ns *btree.Namespace

	// BoundFields is the number of index fields covered by the bound chain.
	// Sketch estimates are only valid when BoundFields == len(Info.FieldNames).
	BoundFields int

	// PointLookup is true when ALL original bounds are equality (Start == End),
	// before AdjustBoundsForNonUnique modifies End. This allows correct sketch estimation.
	PointLookup bool

	// ScalarProven is true when the index provably holds no fan-out (multikey)
	// entries in the query's snapshot — the sticky index-level multikey flag
	// read through the query's own read tx (index.go isScalarProven). False
	// means "unknown or multikey": the flag is only resolved when a consumer
	// needs it (a systemNS point Get per index), so absence of proof must
	// always degrade to the conservative path.
	ScalarProven bool

	// Sort coverage analysis
	ExactSort   bool
	PartialSort bool

	// SortMatchStart is the index-field position where IndexSortMatch aligned
	// the sort run (0 for a prefix match, or the equality prefix when the
	// equality-pinned start wins). shouldReverse reads idx.Reverse[SortMatchStart]
	// to decide forward vs reverse scan. Only load-bearing when ExactSort is true.
	SortMatchStart int

	// rangeSel is the within-index selectivity of this index's RANGE bounds,
	// estimated by BuildPlan via B-tree page interpolation (sum of RangeFraction
	// over the bounds). It is the real ordered-stats estimate the hash sketch
	// cannot give for ranges. Zero means "not computed" (no read txn / namespace,
	// or an equality lookup that uses the sketch instead) — the estimators then
	// fall back to DefaultRangeSelectivity. Planner-internal; not set by callers.
	rangeSel float64

	// EstBounds is the tight-channel tuple bounds (ComputeIndexBoundsTight),
	// set only when they differ from Bounds. DOC-SELECTIVITY ESTIMATION ONLY:
	// rangeSelTight ranks them so a two-sided range's match fraction is rated
	// (lo,hi) instead of (lo,+inf). Seeks — and the SCAN-COST estimate, which
	// must price the entries the chain actually visits — keep Bounds unless
	// the index is proven fan-out-free (then Bounds IS tight and EstBounds is
	// nil). Built through the same reverse-flag transform as Bounds, so
	// RangeFraction ranks them in stored-key space.
	EstBounds query.Bounds

	// rangeSelTight is the interpolated fraction of EstBounds (== rangeSel
	// when EstBounds is nil). It feeds calculateSelectivity's per-field match
	// fraction (how many DOCS match — tight is sound and more accurate there)
	// but never the seek/scan cost terms, which are priced from the bounds
	// the chain executes with (rangeSel over Bounds): costing tight while
	// seeking wide undercharges the seek and picks index scans that still
	// walk half the index. Planner-internal.
	rangeSelTight float64
}

// fullKeyPointBound reports whether the bound chain pins EVERY index field
// with an equality. One definition for its three consumers — the CoverIter
// routing gate and the two IndexIter.FullKeyBound assignments — so they can
// never drift on what "full-key point bound" means (a doc contributes at most
// one entry per full key tuple, even on a multikey index; a trailing RANGE
// bound does not qualify).
func (idx *CBOIndex) fullKeyPointBound() bool {
	return idx.PointLookup && idx.BoundFields == len(idx.Info.FieldNames)
}

// BuildPlan constructs an iterator chain using the Cost-Based Optimizer.
// It evaluates full scan, index seek, and index scan plans, then picks the cheapest.
func BuildPlan(params *PlanParams) *Plan {
	// Vector query: the ANN search is the sole source. Bypass the CBO entirely —
	// no range-index selection, no cost comparison, no full-scan fallback.
	if params.Vector != nil {
		return buildVectorPlan(params)
	}
	// Full-text query: the $text search is the sole source. Same rationale — a
	// $text predicate must drive, so there is no cost decision to make.
	if params.Fts != nil {
		return buildFtsPlan(params)
	}

	needSort := params.Sorter != nil
	needFilter := params.Filter != nil && !isAllFilter(params.Filter)
	totalDocs := float64(params.TotalDocs)
	if totalDocs < 1 {
		totalDocs = 1
	}

	// Range selectivity via B-tree page interpolation. A range predicate
	// ($gt/$lt/$ne/...) has no point estimate — the hash sketch is unordered — so
	// for each non-equality index we interpolate the fraction of index entries its
	// bounds cover directly from the live index B-tree (one descent per endpoint).
	// This is what lets a selective range on a dense index beat a full scan; for a
	// sparse index it composes with the EntryCount bound as e = rangeSel·EntryCount.
	// Runs BEFORE calculateSelectivity so pTotal's range terms can adopt the
	// interpolated fraction. Interpolation ranks the tight-channel bounds
	// (estBounds) so a two-sided range is rated (lo,hi), not (lo,+inf).
	if params.Tx != nil {
		for i := range params.Indexes {
			idx := &params.Indexes[i]
			if idx.PointLookup || idx.Ns == nil || len(idx.Bounds) == 0 {
				continue
			}
			// Interpolation counts index ENTRIES. For a multikey/array index an
			// entry-fraction no longer maps to a document-fraction (one document
			// fans out to many entries, and dedup collapses them), so the estimate
			// would be biased; fall back to the EntryCount-bounded default there.
			// Entries == documents (no fan-out) is exactly EntryCount(0) <= totalDocs.
			if idx.Sketch != nil && idx.Sketch.EntryCount(0) > uint64(params.TotalDocs) {
				continue
			}
			// rangeSel prices the SCAN (the bounds the chain seeks with);
			// rangeSelTight rates the MATCH fraction (tight channel). They
			// differ only for unproven indexes carrying EstBounds.
			idx.rangeSel = interpolateRangeSel(params.Tx, idx, idx.Bounds)
			idx.rangeSelTight = idx.rangeSel
			if idx.EstBounds != nil {
				idx.rangeSelTight = interpolateRangeSel(params.Tx, idx, idx.EstBounds)
			}
		}
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
	// FullScan naturally reads in primary-key order, so sorting by the primary
	// key is free.
	fullScanNeedSort := needSort
	if needSort {
		fields := params.Sorter.Fields()
		pk := params.PrimaryKey
		if pk == "" {
			pk = "id"
		}
		if len(fields) == 1 && fields[0].Field == pk {
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
		if len(idx.Info.FieldNames) == 1 && idx.PointLookup && len(idx.Bounds) > 0 {
			var est float64
			if idx.Info.Unique && !idx.Info.Sparse {
				// Each equality bound matches at most one document (see
				// uniqueFullKeyDocs) — no sketch needed. Sparse is excluded:
				// a sparse index drops null/missing docs, so eq-null bounds
				// can match far more documents than the index has entries.
				est = float64(len(idx.Bounds))
			} else if idx.Sketch != nil {
				est = float64(idx.Sketch.Estimate(0, idx.Bounds[0].Start))
			}
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
		// A sparse index omits documents missing/null in any of its fields, so it
		// can only answer a query that constrains every field to be present —
		// otherwise seeking it would silently drop matching rows.
		if !sparseIndexComplete(idx, params.Filter) {
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
			// Same sparse-completeness gate as Plan B: a sparse index that omits
			// some matching document must not drive the scan even when it covers
			// the sort order (e.g. Sort(a) over an unconstrained sparse index on a
			// would drop every null/missing-a document).
			if !sparseIndexComplete(idx, params.Filter) {
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
		slices.SortFunc(candidates, func(a, b CandidatePlan) int {
			return cmp.Compare(a.Cost, b.Cost)
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

// buildVectorPlan builds the iterator chain for a $knn query:
//
//	VectorIter(ef) -> [FilterIter(residual)] -> LimitIter{Limit:K} -> [SortIter] -> [LimitIter{Offset,Limit}]
//
// The ANN search is the only source. The semantics are filter → cut-to-k →
// sort → page: the residual filter (everything except the $knn clause —
// including any _distance threshold and additional field predicates) runs over
// the _distance-decorated candidates in the source's (distance, docId) order,
// the k-cut then bounds the denoted set to the K nearest survivors, and only
// that set is sorted and paginated. The k-cut sits BEFORE the user sort by
// design — "k selects, Sort orders, Limit paginates" — so the same K documents
// are denoted regardless of presentation order, on every verb.
//
// (LimitIter is not an offsetSkipper, so no pushed-down skip can corrupt the
// k-cut; Count's LimitIter.CountDistinct fast path computes correctly over the
// stacked shape.)
func buildVectorPlan(params *PlanParams) *Plan {
	dataCS := &CursorSource{Tx: params.Tx, Ns: params.DataNs}
	source := &VectorIter{
		Spec: params.Vector,
		Data: dataCS,
		Buf:  params.Buf,
	}
	return buildSearchPlan(params, dataCS, source, params.Vector.K, "KnnSearch", params.Vector.IndexName)
}

// buildFtsPlan builds the iterator chain for a full-text query:
//
//	FtsIter -> [FilterIter(residual)] -> [SortIter] -> [LimitIter]
//
// The BM25 search is the only source. The residual filter (everything except the
// $text clause) and the sort run as ordinary downstream stages over the
// _score-decorated candidate documents. With no sort (or a relevance/textScore
// sort, which the caller maps to nil), the FtsIter's Ordered score-descending
// stream flows straight to LimitIter — no SortIter. An explicit sort on a real
// field inserts a SortIter that re-orders the candidates.
func buildFtsPlan(params *PlanParams) *Plan {
	dataCS := &CursorSource{Tx: params.Tx, Ns: params.DataNs}
	source := &FtsIter{
		Spec: params.Fts,
		Data: dataCS,
		Buf:  params.Buf,
	}
	return buildSearchPlan(params, dataCS, source, 0, "FtsSearch", params.Fts.IndexName)
}

// buildSearchPlan assembles the shared downstream chain of the search-source
// plans (vector/fts):
//
//	source -> [FilterIter(residual)] -> [LimitIter{Limit:kLimit}] -> [SortIter] -> [LimitIter{Offset,Limit}]
//
// kLimit > 0 inserts the vector k-cut between the residual filter and the user
// sort; fts passes 0 (no cut — FtsIter's stream is already score-ordered).
func buildSearchPlan(params *PlanParams, dataCS *CursorSource, source Iterator, kLimit int, name, indexName string) *Plan {
	root := source
	if params.Filter != nil && !isAllFilter(params.Filter) {
		root = &FilterIter{Source: root, Data: dataCS, Filter: params.Filter, Buf: params.Buf}
	}
	if kLimit > 0 {
		root = &LimitIter{Source: root, Limit: kLimit}
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

	plan := &Plan{Root: root, Name: name, IndexName: indexName}
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
		// A full-scan sort must materialize the filtered rows into a slice before
		// it can order them: a linear Go allocation/GC cost the n*log2(n) swap term
		// alone understates. Charging it here lets an order-providing index scan win
		// for selective or LIMIT-capped queries, while the index scan's own
		// per-row fetch cost still protects the poorly-selective case.
		cost += sortCost(estimatedYield) + (estimatedYield * CostMaterialize)
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

// sortTopK returns the bounded heap size for an in-memory SortIter.
//
// The bounded max-heap optimization (keep only the smallest Limit+Offset rows)
// is sound ONLY when a finite Limit caps the result window. With Limit == 0 the
// caller wants the full result tail past Offset, so the heap must be unbounded
// (TopK == 0 → full sort): a heap sized to just Offset would retain exactly the
// rows the subsequent LimitIter then skips, yielding zero results. See the
// offset-without-limit regression in offset/limit tests.
func sortTopK(params *PlanParams) int {
	if params.Limit > 0 {
		return params.Limit + params.Offset
	}
	return 0
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

	// For each index, check if any of its fields have bounds in the filter.
	// Two passes: single-field UNIQUE (non-sparse) indexes claim their fields
	// first — their equality bounds price a field at len(bounds)/N exactly (see
	// uniqueFullKeyDocs), and each field is priced by whichever index claims it
	// first, so letting a compound index (or a shared sketch bucket) claim the
	// same field would re-inflate pTotal and misprice a LIMIT-capped FullScan
	// (fullScanEffective = limit/pTotal).
	for pass := 0; pass < 2; pass++ {
		for i := range indexes {
			idx := &indexes[i]
			uniqueSingle := idx.Info.Unique && !idx.Info.Sparse && len(idx.Info.FieldNames) == 1
			if (pass == 0) != uniqueSingle {
				continue
			}
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

				if isEquality && uniqueSingle {
					// Equality on a single-field UNIQUE index: each bound matches at
					// most one document (see uniqueFullKeyDocs) — bypass the sketch,
					// whose shared-bucket floor would otherwise understate the
					// selectivity and make a LIMIT-capped FullScan look cheap.
					// Sparse uniques never take this branch (pass-0 filter above):
					// a sparse index drops null/missing docs, so eq-null bounds can
					// match far more documents than the index has entries.
					p := float64(len(bounds)) / totalDocs
					if p > 1.0 {
						p = 1.0
					}
					pTotal *= p
				} else if isEquality && idx.Sketch != nil && fi == 0 && sketchLevelTrusted(idx.Sketch, 0) {
					// Equality on the index's leading field: the level-0 sketch holds
					// the count for that field's value alone (the prefix), so this is
					// accurate for both single-field and compound indexes.
					est := idx.Sketch.Estimate(0, bounds[0].Start)
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
					// Range predicate: prefer the interpolated fraction of THIS
					// field's own bound chain (BoundFields==1 means the chain is
					// exactly this leading field) over the flat default — this is
					// what moves a two-sided range from 0.50 to its real ~0.005
					// Same one-way ratchet as selectivityForIndex, same
					// entries→docs conversion via the index population; the
					// interpolation loop already skipped multikey indexes, whose
					// entry fractions don't map to doc fractions.
					p := DefaultRangeSelectivity
					if fi == 0 && idx.BoundFields == 1 && idx.rangeSelTight > 0 && idx.rangeSelTight < p {
						p = idx.rangeSelTight * indexPopulation(idx, totalDocs) / totalDocs
						if p <= 0 {
							p = 0.0001
						}
						if p > 1 {
							p = 1
						}
					}
					pTotal *= p
				}
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

// sketchLevelTrusted reports whether a sketch level holds usable counts. After a
// legacy single-level blob is loaded into a multi-level sketch (NeedsRebuild),
// only the full-key level carries data; the shallow prefix levels are empty
// until a rescan or fresh writes repopulate them, so estimating from them would
// be falsely optimistic. Treat shallow levels of a not-yet-rebuilt sketch as
// untrusted and let the caller fall back to DefaultRangeSelectivity.
func sketchLevelTrusted(s *IndexSketch, level int) bool {
	if !s.NeedsRebuild() {
		return true
	}
	return level == s.NumLevels()-1
}

// indexPopulation returns the number of documents reachable through this index:
// its live entry count at the deepest bound level. For a SPARSE index this count
// is far below the collection size and is itself a selectivity cut, so a range
// fallback measured against it (rather than totalDocs) lets a sparse index win
// natively — e.g. {a:{$ne:""}} on an index where only a few percent of docs have
// `a` touches only those entries. Falls back to totalDocs when the sketch is
// absent or the level is untrusted (legacy blob pending rebuild). Multikey/array
// indexes can report more entries than documents; matching DOCUMENTS can never
// exceed the collection, so the result is capped at totalDocs.
func indexPopulation(idx *CBOIndex, totalDocs float64) float64 {
	if idx.Sketch == nil {
		return totalDocs
	}
	level := max(idx.BoundFields-1, 0)
	level = min(level, idx.Sketch.NumLevels()-1)
	if !sketchLevelTrusted(idx.Sketch, level) {
		return totalDocs
	}
	pop := float64(idx.Sketch.EntryCount(level))
	if pop <= 0 || pop > totalDocs {
		return totalDocs
	}
	return pop
}

// interpolateRangeSel estimates the fraction of this index's entries that its
// range bounds cover, by interpolating positions in the live index B-tree. The
// bounds of a $ne are a two-piece union ((-inf,v) ∪ (v,+inf)), so the per-bound
// fractions are summed. Returns 0 (→ caller falls back to DefaultRangeSelectivity)
// on any read error, leaving the planner's degraded behavior unchanged.
func interpolateRangeSel(tx *btree.ReadTx, idx *CBOIndex, bounds query.Bounds) float64 {
	cur := tx.NewCursor(idx.Ns)
	defer cur.Close()
	var f float64
	for _, b := range bounds {
		bf, err := cur.RangeFraction(b.Start, b.End)
		if err != nil {
			return 0
		}
		f += bf
	}
	if f <= 0 {
		return 0
	}
	if f > 1 {
		f = 1
	}
	return f
}

// sparseIndexComplete reports whether idx can represent every document matching
// filter. A non-sparse index always can: a missing field is encoded as null
// (anyenc marshals a nil value to TypeNull), so every document gets a key. A
// SPARSE index instead drops any key with a missing or null field, so it is
// complete only when the query guarantees all of its fields are present and
// non-null. Without this gate the cost model would happily pick a sparse index
// for a query that leaves one of its fields unconstrained (or constrains it with
// $exists:false) and silently drop the rows that index never stored.
func sparseIndexComplete(idx *CBOIndex, filter query.Filter) bool {
	if !idx.Info.Sparse {
		return true
	}
	for _, field := range idx.Info.FieldNames {
		if !query.GuaranteesPresence(filter, field) {
			return false
		}
	}
	return true
}

// uniqueFullKeyDocs returns the exact row bound for a full-key equality lookup
// on a UNIQUE index: every bound's key matches at most one entry, hence at most
// one document, regardless of what the sketch says. The sketch is a shared-
// bucket frequency estimate whose floor grows with the collection (~N/Size for
// distinct values), so past ~150k docs it inflates a unique point lookup enough
// to flip LIMIT-capped plans to FullScan. SQLite prices a fully-bound unique
// index at one row without consulting stats (whereLoopAddBtreeIndex,
// WHERE_ONEROW); this is the same rule. Partial prefixes are excluded —
// uniqueness of (a,b) bounds nothing for a=x alone. Multikey/sparse entries
// don't break the rule: unique enforcement is per entry, and a concrete
// full-key value still maps to at most one entry.
func uniqueFullKeyDocs(idx *CBOIndex) (float64, bool) {
	if idx.Info.Unique && idx.PointLookup &&
		idx.BoundFields == len(idx.Info.FieldNames) && len(idx.Bounds) > 0 {
		return float64(len(idx.Bounds)), true
	}
	return 0, false
}

// selectivityForIndex returns the selectivity contribution of a specific index.
func selectivityForIndex(idx *CBOIndex, totalDocs float64) float64 {
	if len(idx.Bounds) == 0 {
		return 1.0
	}

	if docs, ok := uniqueFullKeyDocs(idx); ok {
		p := docs / totalDocs
		if p > 1.0 {
			p = 1.0
		}
		return p
	}

	// Use the sketch for any equality prefix: the level-(BoundFields-1) row holds
	// the joint count for the first BoundFields fields, so a partial prefix on a
	// compound index (e.g. a=x on (a,b)) gets a real estimate instead of the
	// DefaultRangeSelectivity fallback. idx.Bounds[0].Start is the encoded prefix
	// of exactly those fields.
	if idx.PointLookup && idx.Sketch != nil && idx.BoundFields >= 1 &&
		idx.BoundFields <= idx.Sketch.NumLevels() &&
		sketchLevelTrusted(idx.Sketch, idx.BoundFields-1) {
		est := idx.Sketch.Estimate(idx.BoundFields-1, idx.Bounds[0].Start)
		p := float64(est) / totalDocs
		if p <= 0 {
			p = 0.0001
		}
		if p > 1.0 {
			p = 1.0
		}
		return p
	}

	// Range / non-equality fallback: bound by the index's own population so a
	// sparse index (EntryCount << totalDocs) is credited its presence cut instead
	// of being charged the full collection. Interpolation acts as a one-way
	// ratchet: the real interpolated rangeSel is adopted only when it is MORE
	// selective than the conservative default — it refines a genuinely selective
	// range downward (so the index wins) but never penalizes an index above the
	// default for a broad range (so no previously-indexed plan regresses).
	f := DefaultRangeSelectivity
	if idx.rangeSel > 0 && idx.rangeSel < f {
		f = idx.rangeSel
	}
	return f * indexPopulation(idx, totalDocs) / totalDocs
}

// estimateIndexDocsWithFieldSel estimates the number of documents an index seek will return,
// using per-field selectivity from single-field indexes for compound indexes with partial bounds.
func estimateIndexDocsWithFieldSel(idx *CBOIndex, totalDocs float64, fieldSel []fieldSelEntry) float64 {
	if len(idx.Bounds) == 0 {
		return totalDocs
	}

	if docs, ok := uniqueFullKeyDocs(idx); ok {
		return docs
	}

	// Equality prefix: the level-(BoundFields-1) sketch row gives the joint count
	// for the bound prefix directly — exact for the full key and accurate for a
	// partial prefix on a compound index alike.
	if idx.PointLookup && idx.Sketch != nil && idx.BoundFields >= 1 &&
		idx.BoundFields <= idx.Sketch.NumLevels() &&
		sketchLevelTrusted(idx.Sketch, idx.BoundFields-1) {
		var total float64
		for _, b := range idx.Bounds {
			total += float64(idx.Sketch.Estimate(idx.BoundFields-1, b.Start))
		}
		return total
	}

	// Range bounds: adopt the interpolated within-index fraction (a real ordered
	// B-tree estimate) only when it is MORE selective than the default — the
	// one-way ratchet (see selectivityForIndex): a selective range is refined down
	// so the index wins, a broad range never inflates e above the default. rangeSel
	// already spans the whole bound union (e.g. both halves of a $ne), so it is
	// applied directly to the population rather than multiplied per field.
	if idx.rangeSel > 0 && idx.rangeSel < DefaultRangeSelectivity {
		return idx.rangeSel * indexPopulation(idx, totalDocs)
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
			return indexPopulation(idx, totalDocs) * sel
		}
	}

	// Fallback: multiply DefaultRangeSelectivity per bound field, against the
	// index's own population so a sparse index is credited its presence cut.
	sel := 1.0
	for range idx.BoundFields {
		sel *= DefaultRangeSelectivity
	}
	return indexPopulation(idx, totalDocs) * sel
}

// buildFullScanChain constructs the iterator chain for a full collection scan.
func buildFullScanChain(params *PlanParams, needFilter, needSort bool) Iterator {
	var root Iterator

	idSorted := false
	if needSort {
		fields := params.Sorter.Fields()
		pk := params.PrimaryKey
		if pk == "" {
			pk = "id"
		}
		if len(fields) == 1 && fields[0].Field == pk {
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
		// Filterless (nil/match-all) scan feeding a raw-capable sorter: the
		// installed match-all filter would force a full parse of every row
		// only to read the sort fields. Drop it and let the scan hand decoded
		// bytes to the SortIter (Plan.DocRaw), whose RawSort path seeks just
		// the sort fields. Rows are only ever accepted either way.
		if !needFilter {
			if fsi, isScan := root.(*FullScanIter); isScan {
				if _, isRaw := params.Sorter.(query.RawSort); isRaw {
					fsi.Filter = nil
					fsi.RawForSort = true
				}
			}
		}
		root = &SortIter{
			Source: root,
			Data: &CursorSource{
				Tx: params.Tx,
				Ns: params.DataNs,
			},
			Sorter: params.Sorter,
			Buf:    params.Buf,
			TopK:   sortTopK(params),
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
	// EVERY index entry key is Tuple(fields..., docId) — unique and non-unique
	// alike (index.go insertKeys appends the docId unconditionally) — so a bare
	// inclusive End sorts strictly BEFORE every entry holding that value.
	// Unbound trailing fields of a compound index add further suffix bytes.
	// In all cases appending 0xff to an inclusive End covers all suffixes.
	if len(idx.Bounds) > 0 {
		idx.Bounds = AdjustBoundsForNonUnique(idx.Bounds)
	}

	// Determine reverse scan direction
	reverse := shouldReverse(params.Sorter, idx)

	// Check for unique index point lookup (CoverIter shortcut).
	// Only safe when ALL index fields are covered by equality bounds;
	// a partial prefix (BoundFields < len(FieldNames)) can match multiple
	// entries with different trailing fields, so a range scan is needed.
	if idx.Info.Unique && idx.fullKeyPointBound() {
		var root Iterator = &CoverIter{
			Source: &CursorSource{
				Tx: params.Tx,
				Ns: idx.Info.Ns,
			},
			IdxInfo: idx.Info,
			Bounds:  idx.Bounds,
		}

		// A unique index can still be multikey (each array element unique
		// across docs), so a multi-bound $in can hit the SAME doc through
		// several of its elements — CoverIter then tags entries multiKey (see
		// its probe). Dedup in-plan, below the Sort/Limit stages added around
		// this chain, for the same reason as the compound wrap below: raw
		// cross-bound repeats must not consume offset/limit/TopK slots.
		// Single-bound lookups can't straddle bounds and skip the wrap.
		if len(idx.Bounds) > 1 {
			root = &DocDedupIter{Source: root}
		}

		// Covering count over the unique point probes: when the bounds fully
		// and exactly represent the filter (indexCoversFilter — every field in
		// the bounded prefix, one predicate per field), the FilterIter
		// re-evaluation below is redundant and its per-row doc fetch+parse is
		// the entire cost of a counting $in/Eq over a unique index. Same
		// elision as the IndexIter covering path below; SQLite analog: WHERE
		// terms consumed by the index probe are marked TERM_CODED and never
		// re-evaluated (whereterm.c disableTerm via wherecode.c
		// codeAllEqualityTerms). Distinctness across bounds is preserved by
		// the DocDedupIter wrap above (multikey-tagged entries) and by
		// countPlanRoot's distinct consumers.
		if params.CountOnly && indexCoversFilter(idx, params.Filter) {
			return root
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
				TopK:   sortTopK(params),
			}
		}

		return root
	}

	// IndexIter consumes range bounds over FULL keys (field values + docId
	// suffix), so make them escape-exact for a reverse tail field. Keep the
	// pre-pad bounds for CanonicalKeyDedupIter below: it tests BARE field
	// values, which the +0x01 Start pad would wrongly exclude. (CoverIter
	// above does not take padded bounds either — it seeks the raw prefix and
	// applies HasExactFieldPrefix instead.)
	dedupBounds := finalizeIndexBounds(idx)

	// Use batched allocation for the common case: IndexIter + FetchIter + FilterIter.
	// This replaces 5 separate heap allocations with 1.
	b := &seekBatch{}
	b.indexCS = CursorSource{Tx: params.Tx, Ns: idx.Info.Ns}
	b.indexIter = IndexIter{
		Source:       &b.indexCS,
		IdxInfo:      idx.Info,
		Bounds:       idx.Bounds,
		Reverse:      reverse,
		PointLookup:  idx.PointLookup,
		FullKeyBound: idx.fullKeyPointBound(),
		ScalarProven: idx.ScalarProven,
	}

	var root Iterator = &b.indexIter

	// Covering index count: when only counting and the index has exact equality
	// bounds (PointLookup) that cover all filter fields, skip FetchIter and
	// FilterIter and return the raw IndexIter. Count() short-circuits via
	// IndexIter.CountEntries.
	//
	// Multi-bound safety: IndexIter.CountEntries inspects each entry's
	// per-entry value byte (see qplanner.IndexEntryFlagMultiKey) and
	// applies a lazy seen-set only for multi-key (or legacy) entries.
	// Scalar entries stream-count without dedup overhead. So a multi-bound
	// $in over a multi-key array index is correctly deduped without a
	// Fetch/Filter wrap, and over a scalar field stays as fast as a
	// single-bound count.
	if params.CountOnly && idx.PointLookup && indexCoversFilter(idx, params.Filter) {
		return root
	}

	// Index verification for count queries: instead of fetching documents to
	// check uncovered filter fields, verify each docId against single-field
	// indexes for those fields. This avoids expensive document fetches.
	//
	// Like the covering fast path above, this skips the residual FilterIter,
	// so the candidate's bounded fields must be EXACTLY represented by their
	// bounds: a field carrying more than one predicate can be PointLookup via
	// the tight channel ({$gte:5,$lte:5,$nin:[5]} collapses to [5,5]) while a
	// bound-LESS conjunct ($nin, $type, $not) still needs the filter — without
	// this gate that conjunct silently vanishes and Count over-counts vs Iter.
	// Mirrors indexCoversFilter condition 2.
	if params.CountOnly && idx.PointLookup && params.FieldBounds != nil &&
		boundedFieldsSinglePredicate(idx, params.Filter) {
		if verifyRoot := buildVerifyChain(params, idx, root); verifyRoot != nil {
			return verifyRoot
		}
	}

	// Compound multikey dedup, BELOW the fetch and every Sort/Limit stage: a
	// doc with arrays in the indexed fields fans out into several entries, and
	// raw entries must not consume TopK/offset/limit slots or duplicate
	// fetches. Single-field indexes get CanonicalKeyDedupIter below instead.
	// See the DocDedupIter doc comment for the placement constraints.
	// A resolved scalar proof makes the stage a provable passthrough — skip
	// it; ScalarProven=false means unknown OR multikey, so the wrap stays.
	if len(idx.Info.FieldPaths) > 1 && !idx.ScalarProven {
		root = &DocDedupIter{Source: root}
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
	//
	// Single-field indexes use canonical-key dedup (O(1) memory, streaming,
	// doc-driven — hence above FetchIter) to filter duplicates before the
	// sort/limit stages. Compound indexes were already deduped by the
	// DocDedupIter inserted below the fetch (canonical-key selection across
	// compound tuples is non-trivial, so they dedup by docId
	// in first-occurrence order instead). Exactly one of the two wraps is
	// present per chain; either way the stream reaching SortIter/LimitIter
	// and the consumers is unique-by-doc, and no per-query map is allocated
	// for fully-scalar streams.
	if len(idx.Info.FieldPaths) == 1 {
		root = &CanonicalKeyDedupIter{
			Source:       root,
			Bounds:       dedupBounds,
			FieldPath:    idx.Info.FieldPaths[0],
			Reverse:      reverse,
			FieldReverse: len(idx.Info.Reverse) > 0 && idx.Info.Reverse[0],
		}
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
			TopK:            sortTopK(params),
			PartiallySorted: idx.PartialSort,
		}
	}

	return root
}

// buildIndexScanChain constructs the iterator chain for an index scan plan
// (scan index in sort order, filter, stop at limit — no in-memory sort needed).
func buildIndexScanChain(params *PlanParams, idx *CBOIndex, needFilter bool) Iterator {
	// Adjust bounds for the docId key suffix (deferred from buildCBOIndexesInto).
	// Applies to unique indexes too — see buildIndexSeekChain.
	if len(idx.Bounds) > 0 {
		idx.Bounds = AdjustBoundsForNonUnique(idx.Bounds)
	}
	// Pre-pad bounds for CanonicalKeyDedupIter (bare field values); padded
	// bounds for IndexIter (full keys) — see buildIndexSeekChain.
	dedupBounds := finalizeIndexBounds(idx)
	reverse := shouldReverse(params.Sorter, idx)

	var root Iterator = &IndexIter{
		Source: &CursorSource{
			Tx: params.Tx,
			Ns: idx.Info.Ns,
		},
		IdxInfo:      idx.Info,
		Bounds:       idx.Bounds, // may be nil for full index scan
		Reverse:      reverse,
		PointLookup:  idx.PointLookup,
		FullKeyBound: idx.fullKeyPointBound(),
		ScalarProven: idx.ScalarProven,
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

	// Compound multikey dedup — see buildIndexSeekChain. Must sit ABOVE
	// IndexFilterIter (per-entry cover verdicts differ across a doc's
	// entries) and below the fetch.
	if len(idx.Info.FieldPaths) > 1 && !idx.ScalarProven {
		root = &DocDedupIter{Source: root}
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

	// Covering elision: when the index-side checks (bound prefix +
	// IndexFilterIter equality fields) fully and exactly represent the
	// filter, the FilterIter re-evaluation is redundant per accepted entry.
	// Same principle as the covering count elision in buildIndexSeekChain;
	// SQLite analog: index-consumed WHERE terms are TERM_CODED and never
	// re-evaluated.
	if needFilter && !indexScanCoversFilter(idx, coverFilters, params.Filter) {
		root = &FilterIter{
			Source: root,
			Data:   scanDataSrc,
			Filter: params.Filter,
			Buf:    params.Buf,
		}
	}

	// Dedup wrap — see buildIndexSeekChain for rationale. Single-field gets
	// the streaming canonical-key dedup here; compound multikey was already
	// deduped by the DocDedupIter below the fetch.
	if len(idx.Info.FieldPaths) == 1 {
		root = &CanonicalKeyDedupIter{
			Source:       root,
			Bounds:       dedupBounds,
			FieldPath:    idx.Info.FieldPaths[0],
			Reverse:      reverse,
			FieldReverse: len(idx.Info.Reverse) > 0 && idx.Info.Reverse[0],
		}
	}

	// No sort needed — index provides the order
	return root
}

// shouldReverse determines if an index scan should go in reverse direction
// based on the sort spec and the index's declared per-field direction.
//
// A forward index scan yields the index's declared per-field directions
// (reverse-flagged fields are stored bitwise-inverted; see index.go
// writeValues). So the scan is FORWARD iff the first matched sort field's
// direction equals the declared direction of the index field it aligns to, and
// REVERSE iff they are opposite. The first matched sort field is always
// fields[0] (IndexSortMatch aligns sortFields[si] → idx.FieldNames[matchStart+si],
// and the matched run is uniform), so reading idx.Reverse[SortMatchStart] is
// sufficient for the whole run.
//
// Note: this is also called for index-SEEK plans where ExactSort==false; there
// the output is re-sorted by a SortIter, so the scan direction is immaterial and
// a stale SortMatchStart (0) is harmless. Direction is only load-bearing when
// ExactSort==true, and then SortMatchStart is set correctly.
func shouldReverse(sorter query.Sort, idx *CBOIndex) bool {
	if sorter == nil {
		return false
	}
	fields := sorter.Fields()
	if len(fields) == 0 {
		return false
	}
	idxRev := false
	if idx != nil {
		ms := idx.SortMatchStart
		if ms >= 0 && ms < len(idx.Reverse) {
			idxRev = idx.Reverse[ms]
		}
	}
	return fields[0].Reverse != idxRev
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
	case *DocDedupIter:
		setPlanRef(v.Source, plan) // no Plan field: dedups by docId alone
	case *VectorIter:
		v.Plan = plan
		// leaf — no upstream source
	case *FtsIter:
		v.Plan = plan
		// leaf — no upstream source
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

// indexCoversFilter reports whether the CountOnly fast path — where index
// bounds alone determine the matching docs and no data fetch (FilterIter) is
// needed — is SOUND for this filter and index. Two conditions must hold:
//
//  1. Every field referenced by the filter is an index field
//     (filterFieldsCoveredBy). Complex nodes (Or/Not/Nor) where field
//     extraction isn't reliable are treated as not covered.
//
//  2. No covered field carries more than one predicate. And.IndexBounds returns
//     a SOUND OVER-APPROXIMATION (a superset of the matches) whenever a field
//     has multiple conjuncts: {a:{$in:[1,2]},$and:[{a:{$gte:5}}]} yields the
//     $in bounds, and a two-sided range {a:{$gte:2,$lte:3}} yields just
//     [2,+inf]. Counting those bounds directly would over-count — the FilterIter
//     the fast path skips is exactly what trims the superset to the real
//     matches (and for ARRAY fields the bounds MUST over-approximate; see
//     query/filter.go:And.IndexBounds). So reject multi-predicate fields here
//     and let the FilterIter path handle them. See docs/known-issues.md (I-04).
//
// Zero-allocation: inline field matching, no maps or slices.
func indexCoversFilter(idx *CBOIndex, filter query.Filter) bool {
	if filter == nil || len(idx.Bounds) == 0 {
		return false
	}
	// Only fields pinned by the CONTIGUOUS bound prefix are actually constrained
	// by the index seek. A filter field that is an index field but lies BEYOND
	// the bound prefix (skip-middle, e.g. {a,c} on index (a,b,c) → BoundFields=1)
	// is NOT enforced by the bounds; counting entries in the bounded range would
	// silently ignore it and over-count. So gate coverage on the bounded prefix
	// only, mirroring buildVerifyChain's FieldNames[:idx.BoundFields].
	boundedFields := idx.Info.FieldNames
	if idx.BoundFields < len(boundedFields) {
		boundedFields = boundedFields[:idx.BoundFields]
	}
	// Condition 1: every filter field is within the bounded index prefix.
	// Returns false on any uncovered field or any complex node (Or/Not/Nor).
	hasFields := false
	if ok := filterFieldsCoveredBy(filter, boundedFields, &hasFields); !ok || !hasFields {
		return false
	}
	// Condition 2: reject when any covered field carries >1 predicate, because
	// And.IndexBounds then over-approximates and the fast path skips the
	// FilterIter that would trim the result.
	for _, field := range boundedFields {
		if countFilterFieldPreds(filter, field) > 1 {
			return false
		}
	}
	return true
}

// indexScanCoversFilter reports whether an ordered-scan chain's index-side
// checks fully and exactly represent filter, making the post-fetch FilterIter
// redundant. Two enforcement channels cover a field: the contiguous bound
// prefix (enforced by IndexIter's Bounds, exactly like indexCoversFilter) and
// the IndexFilterIter equality fields (byte-equality on the stored key value;
// coveringFilterFields only emits a field when its bounds are a single fixed
// point, i.e. the predicate's exact image). Entry-level equality implies
// doc-level match under array-element semantics, and per-doc duplicates are
// handled by the dedup wraps that are present regardless of the FilterIter.
// The single-predicate condition rejects fields whose combined bounds
// over-approximate (see indexCoversFilter condition 2).
func indexScanCoversFilter(idx *CBOIndex, coverFilters []IndexFieldFilter, filter query.Filter) bool {
	if filter == nil || len(coverFilters) == 0 {
		return false
	}
	var fields []string
	if len(idx.Bounds) > 0 {
		fields = append(fields, idx.Info.FieldNames[:min(idx.BoundFields, len(idx.Info.FieldNames))]...)
	}
	for _, f := range coverFilters {
		fields = append(fields, idx.Info.FieldNames[f.FieldIdx])
	}
	hasFields := false
	if ok := filterFieldsCoveredBy(filter, fields, &hasFields); !ok || !hasFields {
		return false
	}
	for _, field := range fields {
		if countFilterFieldPreds(filter, field) > 1 {
			return false
		}
	}
	return true
}

// boundedFieldsSinglePredicate reports whether every field of idx's contiguous
// bound prefix carries at most one predicate in filter — the condition under
// which the bounds exactly represent those fields' constraints and a
// FilterIter-skipping count path (covering count, verify chain) is sound.
func boundedFieldsSinglePredicate(idx *CBOIndex, filter query.Filter) bool {
	boundedFields := idx.Info.FieldNames
	if idx.BoundFields < len(boundedFields) {
		boundedFields = boundedFields[:idx.BoundFields]
	}
	for _, field := range boundedFields {
		if countFilterFieldPreds(filter, field) > 1 {
			return false
		}
	}
	return true
}

// countFilterFieldPreds returns how many leaf predicates in f constrain the
// given field path. A field bound by more than one predicate — two same-field
// conjuncts ({a:{$in:[1,2]},$and:[{a:{$gte:5}}]}) or an inline multi-op
// ({a:{$in:[1,2],$gte:5}}, which parses to a Key wrapping an And) — is NOT
// exactly captured by And.IndexBounds' over-approximation, so indexCoversFilter
// uses this to gate the CountOnly fast path. Zero-allocation.
func countFilterFieldPreds(f query.Filter, field string) int {
	switch ft := f.(type) {
	case query.Key:
		if strings.Join(ft.Path, ".") != field {
			return 0
		}
		// One Key on this field; its inner filter is an And when the field
		// carries several ops inline (e.g. {$in,$gte}), so count those leaves.
		return countInnerPreds(ft.Filter)
	case query.And:
		n := 0
		for _, sub := range ft {
			n += countFilterFieldPreds(sub, field)
		}
		return n
	case *query.And:
		n := 0
		for _, sub := range *ft {
			n += countFilterFieldPreds(sub, field)
		}
		return n
	default:
		return 0
	}
}

// countInnerPreds counts the leaf predicates inside a Key's filter: a bare
// Comp/In is one; an inline And of ops ({a:{$in,$gte}}) is the sum of its
// leaves. The inner filter constrains a single field, so it contains no Keys.
func countInnerPreds(f query.Filter) int {
	switch ft := f.(type) {
	case query.And:
		n := 0
		for _, sub := range ft {
			n += countInnerPreds(sub)
		}
		return n
	case *query.And:
		n := 0
		for _, sub := range *ft {
			n += countInnerPreds(sub)
		}
		return n
	default:
		return 1
	}
}

// filterFieldsCoveredBy walks the filter tree and checks that every referenced
// field name is present in idxFields. Zero-allocation.
func filterFieldsCoveredBy(f query.Filter, idxFields []string, hasFields *bool) bool {
	switch ft := f.(type) {
	case query.Key:
		name := strings.Join(ft.Path, ".")
		if slices.Contains(idxFields, name) {
			*hasFields = true
			return true
		}
		return false
	case query.And:
		for _, sub := range ft {
			if !filterFieldsCoveredBy(sub, idxFields, hasFields) {
				return false
			}
		}
		return true
	case *query.And:
		// Symmetric with collectUncoveredFilterFields's pointer arm.
		// query.MustParseCondition produces *query.And for `{"$and":[...]}`
		// (see query/cond_parse.go:103), so without this case the covering-
		// count fast path is silently disabled for $and-spelled filters.
		for _, sub := range *ft {
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
		if slices.Contains(coveredFields, name) {
			return []string{} // covered
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
		var verifyReverse bool
		for i := range params.Indexes {
			info := params.Indexes[i].Info
			if !info.Unique && len(info.FieldNames) == 1 && info.FieldNames[0] == field {
				verifyNs = info.Ns
				verifyReverse = len(info.Reverse) > 0 && info.Reverse[0]
				break
			}
		}
		if verifyNs == nil {
			return nil
		}

		// The verify index stores its field bitwise-inverted when reverse-declared,
		// so the equality prefix (computed in ascending space) must be inverted to
		// match the stored key bytes; otherwise VerifyIter never finds the entry.
		prefix := bounds[0].Start
		if verifyReverse {
			prefix = invertBytes(prefix)
		}

		current = &VerifyIter{
			Source:   current,
			Tx:       params.Tx,
			VerifyNs: verifyNs,
			Prefix:   prefix,
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
//
// Two channels per field: the WIDE bounds from IndexBounds (the
// sound over-approximation every seek may use) and the TIGHT bounds from
// query.TightIndexBounds (same-field conjuncts intersected — safe for cost
// estimation always, and for seeks only under a fan-out-free proof). For most
// filters the channels are identical and the tight storage stays empty.
type BoundsResult struct {
	Bounds      []query.Bound // flat slice of all WIDE bounds across all fields
	TightBounds []query.Bound // flat slice of tight bounds for fields where they differ
	Fields      []FieldBounds // per-field metadata pointing into Bounds
	// tightFields carries the tight-channel spans, indexed by
	// FieldBounds.TightIdx. Kept out of FieldBounds so the common no-tighten
	// query pays zero struct growth (BoundsResult escapes per query and
	// fieldsBuf is inline — every FieldBounds byte is multiplied by 8).
	tightFields []tightFieldBounds
	boundsBuf   [8]query.Bound
	fieldsBuf   [8]FieldBounds
}

// FieldBounds holds pre-computed bounds for a single filter field.
type FieldBounds struct {
	Field string
	Start int  // start index into BoundsResult.Bounds
	Count int  // number of bounds for this field
	Fixed bool // all bounds are equality (Start == End)

	// TightIdx indexes BoundsResult.tightFields, or -1 when the tight channel
	// is identical to the wide one for this field (also when the tight
	// intersection came out empty: emptiness must NOT be treated as a
	// narrowing by estimation — it is not unsatisfiability under array
	// semantics; the pk-only unsat decision is made at the query layer).
	// int8 packs into Fixed's padding, keeping the struct at its pre-tight
	// size.
	TightIdx int8
}

// tightFieldBounds is a tight-channel span into BoundsResult.TightBounds.
type tightFieldBounds struct {
	Start int
	Count int
	Fixed bool
}

// Build computes bounds for all unique fields across the given indexes.
func (br *BoundsResult) Build(indexInfos []*IndexInfo, filter query.Filter) {
	br.Bounds = br.boundsBuf[:0]
	br.TightBounds = nil
	br.tightFields = nil
	br.Fields = br.fieldsBuf[:0]
	// Single-predicate filters can't tighten; skip the per-field tight walk.
	mayTighten := query.MayTighten(filter)
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

			fb := FieldBounds{
				Field:    field,
				Start:    start,
				Count:    count,
				Fixed:    allBoundsFixedNonEmpty(br.Bounds[start:]),
				TightIdx: -1,
			}
			// countFilterFieldPreds is a zero-alloc pre-check: tight bounds
			// can only differ from wide when THIS field carries more than
			// one predicate — MayTighten alone also fires for multi-FIELD
			// conjunctions ({a:1,b:2}), which would pay an allocating walk
			// per field for an always-equal result.
			if mayTighten && count > 0 && len(br.tightFields) < 127 &&
				countFilterFieldPreds(filter, field) > 1 {
				tight, tEmpty := query.TightIndexBounds(filter, field)
				if !tEmpty && !boundsEqual(tight, bs) {
					fb.TightIdx = int8(len(br.tightFields))
					br.tightFields = append(br.tightFields, tightFieldBounds{
						Start: len(br.TightBounds),
						Count: len(tight),
						Fixed: allBoundsFixedNonEmpty(tight),
					})
					br.TightBounds = append(br.TightBounds, tight...)
				}
			}
			br.Fields = append(br.Fields, fb)
		}
	}
}

// allBoundsFixedNonEmpty reports whether bs is non-empty and every bound is an
// equality point (Start == End). A field with zero bounds has no equality
// constraint and must not be reported as "fixed" (see AllFixed's godoc).
func allBoundsFixedNonEmpty(bs []query.Bound) bool {
	if len(bs) == 0 {
		return false
	}
	for _, b := range bs {
		if len(b.Start) == 0 || !bytes.Equal(b.Start, b.End) {
			return false
		}
	}
	return true
}

// boundsEqual reports whether two bound sets are identical (count, endpoint
// bytes, inclusivity) — the "tight ≠ wide" test gating tight-channel work.
func boundsEqual(a, b query.Bounds) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].StartInclude != b[i].StartInclude || a[i].EndInclude != b[i].EndInclude ||
			!bytes.Equal(a[i].Start, b[i].Start) || !bytes.Equal(a[i].End, b[i].End) {
			return false
		}
	}
	return true
}

// Lookup returns the WIDE bounds for a field name.
func (br *BoundsResult) Lookup(field string) (bounds query.Bounds, fixed bool, found bool) {
	for i := range br.Fields {
		if br.Fields[i].Field == field {
			s := br.Fields[i].Start
			return br.Bounds[s : s+br.Fields[i].Count], br.Fields[i].Fixed, true
		}
	}
	return nil, false, false
}

// LookupTight returns the TIGHT bounds for a field name, falling back to the
// wide bounds when the channels are identical. Callers own the soundness
// argument: estimation may always use these; seeks may not without a
// fan-out-free proof (see query.TightIndexBounds).
func (br *BoundsResult) LookupTight(field string) (bounds query.Bounds, fixed bool, found bool) {
	for i := range br.Fields {
		if br.Fields[i].Field == field {
			ti := br.Fields[i].TightIdx
			if ti < 0 {
				s := br.Fields[i].Start
				return br.Bounds[s : s+br.Fields[i].Count], br.Fields[i].Fixed, true
			}
			tf := br.tightFields[ti]
			return br.TightBounds[tf.Start : tf.Start+tf.Count], tf.Fixed, true
		}
	}
	return nil, false, false
}

// TightDiffers reports whether the tight channel differs from the wide one
// for any of the given fields.
func (br *BoundsResult) TightDiffers(fields []string) bool {
	if len(br.tightFields) == 0 {
		return false
	}
	for _, field := range fields {
		for i := range br.Fields {
			if br.Fields[i].Field == field {
				if br.Fields[i].TightIdx >= 0 {
					return true
				}
				break
			}
		}
	}
	return false
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

// invertBytes returns a fresh bitwise-NOT of b (nil for empty input).
func invertBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	out := make([]byte, len(b))
	for i, c := range b {
		out[i] = ^c
	}
	return out
}

// transformReverseBounds maps per-field bounds from ascending value space into
// the inverted (stored) byte space used for a reverse-flagged index field.
// Inversion reverses byte order, so each bound's Start/End are inverted AND
// swapped, and the inclusivity flags swap with them. An open ascending end
// (empty End, +inf) becomes an open stored start (empty Start), and vice versa.
// Because the per-bound Start keys change, the result is re-sorted so
// IndexIter walks the bounds in cursor order (relevant for $in / $ne, which
// produce multiple bounds). A FRESH slice is always returned — the input
// (which aliases BoundsResult) is never mutated, so ascending-space readers
// such as calculateSelectivity remain correct.
//
// The sort key is compareInvertedStart, NOT plain bytes.Compare: anyenc
// encodings are prefix-free except across the NUL escape (enc(v) is a
// byte-prefix of enc(v+"\x00…")), and bitwise inversion preserves the prefix
// relation without reversing it. The v+"\x00…" continuation group lives at
// inv(enc(v))‖0x00…, BELOW every key of the v group (which continues with a
// byte >= 0x01) — so the bound with the LONGER inverted Start selects the
// SMALLER stored keys and must be walked first, the opposite of what a plain
// byte compare on bare Starts yields. The input arrives merged (ascending
// SortAndMerge upstream) and inversion preserves disjointness, so no merge
// pass is needed — and Bounds.SortAndMerge must not run here, since its
// bytes.Compare ordering is exactly the wrong order for prefix-related
// Starts.
func transformReverseBounds(bs query.Bounds) query.Bounds {
	if len(bs) == 0 {
		return bs
	}
	out := make(query.Bounds, 0, len(bs))
	for _, b := range bs {
		nb := query.Bound{
			Start:        invertBytes(b.End),
			End:          invertBytes(b.Start),
			StartInclude: b.EndInclude,
			EndInclude:   b.StartInclude,
		}
		// An inclusive ascending Start whose bytes are an INCOMPLETE value
		// encoding — a mid-value prefix, as emitted by TypeFilter (bare type
		// tag) and Regexp (tag + string prefix, no EOS) — does not survive
		// inversion as an inclusive End: inversion reverses divergent-byte
		// order but preserves the prefix-extension relation, so every key the
		// prefix is meant to admit sorts ABOVE inv(prefix). The downstream
		// inclusive-End pad (AdjustBoundsForNonUnique's 0xFF append) only
		// rescues continuations that can never be 0xFF — true at tuple
		// boundaries, where the next byte is a docId/next-field tag or a
		// 0x00 escape tail, but false mid-value, where the continuation is
		// inverted payload: a raw 0x00 payload byte (a 1970s ObjectID
		// timestamp, a float ≤ -~9e307, the EOS of an empty string) inverts
		// to 0xFF and the key escapes the padded End — silently dropping
		// rows. Map the prefix to its exact stored-space upper bound instead:
		// the successor of the inverted prefix, exclusive. Complete-value
		// Starts keep the plain inverted End: their continuations obey the
		// tuple-boundary invariant, and the 0xFF pad both admits them and
		// deliberately excludes ascending 0xFF escape tails.
		//
		// The transformed endpoints stay slightly OVER-approximate after the
		// downstream pads (padReverseBounds widens both the exclusive-Start
		// 0xFF pad and this exclusive End with 0x01): a handful of
		// adjacent-value keys can enter the scan and are rejected by the
		// residual FilterIter. Any future exactness-based residual elision
		// must therefore treat prefix-derived bounds as INEXACT.
		if b.StartInclude && len(b.Start) > 0 && anyenc.ValidateRaw(b.Start) != nil {
			nb.End = query.PrefixSuccessor(nb.End)
			nb.EndInclude = false
		}
		out = append(out, nb)
	}
	slices.SortStableFunc(out, func(a, b query.Bound) int {
		return compareInvertedStart(a.Start, b.Start)
	})
	return out
}

// compareInvertedStart orders inverted-space bound Starts in stored-key order.
// An empty Start is -inf. For prefix-related Starts the longer one selects the
// smaller stored keys (the escape-continuation group sorts below the base
// value's own entries in inverted space) and therefore comes first; diverging
// Starts compare bytewise as usual.
func compareInvertedStart(a, b []byte) int {
	switch {
	case len(a) == 0 && len(b) == 0:
		return 0
	case len(a) == 0:
		return -1
	case len(b) == 0:
		return 1
	}
	if len(a) != len(b) {
		if bytes.HasPrefix(b, a) {
			return 1 // a is the shorter prefix: its group starts above b's
		}
		if bytes.HasPrefix(a, b) {
			return -1
		}
	}
	return bytes.Compare(a, b)
}

// padReverseBounds makes inverted-space bounds exact in the presence of
// anyenc escape continuations. The anyenc encoding of value v is a byte
// prefix of the encoding of strings v+"\x00..." (see anyenc/escape.go); in
// inverted space those continuation keys read inv(enc(v)) followed by 0x00,
// while every key whose field value IS v continues with a byte >= 0x01 (a
// docId or next-field type tag, possibly inverted). Without padding, a
// closed stored-space Start at inv(enc(v)) wrongly admits the v+"\x00..."
// group (ascending values > v), and an open End at inv(enc(v)) wrongly
// rejects it:
//
//   - Start inclusive (asc Eq/Lte boundary): pad to inv(enc(v))+0x01 — keeps
//     the whole v group, drops the 0x00 continuations.
//   - Start exclusive (asc Lt boundary): pad to inv(enc(v))+0xFF inclusive —
//     drops the v group AND its 0x00 continuations (both are >= v), keeps
//     every smaller value (they diverge above inv(enc(v)) earlier).
//   - End exclusive (asc Gt boundary): pad to inv(enc(v))+0x01, still
//     exclusive — admits the 0x00 continuations (values > v), drops the
//     v group.
//   - End inclusive (asc Gte/Eq boundary): leave as is —
//     AdjustBoundsForNonUnique appends 0xFF, which already admits both.
//
// Only the LAST chained field of an index bound may be padded: earlier
// (fixed, equality) fields are used as exact byte prefixes that later field
// bounds are concatenated to.

// finalizeIndexBounds applies the bound-normalization sequence every scanning
// chain must share — seek and scan plans have to select identical rows for
// the same query, so this order lives in exactly one place:
//
//	pad the reverse tail (escape-exact full keys for IndexIter), then
//	coalesce overlapping compound tuples (MergeOverlappingBounds; only after
//	the pad is its plain byte sort stored-key order — a bare inverted Start
//	is a byte-prefix of its escape-continuation group and would sort wrongly).
//
// Returns the PRE-pad bounds for consumers that test bare field values
// (CanonicalKeyDedupIter; the +0x01 Start pad would wrongly exclude them) and
// leaves the padded, merged bounds on idx.Bounds for IndexIter.
func finalizeIndexBounds(idx *CBOIndex) (dedupBounds query.Bounds) {
	dedupBounds = idx.Bounds
	idx.Bounds = padBoundsForReverseTail(idx)
	if idx.BoundFields > 1 {
		idx.Bounds = MergeOverlappingBounds(idx.Bounds)
	}
	return dedupBounds
}

// padBoundsForReverseTail applies padReverseBounds when the final field of the
// bound chain is reverse-flagged. Call it only for the CHOSEN index, after
// AdjustBoundsForNonUnique and after PointLookup/sketch estimation (both read
// the raw bound bytes).
func padBoundsForReverseTail(idx *CBOIndex) query.Bounds {
	n := idx.BoundFields
	if n == 0 || n > len(idx.Reverse) || !idx.Reverse[n-1] || len(idx.Bounds) == 0 {
		return idx.Bounds
	}
	return padReverseBounds(idx.Bounds)
}

func padReverseBounds(bs query.Bounds) query.Bounds {
	// Fresh Bound structs: callers keep the unpadded slice for consumers that
	// test bare field values (CanonicalKeyDedupIter), so the originals must
	// not be mutated. The byte appends below leave the original Start/End
	// slices' contents untouched (they extend or reallocate).
	out := make(query.Bounds, len(bs))
	copy(out, bs)
	bs = out
	for i := range bs {
		b := &bs[i]
		if len(b.Start) > 0 {
			if b.StartInclude {
				b.Start = append(b.Start, 0x01)
			} else {
				b.Start = append(b.Start, 0xff)
				b.StartInclude = true
			}
		}
		if len(b.End) > 0 && !b.EndInclude {
			b.End = append(b.End, 0x01)
		}
	}
	return bs
}

// ComputeIndexBounds computes combined tuple bounds for an index
// using pre-computed per-field WIDE bounds from BoundsResult.
func ComputeIndexBounds(idx *IndexInfo, br *BoundsResult) (query.Bounds, int) {
	return computeIndexBounds(idx, br.Lookup)
}

// ComputeIndexBoundsTight is the tight-channel variant, built from
// BoundsResult.LookupTight. Its result is for cost ESTIMATION only (CBOIndex
// EstBounds): feeding it to a seek requires the fan-out-free proof documented
// on query.TightIndexBounds.
func ComputeIndexBoundsTight(idx *IndexInfo, br *BoundsResult) (query.Bounds, int) {
	return computeIndexBounds(idx, br.LookupTight)
}

func computeIndexBounds(idx *IndexInfo, lookup func(string) (query.Bounds, bool, bool)) (query.Bounds, int) {
	type fieldBound struct {
		bounds query.Bounds
		fixed  bool
	}

	var chainBuf [4]fieldBound // stack-allocated for typical compound indexes
	chain := chainBuf[:0]
	for _, field := range idx.FieldNames {
		fb, fixed, found := lookup(field)
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

	// Reverse-flagged fields are stored bitwise-inverted, so their per-field
	// bounds (computed in ascending value space by query/filter.go) must be
	// transformed into the inverted (stored) byte space before being used to
	// seek or concatenated into a compound tuple bound. transformReverseBounds
	// returns a FRESH slice, so the aliased BoundsResult (read by selectivity in
	// ascending space) is never mutated.
	for i := range chain {
		if i < len(idx.Reverse) && idx.Reverse[i] {
			chain[i].bounds = transformReverseBounds(chain[i].bounds)
		}
	}

	// Single-field index: return bounds directly (transformed above if reverse,
	// otherwise the cached ascending bounds — no copy needed).
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
					// A bare prev.Start ending in an INVERTED field encoding
					// admits that value's escape-continuation group, which
					// lives at inv(enc(v))‖0x00… — ascending values GREATER
					// than v that this tuple's range must not select. Append
					// 0x01 to start above the continuations while keeping the
					// whole v group (every v key continues with a byte >=
					// 0x01) — the same rule padReverseBounds applies to a
					// reverse tail and CoverIter applies to its seek prefix.
					if i-1 < len(idx.Reverse) && idx.Reverse[i-1] && len(prev.Start) > 0 {
						arena = append(arena, 0x01)
					}
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

// HasExactFieldPrefix reports whether the stored index key k begins with the
// encoded field prefix p as COMPLETE field values. bytes.HasPrefix alone is
// not enough: the anyenc encoding of value v is a byte prefix of the encoding
// of v+"\x00..." (escaped NUL, see anyenc/escape.go), and such a key continues
// with the escape tail — 0xFF after an ascending field, 0x00 after an
// inverted (descending) one — while every key whose value IS v continues with
// a docId/next-field type tag (0x01..0x0B or 0xF4..0xFE). lastFieldInverted
// is the direction of the final field covered by p.
func HasExactFieldPrefix(k, p []byte, lastFieldInverted bool) bool {
	if !bytes.HasPrefix(k, p) {
		return false
	}
	if len(k) == len(p) {
		return true
	}
	cont := byte(0xff)
	if lastFieldInverted {
		cont = 0x00
	}
	return k[len(p)] != cont
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

// boundsOverlap reports whether b intersects a in key space, given that a
// sorts at or before b by Start. An empty End is +inf; an equal boundary key
// is shared only when both sides include it.
func boundsOverlap(a, b *query.Bound) bool {
	if len(a.End) == 0 {
		return true
	}
	switch c := bytes.Compare(b.Start, a.End); {
	case c < 0:
		return true
	case c == 0:
		return a.EndInclude && b.StartInclude
	default:
		return false
	}
}

// MergeOverlappingBounds sorts key-space bounds ascending by Start and
// coalesces every overlapping pair into its hull. IndexIter walks the bound
// list sequentially and requires it ordered and pairwise disjoint — bounds
// that are disjoint per FIELD can still overlap after compound-tuple
// concatenation, because anyenc string encodings are not prefix-free across
// the NUL escape (enc(v) is a byte-prefix of enc(v+"\x00…")): the 0xff pad
// after a shorter tuple then covers keys the next tuple's range also selects,
// and a shared entry is emitted twice (duplicate rows, double-applied
// modifiers, ErrDocNotFound on delete). Coalescing only ever WIDENS a bound —
// the residual filter rejects the extra keys; it can never drop one.
//
// Callers gate this on compound chains (BoundFields > 1): single-field bound
// lists alias a shared window into BoundsResult.Bounds and must not be
// reordered or compacted in place; they are also already disjoint (one bound
// per family member, no concatenation, wrapped in CanonicalKeyDedupIter).
//
// Deliberately NOT query.Bounds.SortAndMerge: that operates on ascending
// VALUE-space bounds and differs at shared boundaries (it keeps the first
// bound's EndInclude on equal Ends instead of OR-ing, and merges
// touching-adjacent bounds — which here would widen a half-open pair into
// covering a key neither side selects). This one runs on padded KEY-space
// tuples and coalesces only genuine overlaps.
func MergeOverlappingBounds(bounds query.Bounds) query.Bounds {
	if len(bounds) < 2 {
		return bounds
	}
	slices.SortStableFunc(bounds, func(a, b query.Bound) int {
		return bytes.Compare(a.Start, b.Start)
	})
	out := bounds[:1]
	for i := 1; i < len(bounds); i++ {
		cur := &out[len(out)-1]
		next := &bounds[i]
		if !boundsOverlap(cur, next) {
			out = append(out, *next)
			continue
		}
		// Hull: Start stays cur's (sorted first; on equal Starts the include
		// flags are OR-ed), End becomes the larger of the two.
		if bytes.Equal(cur.Start, next.Start) {
			cur.StartInclude = cur.StartInclude || next.StartInclude
		}
		switch {
		case len(cur.End) == 0:
			// already +inf
		case len(next.End) == 0:
			cur.End, cur.EndInclude = nil, false
		default:
			if c := bytes.Compare(next.End, cur.End); c > 0 {
				cur.End, cur.EndInclude = next.End, next.EndInclude
			} else if c == 0 {
				cur.EndInclude = cur.EndInclude || next.EndInclude
			}
		}
	}
	return out
}

// IndexSortMatch checks if an index covers the sort fields.
// equalityPrefix is the number of leading index fields pinned by equality filters;
// these can be skipped when matching sort fields since they're constant within a range.
//
// matchStart is the index-field position where the matched sort run begins
// (0 for a prefix match, or equalityPrefix when the equality-pinned start wins).
// shouldReverse uses it to read the declared direction of the first matched
// field (idx.Reverse[matchStart]) and pick the scan direction. It defaults to 0,
// which is correct when no equality prefix shifts the match.
func IndexSortMatch(idx *IndexInfo, sortFields []query.SortField, equalityPrefix int) (exactSort, partialSort bool, matchStart int) {
	if len(sortFields) == 0 || len(idx.FieldNames) == 0 {
		return false, false, 0
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
			// Reverse-flagged fields are stored bitwise-inverted (index.go
			// writeValues), so a forward index scan yields the index's declared
			// per-field directions. A sort field is "in the same direction" as a
			// forward scan iff the index's declared direction equals the sort's
			// direction. The whole matched run must be uniform under this
			// relation: an all-same run is served by a forward scan, an
			// all-opposite run by a reverse scan, and a run that is mixed
			// relative to the declared directions (e.g. Sort(a,b) on an (a,-b)
			// index) breaks here and falls back to an in-memory sort.
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
	bestStart := 0
	if equalityPrefix > 0 && equalityPrefix < len(idx.FieldNames) {
		if m := matchAt(equalityPrefix); m > bestMatch {
			bestMatch = m
			bestStart = equalityPrefix
		}
	}

	if bestMatch == 0 {
		return false, false, 0
	}
	if bestMatch == len(sortFields) {
		return true, false, bestStart
	}
	return false, true, bestStart
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

		// Reverse-flagged fields are stored bitwise-inverted (index.go
		// writeValues), so IndexFilterIter compares key.FieldBytes(fi) (the
		// inverted stored bytes) against MatchValue. The equality value must
		// therefore be inverted to match. This is a pure equality check
		// (Start == End), so no Start/End swap is needed — only invert.
		matchValue := bounds[0].Start
		if fi < len(idx.Info.Reverse) && idx.Info.Reverse[fi] {
			inv := make([]byte, len(matchValue))
			for j, bb := range matchValue {
				inv[j] = ^bb
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
