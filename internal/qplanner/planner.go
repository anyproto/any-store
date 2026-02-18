package qplanner

import (
	"bytes"

	"github.com/anyproto/any-store/internal/btree"
	"github.com/anyproto/any-store/query"
	"github.com/anyproto/any-store/syncpool"
)

// Plan holds the root iterator of a query execution plan.
type Plan struct {
	Root     Iterator
	DocValue []byte // set by FilterIter/FullScanIter when they fetch doc data
}

// String returns a human-readable description of the full plan chain.
func (p *Plan) String() string {
	if p.Root == nil {
		return "NoPlan"
	}
	return p.Root.String()
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

	// Indexes sorted by weight (highest first), with bounds pre-computed.
	Indexes []PlanIndex
}

// PlanIndex represents an index candidate for the planner.
type PlanIndex struct {
	Info               *IndexInfo
	Bounds             query.Bounds
	Weight             int
	ExactSort          bool
	PartialSort        bool
	PartialSortFields  int
	Used               bool
	FilterFullyCovered bool // index bounds fully express the query filter
}

// BuildPlan constructs an iterator chain based on available indexes and query params.
func BuildPlan(params *PlanParams) *Plan {
	var root Iterator

	// Collect all used indexes
	var usedIndexes []*PlanIndex
	for i := range params.Indexes {
		if params.Indexes[i].Used && params.Indexes[i].Weight >= 1 {
			usedIndexes = append(usedIndexes, &params.Indexes[i])
		}
	}

	// Find the primary index (best one with bounds or sort coverage)
	var primaryIdx *PlanIndex
	var coverIndexes []*PlanIndex
	for _, idx := range usedIndexes {
		if primaryIdx == nil && (len(idx.Bounds) > 0 || idx.ExactSort || idx.PartialSort) {
			primaryIdx = idx
		} else {
			coverIndexes = append(coverIndexes, idx)
		}
	}

	needSort := params.Sorter != nil
	needFilter := params.Filter != nil && !isAllFilter(params.Filter)

	if primaryIdx != nil && len(primaryIdx.Bounds) > 0 {
		// Check if all bounds are fixed-point (Start == End) for cover lookup
		allFixed := primaryIdx.Info.Unique && allBoundsFixed(primaryIdx.Bounds)

		if allFixed {
			root = &CoverIter{
				Source: &CursorSource{
					Tx: params.Tx,
					Ns: primaryIdx.Info.Ns,
				},
				IdxInfo: primaryIdx.Info,
				Bounds:  primaryIdx.Bounds,
			}
		} else {
			// Determine if we should reverse the index scan
			reverse := false
			if (primaryIdx.ExactSort || primaryIdx.PartialSort) && params.Sorter != nil {
				fields := params.Sorter.Fields()
				if len(fields) > 0 && len(primaryIdx.Info.Reverse) > 0 {
					if fields[0].Reverse != primaryIdx.Info.Reverse[0] {
						reverse = true
					}
				}
			}

			root = &IndexIter{
				Source: &CursorSource{
					Tx: params.Tx,
					Ns: primaryIdx.Info.Ns,
				},
				IdxInfo: primaryIdx.Info,
				Bounds:  primaryIdx.Bounds,
				Reverse: reverse,
			}
		}

		// Apply cover filtration from additional indexes
		for _, coverIdx := range coverIndexes {
			if len(coverIdx.Bounds) > 0 {
				root = &CoverFilterIter{
					Source:  root,
					Data:    &CursorSource{Tx: params.Tx, Ns: params.DataNs},
					IdxInfo: coverIdx.Info,
					Index:   &CursorSource{Tx: params.Tx, Ns: coverIdx.Info.Ns},
					Bounds:  coverIdx.Bounds,
					Buf:     params.Buf,
				}
			}
		}

		// After index scan, fetch data and apply remaining filter
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

		// Sort if index doesn't cover sorting fully
		if needSort && !primaryIdx.ExactSort {
			root = &SortIter{
				Source: root,
				Data: &CursorSource{
					Tx: params.Tx,
					Ns: params.DataNs,
				},
				Sorter:    params.Sorter,
				Buf:       params.Buf,
				PreSorted: primaryIdx.PartialSort,
			}
		}
	} else if primaryIdx != nil && (primaryIdx.ExactSort || primaryIdx.PartialSort) && !idBoundsPreferred(params.IDBounds) {
		// Index covers sort (fully or partially) but has no query bounds - scan full index
		reverse := false
		if params.Sorter != nil {
			fields := params.Sorter.Fields()
			if len(fields) > 0 && len(primaryIdx.Info.Reverse) > 0 {
				if fields[0].Reverse != primaryIdx.Info.Reverse[0] {
					reverse = true
				}
			}
		}

		root = &IndexIter{
			Source: &CursorSource{
				Tx: params.Tx,
				Ns: primaryIdx.Info.Ns,
			},
			IdxInfo: primaryIdx.Info,
			Reverse: reverse,
		}

		// Apply cover filtration from additional indexes
		for _, coverIdx := range coverIndexes {
			if len(coverIdx.Bounds) > 0 {
				root = &CoverFilterIter{
					Source:  root,
					Data:    &CursorSource{Tx: params.Tx, Ns: params.DataNs},
					IdxInfo: coverIdx.Info,
					Index:   &CursorSource{Tx: params.Tx, Ns: coverIdx.Info.Ns},
					Bounds:  coverIdx.Bounds,
					Buf:     params.Buf,
				}
			}
		}

		// Fetch data and filter
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

		// If only partial sort, still need a full re-sort (but pre-sorted data helps quicksort)
		if needSort && !primaryIdx.ExactSort {
			root = &SortIter{
				Source: root,
				Data: &CursorSource{
					Tx: params.Tx,
					Ns: params.DataNs,
				},
				Sorter:    params.Sorter,
				Buf:       params.Buf,
				PreSorted: primaryIdx.PartialSort,
			}
		}
	} else {
		// Full scan path
		idSorted := false
		if needSort && !needFilter {
			// Check if sorting by "id" - then we can use data namespace order
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
	}

	// Apply limit/offset
	if params.Limit > 0 || params.Offset > 0 {
		root = &LimitIter{
			Source: root,
			Limit:  params.Limit,
			Offset: params.Offset,
		}
	}

	plan := &Plan{Root: root}

	// Wire plan reference into FilterIter instances for doc value caching
	setPlanRef(root, plan)

	return plan
}

// setPlanRef walks the iterator chain and sets the Plan reference on FilterIter nodes.
// It stops at SortIter because SortIter collects all docs first, making cached values stale.
func setPlanRef(it Iterator, plan *Plan) {
	switch v := it.(type) {
	case *FilterIter:
		v.Plan = plan
		setPlanRef(v.Source, plan)
	case *CoverFilterIter:
		setPlanRef(v.Source, plan)
	case *SortIter:
		// Don't propagate plan ref past SortIter — it collects all docs,
		// so any cached DocValue would be stale by the time Next() returns.
	case *LimitIter:
		setPlanRef(v.Source, plan)
	}
}

// idBoundsPreferred returns true when id bounds are specific enough that
// direct data-namespace lookups + in-memory sort is cheaper than a full index scan.
// This is the case for small $in sets where all bounds are fixed-point.
func idBoundsPreferred(idBounds query.Bounds) bool {
	if len(idBounds) == 0 {
		return false
	}
	// If all id bounds are point lookups, prefer direct data access
	return allBoundsFixed(idBounds)
}

// allBoundsFixed returns true if all bounds have Start == End (point lookups).
func allBoundsFixed(bounds query.Bounds) bool {
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
