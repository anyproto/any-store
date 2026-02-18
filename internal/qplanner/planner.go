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
	Used               bool
	FilterFullyCovered bool // index bounds fully express the query filter
}

// BuildPlan constructs an iterator chain based on available indexes and query params.
func BuildPlan(params *PlanParams) *Plan {
	var root Iterator

	// Try to find a usable index
	var bestIdx *PlanIndex
	for i := range params.Indexes {
		idx := &params.Indexes[i]
		if idx.Weight < 1 || !idx.Used {
			continue
		}
		if len(idx.Bounds) > 0 {
			bestIdx = idx
			break
		}
		if idx.ExactSort {
			bestIdx = idx
			break
		}
	}

	needSort := params.Sorter != nil
	needFilter := params.Filter != nil && !isAllFilter(params.Filter)

	if bestIdx != nil && len(bestIdx.Bounds) > 0 {
		// Check if all bounds are fixed-point (Start == End) for cover lookup
		allFixed := bestIdx.Info.Unique && allBoundsFixed(bestIdx.Bounds)

		if allFixed {
			root = &CoverIter{
				Source: &CursorSource{
					Tx: params.Tx,
					Ns: bestIdx.Info.Ns,
				},
				IdxInfo: bestIdx.Info,
				Bounds:  bestIdx.Bounds,
			}
		} else {
			// Determine if we should reverse the index scan
			reverse := false
			if bestIdx.ExactSort && params.Sorter != nil {
				// Check if first sort field's direction matches index direction
				fields := params.Sorter.Fields()
				if len(fields) > 0 && len(bestIdx.Info.Reverse) > 0 {
					// If sort wants reverse of what index provides, scan backwards
					if fields[0].Reverse != bestIdx.Info.Reverse[0] {
						reverse = true
					}
				}
			}

			root = &IndexIter{
				Source: &CursorSource{
					Tx: params.Tx,
					Ns: bestIdx.Info.Ns,
				},
				IdxInfo: bestIdx.Info,
				Bounds:  bestIdx.Bounds,
				Reverse: reverse,
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

		// Sort if index doesn't cover sorting
		if needSort && !bestIdx.ExactSort {
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
	} else if bestIdx != nil && bestIdx.ExactSort {
		// Index covers sort but has no query bounds - scan full index
		reverse := false
		if params.Sorter != nil {
			fields := params.Sorter.Fields()
			if len(fields) > 0 && len(bestIdx.Info.Reverse) > 0 {
				if fields[0].Reverse != bestIdx.Info.Reverse[0] {
					reverse = true
				}
			}
		}

		root = &IndexIter{
			Source: &CursorSource{
				Tx: params.Tx,
				Ns: bestIdx.Info.Ns,
			},
			IdxInfo: bestIdx.Info,
			Reverse: reverse,
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
		// No sort needed - index covers it
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
	case *SortIter:
		// Don't propagate plan ref past SortIter — it collects all docs,
		// so any cached DocValue would be stale by the time Next() returns.
	case *LimitIter:
		setPlanRef(v.Source, plan)
	}
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
