package qplan

import (
	stdsort "sort"

	"github.com/anyproto/any-store/internal/index"
	"github.com/anyproto/any-store/internal/iterator"
	"github.com/anyproto/any-store/internal/key"
	"github.com/anyproto/any-store/internal/qcontext"
	"github.com/anyproto/any-store/internal/sort"
	"github.com/anyproto/any-store/query"
)

type QPlan struct {
	Indexes   []*index.Index
	Condition query.Filter
	Sort      sort.Sort
	Hint      string
}

func (q QPlan) Make(qCtx *qcontext.QueryContext, needValues bool) iterator.ValueIterator {
	var sortFields []sort.SortField
	if q.Sort != nil && needValues {
		sortFields = q.Sort.Fields()
	}

	if q.Condition == nil {
		q.Condition = query.All{}
	}

	// check for id iterator
	if iter := q.isIdScan(qCtx, sortFields); iter != nil {
		return iter
	}

	var weights indexWeights

	for _, idx := range q.Indexes {
		idxFilter, bounds := q.Condition.IndexFilter(idx.FieldNames[0], nil)
		weight := float64(idx.Stats().Bitmap) / float64(idx.Stats().Count)
		if q.Hint != "" && idx.Name == q.Hint {
			weight += 10000
		}
		if idxFilter != nil {
			weights = append(weights, indexWeight{
				weight: weight,
				idx:    idx,
				filter: idxFilter,
				bounds: bounds,
			})
		}
	}

	if len(sortFields) > 0 {
		for _, idx := range q.Indexes {
			if en := equalNum(sortFields, idx.FieldNames); en > 0 {
				var weightFound bool
				for i, iw := range weights {
					if iw.idx == idx {
						weights[i].reverse = sortFields[0].Reverse
						weights[i].exactSort = len(sortFields) == en
						weightFound = true
						break
					}
				}
				if !weightFound {
					weight := float64(idx.Stats().Bitmap) / float64(idx.Stats().Count)
					if q.Hint != "" && idx.Name == q.Hint {
						weight += 10000
					}
					weights = append(weights, indexWeight{
						idx:       idx,
						reverse:   sortFields[0].Reverse,
						exactSort: len(sortFields) == en,
						weight:    weight,
					})
				}

			}
		}
	}

	stdsort.Sort(stdsort.Reverse(weights))

	var iter iterator.Iterator

	if len(weights) != 0 {
		// found an index
		iw := weights[0]
		// first is index + uniq iterator
		iw.idx.NS.ReuseKey(func(k key.Key) key.Key {
			iw.bounds.SetPrefix(k)
			return k
		})
		iter = iterator.NewIndexIterator(qCtx, iw.idx.NS, iw.bounds, iw.reverse)
		iter = iterator.NewFetchIterator(qCtx, iter.(iterator.IdIterator), q.Condition)

		if len(sortFields) != 0 && !iw.exactSort {
			// fetch+sort iterator if needed
			iter = iterator.NewSortIterator(qCtx, iter.(iterator.ValueIterator), q.Sort)
		}
		return iter.(iterator.ValueIterator)
	} else {
		// no index - full scan
		iter = iterator.NewScanIterator(qCtx, q.Condition, nil, false)
		if len(sortFields) != 0 {
			// add sort iterator if needed
			iter = iterator.NewSortIterator(qCtx, iter.(iterator.ValueIterator), q.Sort)
		}
		return iter.(iterator.ValueIterator)
	}
}

func (q QPlan) isIdScan(qCtx *qcontext.QueryContext, sortFields []sort.SortField) (iter iterator.ValueIterator) {
	if q.Hint != "" && q.Hint != "id" {
		return
	}

	f, bounds := q.Condition.IndexFilter("id", nil)
	sortById := len(sortFields) != 0 && sortFields[0].Field == "id"
	if f == nil && !sortById {
		// no filters or sort by id
		return
	}

	var reverse = sortById && sortFields[0].Reverse
	qCtx.DataNS.ReuseKey(func(k key.Key) key.Key {
		bounds.SetPrefix(k)
		return k
	})
	iter = iterator.NewScanIterator(qCtx, q.Condition, bounds, reverse)
	if (sortById && len(sortFields) > 1) || (!sortById && len(sortFields) != 0) {
		iter = iterator.NewSortIterator(qCtx, iter.(iterator.ValueIterator), q.Sort)
	}
	return iter
}

type indexWeight struct {
	idx       *index.Index
	filter    query.Filter
	bounds    query.Bounds
	weight    float64
	reverse   bool
	exactSort bool
}

type indexWeights []indexWeight

func (iw indexWeights) Len() int {
	return len(iw)
}

func (iw indexWeights) Less(i, j int) bool {
	return iw[i].weight < iw[j].weight
}

func (iw indexWeights) Swap(i, j int) {
	iw[i], iw[j] = iw[j], iw[j]
}

func equalNum(sortFields []sort.SortField, indexFields []string) int {
	m := min(len(sortFields), len(indexFields))
	var prevReverse bool
	for n, sortField := range sortFields[:m] {
		if sortField.Field != indexFields[n] {
			return n
		}
		if n != 0 && prevReverse != sortField.Reverse {
			return n
		}
		prevReverse = sortField.Reverse
	}
	return 0
}
