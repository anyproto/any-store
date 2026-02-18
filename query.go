package anystore

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sort"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/bitmap"
	"github.com/anyproto/any-store/internal/btree"
	"github.com/anyproto/any-store/internal/qplanner"
	"github.com/anyproto/any-store/query"
)

const maxIndexesInQuery = 1

// ModifyResult represents the result of a modification operation.
type ModifyResult struct {
	// Matched is the number of documents matched by the query.
	Matched int

	// Modified is the number of documents that were actually modified.
	Modified int
}

// Query represents a query on a collection.
type Query interface {

	// Limit sets the maximum number of documents to return.
	Limit(limit uint) Query

	// Offset sets the number of documents to skip before starting to return results.
	Offset(offset uint) Query

	// Sort sets the sort order for the query results.
	Sort(sort ...any) Query

	// IndexHint adds or removes boost for some indexes
	IndexHint(hints ...IndexHint) Query

	// Iter executes the query and returns an Iterator for the results.
	Iter(ctx context.Context) (Iterator, error)

	// Count returns the number of documents matching the query.
	Count(ctx context.Context) (count int, err error)

	// Update modifies documents matching the query.
	Update(ctx context.Context, modifier any) (res ModifyResult, err error)

	// Delete removes documents matching the query.
	Delete(ctx context.Context) (res ModifyResult, err error)

	// Explain provides the query execution plan.
	Explain(ctx context.Context) (explain Explain, err error)
}

type Explain struct {
	Sql           string
	SqliteExplain []string
	Indexes       []IndexExplain
}

type IndexExplain struct {
	Name   string
	Weight int
	Used   bool
}

type IndexHint struct {
	IndexName string
	Boost     int
}

type collQuery struct {
	c *collection

	cond query.Filter
	sort query.Sort

	limit, offset uint

	indexesWithWeight weightedIndexes
	sortFields        []query.SortField
	queryFields       []queryField
	indexHints        []IndexHint

	err error
}

type queryField struct {
	field  string
	bounds query.Bounds
}

func (q *collQuery) Cond(filter any) Query {
	var err error
	if q.cond, err = query.ParseCondition(filter); err != nil {
		q.err = errors.Join(err)
	}
	return q
}

func (q *collQuery) Limit(limit uint) Query {
	q.limit = limit
	return q
}

func (q *collQuery) Offset(offset uint) Query {
	q.offset = offset
	return q
}

func (q *collQuery) IndexHint(hints ...IndexHint) Query {
	q.indexHints = hints
	return q
}

func (q *collQuery) Sort(sorts ...any) Query {
	var err error
	if q.sort, err = query.ParseSort(sorts...); err != nil {
		q.err = errors.Join(err)
	}
	return q
}

func (q *collQuery) Iter(ctx context.Context) (iter Iterator, err error) {
	qb, err := q.makeQuery()
	if err != nil {
		return
	}

	tx, err := q.c.db.getReadTx(ctx)
	if err != nil {
		qb.Close()
		return
	}

	buf := q.c.db.syncPool.GetDocBuf()
	btx := tx.btreeReadTx()

	plan := qplanner.BuildPlan(&qplanner.PlanParams{
		Tx:       btx,
		DataNs:   q.c.ns,
		Filter:   q.cond,
		Sorter:   q.sort,
		IDBounds: qb.idBounds,
		Limit:    int(q.limit),
		Offset:   int(q.offset),
		Buf:      buf,
		Indexes:  q.buildPlanIndexes(),
	})

	return &planIterator{
		plan: plan,
		tx:   tx,
		buf:  buf,
		qb:   qb,
		data: &qplanner.CursorSource{Tx: btx, Ns: q.c.ns},
	}, nil
}

func (q *collQuery) Update(ctx context.Context, modifier any) (result ModifyResult, err error) {
	mod, err := query.ParseModifier(modifier)
	if err != nil {
		return
	}
	qb, err := q.makeQuery()
	if err != nil {
		return
	}

	tx, err := q.c.db.WriteTx(ctx)
	if err != nil {
		qb.Close()
		return
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	btWtx := tx.btreeWriteTx()
	cursor := btWtx.NewCursor(q.c.ns)

	var filter query.Filter
	if q.cond != nil {
		filter = q.cond
	}

	iter := &iterator{
		cursor: cursor,
		filter: filter,
		buf:    q.c.db.syncPool.GetDocBuf(),
		tx:     tx,
		qb:     qb,
		limit:  int(q.limit),
		offset: int(q.offset),
	}
	defer func() {
		_ = iter.Close()
	}()

	buf := q.c.db.syncPool.GetDocBuf()
	defer q.c.db.syncPool.ReleaseDocBuf(buf)

	for iter.Next() {
		var doc Doc
		if doc, err = iter.Doc(); err != nil {
			return
		}
		var (
			modifiedVal *anyenc.Value
			isModified  bool
		)
		buf.Arena.Reset()
		modifiedVal, isModified, err = mod.Modify(buf.Arena, copyItem(buf, doc.(item)).val)
		if err != nil {
			return
		}

		result.Matched++
		if !isModified {
			continue
		}

		var it item
		if it, err = newItem(modifiedVal); err != nil {
			return
		}
		if _, err = q.c.update(btWtx, it, doc.(item)); err != nil {
			return
		}
		result.Modified++
	}
	err = iter.Err()
	return
}

func (q *collQuery) Delete(ctx context.Context) (result ModifyResult, err error) {
	qb, err := q.makeQuery()
	if err != nil {
		return
	}

	tx, err := q.c.db.WriteTx(ctx)
	if err != nil {
		qb.Close()
		return
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	btWtx := tx.btreeWriteTx()

	// First collect all docs to delete (can't modify while iterating)
	cursor := btWtx.NewCursor(q.c.ns)

	var filter query.Filter
	if q.cond != nil {
		filter = q.cond
	}

	iterBuf := q.c.db.syncPool.GetDocBuf()
	iter := &iterator{
		cursor: cursor,
		filter: filter,
		buf:    iterBuf,
		qb:     qb,
		limit:  int(q.limit),
		offset: int(q.offset),
	}

	buf := q.c.db.syncPool.GetDocBuf()
	defer q.c.db.syncPool.ReleaseDocBuf(buf)

	// Collect IDs to delete
	var idsToDelete [][]byte
	for iter.Next() {
		var doc Doc
		if doc, err = iter.Doc(); err != nil {
			q.c.db.syncPool.ReleaseDocBuf(iterBuf)
			qb.Close()
			return
		}
		id := doc.(item).appendId(buf.SmallBuf[:0])
		idsToDelete = append(idsToDelete, append([]byte(nil), id...))
	}
	if err = iter.Err(); err != nil {
		q.c.db.syncPool.ReleaseDocBuf(iterBuf)
		qb.Close()
		return
	}
	q.c.db.syncPool.ReleaseDocBuf(iterBuf)
	qb.Close()

	// Now delete collected docs
	for _, id := range idsToDelete {
		if err = q.c.deleteItem(btWtx, buf, id); err != nil {
			return
		}
		result.Matched++
		result.Modified++
	}
	return
}

func (q *collQuery) Count(ctx context.Context) (count int, err error) {
	// Fast path: no filter, no offset, no limit — use lightweight page-header count
	_, isAll := q.cond.(query.All)
	if (q.cond == nil || isAll) && q.offset == 0 && q.limit == 0 && q.sort == nil {
		return q.c.Count(ctx)
	}

	qb, err := q.makeQuery()
	if err != nil {
		return
	}
	defer qb.Close()

	err = q.c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		buf := q.c.db.syncPool.GetDocBuf()
		defer q.c.db.syncPool.ReleaseDocBuf(buf)

		plan := qplanner.BuildPlan(&qplanner.PlanParams{
			Tx:       tx,
			DataNs:   q.c.ns,
			Filter:   q.cond,
			Sorter:   nil, // no sort needed for count
			IDBounds: qb.idBounds,
			Limit:    int(q.limit),
			Offset:   int(q.offset),
			Buf:      buf,
			Indexes:  q.buildPlanIndexes(),
		})

		for {
			_, docId, iterErr := plan.Root.Next()
			if iterErr != nil {
				return iterErr
			}
			if docId == nil {
				break
			}
			count++
		}
		return nil
	})
	return
}

func (q *collQuery) Explain(ctx context.Context) (explain Explain, err error) {
	qb, err := q.makeQuery()
	if err != nil {
		return
	}
	defer qb.Close()

	buf := q.c.db.syncPool.GetDocBuf()
	defer q.c.db.syncPool.ReleaseDocBuf(buf)

	err = q.c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		plan := qplanner.BuildPlan(&qplanner.PlanParams{
			Tx:       tx,
			DataNs:   q.c.ns,
			Filter:   q.cond,
			Sorter:   q.sort,
			IDBounds: qb.idBounds,
			Limit:    int(q.limit),
			Offset:   int(q.offset),
			Buf:      buf,
			Indexes:  q.buildPlanIndexes(),
		})
		explain.Sql = plan.String()
		explain.SqliteExplain = []string{plan.String()}
		return nil
	})
	if err != nil {
		return
	}

	for _, idx := range q.indexesWithWeight {
		explain.Indexes = append(explain.Indexes, IndexExplain{
			Name:   idx.Info().Name,
			Weight: idx.weight,
			Used:   idx.used,
		})
	}
	return
}

type indexWithWeight struct {
	*index
	weight             int
	pos                int
	queryFieldsBits    bitmap.Bitmap256
	sortFieldsBits     bitmap.Bitmap256
	bounds             query.Bounds
	exactSort          bool
	used               bool
	filterFullyCovered bool
}

type weightedIndexes []indexWithWeight

func (w weightedIndexes) Len() int           { return len(w) }
func (w weightedIndexes) Less(i, j int) bool { return w[i].weight > w[j].weight }
func (w weightedIndexes) Swap(i, j int)      { w[i], w[j] = w[j], w[i] }

func (q *collQuery) makeQuery() (qb *queryBuilder, err error) {
	if q.err != nil {
		return nil, q.err
	}

	qb = newQueryBuilder()
	qb.coll = q.c
	qb.limit = int(q.limit)
	qb.offset = int(q.offset)

	if q.cond == nil {
		q.cond = query.All{}
	}

	if q.sort != nil {
		q.sortFields = q.sort.Fields()
	}

	// handle "id" field
	if idBounds := q.cond.IndexBounds("id", nil); len(idBounds) != 0 {
		qb.idBounds = idBounds
	}

	// calculate weights (kept for index choosing logic, not used for actual queries)
	q.indexesWithWeight = make(weightedIndexes, len(q.c.indexes))
	for i, idx := range q.c.indexes {
		q.indexesWithWeight[i].index = idx
		q.indexesWithWeight[i].weight,
			q.indexesWithWeight[i].queryFieldsBits = q.indexQueryWeight(idx)
		if sw, sf := q.indexSortWeight(idx); sw > 0 {
			q.indexesWithWeight[i].weight += sw
			q.indexesWithWeight[i].sortFieldsBits = sf
			q.indexesWithWeight[i].exactSort = sf.CountLeadingOnes() == len(q.sortFields)
		}
		if q.indexesWithWeight[i].weight > 0 {
			for _, hint := range q.indexHints {
				if hint.IndexName == idx.info.Name {
					q.indexesWithWeight[i].weight += hint.Boost
				}
			}
		}
	}
	sort.Sort(q.indexesWithWeight)

	// Build bitmap of all query fields that have bounds
	var allQueryFieldsBits bitmap.Bitmap256
	for i, f := range q.queryFields {
		if len(f.bounds) != 0 && i < 256 {
			allQueryFieldsBits = allQueryFieldsBits.Set(uint8(i))
		}
	}

	// filter useless indexes and mark used ones
	var (
		usedFieldsBits bitmap.Bitmap256
		usedSortBits   bitmap.Bitmap256
		exactSortFound bool
		usedCount      int
	)
	for i, idx := range q.indexesWithWeight {
		if idx.weight < 1 {
			continue
		}
		if usedCount >= maxIndexesInQuery {
			break
		}
		if usedFieldsBits.Subtract(idx.queryFieldsBits).Count() != 0 ||
			usedSortBits.Subtract(idx.sortFieldsBits).Count() != 0 ||
			(!exactSortFound && idx.exactSort) {
			usedFieldsBits = usedFieldsBits.Or(idx.queryFieldsBits)
			usedSortBits = usedSortBits.Or(idx.sortFieldsBits)
			if idx.exactSort {
				exactSortFound = true
			}
			q.indexesWithWeight[i].used = true
			bounds, chainLen := q.computeIndexBounds(idx.index)
			q.indexesWithWeight[i].bounds = bounds
			// Check if the index chain covers ALL query fields with bounds
			if chainLen > 0 && allQueryFieldsBits.Count() > 0 {
				var chainFieldsBits bitmap.Bitmap256
				for ci := range chainLen {
					if ci < len(idx.fieldNames) {
						_, fi := q.queryField(idx.fieldNames[ci])
						if fi < 256 {
							chainFieldsBits = chainFieldsBits.Set(uint8(fi))
						}
					}
				}
				if allQueryFieldsBits.Subtract(chainFieldsBits).Count() == 0 {
					q.indexesWithWeight[i].filterFullyCovered = true
				}
			}
			usedCount++
		}
	}

	return
}

func (q *collQuery) queryField(field string) (queryField, int) {
	for i, f := range q.queryFields {
		if f.field == field {
			return f, i
		}
	}
	bounds := q.cond.IndexBounds(field, nil)
	f := queryField{
		field:  field,
		bounds: bounds,
	}
	q.queryFields = append(q.queryFields, f)
	return f, len(q.queryFields) - 1
}

func (q *collQuery) indexQueryWeight(idx *index) (weight int, fieldBits bitmap.Bitmap256) {
	var isChain = true
	for i, field := range idx.fieldNames {
		qField, fi := q.queryField(field)
		if len(qField.bounds) != 0 {
			if isChain {
				if i == 0 {
					weight = 10
				} else {
					weight *= 2
				}
			} else {
				weight += 2
			}
			if i < 256 {
				fieldBits = fieldBits.Set(uint8(fi))
			}
		} else {
			if isChain {
				isChain = false
				weight -= 1
			}
		}
	}
	if weight > 0 && idx.info.Unique {
		weight++
	}
	return
}

// computeIndexBounds computes combined tuple bounds for an index
// by combining per-field bounds from the query condition.
// It also returns the number of leading chain fields used.
func (q *collQuery) computeIndexBounds(idx *index) (query.Bounds, int) {
	// Collect per-field bounds for leading chain fields
	type fieldBound struct {
		bounds query.Bounds
		fixed  bool // all bounds have Start == End
	}

	var chain []fieldBound
	for _, field := range idx.fieldNames {
		fb := q.cond.IndexBounds(field, nil)
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
			break // can't extend past a range field
		}
	}

	if len(chain) == 0 {
		return nil, 0
	}

	chainLen := len(chain)

	// Build combined bounds by extending prefix through chain
	// Start with bounds from first field
	var result query.Bounds
	for _, b := range chain[0].bounds {
		result = append(result, b)
	}

	// For subsequent fields where all previous are fixed, extend the prefix
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
					// cur.Start is -inf: combined start is just the prefix
					eb.Start = append(anyenc.Tuple(nil), prev.Start...)
					eb.StartInclude = true
				}
				if len(cur.End) > 0 {
					eb.End = append(append(anyenc.Tuple(nil), prev.End...), cur.End...)
				} else {
					// cur.End is +inf: use prefix + 0xff to capture all entries under prefix
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

// buildPlanIndexes converts weighted indexes to PlanIndex entries for the planner.
func (q *collQuery) buildPlanIndexes() []qplanner.PlanIndex {
	result := make([]qplanner.PlanIndex, 0, len(q.indexesWithWeight))
	for _, iw := range q.indexesWithWeight {
		bounds := iw.bounds
		// For non-unique indexes, keys have docId appended after index fields.
		// Adjust End bounds to include all docId suffixes by appending 0xff.
		if !iw.info.Unique && len(bounds) > 0 {
			adjusted := make(query.Bounds, len(bounds))
			for i, b := range bounds {
				adjusted[i] = b
				if len(b.End) > 0 && b.EndInclude {
					adjusted[i].End = append(append(anyenc.Tuple(nil), b.End...), 0xff)
					adjusted[i].EndInclude = true
				}
			}
			bounds = adjusted
		}
		pi := qplanner.PlanIndex{
			Info: &qplanner.IndexInfo{
				Name:       iw.info.Name,
				FieldNames: iw.fieldNames,
				FieldPaths: iw.fieldPaths,
				Reverse:    iw.reverse,
				Unique:     iw.info.Unique,
				Sparse:     iw.info.Sparse,
				Ns:         iw.ns,
			},
			Bounds:             bounds,
			Weight:             iw.weight,
			ExactSort:          iw.exactSort,
			Used:               iw.used,
			FilterFullyCovered: iw.filterFullyCovered,
		}
		result = append(result, pi)
	}
	return result
}

func (q *collQuery) indexSortWeight(idx *index) (weight int, fieldBits bitmap.Bitmap256) {
	var isChain = true
	sortFields := q.sortFields
	if len(sortFields) > 256 {
		sortFields = sortFields[:256]
	}
	for i, sf := range sortFields {
		if isChain && i < len(idx.fieldNames) {
			if idx.fieldNames[i] == sf.Field {
				if i == 0 {
					weight = 11
				} else {
					weight *= 2
					if idx.reverse[i] == sf.Reverse {
						weight += 2
					}
				}
				fieldBits = fieldBits.Set(uint8(i))
				continue
			}
		}
		isChain = false
		if slices.Contains(idx.fieldNames, sf.Field) {
			weight += 5
			fieldBits = fieldBits.Set(uint8(i))
		} else {
			break
		}
	}
	return
}
