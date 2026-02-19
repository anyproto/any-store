package anystore

import (
	"context"
	"errors"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/btree"
	"github.com/anyproto/any-store/internal/qplanner"
	"github.com/anyproto/any-store/query"
)

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
	Sql string

	// Rich explain output: multi-line plan with cost breakdown and candidates
	Plan string

	Indexes []IndexExplain
}

type IndexExplain struct {
	Name string
	Cost float64 // CBO computed cost
	Used bool
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

	indexHints []IndexHint

	err error
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
		Tx:         btx,
		DataNs:     q.c.ns,
		Filter:     q.cond,
		Sorter:     q.sort,
		IDBounds:   qb.idBounds,
		Limit:      int(q.limit),
		Offset:     int(q.offset),
		Buf:        buf,
		TotalDocs:  q.docCount(btx),
		Indexes:    q.buildCBOIndexes(),
		IndexHints: q.buildIndexHints(),
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
			Tx:         tx,
			DataNs:     q.c.ns,
			Filter:     q.cond,
			Sorter:     nil, // no sort needed for count
			IDBounds:   qb.idBounds,
			Limit:      int(q.limit),
			Offset:     int(q.offset),
			Buf:        buf,
			TotalDocs:  q.docCount(tx),
			Indexes:    q.buildCBOIndexes(),
			IndexHints: q.buildIndexHints(),
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

	cboIndexes := q.buildCBOIndexes()

	err = q.c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		plan := qplanner.BuildPlan(&qplanner.PlanParams{
			Tx:         tx,
			DataNs:     q.c.ns,
			Filter:     q.cond,
			Sorter:     q.sort,
			IDBounds:   qb.idBounds,
			Limit:      int(q.limit),
			Offset:     int(q.offset),
			Buf:        buf,
			TotalDocs:  q.docCount(tx),
			Indexes:    cboIndexes,
			IndexHints: q.buildIndexHints(),
		})
		explain.Sql = plan.String()
		explain.Plan = plan.ExplainString()

		// Report indexes with used index first
		for _, idx := range cboIndexes {
			used := idx.Info.Name == plan.IndexName
			ie := IndexExplain{
				Name: idx.Info.Name,
				Cost: plan.Cost,
				Used: used,
			}
			if used {
				explain.Indexes = append([]IndexExplain{ie}, explain.Indexes...)
			} else {
				explain.Indexes = append(explain.Indexes, ie)
			}
		}
		return nil
	})
	return
}

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

	// handle "id" field
	if idBounds := q.cond.IndexBounds("id", nil); len(idBounds) != 0 {
		qb.idBounds = idBounds
	}

	return
}

// docCount returns the total number of documents from the first index's sketch DocCount.
// Falls back to tx.Count() if no indexes have sketches.
func (q *collQuery) docCount(tx interface {
	Count(ns *btree.Namespace) (int, error)
}) int {
	for _, idx := range q.c.indexes {
		if idx.sketch != nil {
			return int(idx.sketch.GetDocCount())
		}
	}
	count, _ := tx.Count(q.c.ns)
	return count
}

// buildCBOIndexes builds CBOIndex entries from the collection's indexes for the CBO planner.
func (q *collQuery) buildCBOIndexes() []qplanner.CBOIndex {
	result := make([]qplanner.CBOIndex, 0, len(q.c.indexes))

	var sortFields []query.SortField
	if q.sort != nil {
		sortFields = q.sort.Fields()
	}

	for _, idx := range q.c.indexes {
		info := &qplanner.IndexInfo{
			Name:       idx.info.Name,
			FieldNames: idx.fieldNames,
			FieldPaths: idx.fieldPaths,
			Reverse:    idx.reverse,
			Unique:     idx.info.Unique,
			Sparse:     idx.info.Sparse,
			Ns:         idx.ns,
		}

		// Compute bounds for this index
		bounds, chainLen := qplanner.ComputeIndexBounds(info, q.cond)
		pointLookup := qplanner.AllBoundsFixed(bounds)
		if !idx.info.Unique && len(bounds) > 0 {
			bounds = qplanner.AdjustBoundsForNonUnique(bounds)
		}

		// Compute equality prefix: number of leading index fields pinned by equality
		equalityPrefix := 0
		if pointLookup && chainLen > 0 {
			equalityPrefix = chainLen
		}

		// Check sort coverage (accounting for equality-pinned prefix)
		exactSort, partialSort := qplanner.IndexSortMatch(info, sortFields, equalityPrefix)

		cboIdx := qplanner.CBOIndex{
			Info:        info,
			Sketch:      idx.sketch,
			Bounds:      bounds,
			Reverse:     idx.reverse,
			PointLookup: pointLookup,
			BoundFields: chainLen,
			ExactSort:   exactSort,
			PartialSort: partialSort,
		}
		result = append(result, cboIdx)
	}
	return result
}

// buildIndexHints converts public IndexHint to internal IndexHintParam.
func (q *collQuery) buildIndexHints() []qplanner.IndexHintParam {
	if len(q.indexHints) == 0 {
		return nil
	}
	hints := make([]qplanner.IndexHintParam, len(q.indexHints))
	for i, h := range q.indexHints {
		hints[i] = qplanner.IndexHintParam{IndexName: h.IndexName, Boost: h.Boost}
	}
	return hints
}
