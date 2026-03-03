package anystore

import (
	"context"
	"errors"

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

	br := q.buildBoundsResult()
	plan := qplanner.BuildPlan(&qplanner.PlanParams{
		Tx:          btx,
		DataNs:      q.c.ns,
		Filter:      q.cond,
		Sorter:      q.sort,
		IDBounds:    qb.idBounds,
		Limit:       int(q.limit),
		Offset:      int(q.offset),
		Buf:         buf,
		TotalDocs:   q.docCount(btx),
		Indexes:     q.buildCBOIndexesInto(nil, &br),
		IndexHints:  q.buildIndexHints(),
		FieldBounds: &br,
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
	defer qb.Close()

	tx, err := q.c.db.WriteTx(ctx)
	if err != nil {
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
	btx := tx.btreeReadTx()

	buf := q.c.db.syncPool.GetDocBuf()
	defer q.c.db.syncPool.ReleaseDocBuf(buf)

	br := q.buildBoundsResult()
	plan := qplanner.BuildPlan(&qplanner.PlanParams{
		Tx:          btx,
		DataNs:      q.c.ns,
		Filter:      q.cond,
		IDBounds:    qb.idBounds,
		Limit:       int(q.limit),
		Offset:      int(q.offset),
		Buf:         buf,
		TotalDocs:   q.docCount(btx),
		Indexes:     q.buildCBOIndexesInto(nil, &br),
		IndexHints:  q.buildIndexHints(),
		FieldBounds: &br,
	})

	// Collect all matching docIds into a contiguous buffer to avoid
	// per-ID allocations and cursor invalidation during updates.
	var idBuf []byte       // contiguous buffer for all IDs
	var idOffsets []uint32  // start offsets of each ID in idBuf
	for {
		_, docId, iterErr := plan.Root.Next()
		if iterErr != nil {
			plan.Close()
			err = iterErr
			return
		}
		if docId == nil {
			break
		}
		idOffsets = append(idOffsets, uint32(len(idBuf)))
		idBuf = append(idBuf, docId...)
	}
	plan.Close()

	// Build slice references into the contiguous buffer.
	idsToUpdate := make([][]byte, len(idOffsets))
	for i, off := range idOffsets {
		end := len(idBuf)
		if i+1 < len(idOffsets) {
			end = int(idOffsets[i+1])
		}
		idsToUpdate[i] = idBuf[off:end]
	}

	modBuf := q.c.db.syncPool.GetDocBuf()
	defer q.c.db.syncPool.ReleaseDocBuf(modBuf)

	var getErr error
	for _, id := range idsToUpdate {
		buf.DocBuf, getErr = btx.AppendValue(q.c.ns, id, buf.DocBuf[:0])
		if getErr != nil {
			err = getErr
			return
		}
		doc, parseErr := buf.Parser.Parse(buf.DocBuf)
		if parseErr != nil {
			err = parseErr
			return
		}

		oldItem, itemErr := newItem(doc)
		if itemErr != nil {
			err = itemErr
			return
		}

		modBuf.Arena.Reset()
		modifiedVal, isModified, modErr := mod.Modify(modBuf.Arena, copyItem(modBuf, oldItem).val)
		if modErr != nil {
			err = modErr
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
		if _, err = q.c.update(btWtx, it, oldItem); err != nil {
			return
		}
		result.Modified++
	}
	if result.Modified > 0 {
		tx.SetModified()
	}
	return
}

func (q *collQuery) Delete(ctx context.Context) (result ModifyResult, err error) {
	qb, err := q.makeQuery()
	if err != nil {
		return
	}
	defer qb.Close()

	tx, err := q.c.db.WriteTx(ctx)
	if err != nil {
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
	btx := tx.btreeReadTx()

	buf := q.c.db.syncPool.GetDocBuf()
	defer q.c.db.syncPool.ReleaseDocBuf(buf)

	br := q.buildBoundsResult()
	plan := qplanner.BuildPlan(&qplanner.PlanParams{
		Tx:          btx,
		DataNs:      q.c.ns,
		Filter:      q.cond,
		IDBounds:    qb.idBounds,
		Limit:       int(q.limit),
		Offset:      int(q.offset),
		Buf:         buf,
		TotalDocs:   q.docCount(btx),
		Indexes:     q.buildCBOIndexesInto(nil, &br),
		IndexHints:  q.buildIndexHints(),
		FieldBounds: &br,
	})

	// Collect IDs to delete into a contiguous buffer (can't modify while iterating).
	var idBuf []byte
	var idOffsets []uint32
	for {
		_, docId, iterErr := plan.Root.Next()
		if iterErr != nil {
			plan.Close()
			err = iterErr
			return
		}
		if docId == nil {
			break
		}
		idOffsets = append(idOffsets, uint32(len(idBuf)))
		idBuf = append(idBuf, docId...)
	}
	plan.Close()

	// Build slice references into the contiguous buffer.
	idsToDelete := make([][]byte, len(idOffsets))
	for i, off := range idOffsets {
		end := len(idBuf)
		if i+1 < len(idOffsets) {
			end = int(idOffsets[i+1])
		}
		idsToDelete[i] = idBuf[off:end]
	}

	// Now delete collected docs
	for _, id := range idsToDelete {
		if err = q.c.deleteItem(btWtx, buf, id); err != nil {
			return
		}
		result.Matched++
		result.Modified++
	}
	if result.Modified > 0 {
		tx.SetModified()
	}
	return
}

func (q *collQuery) Count(ctx context.Context) (count int, err error) {
	// Fast path: no filter, no offset, no limit — use lightweight page-header count
	_, isAll := q.cond.(query.All)
	if (q.cond == nil || isAll) && q.offset == 0 && q.limit == 0 && q.sort == nil {
		return q.c.Count(ctx)
	}

	if q.err != nil {
		return 0, q.err
	}
	if q.cond == nil {
		q.cond = query.All{}
	}

	// Compute idBounds only if filter references "id" field
	var idBounds query.Bounds
	if ib := q.cond.IndexBounds("id", nil); len(ib) != 0 {
		idBounds = ib
	}

	// Fast path: ID-only filter with fixed point lookups and no additional conditions.
	// Skip CBO/planner entirely — just check key existence in data namespace.
	if len(idBounds) > 0 && q.isIDOnlyFilter() && q.offset == 0 && q.limit == 0 {
		err = q.c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
			for i := range idBounds {
				_, gerr := tx.Get(q.c.ns, idBounds[i].Start)
				if gerr == nil {
					count++
				} else if gerr != btree.ErrKeyNotFound {
					return gerr
				}
			}
			return nil
		})
		return
	}

	br := q.buildBoundsResult()
	var cboBuf [8]qplanner.CBOIndex
	cboIndexes := q.buildCBOIndexesInto(cboBuf[:0], &br)

	err = q.c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		buf := q.c.db.syncPool.GetDocBuf()
		defer q.c.db.syncPool.ReleaseDocBuf(buf)

		plan := qplanner.BuildPlan(&qplanner.PlanParams{
			Tx:          tx,
			DataNs:      q.c.ns,
			Filter:      q.cond,
			Sorter:      nil, // no sort needed for count
			IDBounds:    idBounds,
			Limit:       int(q.limit),
			Offset:      int(q.offset),
			Buf:         buf,
			TotalDocs:   q.docCount(tx),
			Indexes:     cboIndexes,
			IndexHints:  q.buildIndexHints(),
			CountOnly:   true,
			FieldBounds: &br,
		})

		// Use batch counting if the root iterator supports it (covering index count)
		if ci, ok := plan.Root.(qplanner.CountableIterator); ok {
			n, cerr := ci.CountEntries()
			if cerr != nil {
				return cerr
			}
			count = n
			return nil
		}
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

	br := q.buildBoundsResult()
	cboIndexes := q.buildCBOIndexesInto(nil, &br)

	err = q.c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		plan := qplanner.BuildPlan(&qplanner.PlanParams{
			Tx:          tx,
			DataNs:      q.c.ns,
			Filter:      q.cond,
			Sorter:      q.sort,
			IDBounds:    qb.idBounds,
			Limit:       int(q.limit),
			Offset:      int(q.offset),
			Buf:         buf,
			TotalDocs:   q.docCount(tx),
			Indexes:     cboIndexes,
			IndexHints:  q.buildIndexHints(),
			FieldBounds: &br,
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

// isIDOnlyFilter returns true if the filter only references the "id" field
// with equality or $in conditions (all fixed bounds). This enables a fast path
// that skips CBO planning entirely for simple ID lookups.
func (q *collQuery) isIDOnlyFilter() bool {
	return isIDOnlyFilterNode(q.cond)
}

func isIDOnlyFilterNode(f query.Filter) bool {
	switch ft := f.(type) {
	case query.Key:
		return len(ft.Path) == 1 && ft.Path[0] == "id"
	case query.And:
		// All children must be id-only
		for _, child := range ft {
			if !isIDOnlyFilterNode(child) {
				return false
			}
		}
		return len(ft) > 0
	default:
		return false
	}
}

// buildBoundsResult computes IndexBounds once per unique field across all indexes.
func (q *collQuery) buildBoundsResult() qplanner.BoundsResult {
	var br qplanner.BoundsResult
	var idxInfoBuf [8]*qplanner.IndexInfo
	idxInfos := idxInfoBuf[:0]
	for i := range q.c.indexes {
		if len(idxInfos) < len(idxInfoBuf) {
			idxInfos = append(idxInfos, q.c.indexes[i].cboInfo)
		}
	}
	br.Build(idxInfos, q.cond)
	return br
}

// buildCBOIndexesInto builds CBOIndex entries into the provided buffer using pre-computed bounds.
func (q *collQuery) buildCBOIndexesInto(buf []qplanner.CBOIndex, br *qplanner.BoundsResult) []qplanner.CBOIndex {
	result := buf

	var sortFields []query.SortField
	if q.sort != nil {
		sortFields = q.sort.Fields()
	}

	for _, idx := range q.c.indexes {
		info := idx.cboInfo

		// Compute bounds for this index
		bounds, chainLen := qplanner.ComputeIndexBounds(info, br)

		pointLookup := qplanner.AllBoundsFixed(bounds)
		// Note: AdjustBoundsForNonUnique is deferred to BuildPlan's
		// buildIndexSeekChain, which only adjusts the CHOSEN index.
		// This avoids allocation overhead for indexes that aren't selected.

		// Compute equality prefix: number of leading index fields pinned by equality
		equalityPrefix := 0
		if pointLookup && chainLen > 0 {
			equalityPrefix = chainLen
		}

		// Check sort coverage (accounting for equality-pinned prefix)
		var exactSort, partialSort bool
		if len(sortFields) > 0 {
			exactSort, partialSort = qplanner.IndexSortMatch(info, sortFields, equalityPrefix)
		}

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
