package anystore

import (
	"context"
	"errors"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/qplanner"
	"github.com/anyproto/any-store/v2/query"
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

	// VectorEf overrides the ANN candidate-list size (numCandidates) for a
	// vector query. 0 (default) auto-sizes it: the index default, raised to
	// cover the limit and over-fetched when a residual filter is present.
	VectorEf(ef uint) Query

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

	// vectorEf overrides the ANN candidate-list size (numCandidates) for a
	// vector query; 0 = auto (index default, raised to cover limit/over-fetch).
	vectorEf uint

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

func (q *collQuery) VectorEf(ef uint) Query {
	q.vectorEf = ef
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

	// Fast path: filter provably matches no documents — return an empty
	// iterator with no transaction, no plan construction, no I/O.
	if isUnsatisfiable(q.cond) {
		qb.Close()
		return &emptyIter{}, nil
	}

	tx, err := q.c.db.getReadTx(ctx)
	if err != nil {
		qb.Close()
		return
	}

	buf := q.c.db.syncPool.GetDocBuf()
	btx := tx.btreeReadTx()

	// $text query: the BM25 search drives the query (CBO bypassed). The residual
	// filter runs as a downstream FilterIter; a relevance/textScore sort is the
	// FtsIter's intrinsic order, a real-field sort inserts a SortIter.
	ftsSpec, ftsResidual, ferr := q.detectFtsQuery()
	if ferr != nil {
		q.c.db.syncPool.ReleaseDocBuf(buf)
		_ = tx.Commit()
		qb.Close()
		return nil, ferr
	}
	if ftsSpec != nil {
		plan := qplanner.BuildPlan(&qplanner.PlanParams{
			Tx:     btx,
			DataNs: q.c.ns,
			Filter: ftsResidual,
			Sorter: ftsSorter(q.sort),
			Limit:  int(q.limit),
			Offset: int(q.offset),
			Buf:    buf,
			Fts:    ftsSpec,
		})
		return &planIterator{
			plan: plan,
			tx:   tx,
			buf:  buf,
			qb:   qb,
			data: &qplanner.CursorSource{Tx: btx, Ns: q.c.ns},
		}, nil
	}

	// Vector query: detect `{vectorField: [..]}` against the collection's vector
	// indexes. When matched, build a vector plan (ANN source) and ignore all
	// other indexes; the residual filter + sort run as downstream stages.
	vspec, residual, verr := q.detectVectorQuery()
	if verr != nil {
		q.c.db.syncPool.ReleaseDocBuf(buf)
		_ = tx.Commit()
		qb.Close()
		return nil, verr
	}
	if vspec != nil {
		sorter := q.sort
		if sorter == nil && !vspec.Ordered {
			// No explicit sort and the source isn't already distance-ordered
			// (brute-force): order by distance ascending. When the ANN source is
			// ordered, we leave sorter nil so the planner skips a redundant SortIter
			// and streams the already-closest-first candidates straight to LimitIter.
			sorter, _ = query.ParseSort(qplanner.DistanceField)
		}
		plan := qplanner.BuildPlan(&qplanner.PlanParams{
			Tx:     btx,
			DataNs: q.c.ns,
			Filter: residual,
			Sorter: sorter,
			Limit:  int(q.limit),
			Offset: int(q.offset),
			Buf:    buf,
			Vector: vspec,
		})
		return &planIterator{
			plan: plan,
			tx:   tx,
			buf:  buf,
			qb:   qb,
			data: &qplanner.CursorSource{Tx: btx, Ns: q.c.ns},
		}, nil
	}

	idxs := q.c.loadIndexes()
	br := q.buildBoundsResult(idxs)
	plan := qplanner.BuildPlan(&qplanner.PlanParams{
		Tx:          btx,
		DataNs:      q.c.ns,
		Filter:      q.cond,
		Sorter:      q.sort,
		IDBounds:    qb.idBounds,
		PrimaryKey:  q.c.primaryKey,
		Limit:       int(q.limit),
		Offset:      int(q.offset),
		Buf:         buf,
		TotalDocs:   q.docCount(btx),
		Indexes:     q.buildCBOIndexesInto(nil, &br, idxs),
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

	// Fast path: filter provably matches no documents — nothing to update,
	// no write tx required. ModifyResult is the zero value (Matched=0,
	// Modified=0); the modifier was already parsed above so a malformed
	// modifier is still surfaced.
	if isUnsatisfiable(q.cond) {
		return
	}

	// Reclaim tombstones this bulk update creates once it commits, mirroring the
	// single-doc mutators. Registered before the commit defer so it runs after
	// commit; no-ops inside a caller-managed tx (the guard in maybeAutoCompactVectors).
	defer func() {
		if err == nil {
			q.c.maybeAutoCompactVectors(ctx)
		}
	}()

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

	plan, isFts, ferr := q.ftsScanPlan(btx, buf)
	if ferr != nil {
		err = ferr
		return
	}
	if !isFts {
		idxs := q.c.loadIndexes()
		br := q.buildBoundsResult(idxs)
		plan = qplanner.BuildPlan(&qplanner.PlanParams{
			Tx:          btx,
			DataNs:      q.c.ns,
			Filter:      q.cond,
			IDBounds:    qb.idBounds,
			PrimaryKey:  q.c.primaryKey,
			Limit:       int(q.limit),
			Offset:      int(q.offset),
			Buf:         buf,
			TotalDocs:   q.docCount(btx),
			Indexes:     q.buildCBOIndexesInto(nil, &br, idxs),
			IndexHints:  q.buildIndexHints(),
			FieldBounds: &br,
		})
	}

	// Collect all matching docIds into a contiguous buffer to avoid
	// per-ID allocations and cursor invalidation during updates. Dedup
	// multi-key entries so an UpdateMany over an array-indexed $in
	// query doesn't apply the modifier twice to the same doc.
	var idBuf []byte       // contiguous buffer for all IDs
	var idOffsets []uint32 // start offsets of each ID in idBuf
	var dedup qplanner.DocDedup
	for {
		_, docId, mk, iterErr := plan.Root.Next()
		if iterErr != nil {
			plan.Close()
			err = iterErr
			return
		}
		if docId == nil {
			break
		}
		if !dedup.Accept(docId, mk) {
			continue
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
		doc, parseErr := buf.Parser.ParseOwned(buf.DocBuf)
		if parseErr != nil {
			err = parseErr
			return
		}

		oldItem, itemErr := q.c.newItem(doc)
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
		if it, err = q.c.newItem(modifiedVal); err != nil {
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

	// Fast path: filter provably matches no documents — nothing to delete,
	// no write tx required.
	if isUnsatisfiable(q.cond) {
		return
	}

	// Reclaim tombstones this bulk delete creates once it commits, mirroring the
	// single-doc mutators. Registered before the commit defer so it runs after
	// commit; no-ops inside a caller-managed tx (the guard in maybeAutoCompactVectors).
	defer func() {
		if err == nil {
			q.c.maybeAutoCompactVectors(ctx)
		}
	}()

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

	plan, isFts, ferr := q.ftsScanPlan(btx, buf)
	if ferr != nil {
		err = ferr
		return
	}
	if !isFts {
		idxs := q.c.loadIndexes()
		br := q.buildBoundsResult(idxs)
		plan = qplanner.BuildPlan(&qplanner.PlanParams{
			Tx:          btx,
			DataNs:      q.c.ns,
			Filter:      q.cond,
			IDBounds:    qb.idBounds,
			PrimaryKey:  q.c.primaryKey,
			Limit:       int(q.limit),
			Offset:      int(q.offset),
			Buf:         buf,
			TotalDocs:   q.docCount(btx),
			Indexes:     q.buildCBOIndexesInto(nil, &br, idxs),
			IndexHints:  q.buildIndexHints(),
			FieldBounds: &br,
		})
	}

	// Collect IDs to delete into a contiguous buffer (can't modify while iterating).
	// Dedup multi-key entries so a doc matched on multiple array values
	// isn't deleted twice / counted twice in the affected count.
	var idBuf []byte
	var idOffsets []uint32
	var dedup qplanner.DocDedup
	for {
		_, docId, mk, iterErr := plan.Root.Next()
		if iterErr != nil {
			plan.Close()
			err = iterErr
			return
		}
		if docId == nil {
			break
		}
		if !dedup.Accept(docId, mk) {
			continue
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

	// $text counts by iterating the full-text plan (respecting any residual
	// predicate and offset/limit). Iter() builds the Fts plan; counting its
	// results is the simplest correct path.
	if _, hasText, terr := findTextFilter(q.cond); terr != nil {
		return 0, terr
	} else if hasText {
		iter, ierr := q.Iter(ctx)
		if ierr != nil {
			return 0, ierr
		}
		defer func() {
			if cerr := iter.Close(); cerr != nil && err == nil {
				err = cerr
			}
		}()
		for iter.Next() {
			count++
		}
		return count, iter.Err()
	}

	// Fast path: filter provably matches no documents (e.g. $in:[]). Skip
	// the planner, the read tx, and the index walk entirely — the answer
	// is unconditionally zero. See isUnsatisfiable.
	if isUnsatisfiable(q.cond) {
		return 0, nil
	}

	// Compute idBounds only if filter references the primary-key field
	var idBounds query.Bounds
	if ib := q.cond.IndexBounds(q.c.primaryKey, nil); len(ib) != 0 {
		idBounds = ib
	}

	// Fast path: ID-only filter with fixed point lookups and no additional conditions.
	// Skip CBO/planner entirely — just check key existence in data namespace.
	if len(idBounds) > 0 && q.isIDOnlyFilter() && q.offset == 0 && q.limit == 0 {
		err = q.c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
			for i := range idBounds {
				found, gerr := tx.Has(q.c.ns, idBounds[i].Start)
				if gerr != nil {
					return gerr
				}
				if found {
					count++
				}
			}
			return nil
		})
		return
	}

	idxs := q.c.loadIndexes()
	br := q.buildBoundsResult(idxs)
	var cboBuf [8]qplanner.CBOIndex
	cboIndexes := q.buildCBOIndexesInto(cboBuf[:0], &br, idxs)

	err = q.c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		buf := q.c.db.syncPool.GetDocBuf()
		defer q.c.db.syncPool.ReleaseDocBuf(buf)

		plan := qplanner.BuildPlan(&qplanner.PlanParams{
			Tx:          tx,
			DataNs:      q.c.ns,
			Filter:      q.cond,
			Sorter:      nil, // no sort needed for count
			IDBounds:    idBounds,
			PrimaryKey:  q.c.primaryKey,
			Limit:       int(q.limit),
			Offset:      int(q.offset),
			Buf:         buf,
			TotalDocs:   q.docCount(tx),
			Indexes:     cboIndexes,
			IndexHints:  q.buildIndexHints(),
			CountOnly:   true,
			FieldBounds: &br,
		})

		// Use batch counting if the root iterator supports it (covering index
		// count). IndexIter.CountEntries handles the multi-bound + multi-key
		// dedup internally via the per-entry value byte; consumers don't need
		// to layer another dedup on top.
		if ci, ok := plan.Root.(qplanner.CountableIterator); ok {
			n, cerr := ci.CountEntries()
			plan.Close() // release cursor resources held by CountEntries
			if cerr != nil {
				return cerr
			}
			count = n
			return nil
		}
		// Generic Next-loop count: dedup multi-key entries so a doc whose
		// array values match multiple bounds is counted once.
		var dedup qplanner.DocDedup
		for {
			_, docId, mk, iterErr := plan.Root.Next()
			if iterErr != nil {
				return iterErr
			}
			if docId == nil {
				break
			}
			if dedup.Accept(docId, mk) {
				count++
			}
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

	idxs := q.c.loadIndexes()
	br := q.buildBoundsResult(idxs)
	cboIndexes := q.buildCBOIndexesInto(nil, &br, idxs)

	err = q.c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		plan, isFts, ferr := q.ftsScanPlan(tx, buf)
		if ferr != nil {
			return ferr
		}
		if !isFts {
			plan = qplanner.BuildPlan(&qplanner.PlanParams{
				Tx:          tx,
				DataNs:      q.c.ns,
				Filter:      q.cond,
				Sorter:      q.sort,
				IDBounds:    qb.idBounds,
				PrimaryKey:  q.c.primaryKey,
				Limit:       int(q.limit),
				Offset:      int(q.offset),
				Buf:         buf,
				TotalDocs:   q.docCount(tx),
				Indexes:     cboIndexes,
				IndexHints:  q.buildIndexHints(),
				FieldBounds: &br,
			})
		}
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

	// handle the primary-key field
	if idBounds := q.cond.IndexBounds(q.c.primaryKey, nil); len(idBounds) != 0 {
		qb.idBounds = idBounds
	}

	return
}

// docCount returns the total number of documents from the first index's sketch DocCount.
// Falls back to tx.Count() if no indexes have sketches.
func (q *collQuery) docCount(tx interface {
	Count(ns *btree.Namespace) (int, error)
}) int {
	for _, idx := range q.c.loadIndexes() {
		if s := idx.loadPubSketch(); s != nil {
			return int(s.GetDocCount())
		}
	}
	count, _ := tx.Count(q.c.ns)
	return count
}

// isUnsatisfiable reports whether the given filter provably matches no
// documents. Used to short-circuit Count/Iter/Update/Delete before we
// open a transaction, build a plan, or read any pages — the operation
// degenerates into "the answer is zero / empty" with no I/O.
//
// Currently detects:
//   - empty $in (query.In{Values: empty}) — never matches anything;
//   - the same propagated through a Key filter ({field: {$in:[]}});
//   - $and containing any unsatisfiable branch (short-circuit on first);
//   - $or whose every branch is unsatisfiable (must be unanimous).
//
// Conservative: returns false for anything it can't prove. Notably,
// $not on an unsatisfiable inner is NOT reported as unsatisfiable (the
// outer would be always-true, not always-false).
func isUnsatisfiable(f query.Filter) bool {
	switch ft := f.(type) {
	case nil:
		return false
	case query.In:
		return len(ft.Values) == 0
	case query.Key:
		return isUnsatisfiable(ft.Filter)
	case query.And:
		for _, sub := range ft {
			if isUnsatisfiable(sub) {
				return true
			}
		}
		return false
	case *query.And:
		for _, sub := range *ft {
			if isUnsatisfiable(sub) {
				return true
			}
		}
		return false
	case query.Or:
		if len(ft) == 0 {
			return false
		}
		for _, sub := range ft {
			if !isUnsatisfiable(sub) {
				return false
			}
		}
		return true
	}
	return false
}

// isIDOnlyFilter returns true if the filter only references the primary-key field
// with equality or $in conditions (all fixed bounds). This enables a fast path
// that skips CBO planning entirely for simple ID lookups.
func (q *collQuery) isIDOnlyFilter() bool {
	return isIDOnlyFilterNode(q.cond, q.c.primaryKey)
}

func isIDOnlyFilterNode(f query.Filter, pk string) bool {
	switch ft := f.(type) {
	case query.Key:
		return len(ft.Path) == 1 && ft.Path[0] == pk
	case query.And:
		// All children must be primary-key-only
		for _, child := range ft {
			if !isIDOnlyFilterNode(child, pk) {
				return false
			}
		}
		return len(ft) > 0
	case *query.And:
		// query.MustParseCondition produces *query.And for `{"$and":[...]}`
		// (see query/cond_parse.go:103). Delegate to the value arm so $and
		// JSON syntax enjoys the same id-only fast path as comma-spelled
		// filters like `{"a":1,"b":2}` (which parse to value query.And).
		return isIDOnlyFilterNode(*ft, pk)
	default:
		return false
	}
}

// buildBoundsResult computes IndexBounds once per unique field across all
// indexes. idxs is the index-set snapshot for this planning pass; the caller
// passes the SAME snapshot to buildCBOIndexesInto so bounds and CBOIndex entries
// stay positionally consistent even if a concurrent reconcile swaps the set.
func (q *collQuery) buildBoundsResult(idxs []*index) qplanner.BoundsResult {
	var br qplanner.BoundsResult
	var idxInfoBuf [8]*qplanner.IndexInfo
	idxInfos := idxInfoBuf[:0]
	for i := range idxs {
		idxInfos = append(idxInfos, idxs[i].cboInfo)
	}
	br.Build(idxInfos, q.cond)
	return br
}

// buildCBOIndexesInto builds CBOIndex entries into the provided buffer using
// pre-computed bounds. idxs MUST be the same snapshot passed to
// buildBoundsResult for this pass.
func (q *collQuery) buildCBOIndexesInto(buf []qplanner.CBOIndex, br *qplanner.BoundsResult, idxs []*index) []qplanner.CBOIndex {
	result := buf

	var sortFields []query.SortField
	if q.sort != nil {
		sortFields = q.sort.Fields()
	}

	for _, idx := range idxs {
		info := idx.cboInfo

		// Compute bounds for this index
		bounds, chainLen := qplanner.ComputeIndexBounds(info, br)

		pointLookup := qplanner.AllBoundsFixed(bounds)
		// Note: AdjustBoundsForNonUnique is deferred to BuildPlan's
		// buildIndexSeekChain, which only adjusts the CHOSEN index.
		// This avoids allocation overhead for indexes that aren't selected.

		// Compute equality prefix: count leading index fields with equality bounds.
		// This handles compound indexes like (t,o) with t=eq, o=range correctly,
		// allowing IndexSortMatch to recognize sort coverage after equality prefix.
		equalityPrefix := 0
		for _, field := range info.FieldNames {
			bounds, fixed, found := br.Lookup(field)
			if !found || len(bounds) == 0 || !fixed {
				break
			}
			equalityPrefix++
		}

		// Check sort coverage (accounting for equality-pinned prefix)
		var exactSort, partialSort bool
		var sortMatchStart int
		if len(sortFields) > 0 {
			exactSort, partialSort, sortMatchStart = qplanner.IndexSortMatch(info, sortFields, equalityPrefix)
		}

		cboIdx := qplanner.CBOIndex{
			Info:           info,
			Sketch:         idx.loadPubSketch(),
			Bounds:         bounds,
			Reverse:        idx.reverse,
			Ns:             idx.ns,
			PointLookup:    pointLookup,
			BoundFields:    chainLen,
			ExactSort:      exactSort,
			PartialSort:    partialSort,
			SortMatchStart: sortMatchStart,
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
