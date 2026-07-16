package anystore

import (
	"context"
	"errors"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/qplanner"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
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

// reportCandidates builds the CBO candidate list for Explain's index report
// on the fts/vector paths, where compilation never consults the CBO. Nil for
// every other verb.
func (q *collQuery) reportCandidates(btx *btree.ReadTx, opts planOpts) []qplanner.CBOIndex {
	if !opts.wantCandidates {
		return nil
	}
	idxs := q.c.loadIndexes()
	br := q.buildBoundsResult(idxs)
	return q.buildCBOIndexesInto(nil, &br, idxs, btx)
}

// planOpts are the only per-verb compilation knobs. Everything else about
// access-path selection is shared — a divergence here needs written rationale
// (the SQLite shape: one WHERE/ORDER BY code generator, verb-specific only at
// the row sink).
type planOpts struct {
	countOnly      bool // Count: PlanParams.CountOnly, Sorter forced nil (a count is order-invariant)
	needScores     bool // Iter only: keep the BM25 sidecar for Iterator.Score()
	exactTotalDocs bool // Explain only: docCountExact (human-facing TotalDocs)
	wantCandidates bool // Explain only: return the CBO candidate report even on fts/vector paths
}

// compilePlan is the single query compiler shared by the verbs. It owns
// $text detection (detectFtsQuery + ftsSorter), vector detection
// (detectVectorQuery + the default distance-sort rule), and CBO input
// assembly (buildBoundsResult, buildCBOIndexesInto, buildIndexHints,
// docCountForPlan/docCountExact) — so every verb selects the same documents
// for the same query by construction.
//
// btx MUST be the snapshot the plan executes on: the multikey-flag probe in
// buildCBOIndexesInto and the bounds interpolation read it. The caller owns
// alive()/unsatisfiable() gating, tx and buf lifetimes, and the sink. cbo is
// non-nil only when opts.wantCandidates (Explain's index report).
func (q *collQuery) compilePlan(btx *btree.ReadTx, buf *syncpool.DocBuffer,
	idBounds query.Bounds, opts planOpts) (plan *qplanner.Plan, cbo []qplanner.CBOIndex, err error) {

	// $text drives the query when present (CBO bypassed). The residual filter
	// runs as a downstream FilterIter; a relevance/textScore sort is the
	// FtsIter's intrinsic order, a real-field sort inserts a SortIter.
	ftsSpec, ftsResidual, err := q.detectFtsQuery()
	if err != nil {
		return nil, nil, err
	}
	if ftsSpec != nil {
		ftsSpec.NeedScores = opts.needScores
		sorter := ftsSorter(q.sort)
		if opts.countOnly {
			// Sound without ordering: FtsIter emits one aggregate per doc
			// (duplicate-free), and the distinct count is order-invariant.
			sorter = nil
		}
		plan = qplanner.BuildPlan(&qplanner.PlanParams{
			Tx:        btx,
			DataNs:    q.c.ns,
			Filter:    ftsResidual,
			Sorter:    sorter,
			Limit:     int(q.limit),
			Offset:    int(q.offset),
			Buf:       buf,
			CountOnly: opts.countOnly,
			Fts:       ftsSpec,
		})
		return plan, q.reportCandidates(btx, opts), nil
	}

	// Vector query: detect `{vectorField: [..]}` against the collection's
	// vector indexes. When matched, build a vector plan (ANN source) and
	// ignore all other indexes; the residual filter + sort run downstream.
	vspec, residual, err := q.detectVectorQuery()
	if err != nil {
		return nil, nil, err
	}
	if vspec != nil {
		sorter := q.sort
		if opts.countOnly {
			sorter = nil
		} else if sorter == nil && !vspec.Ordered {
			// No explicit sort and the source isn't already distance-ordered
			// (brute-force): order by distance ascending. When the ANN source
			// is ordered, we leave sorter nil so the planner skips a redundant
			// SortIter and streams the already-closest-first candidates
			// straight to LimitIter.
			sorter, _ = query.ParseSort(qplanner.DistanceField)
		}
		plan = qplanner.BuildPlan(&qplanner.PlanParams{
			Tx:        btx,
			DataNs:    q.c.ns,
			Filter:    residual,
			Sorter:    sorter,
			Limit:     int(q.limit),
			Offset:    int(q.offset),
			Buf:       buf,
			CountOnly: opts.countOnly,
			Vector:    vspec,
		})
		return plan, q.reportCandidates(btx, opts), nil
	}

	// CBO path. Bounds and candidates are built against btx: the
	// multikey-flag probe gating tight seek bounds must read the same
	// snapshot the scan executes on.
	sorter := q.sort
	if opts.countOnly {
		sorter = nil
	}
	idxs := q.c.loadIndexes()
	br := q.buildBoundsResult(idxs)
	cboIndexes := q.buildCBOIndexesInto(nil, &br, idxs, btx)
	totalDocs := q.docCountForPlan(btx, idxs)
	if opts.exactTotalDocs {
		totalDocs = q.docCountExact(btx, idxs)
	}
	plan = qplanner.BuildPlan(&qplanner.PlanParams{
		Tx:          btx,
		DataNs:      q.c.ns,
		Filter:      q.cond,
		Sorter:      sorter,
		IDBounds:    idBounds,
		PrimaryKey:  q.c.primaryKey,
		Limit:       int(q.limit),
		Offset:      int(q.offset),
		Buf:         buf,
		TotalDocs:   totalDocs,
		Indexes:     cboIndexes,
		IndexHints:  q.buildIndexHints(),
		CountOnly:   opts.countOnly,
		FieldBounds: &br,
	})
	if opts.wantCandidates {
		cbo = cboIndexes
	}
	return plan, cbo, nil
}

func (q *collQuery) Iter(ctx context.Context) (iter Iterator, err error) {
	if err = q.c.alive(); err != nil {
		return
	}
	qb, err := q.makeQuery()
	if err != nil {
		return
	}

	// Fast path: filter provably matches no documents — return an empty
	// iterator with no transaction, no plan construction, no I/O.
	if q.unsatisfiable() {
		qb.Close()
		return &emptyIter{}, nil
	}

	tx, err := q.c.db.getReadTx(ctx)
	if err != nil {
		qb.Close()
		return
	}

	// Re-checked inside the tx scope: the begin-time staleness pass may have
	// just invalidated the handle (a peer's drop frees the pages this
	// snapshot would walk through the stale root).
	if err = q.c.alive(); err != nil {
		_ = tx.Commit()
		qb.Close()
		return
	}

	buf := q.c.db.syncPool.GetDocBuf()
	btx := tx.btreeReadTx()

	plan, _, err := q.compilePlan(btx, buf, qb.idBounds, planOpts{needScores: true})
	if err != nil {
		q.c.db.syncPool.ReleaseDocBuf(buf)
		_ = tx.Commit()
		qb.Close()
		return nil, err
	}

	return &planIterator{
		plan: plan,
		tx:   tx,
		buf:  buf,
		qb:   qb,
		data: &qplanner.CursorSource{Tx: btx, Ns: q.c.ns},
	}, nil
}

func (q *collQuery) Update(ctx context.Context, modifier any) (result ModifyResult, err error) {
	if err = q.c.alive(); err != nil {
		return
	}
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
	if q.unsatisfiable() {
		return
	}

	// Reclaim tombstones this bulk update creates once it commits, mirroring the
	// single-doc mutators. Registered before the commit defer so it runs after
	// commit; no-ops inside a caller-managed tx (the guard in maybeAutoCompactVectors).
	// Gated on an explicit commit, not on err == nil: err is still nil while a
	// panic unwinds, and compaction opens its own write tx — it must never run
	// on a failed operation, let alone mid-panic.
	committed := false
	defer func() {
		if committed {
			q.c.maybeAutoCompactVectors(ctx)
		}
	}()

	tx, err := q.c.db.WriteTx(ctx)
	if err != nil {
		return
	}
	defer func() {
		// A panic (a user modifier's bug) leaves err == nil, so keying the commit
		// on err alone would durably persist the documents modified before it — a
		// torn bulk update. Roll back, then re-panic.
		if r := recover(); r != nil {
			_ = tx.Rollback()
			panic(r)
		}
		if err != nil {
			_ = tx.Rollback()
			return
		}
		if err = tx.Commit(); err == nil {
			committed = true
		}
	}()

	// Re-checked inside the tx scope: the begin-time staleness pass may have
	// just invalidated the handle (see collection.alive) — the entry check
	// alone would let this bulk write proceed through a stale handle.
	if err = q.c.alive(); err != nil {
		return
	}

	btWtx := tx.btreeWriteTx()
	btx := tx.btreeReadTx()

	buf := q.c.db.syncPool.GetDocBuf()
	defer q.c.db.syncPool.ReleaseDocBuf(buf)

	// The shared compiler: a bounded write must select exactly the documents
	// the equivalent Iter returns — same sorter, same access-path detection
	// ($text AND vector; an invalid vector clause errors here like it does on
	// Iter instead of degrading to a literal filter).
	plan, _, ferr := q.compilePlan(btx, buf, qb.idBounds, planOpts{})
	if ferr != nil {
		err = ferr
		return
	}

	// Close-once: the plan must be closed before the write loop below (see the
	// cursor-invalidation note), but a panic in a user Filter inside Next() would
	// skip that call and strand the cursors' pinned pages. The deferred call
	// covers the panic and error paths; Plan.Close is not idempotent, hence the
	// flag. Registered after the tx finalizer so LIFO releases the cursors while
	// the tx is still live.
	planClosed := false
	closePlan := func() {
		if !planClosed {
			planClosed = true
			plan.Close()
		}
	}
	defer closePlan()

	// Materialize the distinct target ids, then release the plan's cursors
	// BEFORE mutating ("can't modify while iterating"). Dedup ensures a bulk
	// update over an array-indexed $in query doesn't apply the modifier twice
	// to the same doc.
	idsToUpdate, err := collectDistinctIDs(plan.Root)
	if err != nil {
		return
	}
	closePlan()

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
	if err = q.c.alive(); err != nil {
		return
	}
	qb, err := q.makeQuery()
	if err != nil {
		return
	}
	defer qb.Close()

	// Fast path: filter provably matches no documents — nothing to delete,
	// no write tx required.
	if q.unsatisfiable() {
		return
	}

	// Reclaim tombstones this bulk delete creates once it commits, mirroring the
	// single-doc mutators. Registered before the commit defer so it runs after
	// commit; no-ops inside a caller-managed tx (the guard in maybeAutoCompactVectors).
	// Gated on an explicit commit, not on err == nil: err is still nil while a
	// panic unwinds, and compaction opens its own write tx — it must never run
	// on a failed operation, let alone mid-panic.
	committed := false
	defer func() {
		if committed {
			q.c.maybeAutoCompactVectors(ctx)
		}
	}()

	tx, err := q.c.db.WriteTx(ctx)
	if err != nil {
		return
	}
	defer func() {
		// A panic in user filter code leaves err == nil, so keying the commit on
		// err alone would durably persist the documents deleted before it — a torn
		// bulk delete. Roll back, then re-panic.
		if r := recover(); r != nil {
			_ = tx.Rollback()
			panic(r)
		}
		if err != nil {
			_ = tx.Rollback()
			return
		}
		if err = tx.Commit(); err == nil {
			committed = true
		}
	}()

	// Re-checked inside the tx scope: the begin-time staleness pass may have
	// just invalidated the handle (see collection.alive) — the entry check
	// alone would let this bulk write proceed through a stale handle.
	if err = q.c.alive(); err != nil {
		return
	}

	btWtx := tx.btreeWriteTx()
	btx := tx.btreeReadTx()

	buf := q.c.db.syncPool.GetDocBuf()
	defer q.c.db.syncPool.ReleaseDocBuf(buf)

	// The shared compiler: a bounded write must select exactly the documents
	// the equivalent Iter returns — same sorter, same access-path detection
	// ($text AND vector; an invalid vector clause errors here like it does on
	// Iter instead of degrading to a literal filter).
	plan, _, ferr := q.compilePlan(btx, buf, qb.idBounds, planOpts{})
	if ferr != nil {
		err = ferr
		return
	}

	// Close-once: the plan must be closed before the delete loop below, but a
	// panic in a user Filter inside Next() would skip that call and strand the
	// cursors' pinned pages. See the matching comment in Update.
	planClosed := false
	closePlan := func() {
		if !planClosed {
			planClosed = true
			plan.Close()
		}
	}
	defer closePlan()

	// Materialize the distinct target ids, then release the plan's cursors
	// BEFORE mutating ("can't modify while iterating"). Dedup ensures a doc
	// matched on multiple array values isn't deleted twice / counted twice.
	idsToDelete, err := collectDistinctIDs(plan.Root)
	if err != nil {
		return
	}
	closePlan()

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
	if err = q.c.alive(); err != nil {
		return
	}
	// A parse error must be reported before anything else: Cond()/Sort() leave
	// q.cond nil when they fail, which is indistinguishable from "no filter" to
	// the fast path below — so a rejected query used to count the WHOLE
	// collection with err=nil, while Iter/Update/Delete (which reach q.err via
	// makeQuery) correctly errored. Count is the only verb that skips makeQuery.
	if q.err != nil {
		return 0, q.err
	}

	// Fast path: no filter, no offset, no limit — use lightweight page-header count
	_, isAll := q.cond.(query.All)
	if (q.cond == nil || isAll) && q.offset == 0 && q.limit == 0 && q.sort == nil {
		return q.c.Count(ctx)
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
	if q.unsatisfiable() {
		return 0, nil
	}

	// Primary-key bounds (tight channel — sound for the pk, see pkBounds)
	idBounds := q.pkBounds()

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

	err = q.c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		buf := q.c.db.syncPool.GetDocBuf()
		defer q.c.db.syncPool.ReleaseDocBuf(buf)

		// Bounds and CBO candidates are built INSIDE the read tx: the
		// multikey-flag probe gating tight seek bounds must read the same
		// snapshot the scan executes on.
		br := q.buildBoundsResult(idxs)
		var cboBuf [8]qplanner.CBOIndex
		cboIndexes := q.buildCBOIndexesInto(cboBuf[:0], &br, idxs, tx)

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
			TotalDocs:   q.docCountForPlan(tx, idxs),
			Indexes:     cboIndexes,
			IndexHints:  q.buildIndexHints(),
			CountOnly:   true,
			FieldBounds: &br,
		})

		// Limit/Offset cutoffs must apply to DISTINCT docs, not raw entry
		// rows: over a multi-key index LimitIter.Next skips/caps entries that
		// later collapse in dedup, diverging from Iter (known-issues I-07).
		// CountDistinct deduplicates before the cutoff.
		if li, ok := plan.Root.(*qplanner.LimitIter); ok {
			n, cerr := li.CountDistinct()
			plan.Close()
			if cerr != nil {
				return cerr
			}
			count = n
			return nil
		}
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
		// Generic distinct-doc count: a doc whose array values match multiple
		// bounds is counted once.
		cerr := qplanner.ForEachDistinct(plan.Root, func([]byte) error {
			count++
			return nil
		})
		plan.Close()
		return cerr
	})
	return
}

// collectDistinctIDs materializes a plan's distinct docIds into slices backed
// by one contiguous arena: one allocation curve, and the ids stay valid after
// plan.Close — bulk writes must release the plan's cursors before mutating,
// and iterators reuse their docId buffers between rows.
func collectDistinctIDs(root qplanner.Iterator) ([][]byte, error) {
	var idBuf []byte       // contiguous buffer for all ids
	var idOffsets []uint32 // start offset of each id in idBuf
	if err := qplanner.ForEachDistinct(root, func(docId []byte) error {
		idOffsets = append(idOffsets, uint32(len(idBuf)))
		idBuf = append(idBuf, docId...)
		return nil
	}); err != nil {
		return nil, err
	}
	ids := make([][]byte, len(idOffsets))
	for i, off := range idOffsets {
		end := len(idBuf)
		if i+1 < len(idOffsets) {
			end = int(idOffsets[i+1])
		}
		ids[i] = idBuf[off:end]
	}
	return ids, nil
}

func (q *collQuery) Explain(ctx context.Context) (explain Explain, err error) {
	if err = q.c.alive(); err != nil {
		return
	}
	qb, err := q.makeQuery()
	if err != nil {
		return
	}
	defer qb.Close()

	buf := q.c.db.syncPool.GetDocBuf()
	defer q.c.db.syncPool.ReleaseDocBuf(buf)

	idxs := q.c.loadIndexes()

	err = q.c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		if aErr := q.c.alive(); aErr != nil {
			return aErr
		}
		// Built inside the read tx so the multikey-flag probe sees the same
		// snapshot the (hypothetical) scan would — Explain must report the
		// bounds the real query would use.
		br := q.buildBoundsResult(idxs)
		cboIndexes := q.buildCBOIndexesInto(nil, &br, idxs, tx)

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
				TotalDocs:   q.docCountExact(tx, idxs),
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
	qb.idBounds = q.pkBounds()

	return
}

// pkBounds computes the primary-key seek bounds. The pk namespace is
// fan-out-free by construction (array pks are rejected on write,
// ErrArrayPrimaryKey), so the TIGHT channel is sound for the actual seek here
// — {id:{$gt:lo,$lt:hi}} seeks (lo,hi) instead of (lo,+inf). When the tight
// intersection is empty the verbs have already short-circuited via
// unsatisfiable(); Explain bypasses that and keeps the wide superset.
func (q *collQuery) pkBounds() query.Bounds {
	idBounds := q.cond.IndexBounds(q.c.primaryKey, nil)
	if len(idBounds) == 0 || !query.MayTighten(q.cond) {
		return idBounds
	}
	if tight, empty := query.TightIndexBounds(q.cond, q.c.primaryKey); !empty && len(tight) != 0 {
		return tight
	}
	return idBounds
}

// countTx is the read-tx capability docCount needs.
type countTx interface {
	Count(ns *btree.Namespace) (int, error)
}

// docCountForPlan returns TotalDocs for the planner's cost model: the first
// index sketch's DocCount, falling back to tx.Count() — a full namespace page
// walk — when no index has a published sketch yet.
//
// With no secondary indexes at all, FullScan is the planner's only candidate,
// so the count can shift cost numbers but never the outcome. Paying an
// O(namespace pages) walk before every query on exactly the collections that
// need no secondary index (pk-ordered schemas) is pure waste — return 0 and
// let BuildPlan clamp it. Explain uses docCountExact instead. idxs is the
// caller's loadIndexes() snapshot — the same one its CBO candidates are built
// from, so the count and the candidates can't disagree about the index set.
func (q *collQuery) docCountForPlan(tx countTx, idxs []*index) int {
	for _, idx := range idxs {
		if s := idx.loadPubSketch(); s != nil {
			return int(s.GetDocCount())
		}
	}
	if len(idxs) == 0 {
		return 0
	}
	count, _ := tx.Count(q.c.ns)
	return count
}

// docCountExact is docCountForPlan without the no-indexes shortcut: Explain
// reports TotalDocs to humans, so it pays the walk for a real number.
func (q *collQuery) docCountExact(tx countTx, idxs []*index) int {
	for _, idx := range idxs {
		if s := idx.loadPubSketch(); s != nil {
			return int(s.GetDocCount())
		}
	}
	count, _ := tx.Count(q.c.ns)
	return count
}

// unsatisfiable reports whether this query's filter provably matches no
// documents: structurally (isUnsatisfiable), or via a contradictory
// primary-key constraint ({id:{$gt:5,$lt:3}}, {$and:[{id:1},{id:2}]}) whose
// tight bounds intersect to the empty set.
//
// The pk is the ONLY field this emptiness argument is sound for: pk values
// are scalars by construction (array pks rejected on write,
// ErrArrayPrimaryKey), so an empty value set really means no doc can match.
// For any other field — indexed or not — a contradictory-looking range can
// still match array values element-wise ({a:{$gt:5,$lt:3}} matches
// {a:[6,1]}), so tight-empty must NEVER short-circuit there. Contradictions
// inside $or branches cannot leak here: the tight channel delegates Or wide.
func (q *collQuery) unsatisfiable() bool {
	if isUnsatisfiable(q.cond) {
		return true
	}
	// MayTighten gates the walk: single-predicate filters (the hot id-lookup
	// shapes) can't intersect to empty, so they skip the extra tree walk.
	if q.cond != nil && query.MayTighten(q.cond) {
		if _, empty := query.TightIndexBounds(q.cond, q.c.primaryKey); empty {
			return true
		}
	}
	return false
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

// isIDOnlyFilter returns true if the filter is EXACTLY ONE Eq or $in predicate
// on the primary-key field, i.e. its match set equals its IndexBounds point set.
// Only then may Count's tx.Has fast path (which never runs the residual filter)
// skip CBO planning. Anything else — extra conjuncts ({$in,$ne}, {$in,$gt},
// {$in,$type}), ranges, multiple pk Keys ({$and:[{id:1},{id:2}]} matches
// nothing yet has two point bounds) — is NOT exactly represented by its bounds
// and must go through the planner.
func (q *collQuery) isIDOnlyFilter() bool {
	return isIDOnlyFilterNode(q.cond, q.c.primaryKey)
}

func isIDOnlyFilterNode(f query.Filter, pk string) bool {
	switch ft := f.(type) {
	case query.Key:
		if len(ft.Path) != 1 || ft.Path[0] != pk {
			return false
		}
		switch inner := ft.Filter.(type) {
		case *query.Comp:
			return inner.CompOp == query.CompOpEq
		case query.In:
			return true
		}
		return false
	case query.And:
		// A conjunction qualifies only as a transparent single-child wrapper;
		// two predicates on the pk are already inexact (see doc comment).
		return len(ft) == 1 && isIDOnlyFilterNode(ft[0], pk)
	case *query.And:
		// query.MustParseCondition produces *query.And for `{"$and":[...]}`
		// (see query/cond_parse.go:103). Delegate to the value arm so $and
		// JSON syntax enjoys the same id-only fast path as the plain
		// `{"id":1}` spelling.
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
//
// tx is the READ TX THE QUERY WILL EXECUTE WITH; when a candidate's tight
// bounds differ from the wide ones, it gates one point Get of the index's
// multikey flag on that same snapshot. A scalar-proven index gets its ENTIRE
// variant — bounds plus every flag derived from them (PointLookup,
// equalityPrefix→ExactSort/SortMatchStart, and downstream dedupBounds/padding
// in BuildPlan) — rebuilt from the tight channel: swapping only the bounds
// under wide-derived flags (or vice versa) yields wrong plans and wrong rows
// (a stale PointLookup routes a multikey unique index into CoverIter, which
// ignores End; a stale ExactSort skips the SortIter). An unproven index keeps
// the wide variant and carries the tight bounds in EstBounds for estimation
// only. tx == nil (unit tests) means no proof — wide seeks.
func (q *collQuery) buildCBOIndexesInto(buf []qplanner.CBOIndex, br *qplanner.BoundsResult, idxs []*index, tx *btree.ReadTx) []qplanner.CBOIndex {
	result := buf

	var sortFields []query.SortField
	if q.sort != nil {
		sortFields = q.sort.Fields()
	}

	for _, idx := range idxs {
		cboIdx := q.buildCBOIndex(idx, br, sortFields, false)
		if br.TightDiffers(idx.cboInfo.FieldNames) {
			if tx != nil && idx.isScalarProven(tx) {
				cboIdx = q.buildCBOIndex(idx, br, sortFields, true)
			} else {
				// Estimation-only tight bounds; seeks keep the wide Bounds.
				cboIdx.EstBounds, _ = qplanner.ComputeIndexBoundsTight(idx.cboInfo, br)
			}
		}
		result = append(result, cboIdx)
	}
	return result
}

// buildCBOIndex builds one candidate's CBOIndex with bounds AND all
// bounds-derived flags taken consistently from a single channel (wide or
// tight) — see buildCBOIndexesInto for why mixing channels is forbidden.
func (q *collQuery) buildCBOIndex(idx *index, br *qplanner.BoundsResult, sortFields []query.SortField, tight bool) qplanner.CBOIndex {
	info := idx.cboInfo

	// Compute bounds for this index
	var bounds query.Bounds
	var chainLen int
	lookup := br.Lookup
	if tight {
		bounds, chainLen = qplanner.ComputeIndexBoundsTight(info, br)
		lookup = br.LookupTight
	} else {
		bounds, chainLen = qplanner.ComputeIndexBounds(info, br)
	}

	pointLookup := qplanner.AllBoundsFixed(bounds)
	// Note: AdjustBoundsForNonUnique is deferred to BuildPlan's
	// buildIndexSeekChain, which only adjusts the CHOSEN index.
	// This avoids allocation overhead for indexes that aren't selected.

	// Compute equality prefix: count leading index fields pinned to a SINGLE
	// point value. This handles compound indexes like (t,o) with t=eq, o=range,
	// allowing IndexSortMatch to recognize sort coverage after the prefix.
	//
	// Exactly one fixed bound is required, not merely "all bounds fixed": a
	// multi-bound equality set ($in with >=2 values, or an $or of equalities) is
	// "fixed" per allBoundsFixedNonEmpty, but IndexIter walks the bound list
	// sequentially, emitting one run per bound. Suffix fields are ordered only
	// WITHIN a run, not across runs, so claiming ExactSort for a suffix sort
	// drops the SortIter and yields silently misordered rows — and, under Limit,
	// the wrong rows. Break rather than skip: a later single-bound field must not
	// re-extend the prefix past the multi-bound one. See BUG-26.
	//
	// A sort that STARTS at the multi-bound field is still served by
	// IndexSortMatch's matchAt() paths: the bound list is ascending and pairwise
	// disjoint, and IndexIter consumes it from the top on a reverse scan, so the
	// concatenated runs are globally ordered in that field.
	equalityPrefix := 0
	for _, field := range info.FieldNames {
		bounds, fixed, found := lookup(field)
		if !found || len(bounds) != 1 || !fixed {
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

	return qplanner.CBOIndex{
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
