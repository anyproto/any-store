package anystore

import (
	"context"
	"errors"
	"fmt"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/aggregate"
	"github.com/anyproto/any-store/v2/internal/btree"
)

// $merge / $out execution: the sink stages materialize pipeline results into
// a collection. The model is buffer-then-write:
//
//  1. The read part of the pipeline runs to EOF inside its own read
//     transaction (snapshot), exactly like a plain aggregation; every result
//     document is buffered as raw marshaled bytes. The buffered bytes count
//     against the pipeline's shared memory budget (MemoryLimit) — exceeding
//     it fails with ErrAggMemoryLimitExceeded before anything is written.
//  2. The read transaction is closed, THEN one write transaction applies the
//     buffered documents to the target through the regular collection write
//     path (insert/update/delete items), so range, full-text and vector
//     indexes — and schema cookies — stay correct and the materialized
//     documents are immediately queryable through every index type.
//
// The write is atomic: a single write tx covers target creation, the $out
// truncate and every document write, so readers in this process and in other
// processes see either the old contents or the new, never a mix, and any
// error (including the $merge "fail" modes) rolls the whole write back. The
// two phases run in different transactions: results reflect the read
// snapshot, and a write committed between the phases by another writer is
// simply overwritten ($out) or merged against ($merge) — same
// read-committed-style window Mongo has between its cursor and its writes.

var (
	// ErrAggregateIntoSource is returned when $merge/$out targets the
	// aggregated collection itself: the buffered-write model gives no
	// coherent semantics for replacing the pipeline's own source.
	ErrAggregateIntoSource = errors.New("any-store: aggregate: $merge/$out cannot target the aggregated collection")

	// ErrMergeNoId is returned when a $merge result document lacks the "id"
	// field: without the key there is nothing to match on.
	ErrMergeNoId = errors.New(`any-store: aggregate: $merge requires every result document to carry an "id" field`)

	// ErrMergeMatched is returned by $merge whenMatched "fail" when the
	// target already has a document with a result's id. Nothing is persisted.
	ErrMergeMatched = errors.New(`any-store: aggregate: $merge whenMatched "fail": target already has a matching document`)

	// ErrMergeNotMatched is returned by $merge whenNotMatched "fail" when the
	// target has no document with a result's id. Nothing is persisted.
	ErrMergeNotMatched = errors.New(`any-store: aggregate: $merge whenNotMatched "fail": target has no matching document`)

	errAggMergePrimaryKey = errors.New(`any-store: aggregate: $merge on "id" requires the target collection's primary key to be "id"`)
)

// materialize executes a $merge/$out pipeline: drains the read part (rest),
// buffering marshaled result documents, then applies them to the sink's
// target collection in one write transaction. Returns the number of
// documents written (inserted, replaced or merged; keepExisting/discard
// skips and no-op replaces don't count).
func (q *aggQuery) materialize(ctx context.Context, cq *collQuery, rest aggregate.Pipeline, sink aggregate.StageSpec) (written int, err error) {
	var (
		targetName string
		merge      aggregate.MergeSpec
		isMerge    bool
	)
	switch sp := sink.(type) {
	case aggregate.OutSpec:
		targetName = sp.Coll
	case aggregate.MergeSpec:
		targetName, merge, isMerge = sp.Into, sp, true
	default:
		return 0, fmt.Errorf("any-store: aggregate: unsupported sink stage: %T", sink)
	}
	if targetName == q.c.Name() {
		return 0, fmt.Errorf("%w: %q", ErrAggregateIntoSource, targetName)
	}

	// Phase 1: drain the read pipeline, buffering raw marshaled documents in
	// one slab (compact bytes, not parsed values). offs[i] is doc i's end.
	it, err := q.iterRest(ctx, cq, rest)
	if err != nil {
		return 0, err
	}
	var (
		slab []byte
		offs []int
	)
	for it.Next() {
		doc, derr := it.Doc()
		if derr != nil {
			err = derr
			break
		}
		v := doc.Value()
		if isMerge && v.Get("id") == nil {
			err = fmt.Errorf("%w (result document %d)", ErrMergeNoId, len(offs))
			break
		}
		start := len(slab)
		slab = v.MarshalTo(slab)
		offs = append(offs, len(slab))
		if err = it.actx.Mem.Add(len(slab) - start); err != nil {
			break
		}
	}
	if err == nil {
		err = it.Err()
	}
	if cerr := it.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		return 0, err
	}

	// An empty $merge is a pure no-op: nothing to match or insert, the
	// target is not even created. $out always applies — it creates the
	// target and replaces its (possibly empty) contents.
	if isMerge && len(offs) == 0 {
		return 0, nil
	}

	// Phase 2: one write tx (a savepoint inside an ambient WriteTx) covering
	// target open-or-create and every document write.
	var targetC *collection
	err = q.c.db.doWriteTxW(ctx, func(wtx WriteTx, tx *btree.WriteTx) error {
		// wtx.Context carries the tx: a missing target is created inside
		// this same transaction, so a failed apply leaves no empty shell.
		target, terr := q.c.db.Collection(wtx.Context(), targetName)
		if terr != nil {
			return terr
		}
		tc := target.(*collection)
		if terr = tc.alive(); terr != nil {
			return terr
		}
		if isMerge {
			if tc.primaryKey != "id" {
				return fmt.Errorf("%w: collection %q's primary key is %q", errAggMergePrimaryKey, targetName, tc.primaryKey)
			}
			written, terr = applyMerge(tx, tc, slab, offs, merge)
		} else {
			written, terr = applyOut(tx, tc, slab, offs)
		}
		if terr != nil {
			return terr
		}
		targetC = tc
		return nil
	})
	if err != nil {
		return 0, err
	}
	// Same post-write maintenance hook as Insert/UpdateOne on the target.
	targetC.maybeAutoCompactVectors(ctx)
	return written, nil
}

// applyOut replaces tc's contents with the buffered documents: every existing
// document is deleted and every result inserted through the regular item
// write path, inside the caller's tx. Declared indexes are untouched as
// objects and rebuilt entry-by-entry by the same maintenance the normal write
// path runs. A duplicate id among the results fails with ErrDocExists; a
// result without tc's primary key fails with ErrDocWithoutId (same contract
// as Insert).
func applyOut(tx *btree.WriteTx, tc *collection, slab []byte, offs []int) (written int, err error) {
	buf := tc.db.syncPool.GetDocBuf()
	defer tc.db.syncPool.ReleaseDocBuf(buf)

	// Collect the current keys first — deleteItem mutates the tree under a
	// live cursor otherwise.
	var (
		keySlab []byte
		keyOffs []int
	)
	cursor := tx.NewCursor(tc.ns)
	if err = cursor.First(); err != nil {
		cursor.Close()
		return 0, err
	}
	for cursor.Valid() {
		key, kerr := cursor.Key()
		if kerr != nil {
			cursor.Close()
			return 0, kerr
		}
		keySlab = append(keySlab, key...)
		keyOffs = append(keyOffs, len(keySlab))
		if err = cursor.Next(); err != nil {
			cursor.Close()
			return 0, err
		}
	}
	cursor.Close()

	start := 0
	for _, end := range keyOffs {
		if err = tc.deleteItem(tx, buf, keySlab[start:end]); err != nil {
			return 0, err
		}
		start = end
	}

	start = 0
	for _, end := range offs {
		buf.Arena.Reset()
		doc, perr := buf.Parser.ParseOwned(slab[start:end])
		if perr != nil {
			return written, perr
		}
		it, ierr := tc.newItem(doc)
		if ierr != nil {
			return written, ierr
		}
		if err = tc.insertItem(tx, buf, it); err != nil {
			return written, err
		}
		written++
		start = end
	}
	return written, nil
}

// applyMerge upserts the buffered documents into tc by primary key, inside
// the caller's tx. Matched/not-matched routing follows spec; a "fail" mode
// returns an error naming the offending id, aborting the whole tx.
func applyMerge(tx *btree.WriteTx, tc *collection, slab []byte, offs []int, spec aggregate.MergeSpec) (written int, err error) {
	buf := tc.db.syncPool.GetDocBuf() // result-doc parse + insert scratch
	defer tc.db.syncPool.ReleaseDocBuf(buf)
	bufPrev := tc.db.syncPool.GetDocBuf() // existing-doc load
	defer tc.db.syncPool.ReleaseDocBuf(bufPrev)
	bufMerged := tc.db.syncPool.GetDocBuf() // merged-doc copy
	defer tc.db.syncPool.ReleaseDocBuf(bufMerged)

	start := 0
	for _, end := range offs {
		doc, perr := buf.Parser.ParseOwned(slab[start:end])
		if perr != nil {
			return written, perr
		}
		start = end
		it, ierr := tc.newItem(doc) // pk presence + array-pk validation
		if ierr != nil {
			return written, ierr
		}
		buf.SmallBuf = tc.appendId(buf.SmallBuf[:0], doc)
		_, gerr := tx.Get(tc.ns, buf.SmallBuf)
		if gerr != nil && !errors.Is(gerr, btree.ErrKeyNotFound) {
			return written, gerr
		}
		if gerr == nil { // matched
			switch spec.WhenMatched {
			case aggregate.MergeMatchedKeepExisting:
			case aggregate.MergeMatchedFail:
				return written, fmt.Errorf("%w: id %s", ErrMergeMatched, doc.Get(tc.primaryKey))
			case aggregate.MergeMatchedReplace:
				modified, uerr := tc.update(tx, it, item{})
				if uerr != nil {
					return written, uerr
				}
				if modified {
					written++
				}
			case aggregate.MergeMatchedMerge:
				prevIt, lerr := tc.loadById(tx, bufPrev, buf.SmallBuf)
				if lerr != nil {
					return written, lerr
				}
				// Shallow merge (Mongo whenMatched "merge"): copy the
				// existing document, then overlay the result's top-level
				// fields; fields the result lacks keep their existing values.
				// prevIt stays untouched — update derives the old index
				// entries from it.
				mergedIt := copyItem(bufMerged, prevIt)
				resObj, oerr := doc.Object()
				if oerr != nil {
					return written, oerr
				}
				resObj.Visit(func(key []byte, val *anyenc.Value) {
					mergedIt.val.Set(string(key), val)
				})
				if _, err = tc.update(tx, mergedIt, prevIt); err != nil {
					return written, err
				}
				written++
			}
		} else { // not matched
			switch spec.WhenNotMatched {
			case aggregate.MergeNotMatchedDiscard:
			case aggregate.MergeNotMatchedFail:
				return written, fmt.Errorf("%w: id %s", ErrMergeNotMatched, doc.Get(tc.primaryKey))
			case aggregate.MergeNotMatchedInsert:
				if err = tc.insertItem(tx, buf, it); err != nil {
					return written, err
				}
				written++
			}
		}
	}
	return written, nil
}

// sinkIterator is the empty cursor a $merge/$out pipeline returns from Iter:
// the write already executed inside Iter itself.
type sinkIterator struct {
	closed bool
}

func (it *sinkIterator) Next() bool { return false }

func (it *sinkIterator) Doc() (Doc, error) { return nil, ErrDocNotFound }

func (it *sinkIterator) Distance() float32 { return 0 }

func (it *sinkIterator) Score() float64 { return 0 }

func (it *sinkIterator) Err() error { return nil }

func (it *sinkIterator) Close() error {
	if it.closed {
		return ErrIterClosed
	}
	it.closed = true
	return nil
}
