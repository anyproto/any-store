package anystore

import (
	"errors"
	"io"
	"sync/atomic"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/qplanner"
	"github.com/anyproto/any-store/v2/syncpool"
)

// Iterator represents an iterator over query results.
//
// Writing the iterated collection through the same write transaction while
// the iterator is open (inserting, updating or deleting documents mid-loop)
// never corrupts the store. A scan the plan drives with a cursor — a full
// scan, an index scan, in the plan's order — goes on from the key it stood
// on, as a cursor of the same SQLite connection does: the current document
// deleted, the scan continues with its successor; a document inserted ahead
// of the position is visited, one inserted behind it is not; a write that
// moves a document within the scan's order — an update of an indexed field
// under an index scan — can make the scan visit the document again at its
// new place, or miss it. A plan that collects its result before yielding
// it — a sort the planner runs in memory, a $knn probe — yields what it
// collected: a document inserted since is not visited, one deleted since
// makes its Doc fail with ErrDocNotFound. The $knn search over the vector
// index collects its candidates the same way and skips one deleted since:
// it yields the first k that remain, fewer once they run out. Doc returns
// the document as Next read it. To mutate every matched document once,
// collect the ids during iteration and mutate after Close — any-store's
// own Query.Update/Delete do exactly that internally — or use those verbs
// directly. Through the same transaction, Drop, DropIndex and
// CompactVectorIndex of the collection fail with ErrIterOpen until the
// iterator is closed, advanced or not: they free the trees it reads.
//
// An Iterator belongs to one goroutine at a time. Opened with the context
// of a transaction, its methods are calls on the transaction (see
// commonTx), and once the transaction has ended Next and Doc fail with
// ErrTxIsUsed. Once a savepoint that was open as the iterator was opened or
// last moved is rolled back, they fail with ErrIterRolledBack; an iterator
// that last moved before the savepoint opened goes on. A write that fails
// rolls back a savepoint of its own: an iterator a query.Modifier moved
// while the write ran ends with it.
type Iterator interface {
	// Next advances the iterator to the next document.
	Next() bool

	// Doc returns the current document.
	Doc() (Doc, error)

	// Distance returns the vector distance of the current document to the query
	// for a vector search (smaller is closer). It returns 0 for non-vector
	// queries.
	Distance() float32

	// Score returns the BM25 relevance score of the current document for a $text
	// search (larger is more relevant). It returns 0 for non-fts queries.
	Score() float64

	// Err returns any error encountered during the lifetime of the iterator.
	Err() error

	// Close closes the iterator and releases any associated resources.
	Close() error
}

// planIterator wraps a qplanner.Plan to implement the public Iterator interface.
type planIterator struct {
	tx ReadTx
	// shared is tx when other callers can reach it — the transaction the
	// context carries — for the call each method begins on it (see
	// commonTx); nil for the iterator's own read transaction. The
	// transaction knows the iterator (iterOpened) until Close, and trips it
	// as it ends.
	shared ReadTx
	closed bool
	// tripped: the cursors' pages went with the transaction's end, or with
	// the rollback of a savepoint the iterator moved inside (trip); the
	// methods report tripErr, and Close leaves the cursors alone. Atomic:
	// Doc reads it outside a call when the document is the iterator's own
	// memory.
	tripped atomic.Bool
	tripErr error
	// mark is the transaction's savepoint mark as the iterator was opened
	// or last moved (iterOpened, savepointMark): the savepoints whose
	// rollback trips it are the ones opened by then and still open
	// (commonTx.spSeq).
	mark uint64
	// s is the schema version the plan was compiled against, resolved for tx.
	s          *collSchema
	err        error
	plan       *qplanner.Plan
	buf        *syncpool.DocBuffer
	qb         *queryBuilder
	data       *qplanner.CursorSource
	dataCursor *btree.Cursor
	docId      []byte
	dedup      qplanner.DocDedup // lazy-allocated when upstream emits multiKey=true
	distArena  anyenc.Arena      // _distance re-injection on the post-sort re-fetch path
}

func (pi *planIterator) Next() bool {
	if pi.err != nil || pi.closed {
		return false
	}
	if pi.shared != nil {
		pi.shared.enter()
		defer pi.shared.exit()
		if err := pi.trippedErr(); err != nil {
			pi.err = err
			return false
		}
		pi.mark = pi.shared.savepointMark()
	}
	for {
		pi.plan.DocParsed = nil
		_, docId, mk, err := pi.plan.Root.Next()
		if err != nil {
			pi.err = err
			return false
		}
		if docId == nil {
			return false
		}
		// Skip duplicates emitted by multi-key sources (e.g. an
		// $in over an array index where a doc matches multiple bounds).
		// multiKey=false is a hard guarantee from upstream; the helper
		// bypasses the seen-set entirely in that case. This streaming pull
		// loop is the twin of qplanner.ForEachDistinct (the batch consumer
		// behind bulk writes and the generic count) — both must implement
		// the identical Accept contract.
		if !pi.dedup.Accept(docId, mk) {
			continue
		}
		if pi.plan.DocParsed == nil || pi.plan.Distances != nil || pi.plan.Scores != nil {
			// Copy docId when we'll need it in Doc() fallback, or to look up
			// the distance/score sidecar for a vector/fts query.
			pi.docId = append(pi.docId[:0], docId...)
		}
		return true
	}
}

// Distance returns the ANN distance of the current document for a vector query
// (0 for non-vector queries).
func (pi *planIterator) Distance() float32 {
	if pi.plan == nil || pi.plan.Distances == nil {
		return 0
	}
	d, _ := pi.plan.Distances.Get(pi.docId)
	return float32(d)
}

// Score returns the BM25 relevance score of the current document for a $text
// query (0 for non-fts queries).
func (pi *planIterator) Score() float64 {
	if pi.plan == nil || pi.plan.Scores == nil {
		return 0
	}
	s, _ := pi.plan.Scores.Get(pi.docId)
	return s
}

func (pi *planIterator) Doc() (Doc, error) {
	perf := pipelinePerfEnabled()
	if perf {
		pipePerf.docCalls.Add(1)
	}
	if pi.closed {
		return nil, ErrIterClosed
	}
	if pi.err != nil && !errors.Is(pi.err, io.EOF) {
		return nil, pi.err
	}
	var doc *anyenc.Value
	if pi.plan.DocParsed != nil {
		// The document is the iterator's own memory; ended, the
		// transaction has nothing more to give.
		if pi.shared != nil {
			if err := pi.trippedErr(); err != nil {
				return nil, err
			}
			if pi.shared.Done() {
				return nil, ErrTxIsUsed
			}
		}
		if perf {
			pipePerf.docParsedHits.Add(1)
		}
		doc = pi.plan.DocParsed
	} else {
		if perf {
			pipePerf.docFallbacks.Add(1)
		}
		if pi.shared != nil {
			pi.shared.enter()
			defer pi.shared.exit()
			if err := pi.trippedErr(); err != nil {
				return nil, err
			}
			pi.mark = pi.shared.savepointMark()
		}
		if pi.dataCursor == nil {
			pi.dataCursor = pi.data.NewCursor()
		}
		var seekStart time.Time
		if perf {
			seekStart = time.Now()
		}
		if err := pi.dataCursor.SeekExact(pi.docId); err != nil {
			if errors.Is(err, btree.ErrKeyNotFound) {
				// Deleted since the plan collected it.
				return nil, ErrDocNotFound
			}
			return nil, err
		}
		if perf {
			pipePerf.docFallbackSeekNs.Add(perfSinceNs(seekStart))
		}
		val, err := pi.dataCursor.Value()
		if err != nil {
			return nil, err
		}
		pi.buf.DocBuf = append(pi.buf.DocBuf[:0], val...)
		var parseStart time.Time
		if perf {
			parseStart = time.Now()
		}
		var perr error
		doc, perr = pi.buf.Parser.ParseOwned(pi.buf.DocBuf)
		if perf {
			pipePerf.docFallbackParseNs.Add(perfSinceNs(parseStart))
		}
		if perr != nil {
			return nil, perr
		}
		// Re-decorate the synthetic _distance on the re-fetch path: SortIter
		// clears DocParsed on emit, so a sorted $knn result would otherwise
		// hand back the raw stored document with the documented _distance
		// field missing. The sidecar always exists on the read verb (the only
		// verb with a Doc()), keyed by docId.
		if pi.plan.Distances != nil {
			if d, ok := pi.plan.Distances.Get(pi.docId); ok {
				pi.distArena.Reset()
				doc.Set(qplanner.DistanceField, pi.distArena.NewNumberFloat64(d))
			}
		}
	}
	return pi.qb.coll.newItem(doc)
}

func (pi *planIterator) Err() error {
	if pi.err != nil && errors.Is(pi.err, io.EOF) {
		return nil
	}
	return pi.err
}

func (pi *planIterator) Close() (err error) {
	if pi.closed {
		return ErrIterClosed
	}
	if pi.shared != nil {
		// Before closed is set: a refused call closes nothing.
		pi.shared.enter()
		defer pi.shared.exit()
		// Ended without a trip (a panic in the trip): the pages are gone
		// with the transaction all the same.
		if pi.shared.Done() {
			pi.tripped.Store(true)
		}
		if !pi.tripped.Load() {
			pi.shared.iterClosed(pi)
		}
	}
	pi.closed = true
	pi.releaseCursors()
	if pi.tx != nil {
		err = errors.Join(err, pi.tx.Commit())
	}
	if pi.buf != nil && pi.qb != nil {
		pi.qb.coll.db.syncPool.ReleaseDocBuf(pi.buf)
	}
	if pi.qb != nil {
		pi.qb.Close()
	}
	return
}

func (pi *planIterator) String() string {
	return pi.plan.String()
}

// trip is the transaction ending, or a savepoint the iterator moved inside
// being rolled back, inside a call on the transaction (commonTx.tripIters,
// tripItersFrom): the cursors' pages go with it, and the methods report err.
func (pi *planIterator) trip(err error) {
	pi.releaseCursors()
	pi.tripErr = err
	pi.tripped.Store(true)
}

// trippedErr is the error a tripped iterator reports, nil while it is not
// tripped. Reads the flag atomically: for a caller outside a call.
func (pi *planIterator) trippedErr() error {
	if pi.tripped.Load() {
		return pi.tripErr
	}
	return nil
}

// releaseCursors closes the cursors once: Plan.Close is not idempotent.
func (pi *planIterator) releaseCursors() {
	if pi.tripped.Load() {
		return
	}
	if pi.dataCursor != nil {
		pi.dataCursor.Close()
		pi.dataCursor = nil
	}
	if pi.plan != nil {
		pi.plan.Close()
	}
}

// emptyIter is the public Iterator returned by Find().Iter() when the
// filter is provably empty (see isUnsatisfiable in query.go). It opens
// no transaction, allocates no buffers, and yields nothing — Next is
// always false. Used to short-circuit Iter on filters like $in:[]
// without paying for plan construction or any I/O.
type emptyIter struct{ closed bool }

func (e *emptyIter) Next() bool { return false }
func (e *emptyIter) Doc() (Doc, error) {
	return nil, ErrDocNotFound
}
func (e *emptyIter) Distance() float32 { return 0 }

func (e *emptyIter) Score() float64 { return 0 }
func (e *emptyIter) Err() error     { return nil }
func (e *emptyIter) Close() error {
	if e.closed {
		return ErrIterClosed
	}
	e.closed = true
	return nil
}
