package anystore

import (
	"errors"
	"io"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/qplanner"
	"github.com/anyproto/any-store/v2/syncpool"
)

// Iterator represents an iterator over query results.
type Iterator interface {
	// Next advances the iterator to the next document.
	Next() bool

	// Doc returns the current document.
	Doc() (Doc, error)

	// Err returns any error encountered during the lifetime of the iterator.
	Err() error

	// Close closes the iterator and releases any associated resources.
	Close() error
}

// planIterator wraps a qplanner.Plan to implement the public Iterator interface.
type planIterator struct {
	tx         ReadTx
	err        error
	plan       *qplanner.Plan
	buf        *syncpool.DocBuffer
	qb         *queryBuilder
	data       *qplanner.CursorSource
	dataCursor *btree.Cursor
	docId      []byte
	closed     bool
	dedup      qplanner.DocDedup // lazy-allocated when upstream emits multiKey=true
}

func (pi *planIterator) Next() bool {
	if pi.err != nil || pi.closed {
		return false
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
		// bypasses the seen-set entirely in that case.
		if !pi.dedup.Accept(docId, mk) {
			continue
		}
		if pi.plan.DocParsed == nil {
			// Only copy docId when we'll need it in Doc() fallback
			pi.docId = append(pi.docId[:0], docId...)
		}
		return true
	}
}

func (pi *planIterator) Doc() (Doc, error) {
	perf := pipelinePerfEnabled()
	if perf {
		pipePerf.docCalls.Add(1)
	}
	if pi.err != nil && !errors.Is(pi.err, io.EOF) {
		return nil, pi.err
	}
	var doc *anyenc.Value
	if pi.plan.DocParsed != nil {
		if perf {
			pipePerf.docParsedHits.Add(1)
		}
		doc = pi.plan.DocParsed
	} else {
		if perf {
			pipePerf.docFallbacks.Add(1)
		}
		if pi.dataCursor == nil {
			pi.dataCursor = pi.data.NewCursor()
		}
		var seekStart time.Time
		if perf {
			seekStart = time.Now()
		}
		if err := pi.dataCursor.SeekExact(pi.docId); err != nil {
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
	pi.closed = true
	if pi.dataCursor != nil {
		pi.dataCursor.Close()
	}
	if pi.plan != nil {
		pi.plan.Close()
	}
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
func (e *emptyIter) Err() error { return nil }
func (e *emptyIter) Close() error {
	if e.closed {
		return ErrIterClosed
	}
	e.closed = true
	return nil
}
