package anystore

import (
	"errors"
	"io"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/btree"
	"github.com/anyproto/any-store/internal/qplanner"
	"github.com/anyproto/any-store/syncpool"
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
}

func (pi *planIterator) Next() bool {
	if pi.err != nil || pi.closed {
		return false
	}
	pi.plan.DocParsed = nil
	_, docId, err := pi.plan.Root.Next()
	if err != nil {
		pi.err = err
		return false
	}
	if docId == nil {
		return false
	}
	if pi.plan.DocParsed == nil {
		// Only copy docId when we'll need it in Doc() fallback
		pi.docId = append(pi.docId[:0], docId...)
	}
	return true
}

func (pi *planIterator) Doc() (Doc, error) {
	if pi.err != nil && !errors.Is(pi.err, io.EOF) {
		return nil, pi.err
	}
	var doc *anyenc.Value
	if pi.plan.DocParsed != nil {
		doc = pi.plan.DocParsed
	} else {
		if pi.dataCursor == nil {
			pi.dataCursor = pi.data.NewCursor()
		}
		if err := pi.dataCursor.SeekExact(pi.docId); err != nil {
			return nil, err
		}
		val, err := pi.dataCursor.Value()
		if err != nil {
			return nil, err
		}
		pi.buf.DocBuf = append(pi.buf.DocBuf[:0], val...)
		var perr error
		doc, perr = pi.buf.Parser.Parse(pi.buf.DocBuf)
		if perr != nil {
			return nil, perr
		}
	}
	return newItem(doc)
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
