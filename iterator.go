package anystore

import (
	"errors"
	"io"

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
	plan   *qplanner.Plan
	tx     ReadTx
	buf    *syncpool.DocBuffer
	qb     *queryBuilder
	data   *qplanner.CursorSource
	err    error
	closed bool
	docId  []byte
}

func (pi *planIterator) Next() bool {
	if pi.err != nil || pi.closed {
		return false
	}
	pi.plan.DocValue = pi.plan.DocValue[:0]
	_, docId, err := pi.plan.Root.Next()
	if err != nil {
		pi.err = err
		return false
	}
	if docId == nil {
		return false
	}
	pi.docId = append(pi.docId[:0], docId...)
	return true
}

func (pi *planIterator) Doc() (Doc, error) {
	if pi.err != nil && !errors.Is(pi.err, io.EOF) {
		return nil, pi.err
	}
	// Use cached doc value from FilterIter if available (avoids double-fetch)
	if len(pi.plan.DocValue) > 0 {
		pi.buf.DocBuf = append(pi.buf.DocBuf[:0], pi.plan.DocValue...)
	} else {
		val, err := pi.data.Get(pi.docId)
		if err != nil {
			return nil, err
		}
		pi.buf.DocBuf = append(pi.buf.DocBuf[:0], val...)
	}
	doc, err := pi.buf.Parser.Parse(pi.buf.DocBuf)
	if err != nil {
		return nil, err
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
