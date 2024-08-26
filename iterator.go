package anystore

import (
	"errors"
	"io"

	"zombiezen.com/go/sqlite"

	"github.com/anyproto/any-store/internal/syncpool"
)

// Iterator represents an iterator over query results.
//
// Example of usage:
//
//	for iter.Next() {
//	    doc, err := iter.Doc()
//	    if err != nil {
//	        log.Fatalf("error retrieving document: %v", err)
//	    }
//	    fmt.Println("Document:", doc.Value().String())
//	}
//	if err := iter.Close(); err != nil {
//	    log.Fatalf("iteration error: %v", err)
//	}
type Iterator interface {
	// Next advances the iterator to the next document.
	// Returns false if there are no more documents.
	Next() bool

	// Doc returns the current document.
	// Returns an error if there is an issue retrieving the document.
	Doc() (Doc, error)

	// Err returns any error encountered during the lifetime of the iterator,
	Err() error

	// Close closes the iterator and releases any associated resources.
	// Returns an error if there is an issue closing the iterator or any other error encountered during its lifetime.
	Close() error
}

type iterator struct {
	tx     ReadTx
	buf    *syncpool.DocBuffer
	qb     *queryBuilder
	err    error
	closed bool
	stmt   *sqlite.Stmt
}

func (i *iterator) Next() bool {
	if i.err != nil {
		return false
	}
	hasRow, stepErr := i.stmt.Step()
	if stepErr != nil {
		i.err = stepErr
		return false
	}
	return hasRow
}

func (i *iterator) Doc() (Doc, error) {
	if i.err != nil && !errors.Is(i.err, io.EOF) {
		return nil, i.err
	}
	i.buf.DocBuf = readBytes(i.stmt, i.buf.DocBuf)
	val, err := i.buf.Parser.ParseBytes(i.buf.DocBuf)
	if err != nil {
		return nil, err
	}
	return newItem(val, nil, false)
}

func (i *iterator) Err() error {
	if i.err != nil && errors.Is(i.err, io.EOF) {
		return nil
	}
	return i.err
}

func (i *iterator) Close() (err error) {
	if i.closed {
		return ErrIterClosed
	}
	i.closed = true
	if i.stmt != nil {
		err = errors.Join(err, i.stmt.Finalize())
	}
	if i.tx != nil {
		err = errors.Join(err, i.tx.Commit())
	}
	if i.buf != nil && i.qb != nil {
		i.qb.coll.db.syncPool.ReleaseDocBuf(i.buf)
	}
	if i.qb != nil {
		i.qb.Close()
	}
	return
}
