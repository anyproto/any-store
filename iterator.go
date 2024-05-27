package anystore

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"

	"github.com/anyproto/any-store/internal/syncpool"
)

type Iterator interface {
	Next() bool
	Doc() (Doc, error)
	Err() error
	Close() error
}

type iterator struct {
	rows   driver.Rows
	tx     ReadTx
	dest   []driver.Value
	buf    *syncpool.DocBuffer
	db     *db
	err    error
	closed bool
}

func (i *iterator) Next() bool {
	if i.err != nil {
		return false
	}
	if i.err = i.rows.Next(i.dest); i.err != nil {
		return false
	}
	return true
}

func (i *iterator) Doc() (Doc, error) {
	if i.err != nil && !errors.Is(i.err, io.EOF) {
		return nil, i.err
	}
	if i.dest[0] == nil {
		return nil, fmt.Errorf("should be called after Next")
	}
	val, err := i.buf.Parser.ParseBytes(i.dest[0].([]byte))
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
	if i.rows != nil {
		err = errors.Join(err, i.rows.Close())
	}
	if i.tx != nil {
		err = errors.Join(i.tx.Commit())
	}
	if i.buf != nil && i.db != nil {
		i.db.syncPool.ReleaseDocBuf(i.buf)
	}
	return
}
