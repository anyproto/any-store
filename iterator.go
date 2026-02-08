package anystore

import (
	"bytes"
	"errors"
	"io"
	"sort"

	"github.com/anyproto/any-store/internal/btree"
	"github.com/anyproto/any-store/query"
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

type iterator struct {
	tx     ReadTx
	buf    *syncpool.DocBuffer
	qb     *queryBuilder
	cursor *btree.Cursor
	filter query.Filter
	sorter query.Sort
	err    error
	closed bool
	limit  int
	offset int

	// for non-sorted iteration
	started bool
	skipped int
	count   int

	// for sorted iteration
	sortedDocs [][]byte
	sortedIdx  int
	sortedInit bool
}

func (i *iterator) Next() bool {
	if i.err != nil || i.closed {
		return false
	}

	if i.sorter != nil {
		return i.nextSorted()
	}
	return i.nextUnsorted()
}

func (i *iterator) nextUnsorted() bool {
	for {
		if !i.started {
			i.started = true
			if err := i.cursor.First(); err != nil {
				i.err = err
				return false
			}
		} else {
			if err := i.cursor.Next(); err != nil {
				i.err = err
				return false
			}
		}

		if !i.cursor.Valid() {
			return false
		}

		if i.filter != nil {
			val, err := i.cursor.Value()
			if err != nil {
				i.err = err
				return false
			}
			doc, err := i.buf.Parser.Parse(val)
			if err != nil {
				i.err = err
				return false
			}
			if !i.filter.Ok(doc, i.buf) {
				continue
			}
		}

		if i.offset > 0 && i.skipped < i.offset {
			i.skipped++
			continue
		}

		if i.limit > 0 && i.count >= i.limit {
			return false
		}
		i.count++
		return true
	}
}

func (i *iterator) nextSorted() bool {
	if !i.sortedInit {
		i.sortedInit = true
		if err := i.collectAndSort(); err != nil {
			i.err = err
			return false
		}
	}

	if i.sortedIdx >= len(i.sortedDocs) {
		return false
	}

	// Set buf so Doc() can read from it
	i.buf.DocBuf = append(i.buf.DocBuf[:0], i.sortedDocs[i.sortedIdx]...)
	i.sortedIdx++
	return true
}

func (i *iterator) collectAndSort() error {
	// Collect all matching documents
	type docEntry struct {
		data    []byte
		sortKey []byte
	}
	var entries []docEntry

	if err := i.cursor.First(); err != nil {
		return err
	}
	for i.cursor.Valid() {
		val, err := i.cursor.Value()
		if err != nil {
			return err
		}
		doc, err := i.buf.Parser.Parse(val)
		if err != nil {
			return err
		}
		if i.filter != nil && !i.filter.Ok(doc, i.buf) {
			if err := i.cursor.Next(); err != nil {
				return err
			}
			continue
		}
		sortKey := i.sorter.AppendKey(nil, doc)
		entries = append(entries, docEntry{
			data:    append([]byte(nil), val...),
			sortKey: sortKey,
		})
		if err := i.cursor.Next(); err != nil {
			return err
		}
	}

	// Sort
	sort.SliceStable(entries, func(a, b int) bool {
		return bytes.Compare(entries[a].sortKey, entries[b].sortKey) < 0
	})

	// Apply offset and limit
	start := i.offset
	if start > len(entries) {
		start = len(entries)
	}
	end := len(entries)
	if i.limit > 0 && start+i.limit < end {
		end = start + i.limit
	}

	i.sortedDocs = make([][]byte, 0, end-start)
	for j := start; j < end; j++ {
		i.sortedDocs = append(i.sortedDocs, entries[j].data)
	}
	return nil
}

func (i *iterator) Doc() (Doc, error) {
	if i.err != nil && !errors.Is(i.err, io.EOF) {
		return nil, i.err
	}

	if i.sorter != nil {
		// For sorted iteration, data is already in buf.DocBuf
		doc, err := i.buf.Parser.Parse(i.buf.DocBuf)
		if err != nil {
			return nil, err
		}
		return newItem(doc)
	}

	val, err := i.cursor.Value()
	if err != nil {
		return nil, err
	}
	i.buf.DocBuf = append(i.buf.DocBuf[:0], val...)
	doc, err := i.buf.Parser.Parse(i.buf.DocBuf)
	if err != nil {
		return nil, err
	}
	return newItem(doc)
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
