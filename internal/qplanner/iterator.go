package qplanner

import (
	"fmt"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/btree"
)

// Iterator is the public interface for all query plan iterators.
// Iterators form a chain: each iterator produces (key, docId) pairs
// that the next iterator in the chain consumes.
type Iterator interface {
	// Next advances to the next result. Returns false when exhausted or on error.
	Next() (key []byte, docId []byte, err error)

	// Close releases any resources held by this iterator (e.g. btree cursors).
	Close()

	// String returns a human-readable description of this iterator (for explain).
	fmt.Stringer
}

// CountableIterator is an optional interface for iterators that support
// efficient batch counting without extracting individual keys.
type CountableIterator interface {
	CountEntries() (int, error)
}

// CursorSource provides cursors and direct lookups for a btree namespace.
type CursorSource struct {
	Tx *btree.ReadTx
	Ns *btree.Namespace
}

// NewCursor creates a new btree cursor for this source.
func (cs *CursorSource) NewCursor() *btree.Cursor {
	return cs.Tx.NewCursor(cs.Ns)
}

// Get performs a direct key lookup.
func (cs *CursorSource) Get(key []byte) ([]byte, error) {
	return cs.Tx.Get(cs.Ns, key)
}

// AppendValue appends the value for key to buf and returns the extended buffer.
// Zero alloc when buf has sufficient capacity.
func (cs *CursorSource) AppendValue(key, buf []byte) ([]byte, error) {
	return cs.Tx.AppendValue(cs.Ns, key, buf)
}

// IndexInfo holds metadata about an index needed by the planner.
type IndexInfo struct {
	Name       string
	FieldNames []string
	FieldPaths [][]string
	Reverse    []bool
	Unique     bool
	Sparse     bool
	Ns         *btree.Namespace
}

// AppendIndexKey appends the value for field at position i to the tuple.
// For reverse fields the value is inverted.
func (ii *IndexInfo) AppendIndexKey(tuple anyenc.Tuple, v *anyenc.Value, fieldIdx int) anyenc.Tuple {
	fv := v.Get(ii.FieldPaths[fieldIdx]...)
	if fieldIdx < len(ii.Reverse) && ii.Reverse[fieldIdx] {
		return tuple.AppendInverted(fv)
	}
	return tuple.Append(fv)
}
