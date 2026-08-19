package qplanner

import (
	"fmt"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
)

// Iterator is the public interface for all query plan iterators.
// Iterators form a chain: each iterator produces (key, docId, multiKey)
// triples that the next iterator in the chain consumes.
type Iterator interface {
	// Next advances to the next result.
	//
	// multiKey signals that another entry with the same docId may appear
	// later in this iteration (typically because the source index is
	// multi-key — an array-valued field — and a single document
	// contributes more than one index entry). Consumers that
	// count/yield by docId must dedup multiKey=true entries (see
	// DocDedup helper). multiKey=false is a hard guarantee of
	// uniqueness across the entire remaining stream — consumers can
	// skip dedup entirely.
	//
	// At end of iteration: returns (nil, nil, false, nil).
	Next() (key []byte, docId []byte, multiKey bool, err error)

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

// offsetSkipper is an optional interface for iterators that can advance
// past the first n LOGICAL result rows without fetching/parsing the
// skipped documents (a cursor-level skip at the index leaf).
//
// It is used to stream an OFFSET on index-ordered scans instead of the
// fetch-then-discard behaviour of LimitIter: skipped rows never reach
// FetchIter, so only the limit result docs are point-looked-up + parsed.
// This mirrors SQLite folding OFFSET into the limit counter so skipped
// rows never build output records (see sqlitec/src/select.c codeOffset /
// iOffset).
//
// Contract:
//
//   - skipOffset(n) attempts to skip up to n logical result rows from the
//     CURRENT position. It returns the number of rows it could NOT skip
//     (remaining); the caller must skip those itself via the normal
//     Next() path (e.g. LimitIter's per-row loop). remaining == 0 means
//     the full offset was absorbed.
//
//   - After skipOffset returns, subsequent Next() calls MUST resume the
//     stream exactly where the skip stopped: the next Next() yields the
//     first row that was NOT skipped. This makes the handoff to the
//     per-row remainder skip seamless.
//
//   - CORRECTNESS: an implementation must only count a skipped index
//     entry as one logical row when that entry is the doc's ONLY entry in
//     this scan (multiKey==false). If it encounters a multi-key entry (a
//     doc that contributes >1 index entries — array/legacy), it MUST stop
//     skipping and return the unskipped remainder, leaving the cursor on
//     that multi-key entry so the consumer-side dedup + LimitIter handle
//     the rest correctly. This keeps the optimization scoped to the case
//     where one index entry == one logical row.
//
// Only iterators that preserve a 1:1 row mapping with their source may
// delegate skipOffset downward. FilterIter / IndexFilterIter / SortIter
// reduce or reorder rows and therefore intentionally do NOT implement
// this interface, which breaks the delegation chain and forces the safe
// fetch-then-skip path whenever a filter or sort sits below the limit.
type offsetSkipper interface {
	skipOffset(n int) (remaining int, err error)
}

// delegateSkipOffset forwards a skipOffset call to src when it implements
// offsetSkipper; otherwise the full offset remains for the caller's per-row
// path. Shared body of the 1:1 pass-through iterators' skipOffset methods.
func delegateSkipOffset(src Iterator, n int) (remaining int, err error) {
	if s, ok := src.(offsetSkipper); ok {
		return s.skipOffset(n)
	}
	return n, nil
}

// Index entry value encoding (bitmask, 1 byte per entry):
//
//	bit 0 — multi-key: the document that produced this entry has more
//	        than one entry in this index (e.g. an array-valued field
//	        contributed >1 keys for this doc).
//	bits 1-7 — reserved; must be zero in this version.
//
// Read interpretation by IndexIter:
//
//	len(value) == 0           → legacy entry written before this byte
//	                            existed; treat as multi-key (true) for
//	                            safety, since pre-flag inserts may have
//	                            been multi-key without recording it.
//	len(value) >= 1 && bit 0  → confirmed multi-key.
//	len(value) >= 1 && !bit 0 → confirmed scalar (this is this doc's
//	                            only entry in this index).
//
// Sentinels are package-level []byte slices so callers can pass them to
// tx.Put without per-call allocation.
const IndexEntryFlagMultiKey byte = 0x01

var (
	// IndexValueScalar is the entry value written when a document
	// produces exactly one index entry (scalar field, or a single-element
	// array). Bit 0 cleared.
	IndexValueScalar = []byte{0x00}

	// IndexValueMultiKey is the entry value written when a document
	// produces two or more index entries (multi-element array field, or
	// compound index whose array dimension expanded to >1 keys). Bit 0
	// set.
	IndexValueMultiKey = []byte{IndexEntryFlagMultiKey}
)

// EntryValueIsMultiKey decodes the per-entry value byte. Pure, no allocs.
// Defined alongside the sentinels so encoders and decoders agree on the
// legacy-empty path.
func EntryValueIsMultiKey(val []byte) bool {
	if len(val) == 0 {
		return true // legacy entries: assume multi-key for safety
	}
	return val[0]&IndexEntryFlagMultiKey != 0
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

// AppendSeekKey finds the first key >= prefix and appends it to buf.
// Single-shot traversal without cursor allocation.
func (cs *CursorSource) AppendSeekKey(prefix, buf []byte) ([]byte, error) {
	return cs.Tx.AppendSeekKey(cs.Ns, prefix, buf)
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
