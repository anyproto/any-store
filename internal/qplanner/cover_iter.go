package qplanner

import (
	"fmt"

	"github.com/anyproto/any-store/v2/query"
)

// CoverIter handles fixed-point lookups where Start == End in bounds.
// Uses single-shot btree SeekKey + prefix comparison to find matching entries.
type CoverIter struct {
	Source  *CursorSource
	IdxInfo *IndexInfo
	Bounds  query.Bounds
	// ScalarProven mirrors CBOIndex.ScalarProven (see IndexIter).
	ScalarProven bool

	idx     int
	keyBuf  []byte // reusable buffer for SeekKey results
	seekBuf []byte // reusable buffer for the padded seek key (inverted tail)

	multiKeyProbed bool // lazy: probe runs on the first Next when len(Bounds) > 1
	hasMultiKey    bool
}

func (it *CoverIter) Next() (key []byte, docId []byte, multiKey bool, err error) {
	// A unique index CAN hold array entries (each element unique across docs),
	// so a multi-bound $in can match the SAME doc through several of its array
	// values. Tag yielded entries multiKey so the DocDedupIter wrapped around
	// multi-bound lookups (and any DocDedup consumer) collapses the
	// cross-bound repeats. Dedup keys on docId, so a conservative
	// index-level true never merges distinct docs. Single-bound lookups can't
	// straddle bounds, so they keep the zero-cost multiKey=false.
	if len(it.Bounds) > 1 && !it.multiKeyProbed {
		if len(it.IdxInfo.FieldNames) > 1 {
			// The probe detects the whole-array tag at byte 0 of the key —
			// field 0 only. A compound index's array field can sit at any
			// position (mid-key tags are unreachable by a prefix seek), so
			// probing would FALSE-NEGATIVE on e.g. (a, arrayField) and let
			// cross-bound duplicates through the dedup fast path. Assume
			// multikey instead: the only cost is a docId map in the dedup
			// for the (already rare) multi-bound unique compound lookup.
			it.hasMultiKey = true
		} else {
			// A fan-out through an array of objects writes scalar entries
			// with no whole-array key, so only the sticky scalar-proven flag
			// can rule multikey out.
			it.hasMultiKey = !it.ScalarProven
		}
		it.multiKeyProbed = true
	}
	for it.idx < len(it.Bounds) {
		b := it.Bounds[it.idx]
		it.idx++

		if len(b.Start) == 0 {
			continue
		}

		// Escape-aware lookup: a bare HasPrefix would also accept a key whose
		// last field value extends b.Start's value with an escaped NUL ("a" vs
		// "a\x00b"), returning a doc with the WRONG field value. For an
		// inverted tail field those continuation keys (prefix+0x00...) also
		// sort BEFORE the real match (prefix+tag, tag >= 0x01), so the seek
		// starts at prefix+0x01 to hop over them; the ascending continuations
		// (prefix+0xFF...) sort after every real match and need no hop.
		lastInverted := len(it.IdxInfo.Reverse) >= len(it.IdxInfo.FieldNames) &&
			it.IdxInfo.Reverse[len(it.IdxInfo.FieldNames)-1]
		seekStart := b.Start
		if lastInverted {
			it.seekBuf = append(append(it.seekBuf[:0], b.Start...), 0x01)
			seekStart = it.seekBuf
		}
		var kerr error
		it.keyBuf, kerr = it.Source.AppendSeekKey(seekStart, it.keyBuf[:0])
		if kerr != nil {
			continue // key not found
		}
		if !HasExactFieldPrefix(it.keyBuf, b.Start, lastInverted) {
			continue
		}
		docID := extractDocId(it.keyBuf, len(it.IdxInfo.FieldNames))
		return it.keyBuf, docID, it.hasMultiKey, nil
	}
	return nil, nil, false, nil
}

// Close releases resources (CoverIter uses single-shot lookups, no cursor to close).
func (it *CoverIter) Close() {}

func (it *CoverIter) String() string {
	return fmt.Sprintf("CoverLookup(%s)[%d bounds]", it.IdxInfo.Name, len(it.Bounds))
}
