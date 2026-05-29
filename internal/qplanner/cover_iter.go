package qplanner

import (
	"bytes"
	"fmt"

	"github.com/anyproto/any-store/v2/query"
)

// CoverIter handles fixed-point lookups where Start == End in bounds.
// Uses single-shot btree SeekKey + prefix comparison to find matching entries.
type CoverIter struct {
	Source  *CursorSource
	IdxInfo *IndexInfo
	Bounds  query.Bounds

	idx    int
	keyBuf []byte // reusable buffer for SeekKey results

	multiKeyProbed bool // lazy: probe runs on the first Next when len(Bounds) > 1
	hasMultiKey    bool
}

func (it *CoverIter) Next() (key []byte, docId []byte, multiKey bool, err error) {
	// A unique index CAN hold array entries (each element unique across docs),
	// so a multi-bound $in can match the SAME doc through several of its array
	// values. Probe once for any 0x06 (array) entry; if present, tag yielded
	// entries multiKey so the consumer's DocDedup collapses the cross-bound
	// repeats (I-06). DocDedup keys on docId, so a conservative index-level
	// true never merges distinct docs. Single-bound lookups can't straddle
	// bounds, so they skip the probe and keep the zero-cost multiKey=false.
	if len(it.Bounds) > 1 && !it.multiKeyProbed {
		it.hasMultiKey, err = indexProbeAnyMultiKey(it.Source)
		if err != nil {
			return nil, nil, false, err
		}
		it.multiKeyProbed = true
	}
	for it.idx < len(it.Bounds) {
		b := it.Bounds[it.idx]
		it.idx++

		if len(b.Start) == 0 {
			continue
		}

		var kerr error
		it.keyBuf, kerr = it.Source.AppendSeekKey(b.Start, it.keyBuf[:0])
		if kerr != nil {
			continue // key not found
		}
		if !bytes.HasPrefix(it.keyBuf, b.Start) {
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
