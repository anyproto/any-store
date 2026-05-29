package qplanner

import (
	"bytes"
	"sort"
	"sync"
)

// sortDedupArena is reusable scratch for countEntriesViaSortDedup: a byte
// arena holding every matched docId back-to-back, plus spans indexing into it.
// Both grow by slice-doubling, so a warm (pooled) arena performs zero backing
// allocations. reset() keeps capacity; slices into buf must not outlive a call.
type sortDedupArena struct {
	buf   []byte
	spans []sortDedupSpan
}

type sortDedupSpan struct{ off, length int }

func (a *sortDedupArena) reset() {
	a.buf = a.buf[:0]
	a.spans = a.spans[:0]
}

var sortDedupPool = sync.Pool{New: func() any { return new(sortDedupArena) }}

// countEntriesViaSortDedup counts DISTINCT docIds across all of it.Bounds.
// It copies each in-bound entry's docId into a pooled byte arena (the docId
// aliases the cursor page, so the copy is mandatory), records an (off,len)
// span, sorts the spans by docId bytes, and counts adjacent-distinct runs.
//
// Every entry is deduped uniformly — no scalar/multi-key special-casing — so a
// doc whose array values straddle several $in bounds is counted once. This is
// the safe multi-bound count path for both single-field multi-key indexes
// (CountEntries Branch 4) and compound / non-PointLookup indexes (Branch 2),
// where the canonical-key probe does not apply.
//
// Allocation profile: O(log n) amortized (arena and spans double a logarithmic
// number of times) and zero backing allocations once the pooled arena is warm.
// Live footprint is O(n) total docId bytes held during the sort.
func (it *IndexIter) countEntriesViaSortDedup() (int, error) {
	if it.cursor == nil {
		it.cursor = it.Source.NewCursor()
	}
	numFields := len(it.IdxInfo.FieldNames)

	arena := sortDedupPool.Get().(*sortDedupArena)
	arena.reset()
	defer sortDedupPool.Put(arena)

	for _, b := range it.Bounds {
		if err := it.seekBoundStart(b); err != nil {
			return 0, err
		}
		for it.cursor.Valid() {
			k, kerr := it.cursor.Key()
			if kerr != nil {
				return 0, kerr
			}
			if len(b.End) > 0 {
				cmp := bytes.Compare(k, b.End)
				if cmp > 0 || (cmp == 0 && !b.EndInclude) {
					break
				}
			}
			docID := extractDocId(k, numFields)
			off := len(arena.buf)
			arena.buf = append(arena.buf, docID...) // copy out of the cursor page
			arena.spans = append(arena.spans, sortDedupSpan{off: off, length: len(docID)})
			if err := it.cursor.Next(); err != nil {
				return 0, err
			}
		}
	}

	if len(arena.spans) == 0 {
		return 0, nil
	}

	get := func(s sortDedupSpan) []byte { return arena.buf[s.off : s.off+s.length] }
	sort.Slice(arena.spans, func(i, j int) bool {
		return bytes.Compare(get(arena.spans[i]), get(arena.spans[j])) < 0
	})

	distinct := 1
	prev := get(arena.spans[0])
	for _, s := range arena.spans[1:] {
		cur := get(s)
		if !bytes.Equal(cur, prev) {
			distinct++
			prev = cur
		}
	}
	return distinct, nil
}
