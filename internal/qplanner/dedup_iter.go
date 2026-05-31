package qplanner

import (
	"bytes"
	"fmt"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
)

// CanonicalKeyDedupIter removes duplicate document emissions that result
// from scanning a single-field multi-key (array-valued) index against
// multiple bounds or a range bound. For each document, only the hit
// whose key matches the canonical representative of the doc's in-bounds
// values is emitted; all other hits for the same doc are skipped.
//
// "Canonical" = minimum encoded value in forward scans, maximum in
// reverse scans. This is the first hit the cursor will encounter for
// the doc, so we emit on the first hit and skip the rest.
//
// Memory: O(1). No per-doc state survives between Next() calls.
//
// Prerequisites:
//   - Source must be a FetchIter (or anything that populates
//     Plan.DocParsed) so the parsed document is available without an
//     extra fetch.
//   - Source must emit keys in the form (indexField, docId), the
//     standard any-store index layout for a single-field index.
//
// Limitations:
//   - Correct only for single-field indexes. Compound indexes must not
//     be wrapped with this iterator — the caller (planner) is
//     responsible for gating by len(IndexInfo.FieldPaths) == 1. For
//     compound cases the planner emits no dedup wrap and consumers
//     dedup at the boundary via DocDedup driven by the multiKey return
//     value of Iterator.Next.
type CanonicalKeyDedupIter struct {
	Source    Iterator
	Plan      *Plan
	Bounds    query.Bounds
	FieldPath []string // path of the multi-key field (e.g. ["tags"] or ["meta", "labels"])
	Reverse   bool     // match IndexIter.Reverse (physical scan direction)

	// FieldReverse is the index field's declared reverse flag. When true the
	// index stores the field bitwise-inverted, so fieldVal (sliced from the
	// stored key) and it.Bounds are both in the inverted (stored) byte space.
	// Array elements are then re-encoded inverted before comparison so the
	// canonical-element selection happens in the same space the cursor walks.
	FieldReverse bool

	keyBuf []byte // reusable encode buffer for min/max comparison
	best   []byte // reusable buffer for the canonical element
}

func (it *CanonicalKeyDedupIter) Next() (key []byte, docId []byte, multiKey bool, err error) {
	for {
		// Read upstream's multiKey but don't propagate it: this iterator's
		// contract is that emitted entries are unique-by-docId, so we
		// always return multiKey=false. Upstream may flag entries
		// multiKey=true (e.g. IndexIter on a multi-key index); we filter
		// the duplicates so the consumer sees a clean unique stream.
		key, docId, _, err = it.Source.Next()
		if err != nil || docId == nil {
			return nil, nil, false, err
		}

		// Strip docId suffix to get the field-value portion of the key.
		if len(key) < len(docId) {
			return key, docId, false, nil // defensive; shouldn't happen
		}
		fieldVal := key[:len(key)-len(docId)]

		if it.Plan == nil || it.Plan.DocParsed == nil {
			return key, docId, false, nil // no doc available; can't dedup — pass through
		}
		v := it.Plan.DocParsed.Get(it.FieldPath...)
		if v == nil || v.Type() != anyenc.TypeArray {
			// Scalar field — only one entry per doc, no dedup needed.
			return key, docId, false, nil
		}

		items, _ := v.Array()
		it.best = it.best[:0]
		noBounds := len(it.Bounds) == 0
		for _, item := range items {
			// Encode in the same byte space as fieldVal and Bounds: inverted
			// (stored) for a reverse field, plain ascending otherwise.
			if it.FieldReverse {
				it.keyBuf = anyenc.Tuple(it.keyBuf[:0]).AppendInverted(item)
			} else {
				it.keyBuf = item.MarshalTo(it.keyBuf[:0])
			}
			if !noBounds && !it.Bounds.Contains(it.keyBuf) {
				continue
			}
			if len(it.best) == 0 {
				it.best = append(it.best[:0], it.keyBuf...)
				continue
			}
			cmp := bytes.Compare(it.keyBuf, it.best)
			if (!it.Reverse && cmp < 0) || (it.Reverse && cmp > 0) {
				it.best = append(it.best[:0], it.keyBuf...)
			}
		}
		if len(it.best) == 0 {
			// No array element hits the bounds — upstream shouldn't have
			// emitted this doc, but pass through conservatively.
			return key, docId, false, nil
		}
		if bytes.Equal(fieldVal, it.best) {
			return key, docId, false, nil
		}
		// Not the canonical hit — skip.
	}
}

// skipOffset delegates a cursor-level offset skip to the source. This is
// correct precisely because the source (IndexIter, via FetchIter) only
// fast-skips SCALAR index entries and stops at the first multi-key one:
//
//   - For a scalar entry, this iterator is pass-through (Next returns it
//     immediately — see the non-array branch in Next). So every entry the
//     source skipped would have flowed through this iterator 1:1; skipping
//     it below us removes exactly the same logical rows we would have
//     emitted. No dedup decision is bypassed.
//
//   - The instant the source meets a multi-key (array) entry it stops
//     skipping and returns the remainder, leaving the cursor on that entry.
//     From there normal Next() runs, and THIS iterator performs its
//     canonical-key dedup over the array hits as usual; the remaining
//     offset is then applied by LimitIter on the deduped (logical-row)
//     stream. So multi-key dedup correctness is fully preserved.
func (it *CanonicalKeyDedupIter) skipOffset(n int) (remaining int, err error) {
	if src, ok := it.Source.(offsetSkipper); ok {
		return src.skipOffset(n)
	}
	return n, nil
}

func (it *CanonicalKeyDedupIter) Close() {
	if it.Source != nil {
		it.Source.Close()
	}
}

func (it *CanonicalKeyDedupIter) String() string {
	return fmt.Sprintf("%s -> Dedup(canonical)", it.Source)
}

// SeenSetDedupIter was removed in favour of consumer-side DocDedup
// threaded through the iterator chain via the multiKey return value of
// Iterator.Next. Compound multi-key indexes are now deduped at the
// boundary (planIterator.Next, query.go count/Update/Delete loops)
// without an extra pipeline stage. See
// docs/plans/2026-04-29-multikey-bit-and-dedup-pipeline.md.
