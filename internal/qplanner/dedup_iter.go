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

	// Bounds are the SCAN bounds — padded for full keys exactly as IndexIter
	// walks them — and an element is tested as the least key it can have,
	// enc(elem)‖0x01 (every real suffix starts with a type tag or docId byte
	// >= 0x01). That makes membership exact under the escape continuation
	// (enc(v) is a prefix of enc(v‖"\x00…")) in both byte spaces, and, more
	// importantly, a SUBSET of what the scan yields: an element elected
	// canonical here is always an entry the scan emits. Testing the bare
	// encoding against unpadded bounds admitted continuation elements the
	// padded scan skips, so the doc was elected on an entry never seen and
	// dropped — or, with the continuation on the other side, emitted twice.

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

		if it.isCanonical(v, fieldVal) {
			return key, docId, false, nil
		}
		// Not the canonical hit — skip.
	}
}

// isCanonical elects the array's canonical in-bounds element and reports
// whether fieldVal is it. Kept out of Next so the scalar pass-through — every
// row of a single-field index scan — stays a short loop.
func (it *CanonicalKeyDedupIter) isCanonical(v *anyenc.Value, fieldVal []byte) bool {
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
		if !noBounds {
			it.keyBuf = append(it.keyBuf, 0x01)
			in := it.Bounds.Contains(it.keyBuf)
			it.keyBuf = it.keyBuf[:len(it.keyBuf)-1]
			if !in {
				continue
			}
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
	// No array element hits the bounds — upstream shouldn't have emitted
	// this doc, but pass through conservatively.
	return len(it.best) == 0 || bytes.Equal(fieldVal, it.best)
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
	return delegateSkipOffset(it.Source, n)
}

func (it *CanonicalKeyDedupIter) Close() {
	if it.Source != nil {
		it.Source.Close()
	}
}

func (it *CanonicalKeyDedupIter) String() string {
	return fmt.Sprintf("%s -> Dedup(canonical)", it.Source)
}

// DocDedupIter collapses a compound multikey index's entry fan-out to one row
// per distinct document, in first-occurrence order. A doc whose indexed fields
// hold arrays emits one entry per element combination PLUS a whole-array entry
// per array field (index.go writeValues), all flagged multiKey; without an
// in-plan dedup those raw entries reach SortIter's TopK heap and LimitIter's
// offset/limit cutoffs, so one document consumes several slots and every verb
// disagrees with Count (whose LimitIter.CountDistinct dedups before the
// cutoff). This iterator restores "one row == one document" BELOW those
// stages.
//
// History: consumer-boundary dedup via the multiKey flag (planIterator.Next,
// ForEachDistinct) replaced an earlier pipeline stage here — see
// any-store-tests:docs/any-store/plans/2026-04-29-multikey-bit-and-dedup-pipeline.md — but the boundary
// sits ABOVE Sort/Limit, which is exactly where the slots are consumed. The
// consumer dedup remains as the contract backstop; it sees multiKey=false
// from this iterator and degenerates to a passthrough.
//
// Placement (three homes): directly above the entry source and below any
// doc-level stage —
//   - seek/scan chains: above IndexIter/IndexFilterIter, below FetchIter. It
//     must stay downstream of IndexFilterIter: cover filters test the ENTRY
//     key tuple, and a doc's entries differ in covered fields, so deduping
//     first could emit only an entry the cover filter rejects; the residual
//     FilterIter's verdict is per-DOCUMENT (identical across a doc's
//     entries), so deduping below FetchIter is sound and skips the
//     fetch+parse+filter of every duplicate entry.
//   - multi-bound CoverIter lookups (unique full-key $in): directly above
//     CoverIter — no FetchIter exists there, the FilterIter (if any) sits
//     above and is per-doc, and CoverIter tags compound entries multiKey
//     conservatively (its byte-0 probe cannot see mid-key array fields).
//
// Emits multiKey=false unconditionally: emitted rows are unique by docId.
// Scalar entries pass through with zero cost (DocDedup.Accept fast path, no
// map). Memory is O(distinct multikey docs pulled) — bounded by the limit for
// bounded queries, and otherwise the same map the consumer allocated before.
//
// Single-field indexes use CanonicalKeyDedupIter instead (O(1) memory,
// doc-driven canonical-element selection); the planner picks exactly one of
// the two per chain.
type DocDedupIter struct {
	Source Iterator
	dedup  DocDedup
}

func (it *DocDedupIter) Next() (key []byte, docId []byte, multiKey bool, err error) {
	for {
		key, docId, multiKey, err = it.Source.Next()
		if err != nil || docId == nil {
			return nil, nil, false, err
		}
		if !it.dedup.Accept(docId, multiKey) {
			continue
		}
		return key, docId, false, nil
	}
}

// skipOffset delegates a cursor-level offset skip to the source. Sound for
// the same reason as CanonicalKeyDedupIter.skipOffset: the source only ever
// fast-skips SCALAR-flagged entries, and a scalar entry is by construction
// its doc's ONLY entry in the index (index.go insertKeys), so every skipped
// row is one distinct doc that can never reappear — no dedup decision is
// bypassed and no seen-set recording is missed. The skip stops at the first
// multikey entry, from which normal Next() dedup resumes.
func (it *DocDedupIter) skipOffset(n int) (remaining int, err error) {
	return delegateSkipOffset(it.Source, n)
}

func (it *DocDedupIter) Close() {
	if it.Source != nil {
		it.Source.Close()
	}
}

func (it *DocDedupIter) String() string {
	return fmt.Sprintf("%s -> Dedup(docid)", it.Source)
}
