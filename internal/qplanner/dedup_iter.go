package qplanner

import (
	"bytes"
	"fmt"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/query"
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
//     compound cases, SeenSetDedupIter is the drop-in alternative.
type CanonicalKeyDedupIter struct {
	Source    Iterator
	Plan      *Plan
	Bounds    query.Bounds
	FieldPath []string // path of the multi-key field (e.g. ["tags"] or ["meta", "labels"])
	Reverse   bool     // match IndexIter.Reverse

	keyBuf []byte // reusable encode buffer for min/max comparison
	best   []byte // reusable buffer for the canonical element
}

func (it *CanonicalKeyDedupIter) Next() (key []byte, docId []byte, err error) {
	for {
		key, docId, err = it.Source.Next()
		if err != nil || docId == nil {
			return nil, nil, err
		}

		// Strip docId suffix to get the field-value portion of the key.
		if len(key) < len(docId) {
			return key, docId, nil // defensive; shouldn't happen
		}
		fieldVal := key[:len(key)-len(docId)]

		if it.Plan == nil || it.Plan.DocParsed == nil {
			return key, docId, nil // no doc available; can't dedup — pass through
		}
		v := it.Plan.DocParsed.Get(it.FieldPath...)
		if v == nil || v.Type() != anyenc.TypeArray {
			// Scalar field — only one entry per doc, no dedup needed.
			return key, docId, nil
		}

		items, _ := v.Array()
		it.best = it.best[:0]
		noBounds := len(it.Bounds) == 0
		for _, item := range items {
			it.keyBuf = item.MarshalTo(it.keyBuf[:0])
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
			return key, docId, nil
		}
		if bytes.Equal(fieldVal, it.best) {
			return key, docId, nil
		}
		// Not the canonical hit — skip.
	}
}

func (it *CanonicalKeyDedupIter) Close() {
	if it.Source != nil {
		it.Source.Close()
	}
}

func (it *CanonicalKeyDedupIter) String() string {
	return fmt.Sprintf("%s -> Dedup(canonical)", it.Source)
}
