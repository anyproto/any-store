package qplanner

import "fmt"

// LimitIter applies offset and limit to the upstream iterator.
type LimitIter struct {
	Source Iterator
	Offset int
	Limit  int

	skipped       int
	count         int
	triedFastSkip bool
}

func (it *LimitIter) Next() (key []byte, docId []byte, multiKey bool, err error) {
	// Push the offset down to the source as a cursor-level skip when the
	// source supports it (index-ordered scans with a 1:1 row mapping below
	// us). This skips offset rows WITHOUT fetching/parsing them. Any rows
	// the source could not skip (it stopped at a multi-key entry, or the
	// chain is filtered/sorted and doesn't implement the interface) fall
	// back to the per-row skip loop below, preserving correctness.
	if !it.triedFastSkip {
		it.triedFastSkip = true
		if it.Offset > 0 {
			if src, ok := it.Source.(offsetSkipper); ok {
				remaining, serr := src.skipOffset(it.Offset)
				if serr != nil {
					return nil, nil, false, serr
				}
				// Account for the rows already skipped at the cursor level;
				// the loop below skips only the remainder.
				it.skipped = it.Offset - remaining
			}
		}
	}

	for {
		key, docId, multiKey, err = it.Source.Next()
		if err != nil || docId == nil {
			return nil, nil, false, err
		}

		if it.Offset > 0 && it.skipped < it.Offset {
			it.skipped++
			continue
		}

		if it.Limit > 0 && it.count >= it.Limit {
			return nil, nil, false, nil
		}
		it.count++
		return key, docId, multiKey, nil
	}
}

// CountDistinct counts the DISTINCT documents this LimitIter chain would
// yield, i.e. min(Limit, max(0, distinct(source) - Offset)). Count consumers
// must use this instead of pulling Next(): Next applies Offset/Limit to raw
// source ROWS, which over a multi-key index can be several entries per doc —
// the offset then skips fewer distinct docs than requested and the limit caps
// entry rows that later collapse in the consumer's dedup, so Count diverges
// from Iter (known-issues I-07). Here dedup runs BEFORE the cutoff:
// offset and limit are applied to distinct-doc counts, with early exit once
// Offset+Limit distinct docs are seen.
//
// The cursor-level fast skip stays sound for distinct-doc arithmetic: the
// offsetSkipper contract skips only entries recorded as scalar (the doc's
// single entry in that namespace), so each skipped row IS one distinct doc,
// and a skipped doc cannot reappear later marked multi-key.
func (it *LimitIter) CountDistinct() (int, error) {
	remainingOffset := it.Offset
	if it.Offset > 0 {
		if src, ok := it.Source.(offsetSkipper); ok {
			rem, err := src.skipOffset(it.Offset)
			if err != nil {
				return 0, err
			}
			remainingOffset = rem
		}
	}

	target := -1 // no early exit for offset-only cutoffs: the full tail counts
	if it.Limit > 0 {
		target = it.Limit + remainingOffset
	}
	var dedup DocDedup
	distinct := 0
	for {
		_, docId, multiKey, err := it.Source.Next()
		if err != nil {
			return 0, err
		}
		if docId == nil {
			break
		}
		if dedup.Accept(docId, multiKey) {
			distinct++
			if target >= 0 && distinct >= target {
				break
			}
		}
	}

	n := max(distinct-remainingOffset, 0)
	if it.Limit > 0 && n > it.Limit {
		n = it.Limit
	}
	return n, nil
}

// Close releases resources by closing the source iterator.
func (it *LimitIter) Close() {
	if it.Source != nil {
		it.Source.Close()
	}
}

func (it *LimitIter) String() string {
	if it.Offset > 0 && it.Limit > 0 {
		return fmt.Sprintf("%s -> Limit(offset=%d,limit=%d)", it.Source, it.Offset, it.Limit)
	}
	if it.Offset > 0 {
		return fmt.Sprintf("%s -> Limit(offset=%d)", it.Source, it.Offset)
	}
	return fmt.Sprintf("%s -> Limit(%d)", it.Source, it.Limit)
}
