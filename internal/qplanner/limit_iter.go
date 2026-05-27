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
