package qplanner

import "fmt"

// LimitIter applies offset and limit to the upstream iterator.
type LimitIter struct {
	Source Iterator
	Offset int
	Limit  int

	skipped int
	count   int
}

func (it *LimitIter) Next() (key []byte, docId []byte, multiKey bool, err error) {
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
