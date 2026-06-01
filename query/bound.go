package query

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"
)

type Bound struct {
	Start, End   anyenc.Tuple
	prefix       anyenc.Tuple
	StartInclude bool
	EndInclude   bool
}

func (b Bound) String() string {
	stripTrailing0xff := func(k anyenc.Tuple) anyenc.Tuple {
		if len(k) > 0 && k[len(k)-1] == 0xff {
			return k[:len(k)-1]
		}
		return k
	}
	stripPrefixString := func(k anyenc.Tuple) string {
		k = stripTrailing0xff(k)
		if len(b.prefix) != 0 && len(k) > len(b.prefix) {
			return k[len(b.prefix):].String()
		}
		return k.String()
	}

	var as, bs string
	if len(b.Start) == 0 || bytes.Equal(b.prefix, b.Start) || bytes.Equal(append(b.prefix, 255), b.Start) {
		as = "[-inf"
	} else {
		if b.StartInclude {
			as = "['" + stripPrefixString(b.Start) + "'"
		} else {
			as = "('" + stripPrefixString(b.Start) + "'"
		}
	}
	if len(b.End) == 0 || bytes.Equal(b.prefix, b.End) || bytes.Equal(append(b.prefix, 255), b.End) {
		bs = "inf]"
	} else {
		if b.EndInclude {
			bs = "'" + stripPrefixString(b.End) + "']"
		} else {
			bs = "'" + stripPrefixString(b.End) + "')"
		}
	}
	return fmt.Sprintf("%s,%s", as, bs)
}

type Bounds []Bound

func (bs Bounds) String() string {
	if len(bs) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("Bounds{")
	for i, b := range bs {
		if i != 0 {
			sb.WriteString(",")
		}
		sb.WriteString(b.String())
	}
	sb.WriteString("}")
	return sb.String()
}
func (bs Bounds) Append(b Bound) Bounds {
	if len(bs) == 0 {
		return Bounds{b}
	}

	// Fast path for single existing bound (covers Eq, Gte, Lte and second call from Ne).
	if len(bs) == 1 {
		if isOverlap(bs[0], b) && isOverlap(b, bs[0]) {
			return Bounds{mergeBounds(bs[0], b)}
		}
		if bytes.Compare(b.Start, bs[0].Start) < 0 {
			return Bounds{b, bs[0]}
		}
		return Bounds{bs[0], b}
	}

	result := make(Bounds, 0, len(bs)+1)
	for _, existingBound := range bs {
		if isOverlap(existingBound, b) && isOverlap(b, existingBound) {
			b = mergeBounds(existingBound, b)
		} else {
			result = append(result, existingBound)
		}
	}

	result = append(result, b)
	sort.Sort(result)
	return result
}

// SortAndMerge sorts bounds by Start key and merges overlapping/adjacent entries
// in a single O(N log N) pass. Use after batch-appending multiple bounds.
func (bs Bounds) SortAndMerge() Bounds {
	if len(bs) <= 1 {
		return bs
	}
	sort.Sort(bs)
	result := bs[:1] // reuse backing array, safe since result grows ≤ input
	for _, b := range bs[1:] {
		last := &result[len(result)-1]
		if isOverlap(*last, b) && isOverlap(b, *last) {
			*last = mergeBounds(*last, b)
		} else {
			result = append(result, b)
		}
	}
	return result
}

func isOverlap(a, b Bound) bool {
	// a {x, inf} or b {-inf, x}
	if len(a.End) == 0 || len(b.Start) == 0 {
		return true
	}
	switch bytes.Compare(a.End, b.Start) {
	case 0:
		if a.EndInclude || b.StartInclude {
			return true
		} else {
			return false
		}
	case 1:
		return true
	}
	return false
}

func mergeBounds(a, b Bound) Bound {
	start, startInclude := minStartKey(a, b)
	end, endInclude := maxEndKey(a, b)
	merged := Bound{
		Start:        start,
		End:          end,
		StartInclude: startInclude,
		EndInclude:   endInclude,
	}
	return merged
}

func minStartKey(a, b Bound) ([]byte, bool) {
	if len(a.Start) == 0 {
		return a.Start, true
	}
	if len(b.Start) == 0 {
		return b.Start, true
	}
	if bytes.Compare(a.Start, b.Start) <= 0 {
		return a.Start, a.StartInclude
	}
	return b.Start, b.StartInclude
}

func maxEndKey(a, b Bound) ([]byte, bool) {
	if len(a.End) == 0 {
		return a.End, true
	}
	if len(b.End) == 0 {
		return b.End, true
	}
	if bytes.Compare(a.End, b.End) >= 0 {
		return a.End, a.EndInclude
	}
	return b.End, b.EndInclude
}

func (bs Bounds) Len() int {
	return len(bs)
}

func (bs Bounds) Less(i, j int) bool {
	return bytes.Compare(bs[i].Start, bs[j].Start) == -1
}

func (bs Bounds) Swap(i, j int) {
	bs[i], bs[j] = bs[j], bs[i]
}

// Contains reports whether val lies within any range in bs.
// val is a raw anyenc-encoded tuple component (e.g. the field-value
// prefix of an index key, with docId suffix stripped).
func (bs Bounds) Contains(val []byte) bool {
	for _, b := range bs {
		if b.contains(val) {
			return true
		}
	}
	return false
}

func (b Bound) contains(val []byte) bool {
	if len(b.Start) > 0 {
		cmp := bytes.Compare(val, b.Start)
		if cmp < 0 || (cmp == 0 && !b.StartInclude) {
			return false
		}
	}
	if len(b.End) > 0 {
		cmp := bytes.Compare(val, b.End)
		if cmp > 0 || (cmp == 0 && !b.EndInclude) {
			return false
		}
	}
	return true
}
