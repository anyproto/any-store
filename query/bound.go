package query

import (
	"bytes"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/anyproto/any-store/key"
)

type Bound struct {
	Start, End   key.Key
	StartInclude bool
	EndInclude   bool
	prefix       key.Key
}

func (b Bound) String() string {
	stripPrefixString := func(k key.Key) string {
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
	var result = bs[:0]

	// Flag to check if the new bound has been merged
	merged := false

	// Iterate through existing bounds
	for _, existingBound := range bs {
		// Check for overlap
		if isOverlap(existingBound, b) && isOverlap(b, existingBound) {
			// Merge overlapping bounds
			mergedBound := mergeBounds(existingBound, b)
			result = append(result, mergedBound)
			merged = true
		} else {
			// No overlap, add the existing bound as it is
			result = append(result, existingBound)
		}
	}

	// If the new bound wasn't merged, add it separately
	if !merged {
		result = append(result, b)
		sort.Sort(result)
	}

	return result
}

func (bs Bounds) Merge() Bounds {
	var nbs = bs[:0]
	var needMerge bool
	for i := 0; i < bs.Len()-1; i++ {
		if isOverlap(bs[i], bs[i+1]) && isOverlap(bs[i+1], bs[i]) {
			needMerge = true
			break
		}
	}
	if needMerge {
		for i := range bs {
			nbs = nbs.Append(bs[i])
		}
		return nbs.Merge()
	} else {
		return bs
	}
}

func (bs Bounds) Reverse() {
	slices.Reverse(bs)
	for i, b := range bs {
		if b.EndInclude {
			// add the extra byte to have the correct position on badger.Seek with reverse iteration
			b.End = append(b.End, 255)
		}
		bs[i] = Bound{
			Start:        b.End,
			End:          b.Start,
			StartInclude: b.EndInclude,
			EndInclude:   b.StartInclude,
			prefix:       b.prefix,
		}
	}
}

func (bs Bounds) SetPrefix(k key.Key) {
	var prefix = k.Copy()
	for i, b := range bs {
		if len(b.Start) != 0 {
			bs[i].Start = append(k.Copy(), b.Start...)
		} else if i == 0 {
			bs[i].Start = prefix
			bs[i].StartInclude = true
		}
		if len(b.End) != 0 {
			bs[i].End = append(k.Copy(), b.End...)
		} else if i == len(bs)-1 {
			bs[i].End = append(prefix, 255)
			bs[i].EndInclude = true
		}
		bs[i].prefix = prefix
	}
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
