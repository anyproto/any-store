package query

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/anyproto/any-store/internal/key"
)

type Bound struct {
	Start, End   key.Key
	StartInclude bool
	EndInclude   bool
}

func (b Bound) String() string {
	var as, bs string
	if len(b.Start) == 0 {
		as = "[-inf"
	} else {
		if b.StartInclude {
			as = "['" + b.Start.String() + "'"
		} else {
			as = "('" + b.Start.String() + "'"
		}
	}
	if len(b.End) == 0 {
		bs = "inf]"
	} else {
		if b.EndInclude {
			bs = "'" + b.End.String() + "']"
		} else {
			bs = "'" + b.End.String() + "')"
		}
	}
	return fmt.Sprintf("%s,%s", as, bs)
}

type Bounds []Bound

func (bs Bounds) String() string {
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
