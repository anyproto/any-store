package query

import (
	"bytes"
	"fmt"
	"slices"
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
			k = k[len(b.prefix):]
		}
		if len(k) == 1 {
			// A bare type tag is a bracket edge (Comp.IndexBounds), not a value;
			// a reverse index stores it inverted.
			if k[0] >= 0x80 {
				return "<^" + anyenc.Type(^k[0]).String() + ">"
			}
			return "<" + anyenc.Type(k[0]).String() + ">"
		}
		return k.String()
	}

	var as, bs string
	if len(b.Start) == 0 || bytes.Equal(b.prefix, b.Start) || bytes.Equal(append(b.prefix, 255), b.Start) {
		as = "[-inf"
	} else {
		// An inclusive Start ending in 0xFF is the planner's exclusive-Start
		// pad (padForwardBounds): render the logical bound, not the key.
		if b.StartInclude && b.Start[len(b.Start)-1] != 0xff {
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

// PrefixSuccessor returns the smallest byte string greater than every string
// prefixed by p: trailing 0xFF bytes are stripped and the last remaining byte
// incremented, into a fresh slice. Used as an EXCLUSIVE End, the result
// admits exactly the p-prefixed keys — unlike the append-0xFF inclusive
// idiom, which excludes any key continuing with a 0xFF byte after the prefix
// (longer sorts greater). Returns nil (+inf) when p is all-0xFF or empty.
func PrefixSuccessor(p []byte) []byte {
	i := len(p) - 1
	for i >= 0 && p[i] == 0xff {
		i--
	}
	if i < 0 {
		return nil
	}
	out := make([]byte, i+1)
	copy(out, p[:i+1])
	out[i]++
	return out
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
	sortBounds(result)
	return result
}

// SortAndMerge sorts bounds by Start key and merges overlapping/adjacent entries
// in a single O(N log N) pass. Use after batch-appending multiple bounds.
func (bs Bounds) SortAndMerge() Bounds {
	if len(bs) <= 1 {
		return bs
	}
	sortBounds(bs)
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

// sortBounds orders bounds by Start key (unstable, Start only).
func sortBounds(bs Bounds) {
	slices.SortFunc(bs, func(a, b Bound) int {
		return bytes.Compare(a.Start, b.Start)
	})
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

// SortedDisjoint reports whether bs is in canonical form: sorted ascending by
// Start with pairwise non-overlapping ranges — the precondition of
// ContainsSorted. IndexBounds results are canonical (SortAndMerge), but a
// caller holding bounds of unknown provenance must check before switching from
// the linear Contains to the binary-search path.
func (bs Bounds) SortedDisjoint() bool {
	for i := 1; i < len(bs); i++ {
		if bytes.Compare(bs[i-1].Start, bs[i].Start) > 0 {
			return false
		}
		if isOverlap(bs[i-1], bs[i]) && isOverlap(bs[i], bs[i-1]) {
			return false
		}
	}
	return true
}

// ContainsSorted reports whether val lies within any range of a CANONICAL
// (sorted, disjoint — see SortedDisjoint) bound set, by binary search. On a
// non-canonical set the answer is unsound BY DESIGN — the caller owns the
// precondition; this is the hot-loop variant of Contains (which stays linear
// and assumption-free).
func (bs Bounds) ContainsSorted(val []byte) bool {
	lo, hi := 0, len(bs)
	for lo < hi {
		mid := (lo + hi) / 2
		b := &bs[mid]
		if len(b.Start) > 0 {
			if c := bytes.Compare(val, b.Start); c < 0 || (c == 0 && !b.StartInclude) {
				hi = mid
				continue
			}
		}
		if len(b.End) > 0 {
			if c := bytes.Compare(val, b.End); c > 0 || (c == 0 && !b.EndInclude) {
				lo = mid + 1
				continue
			}
		}
		return true
	}
	return false
}

// Intersect returns the lex-intersection of this bound set with other.
// A bound set is a union of value ranges; the intersection is the set of
// values present in BOTH unions. Used by TightIndexBounds to combine the
// bounds contributed by multiple conjuncts on the same field — NEVER by
// IndexBounds itself, whose over-approximation contract (see And.IndexBounds)
// is what keeps array/multi-key seeks sound.
//
// An empty result (returned as a zero-length Bounds) means the two sets
// share no values — distinct from "no narrowing", which preserves the input.
// Callers must not feed a zero-length result into code that reads len==0 as
// "unconstrained". The pairwise intersections are sorted and merged before
// returning. Result endpoints alias the input bounds' Start/End bytes and are
// never mutated, preserving the cap==len sharing contract
// (docs/query-filter-contract.md).
func (bs Bounds) Intersect(other Bounds) Bounds {
	if len(bs) == 0 || len(other) == 0 {
		return Bounds{}
	}
	var result Bounds
	for _, a := range bs {
		for _, b := range other {
			if p, ok := intersectPair(a, b); ok {
				result = append(result, p)
			}
		}
	}
	if len(result) == 0 {
		return Bounds{}
	}
	return result.SortAndMerge()
}

// intersectPair returns the intersection of two single bounds and whether it
// is non-empty. The intersection's lower bound is the tighter (greater) of the
// two starts; its upper bound the tighter (lesser) of the two ends.
func intersectPair(a, b Bound) (Bound, bool) {
	start, startInclude := maxStartKey(a, b)
	end, endInclude := minEndKey(a, b)
	// With both ends finite, a start past the end is empty, and a start equal
	// to the end is a single point only if both sides include it.
	if len(start) > 0 && len(end) > 0 {
		switch bytes.Compare(start, end) {
		case 1: // start > end
			return Bound{}, false
		case 0:
			if !startInclude || !endInclude {
				return Bound{}, false
			}
		}
	}
	return Bound{
		Start:        start,
		End:          end,
		StartInclude: startInclude,
		EndInclude:   endInclude,
	}, true
}

// maxStartKey returns the tighter (greater) lower bound of a and b — the
// inverse of minStartKey. An empty Start means -∞, so the other side wins.
// At equal starts the point is included only if both sides include it.
func maxStartKey(a, b Bound) ([]byte, bool) {
	if len(a.Start) == 0 {
		return b.Start, b.StartInclude
	}
	if len(b.Start) == 0 {
		return a.Start, a.StartInclude
	}
	switch bytes.Compare(a.Start, b.Start) {
	case 0:
		return a.Start, a.StartInclude && b.StartInclude
	case 1: // a.Start > b.Start
		return a.Start, a.StartInclude
	default: // a.Start < b.Start
		return b.Start, b.StartInclude
	}
}

// minEndKey returns the tighter (lesser) upper bound of a and b — the inverse
// of maxEndKey. An empty End means +∞, so the other side wins. At equal ends
// the point is included only if both sides include it.
func minEndKey(a, b Bound) ([]byte, bool) {
	if len(a.End) == 0 {
		return b.End, b.EndInclude
	}
	if len(b.End) == 0 {
		return a.End, a.EndInclude
	}
	switch bytes.Compare(a.End, b.End) {
	case 0:
		return a.End, a.EndInclude && b.EndInclude
	case -1: // a.End < b.End
		return a.End, a.EndInclude
	default: // a.End > b.End
		return b.End, b.EndInclude
	}
}
