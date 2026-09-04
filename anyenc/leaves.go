package anyenc

import "math"

// Leaf is one value a dotted path resolves to (see Value.AppendLeaves).
type Leaf struct {
	// Value is the resolved value; nil is a missing leaf.
	Value *Value
	// Positional is set when the last path segment is numeric and picked
	// this leaf out of an array by position. MongoDB compares such a leaf
	// whole: {"a.0":1} does not look inside a first element [1,2], while
	// {"a.b":1} does look inside a leaf array under b. Consumers honour it
	// the same way (filters skip element iteration, index keys and sort keys
	// take the whole value).
	Positional bool
}

// AppendLeaves appends to dst every value the dotted path resolves to under
// MongoDB's query-matcher field-path semantics and returns the extended
// slice. dst is reused across calls, so the walk allocates nothing once its
// capacity is warm.
//
// Resolution per path segment:
//   - object: descend by key; an absent key is a missing leaf.
//   - array, numeric segment: the element at that index; out of range is a
//     missing leaf. Engine extension over Mongo, which also treats the numeral
//     as a key of object elements.
//   - array, other segment: the remaining path (this segment included) is
//     mapped over the elements. An object element continues the walk; an
//     element that cannot carry the path (scalar, null, nested array) is a
//     missing leaf; an empty array is one missing leaf.
//   - scalar or null met before the last segment: a missing leaf.
//
// A leaf that is itself an array is appended whole — the caller decides
// whether to look inside it (filters do, per element, unless the leaf is
// Positional; sorts take the extremum). With no array on the path the result
// is exactly one entry, equal to Get(path...).
//
// Missing leaves are deliberate: they carry the "no value here" fact that
// the null-equality family, $exists and sort keys all need, and keep every
// consumer (filter, sort, index key) on one definition. Mongo's matcher
// skips non-object elements instead, which is why its {"a.b":null} does not
// match {"a":[1]} while its sort and index keys do treat it as null.
func (v *Value) AppendLeaves(dst []Leaf, path ...string) []Leaf {
	walkLeaves(v, path, func(l Leaf) { dst = append(dst, l) })
	return dst
}

// GetLeaf is the fast path of AppendLeaves: when the path crosses no array
// by a non-numeric segment and does not end in an array index, the leaf set
// has exactly one non-positional entry and GetLeaf returns it (nil for a
// missing leaf) with single=true, at the cost of Get. single=false means
// the caller must resolve the full leaf set.
func (v *Value) GetLeaf(path ...string) (leaf *Value, single bool) {
	for i, seg := range path {
		if v == nil {
			return nil, true
		}
		switch v.t {
		case TypeObject:
			v = v.o.Get(seg)
		case TypeArray:
			n, ok := ParseIndexSegment(seg)
			if !ok {
				return nil, false
			}
			if n < 0 || n >= len(v.a) {
				return nil, true
			}
			if i == len(path)-1 {
				return nil, false // positional leaf
			}
			v = v.a[n]
		default:
			return nil, true
		}
	}
	return v, true
}

// WalkLeaves calls fn for every leaf of the path, in AppendLeaves order,
// without materialising the leaf set. fn must not retain the Leaf's Value
// past the walk of a pooled document.
func (v *Value) WalkLeaves(path []string, fn func(Leaf)) {
	walkLeaves(v, path, fn)
}

func walkLeaves(v *Value, path []string, fn func(Leaf)) {
	for i, seg := range path {
		if v == nil {
			fn(Leaf{})
			return
		}
		switch v.t {
		case TypeObject:
			v = v.o.Get(seg)
		case TypeArray:
			if n, ok := ParseIndexSegment(seg); ok {
				if n < 0 || n >= len(v.a) {
					fn(Leaf{})
					return
				}
				v = v.a[n]
				if i == len(path)-1 {
					fn(Leaf{Value: v, Positional: true})
					return
				}
				continue
			}
			if len(v.a) == 0 {
				fn(Leaf{})
				return
			}
			rest := path[i:]
			for _, el := range v.a {
				if el.t == TypeObject {
					walkLeaves(el, rest, fn)
				} else {
					fn(Leaf{})
				}
			}
			return
		default:
			fn(Leaf{})
			return
		}
	}
	fn(Leaf{Value: v})
}

// AppendIndexValues appends the values an index stores for a leaf set, in
// key order: a missing leaf as nil (it encodes as null), a scalar leaf as
// itself, and a non-empty array leaf as each element followed by the array
// itself — unless the leaf is Positional, which stores the array whole
// only. One definition for index maintenance, index-order dedup and the
// planner's key reasoning; sort keys differ only in never taking the whole
// array (Value.WalkLeaves feeds them directly).
func AppendIndexValues(dst []*Value, leaves []Leaf) []*Value {
	for _, l := range leaves {
		if l.Value != nil && l.Value.t == TypeArray && !l.Positional {
			dst = append(dst, l.Value.a...)
		}
		dst = append(dst, l.Value)
	}
	return dst
}

// AppendElementValues appends the candidates a sort key is chosen from: the
// same as AppendIndexValues minus the whole-array entry of a non-empty
// array leaf (an empty array keeps it — that is its only entry). An ordered
// index scan's canonical entry for a document is elected among these, so
// the whole-array entry, which the scan meets before or after the elements
// depending on direction and element types, never decides the order.
func AppendElementValues(dst []*Value, leaves []Leaf) []*Value {
	for _, l := range leaves {
		if l.Value != nil && l.Value.t == TypeArray && !l.Positional && len(l.Value.a) > 0 {
			dst = append(dst, l.Value.a...)
			continue
		}
		dst = append(dst, l.Value)
	}
	return dst
}

// ParseIndexSegment reports whether seg is a numeric path segment and the
// array index it names. It accepts strconv.Atoi syntax (optional sign,
// decimal digits) without Atoi's allocating error path; a numeric segment
// that cannot index anything (negative, overflowing) is reported as ok with
// n = -1, so callers turn it into "out of range" exactly as Atoi's error
// would.
func ParseIndexSegment(seg string) (n int, ok bool) {
	i := 0
	neg := false
	if len(seg) > 0 && (seg[0] == '+' || seg[0] == '-') {
		neg = seg[0] == '-'
		i = 1
	}
	if i == len(seg) {
		return 0, false
	}
	for ; i < len(seg); i++ {
		c := seg[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		if n > (math.MaxInt32-9)/10 { // pre-multiply guard: no int wrap on 32-bit
			return -1, true
		}
		n = n*10 + int(c-'0')
	}
	if neg && n != 0 {
		return -1, true
	}
	return n, true
}
