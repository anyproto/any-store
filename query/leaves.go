package query

import (
	"sync"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/syncpool"
)

// leafBuf is the leaf-set scratch Key.Ok borrows when the caller passes no
// DocBuffer.
type leafBuf struct {
	leaves []anyenc.Leaf
}

var leafBufPool = sync.Pool{New: func() any { return &leafBuf{} }}

// LeafFilter evaluates a filter against the LEAF SET of a dotted path — the
// values anyenc.Value.AppendLeaves yields when the path crosses an array of
// objects (a nil Value is a missing leaf). Key.Ok calls it whenever a path
// resolves to more than one leaf; a single leaf goes through Ok (or OkElem)
// unchanged.
//
// MongoDB quantifies per operator: a positive predicate matches when ANY
// leaf satisfies it, a negation ($ne, $nin, $not, $nor) when NO leaf
// satisfies the negated predicate, and sibling operators on one path
// quantify independently — {"a.b":{"$gt":1,"$lt":3}} may pick its two
// witnesses from different elements ($elemMatch binds them). Filters that do
// not implement LeafFilter get the positive rule (okLeaves); the negation
// forms implement it themselves.
type LeafFilter interface {
	OkLeaves(leaves []anyenc.Leaf, docBuf *syncpool.DocBuffer) bool
}

// ElemFilter evaluates a filter against ONE value with no array iteration:
// an array value is compared whole. Used for positional leaves ({"a.0":…}
// on an element that is itself an array) and for the elements of an
// $elemMatch operator form. Filters that do not implement it are evaluated
// by Ok, which is right for every filter whose Ok never iterates arrays.
type ElemFilter interface {
	OkElem(v *anyenc.Value, docBuf *syncpool.DocBuffer) bool
}

// okLeaf evaluates f on one leaf, honouring Positional.
func okLeaf(f Filter, l anyenc.Leaf, docBuf *syncpool.DocBuffer) bool {
	if l.Positional {
		return okElem(f, l.Value, docBuf)
	}
	return f.Ok(l.Value, docBuf)
}

func okElem(f Filter, v *anyenc.Value, docBuf *syncpool.DocBuffer) bool {
	if ef, ok := f.(ElemFilter); ok {
		return ef.OkElem(v, docBuf)
	}
	return f.Ok(v, docBuf)
}

// okLeaves is the positive rule: any leaf satisfies f.
func okLeaves(f Filter, leaves []anyenc.Leaf, docBuf *syncpool.DocBuffer) bool {
	if lf, ok := f.(LeafFilter); ok {
		return lf.OkLeaves(leaves, docBuf)
	}
	for _, l := range leaves {
		if okLeaf(f, l, docBuf) {
			return true
		}
	}
	return false
}

// OkLeaves: $ne is a negation — no leaf may equal the operand (a leaf array
// is checked whole and per element by Ok); every other op is positive.
func (e *Comp) OkLeaves(leaves []anyenc.Leaf, docBuf *syncpool.DocBuffer) bool {
	if e.CompOp == CompOpNe {
		for _, l := range leaves {
			if !okLeaf(e, l, docBuf) {
				return false
			}
		}
		return true
	}
	for _, l := range leaves {
		if okLeaf(e, l, docBuf) {
			return true
		}
	}
	return false
}

// OkElem compares the value whole: no element iteration for an array.
func (e *Comp) OkElem(v *anyenc.Value, docBuf *syncpool.DocBuffer) bool {
	if e.isOrderingOp() && e.eqIsVector() {
		return false
	}
	if v == nil {
		return e.comp(encodedNull)
	}
	return e.okScalar(v, docBuf)
}

// OkElem: membership of the value itself.
func (e In) OkElem(v *anyenc.Value, docBuf *syncpool.DocBuffer) bool {
	if v == nil {
		_, ok := e.Values[string(encodedNull)]
		return ok
	}
	if docBuf == nil {
		docBuf = &syncpool.DocBuffer{}
	}
	return e.containsValue(v, docBuf)
}

// OkElem: the value must be a string.
func (r Regexp) OkElem(v *anyenc.Value, docBuf *syncpool.DocBuffer) bool {
	if v == nil || v.Type() != anyenc.TypeString {
		return false
	}
	exp, _ := v.StringBytes()
	return r.Regexp.Match(exp)
}

// OkElem: the value's own type.
func (e TypeFilter) OkElem(v *anyenc.Value, docBuf *syncpool.DocBuffer) bool {
	return v != nil && v.Type() == e.Type
}

// OkLeaves quantifies each conjunct independently.
func (e And) OkLeaves(leaves []anyenc.Leaf, docBuf *syncpool.DocBuffer) bool {
	for _, f := range e {
		if !okLeaves(f, leaves, docBuf) {
			return false
		}
	}
	return true
}

func (e And) OkElem(v *anyenc.Value, docBuf *syncpool.DocBuffer) bool {
	for _, f := range e {
		if !okElem(f, v, docBuf) {
			return false
		}
	}
	return true
}

func (e Or) OkLeaves(leaves []anyenc.Leaf, docBuf *syncpool.DocBuffer) bool {
	for _, f := range e {
		if okLeaves(f, leaves, docBuf) {
			return true
		}
	}
	return false
}

func (e Or) OkElem(v *anyenc.Value, docBuf *syncpool.DocBuffer) bool {
	for _, f := range e {
		if okElem(f, v, docBuf) {
			return true
		}
	}
	return false
}

// OkLeaves: no leaf satisfies any branch.
func (e Nor) OkLeaves(leaves []anyenc.Leaf, docBuf *syncpool.DocBuffer) bool {
	for _, f := range e {
		if okLeaves(f, leaves, docBuf) {
			return false
		}
	}
	return true
}

func (e Nor) OkElem(v *anyenc.Value, docBuf *syncpool.DocBuffer) bool {
	for _, f := range e {
		if okElem(f, v, docBuf) {
			return false
		}
	}
	return true
}

// OkLeaves: no leaf satisfies the inner filter.
func (e Not) OkLeaves(leaves []anyenc.Leaf, docBuf *syncpool.DocBuffer) bool {
	return !okLeaves(e.Filter, leaves, docBuf)
}

func (e Not) OkElem(v *anyenc.Value, docBuf *syncpool.DocBuffer) bool {
	return !okElem(e.Filter, v, docBuf)
}
