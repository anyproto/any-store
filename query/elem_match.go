package query

import (
	"fmt"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/syncpool"
)

// ElemMatch is {"$elemMatch": …}: the value must be an array with at least
// one element satisfying Cond as a whole — the one way to bind several
// predicates to the SAME element, where {"a.b":{"$gt":1,"$lt":3}} may pick
// two different ones.
//
// Two forms, decided by the operand's first key (MongoDB's rule):
//   - object form — {"$elemMatch": {"b": 1, "c": {"$gt": 2}}}, also
//     {"$or": …} / {} — Cond is a document condition applied to each
//     OBJECT element. Engine divergence: Mongo also admits array elements,
//     matched as objects keyed by position ("0", "1", …); here they never
//     match, so a path inside Cond cannot silently traverse a nested array.
//   - value form — {"$elemMatch": {"$gt": 1, "$lt": 3}} — Cond is an
//     operator set applied to each element as ONE value (ElemFilter): an
//     element that is itself an array is compared whole, exactly as a
//     positional leaf is.
//
// $text and $knn are rejected inside either form at parse time.
type ElemMatch struct {
	Cond      Filter
	ValueForm bool
}

func (e ElemMatch) Ok(v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	if v == nil || v.Type() != anyenc.TypeArray {
		return false
	}
	arr, _ := v.Array()
	for _, el := range arr {
		if e.ValueForm {
			if okElem(e.Cond, el, buf) {
				return true
			}
		} else if el.Type() == anyenc.TypeObject && e.Cond.Ok(el, buf) {
			return true
		}
	}
	return false
}

// IndexBounds for the value form are Cond's own bounds on the field: a
// matching element's value is one of the field's fan-out entries, so the
// element-level bounds are a sound (wide-channel) superset — except when the
// field is a positional path, whose leaf is stored whole (no element
// entries), or when Cond nests another $elemMatch, which looks inside an
// element the index stores whole; both contribute nothing. The object form
// contributes nothing on its own field; Key.IndexBounds re-keys its Cond
// under the sub-fields (elemMatchSubField).
func (e ElemMatch) IndexBounds(fieldName string, bs Bounds) (bounds Bounds) {
	if !e.ValueForm || lastSegmentNumeric(fieldName) || ContainsElemMatch(e.Cond) {
		return bs
	}
	return e.Cond.IndexBounds(fieldName, bs)
}

// elemMatchSubField reports whether the Key holds an object-form $elemMatch
// — or an $all conjunction of them — whose condition constrains fieldName
// below the Key's path, and returns that condition with the sub-field name.
// Sound because the index fans an element's values out under the sub-field,
// so a matching element's value is an entry there. Not on a positional path:
// its leaf is stored whole and has no element entries to seek.
func (e Key) elemMatchSubField(fieldName string) (cond Filter, sub string, ok bool) {
	path := strings.Join(e.Path, ".")
	if len(fieldName) <= len(path)+1 || fieldName[len(path)] != '.' ||
		!strings.HasPrefix(fieldName, path) || lastSegmentNumeric(path) {
		return nil, "", false
	}
	sub = fieldName[len(path)+1:]
	objectForm := func(f Filter) (Filter, bool) {
		switch em := f.(type) {
		case ElemMatch:
			return em.Cond, !em.ValueForm
		case *ElemMatch:
			return em.Cond, !em.ValueForm
		}
		return nil, false
	}
	if c, ok := objectForm(e.Filter); ok {
		return c, sub, true
	}
	var children []Filter
	switch ft := e.Filter.(type) {
	case And:
		children = ft
	case *And:
		children = *ft
	default:
		return nil, "", false
	}
	if len(children) == 0 {
		return nil, "", false
	}
	conds := make(And, 0, len(children))
	for _, child := range children {
		c, ok := objectForm(child)
		if !ok {
			return nil, "", false
		}
		conds = append(conds, c)
	}
	return conds, sub, true
}

func (e ElemMatch) String() string {
	return fmt.Sprintf(`{"$elemMatch": %s}`, e.Cond.String())
}

// lastSegmentNumeric reports whether the dotted field name ends in an array
// index segment — a positional path, whose leaf the index stores whole.
func lastSegmentNumeric(fieldName string) bool {
	last := fieldName
	if i := strings.LastIndexByte(fieldName, '.'); i >= 0 {
		last = fieldName[i+1:]
	}
	_, numeric := anyenc.ParseIndexSegment(last)
	return numeric
}

// parseElemMatch compiles a $elemMatch operand. The first key decides the
// form: a field-level operator ($gt, $not, $in, …, $elemMatch itself) means
// the value form; anything else — a field name, {}, or a top-level logical
// operator like $or — means the object form.
func parseElemMatch(v *anyenc.Value) (Filter, error) {
	obj, err := v.Object()
	if err != nil {
		return nil, &ParseError{Op: "$elemMatch", Reason: "$elemMatch must be an object"}
	}
	valueForm := false
	first := true
	obj.Visit(func(key []byte, _ *anyenc.Value) {
		if !first {
			return
		}
		first = false
		isOp, op, opErr := isOperator(key)
		valueForm = opErr == nil && isOp && !isTopLevel(op)
	})
	if valueForm {
		hasOp, cond, err := parseCompObjOp(v)
		if err != nil {
			return nil, err
		}
		if !hasOp {
			return nil, &ParseError{Op: "$elemMatch", Reason: "expected an object of operators"}
		}
		if ContainsKnn(cond) {
			return nil, &ParseError{Op: "$knn", Reason: "$knn is not allowed under $elemMatch"}
		}
		return ElemMatch{Cond: cond, ValueForm: true}, nil
	}
	cond, err := parseAnd(v)
	if err != nil {
		return nil, err
	}
	if cond == nil {
		cond = All{} // {"$elemMatch": {}}: any object element
	}
	if ContainsSourceFilter(cond) {
		return nil, &ParseError{Op: "$elemMatch", Reason: "$text and $knn are not allowed under $elemMatch"}
	}
	return ElemMatch{Cond: cond}, nil
}

// parseAllElemMatch handles {"$all": [{"$elemMatch": …}, …]}: every element
// must be a single-key {"$elemMatch": …} object (Mongo's rule — mixing
// $elemMatch with plain values is a rejection), and the result is the
// conjunction of the element matches. ok=false means no element is an
// $elemMatch object and the caller keeps the plain equality form.
func parseAllElemMatch(vals []*anyenc.Value) (f Filter, ok bool, err error) {
	n := 0
	for _, el := range vals {
		if isElemMatchObject(el) {
			n++
		}
	}
	if n == 0 {
		return nil, false, nil
	}
	if n != len(vals) {
		return nil, false, &ParseError{Op: "$all", Reason: "$all cannot mix $elemMatch with values"}
	}
	fs := make(And, 0, len(vals))
	for i, el := range vals {
		em, err := parseElemMatch(el.Get("$elemMatch"))
		if err != nil {
			return nil, false, atPath(atPath(err, "$elemMatch"), fmt.Sprint(i))
		}
		fs = append(fs, em)
	}
	if len(fs) == 1 {
		return fs[0], true, nil
	}
	return fs, true, nil
}

func isElemMatchObject(v *anyenc.Value) bool {
	obj, err := v.Object()
	if err != nil || obj.Len() != 1 {
		return false
	}
	return v.Get("$elemMatch") != nil
}
