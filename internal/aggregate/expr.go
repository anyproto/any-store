package aggregate

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
)

// Expr is an aggregation expression evaluated against one document.
//
// Supported: field references ("$a.b.c"), literals ({"$literal": x} or any
// plain value), document/array expressions (objects and arrays whose members
// are themselves expressions, Mongo semantics), and the compute operators in
// exprOpParsers — arithmetic/string ($add, $round, $concat, ...), conditional
// ($cond, $switch, $ifNull), comparison ($eq ... $lte, $cmp) and date
// ($dateAdd, $dateDiff, $dateTrunc, $year, $week). Remaining compute
// operators (string transforms, ...) are additive future work: composite
// Expr implementations dispatched in ParseExpr.
type Expr interface {
	// Eval returns the expression value for doc; nil means "missing".
	// Results may be allocated on a; field refs alias doc (zero-copy),
	// except that implicit array traversal collects doc-aliasing elements
	// into an a-allocated array container. Either way the result is valid
	// only while both doc and a are.
	Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error)
	fmt.Stringer
}

// FieldRefExpr resolves a pre-split document path ("$a.b.c") with Mongo's
// aggregation field-path semantics, including implicit array traversal; see
// resolveFieldPath.
type FieldRefExpr struct {
	Field string // original spelling without "$", for String()
	Path  []string
}

func (e *FieldRefExpr) Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	return resolveFieldPath(a, doc, e.Path), nil
}

// resolveFieldPath resolves path against v with Mongo's aggregation
// field-path semantics (ExpressionFieldPath::evaluatePath): nil means
// missing. A non-numeric segment applied to an array maps the remaining
// path (segment included) over the array's elements and collects the
// non-missing results, in order, into an a-allocated array: non-object
// elements — scalars and nested arrays — are skipped, elements lacking the
// field contribute nothing (no null padding), and the result is an array
// even when nothing collects ([] for an empty array or no element with the
// field — never missing). Collected values that are themselves arrays stay
// nested (no flattening), and traversal applies again at each array met
// deeper in the path. A terminal segment's value is returned as is, so "$a"
// naming an array yields the whole array.
//
// Engine extension over Mongo: a numeric segment indexes an array
// (anyenc.Value.Get semantics — out of range means missing) instead of
// collecting field "0"-style names from object elements.
//
// Recursion only descends into object elements after consuming at least one
// segment, so the depth is bounded by len(path).
func resolveFieldPath(a *anyenc.Arena, v *anyenc.Value, path []string) *anyenc.Value {
	for i, seg := range path {
		if v.Type() != anyenc.TypeArray {
			v = v.Get(seg)
			if v == nil {
				return nil
			}
			continue
		}
		if n, numeric := anyenc.ParseIndexSegment(seg); numeric {
			arr, _ := v.Array()
			if n < 0 || n >= len(arr) {
				return nil
			}
			v = arr[n]
			continue
		}
		arr, _ := v.Array()
		out := a.NewArray()
		n := 0
		for _, el := range arr {
			if el.Type() != anyenc.TypeObject {
				continue
			}
			if r := resolveFieldPath(a, el, path[i:]); r != nil {
				out.SetArrayItem(n, r)
				n++
			}
		}
		return out
	}
	return v
}

func (e *FieldRefExpr) String() string { return "$" + e.Field }

// LiteralExpr yields the same constant value for every document. The value is
// detached from the input at parse time (owned bytes + dedicated parser), so
// it stays valid for the lifetime of the pipeline regardless of caller arenas.
type LiteralExpr struct {
	raw []byte
	v   *anyenc.Value
}

func NewLiteralExpr(v *anyenc.Value) (*LiteralExpr, error) {
	e := &LiteralExpr{raw: v.MarshalTo(nil)}
	p := &anyenc.Parser{}
	var err error
	if e.v, err = p.Parse(e.raw); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *LiteralExpr) Eval(_ *anyenc.Arena, _ *anyenc.Value) (*anyenc.Value, error) {
	return e.v, nil
}

func (e *LiteralExpr) String() string { return e.v.String() }

// ObjectExpr is a document expression: an object whose values are expressions
// (Mongo semantics for non-operator objects in expression context, e.g.
// compound $group keys {"a":"$x","b":"$y"}). Missing member values are omitted
// from the result, like Mongo.
type ObjectExpr struct {
	Names []string
	Exprs []Expr
}

func (e *ObjectExpr) Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	out := a.NewObject()
	for i, sub := range e.Exprs {
		v, err := sub.Eval(a, doc)
		if err != nil {
			return nil, err
		}
		if v == nil {
			continue
		}
		out.Set(e.Names[i], v)
	}
	return out, nil
}

func (e *ObjectExpr) String() string {
	var b strings.Builder
	b.WriteByte('{')
	for i, n := range e.Names {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%q:%s", n, e.Exprs[i].String())
	}
	b.WriteByte('}')
	return b.String()
}

// ArrayExpr is an array expression: every element is an expression. Missing
// elements evaluate to null so the array length is preserved.
type ArrayExpr struct {
	Exprs []Expr
}

func (e *ArrayExpr) Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	out := a.NewArray()
	for i, sub := range e.Exprs {
		v, err := sub.Eval(a, doc)
		if err != nil {
			return nil, err
		}
		if v == nil {
			v = a.NewNull()
		}
		out.SetArrayItem(i, v)
	}
	return out, nil
}

func (e *ArrayExpr) String() string {
	var b strings.Builder
	b.WriteByte('[')
	for i, sub := range e.Exprs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(sub.String())
	}
	b.WriteByte(']')
	return b.String()
}

// ParseExpr parses an aggregation expression value (Mongo expression-context
// rules): "$path" strings are field references, {"$literal": x} is an escaped
// literal, single-key $-objects in exprOpParsers are compute operators,
// non-operator objects are document expressions, arrays are arrays of
// expressions, everything else is a literal. Unknown $-operators are rejected.
func ParseExpr(v *anyenc.Value) (Expr, error) {
	switch v.Type() {
	case anyenc.TypeString:
		s := string(v.GetStringBytes())
		if !strings.HasPrefix(s, "$") {
			return NewLiteralExpr(v)
		}
		if strings.HasPrefix(s, "$$") {
			return nil, &query.ParseError{Reason: "variables are not supported: " + s}
		}
		field := s[1:]
		if field == "" {
			return nil, &query.ParseError{Reason: `empty field reference: "$"`}
		}
		return &FieldRefExpr{Field: field, Path: splitPath(field)}, nil
	case anyenc.TypeObject:
		obj, _ := v.Object()
		// {"$literal": x} escape and operator detection.
		var (
			opKey   string
			opVal   *anyenc.Value
			names   []string
			exprs   []Expr
			perr    error
			nonOpct int
		)
		obj.Visit(func(key []byte, item *anyenc.Value) {
			if perr != nil {
				return
			}
			if len(key) > 0 && key[0] == '$' {
				if opKey != "" || nonOpct > 0 {
					perr = atPath(&query.ParseError{
						Op:     string(key),
						Reason: "operator " + string(key) + " cannot be mixed with other fields",
					}, string(key))
					return
				}
				opKey, opVal = string(key), item
				return
			}
			if opKey != "" {
				perr = atPath(&query.ParseError{
					Op:     opKey,
					Reason: "operator " + opKey + " cannot be mixed with other fields",
				}, string(key))
				return
			}
			nonOpct++
			sub, e := ParseExpr(item)
			if e != nil {
				perr = atPath(e, string(key))
				return
			}
			names = append(names, string(key))
			exprs = append(exprs, sub)
		})
		if perr != nil {
			return nil, perr
		}
		if opKey != "" {
			if opKey == "$literal" {
				return NewLiteralExpr(opVal)
			}
			if parse, ok := exprOpParsers[opKey]; ok {
				e, err := parse(opVal)
				if err != nil {
					return nil, atPath(err, opKey)
				}
				return e, nil
			}
			// A vocabulary miss: neither the $literal escape nor a supported
			// compute operator.
			return nil, atPath(&query.ParseError{
				Op:     opKey,
				Reason: "unsupported expression operator: " + opKey,
				Err:    query.ErrUnknownOperator,
			}, opKey)
		}
		return &ObjectExpr{Names: names, Exprs: exprs}, nil
	case anyenc.TypeArray:
		arr, _ := v.Array()
		exprs := make([]Expr, len(arr))
		for i, item := range arr {
			sub, err := ParseExpr(item)
			if err != nil {
				return nil, atPath(err, strconv.Itoa(i))
			}
			exprs[i] = sub
		}
		return &ArrayExpr{Exprs: exprs}, nil
	default:
		return NewLiteralExpr(v)
	}
}

func splitPath(field string) []string {
	if !strings.Contains(field, ".") {
		return []string{field}
	}
	return strings.Split(field, ".")
}
