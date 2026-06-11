package aggregate

import (
	"fmt"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"
)

// Expr is an aggregation expression evaluated against one document.
//
// v1 supports field references ("$a.b.c"), literals ({"$literal": x} or any
// plain value), and document/array expressions (objects and arrays whose
// members are themselves expressions, Mongo semantics). Compute operators
// ($add, $cond, ...) are additive future work: they become composite Expr
// implementations dispatched in ParseExpr.
type Expr interface {
	// Eval returns the expression value for doc; nil means "missing".
	// Results may be allocated on a; field refs alias doc (zero-copy) and are
	// valid only while doc is.
	Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error)
	fmt.Stringer
}

// FieldRefExpr resolves a pre-split document path ("$a.b.c").
type FieldRefExpr struct {
	Field string // original spelling without "$", for String()
	Path  []string
}

func (e *FieldRefExpr) Eval(_ *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	return doc.Get(e.Path...), nil
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
// literal, non-operator objects are document expressions, arrays are arrays of
// expressions, everything else is a literal. Unknown $-operators are rejected
// (compute operators are not supported in v1).
func ParseExpr(v *anyenc.Value) (Expr, error) {
	switch v.Type() {
	case anyenc.TypeString:
		s := string(v.GetStringBytes())
		if !strings.HasPrefix(s, "$") {
			return NewLiteralExpr(v)
		}
		if strings.HasPrefix(s, "$$") {
			return nil, fmt.Errorf("aggregate: variables are not supported: %s", s)
		}
		field := s[1:]
		if field == "" {
			return nil, fmt.Errorf("aggregate: empty field reference")
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
					perr = fmt.Errorf("aggregate: operator %s cannot be mixed with other fields", string(key))
					return
				}
				opKey, opVal = string(key), item
				return
			}
			if opKey != "" {
				perr = fmt.Errorf("aggregate: operator %s cannot be mixed with other fields", opKey)
				return
			}
			nonOpct++
			sub, e := ParseExpr(item)
			if e != nil {
				perr = e
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
			return nil, fmt.Errorf("aggregate: unsupported expression operator: %s", opKey)
		}
		return &ObjectExpr{Names: names, Exprs: exprs}, nil
	case anyenc.TypeArray:
		arr, _ := v.Array()
		exprs := make([]Expr, len(arr))
		for i, item := range arr {
			sub, err := ParseExpr(item)
			if err != nil {
				return nil, err
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
