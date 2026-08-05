package aggregate

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
)

// exprOpParsers is the compute-operator vocabulary as data — same pattern as
// stageParsers: dispatch and rejection cannot drift apart. $literal is not
// here: it is an escape handled inline by ParseExpr, not a compute operator.
// Filled by init: the parsers recurse into ParseExpr, so a composite literal
// would be an initialization cycle.
var exprOpParsers map[string]func(*anyenc.Value) (Expr, error)

func init() {
	exprOpParsers = map[string]func(*anyenc.Value) (Expr, error){
		"$add":      func(v *anyenc.Value) (Expr, error) { return parseArith(OpAdd, v) },
		"$subtract": func(v *anyenc.Value) (Expr, error) { return parseArith(OpSubtract, v) },
		"$multiply": func(v *anyenc.Value) (Expr, error) { return parseArith(OpMultiply, v) },
		"$divide":   func(v *anyenc.Value) (Expr, error) { return parseArith(OpDivide, v) },
		"$abs":      parseAbs,
		"$round":    parseRound,
		"$concat":   parseConcat,
	}
}

// parseOperands parses an operator's operand list. A non-array operand is a
// one-element list (Mongo's single-operand shorthand). Arity is enforced
// here, at parse time; max <= 0 means variadic.
func parseOperands(op string, v *anyenc.Value, min, max int) ([]Expr, error) {
	items := []*anyenc.Value{v}
	fromArray := v.Type() == anyenc.TypeArray
	if fromArray {
		items, _ = v.Array()
	}
	if len(items) < min || (max > 0 && len(items) > max) {
		return nil, arityError(op, min, max, len(items))
	}
	exprs := make([]Expr, len(items))
	for i, item := range items {
		sub, err := ParseExpr(item)
		if err != nil {
			if !fromArray {
				return nil, err // shorthand operand: no list, no index segment
			}
			return nil, atPath(err, strconv.Itoa(i))
		}
		exprs[i] = sub
	}
	return exprs, nil
}

func arityError(op string, min, max, got int) error {
	var want string
	switch {
	case max <= 0 && min == 1:
		want = "at least 1 operand"
	case max <= 0:
		want = fmt.Sprintf("at least %d operands", min)
	case min == max && min == 1:
		want = "exactly 1 operand"
	case min == max:
		want = fmt.Sprintf("exactly %d operands", min)
	default:
		want = fmt.Sprintf("%d to %d operands", min, max)
	}
	return &query.ParseError{
		Op:     op,
		Reason: fmt.Sprintf("%s requires %s, got %d", op, want, got),
	}
}

// opString renders {$op:[args...]} — the canonical operand-list spelling,
// operator key unquoted like the stage spec Strings.
func opString(op string, args []Expr) string {
	var b strings.Builder
	b.WriteByte('{')
	b.WriteString(op)
	b.WriteString(":[")
	for i, arg := range args {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(arg.String())
	}
	b.WriteString("]}")
	return b.String()
}

// evalNumber evaluates sub as a numeric operand. ok=false means the operator
// result is null: the operand is missing, null, or non-numeric (TypeDateTime
// included — date arithmetic is future work). Mongo raises a runtime error
// for non-numeric operands; streaming eval has no per-document error channel,
// so null stands in (see docs/aggregation.md).
func evalNumber(sub Expr, a *anyenc.Arena, doc *anyenc.Value) (f float64, ok bool, err error) {
	v, err := sub.Eval(a, doc)
	if err != nil || v == nil || v.Type() != anyenc.TypeNumber {
		return 0, false, err
	}
	f, _ = v.Float64()
	return f, true, nil
}

// newNumber creates an arena number; a non-finite result (overflow, NaN) maps
// to null — same no-error-channel rationale as evalNumber.
func newNumber(a *anyenc.Arena, f float64) *anyenc.Value {
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return a.NewNull()
	}
	return a.NewNumberFloat64(f)
}

// ArithOp identifies an arithmetic expression operator.
type ArithOp uint8

const (
	OpAdd ArithOp = iota
	OpSubtract
	OpMultiply
	OpDivide
)

var arithOpNames = [...]string{"$add", "$subtract", "$multiply", "$divide"}

func (op ArithOp) String() string { return arithOpNames[op] }

// ArithExpr evaluates $add/$subtract/$multiply/$divide over numeric operands.
// Any missing, null, or non-numeric operand makes the result null, as do
// division by zero and a non-finite result (Mongo errors for these; see
// evalNumber). An empty operand list yields the operator's identity, Mongo
// semantics ($add 0, $multiply 1).
type ArithExpr struct {
	Op   ArithOp
	Args []Expr
}

func parseArith(op ArithOp, v *anyenc.Value) (Expr, error) {
	min, max := 0, 0 // $add/$multiply: variadic, empty allowed
	if op == OpSubtract || op == OpDivide {
		min, max = 2, 2
	}
	args, err := parseOperands(op.String(), v, min, max)
	if err != nil {
		return nil, err
	}
	return &ArithExpr{Op: op, Args: args}, nil
}

func (e *ArithExpr) Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	if len(e.Args) == 0 {
		if e.Op == OpMultiply {
			return a.NewNumberFloat64(1), nil
		}
		return a.NewNumberFloat64(0), nil // $add
	}
	// The first operand seeds the accumulator, so one loop serves both the
	// variadic ($add/$multiply) and binary ($subtract/$divide) forms.
	var acc float64
	for i, arg := range e.Args {
		f, ok, err := evalNumber(arg, a, doc)
		if err != nil {
			return nil, err
		}
		if !ok {
			return a.NewNull(), nil
		}
		if i == 0 {
			acc = f
			continue
		}
		switch e.Op {
		case OpAdd:
			acc += f
		case OpSubtract:
			acc -= f
		case OpMultiply:
			acc *= f
		case OpDivide:
			if f == 0 {
				return a.NewNull(), nil
			}
			acc /= f
		}
	}
	return newNumber(a, acc), nil
}

func (e *ArithExpr) String() string { return opString(e.Op.String(), e.Args) }

// AbsExpr evaluates $abs; missing/null/non-numeric operand → null.
type AbsExpr struct{ Arg Expr }

func parseAbs(v *anyenc.Value) (Expr, error) {
	args, err := parseOperands("$abs", v, 1, 1)
	if err != nil {
		return nil, err
	}
	return &AbsExpr{Arg: args[0]}, nil
}

func (e *AbsExpr) Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	f, ok, err := evalNumber(e.Arg, a, doc)
	if err != nil {
		return nil, err
	}
	if !ok {
		return a.NewNull(), nil
	}
	return newNumber(a, math.Abs(f)), nil
}

func (e *AbsExpr) String() string { return opString("$abs", []Expr{e.Arg}) }

// RoundExpr evaluates $round with half-to-even (banker's) rounding, Mongo
// semantics: place in [-20, 100], default 0, negative rounds left of the
// decimal point; an out-of-range or non-integer place → null. Precision is
// float64: values round by their binary double value ({"$round":[2.345,2]} is
// 2.35 — the stored double sits above the midpoint), and a place beyond
// float64 resolution leaves the value unchanged.
type RoundExpr struct {
	X     Expr
	Place Expr // nil: round to integer
}

func parseRound(v *anyenc.Value) (Expr, error) {
	args, err := parseOperands("$round", v, 1, 2)
	if err != nil {
		return nil, err
	}
	e := &RoundExpr{X: args[0]}
	if len(args) == 2 {
		e.Place = args[1]
	}
	return e, nil
}

func (e *RoundExpr) Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	x, ok, err := evalNumber(e.X, a, doc)
	if err != nil {
		return nil, err
	}
	if !ok {
		return a.NewNull(), nil
	}
	place := 0.0
	if e.Place != nil {
		p, ok, err := evalNumber(e.Place, a, doc)
		if err != nil {
			return nil, err
		}
		if !ok || p != math.Trunc(p) || p < -20 || p > 100 {
			return a.NewNull(), nil
		}
		place = p
	}
	return newNumber(a, roundHalfEven(x, int(place))), nil
}

func (e *RoundExpr) String() string {
	if e.Place == nil {
		return opString("$round", []Expr{e.X})
	}
	return opString("$round", []Expr{e.X, e.Place})
}

// roundHalfEven rounds x to place decimal digits, half to even. Scaling by a
// power of ten keeps the canonical cases exact (1.5→2, 2.5→2 at place 0);
// once the scaled value has no fractional bits (|s| >= 2^52, including
// overflow to Inf) rounding is an identity, and returning x directly avoids
// the round-trip division error.
func roundHalfEven(x float64, place int) float64 {
	switch {
	case place == 0:
		return math.RoundToEven(x)
	case place > 0:
		p := math.Pow10(place)
		s := x * p
		if math.IsInf(s, 0) || math.Abs(s) >= 1<<52 {
			return x
		}
		return math.RoundToEven(s) / p
	default:
		p := math.Pow10(-place)
		s := x / p
		if math.Abs(s) >= 1<<52 {
			return x
		}
		return math.RoundToEven(s) * p
	}
}

// ConcatExpr evaluates $concat over string operands into a reusable scratch
// buffer (expressions are per-pipeline, single-goroutine), then creates one
// arena string value. A missing, null, or non-string operand makes the result
// null (Mongo errors on non-strings; same rationale as evalNumber). An empty
// operand list yields "", Mongo semantics.
type ConcatExpr struct {
	Args []Expr

	buf []byte // scratch, reused across Eval calls
}

func parseConcat(v *anyenc.Value) (Expr, error) {
	args, err := parseOperands("$concat", v, 0, 0)
	if err != nil {
		return nil, err
	}
	return &ConcatExpr{Args: args}, nil
}

func (e *ConcatExpr) Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	e.buf = e.buf[:0]
	for _, arg := range e.Args {
		v, err := arg.Eval(a, doc)
		if err != nil {
			return nil, err
		}
		if v == nil || v.Type() != anyenc.TypeString {
			return a.NewNull(), nil
		}
		e.buf = append(e.buf, v.GetStringBytes()...)
	}
	return a.NewStringBytes(e.buf), nil
}

func (e *ConcatExpr) String() string { return opString("$concat", e.Args) }
