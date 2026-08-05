package aggregate

import (
	"bytes"
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
		"$cond":     parseCond,
		"$switch":   parseSwitch,
		"$ifNull":   parseIfNull,
		"$eq":       func(v *anyenc.Value) (Expr, error) { return parseCompare(CmpEq, v) },
		"$ne":       func(v *anyenc.Value) (Expr, error) { return parseCompare(CmpNe, v) },
		"$gt":       func(v *anyenc.Value) (Expr, error) { return parseCompare(CmpGt, v) },
		"$gte":      func(v *anyenc.Value) (Expr, error) { return parseCompare(CmpGte, v) },
		"$lt":       func(v *anyenc.Value) (Expr, error) { return parseCompare(CmpLt, v) },
		"$lte":      func(v *anyenc.Value) (Expr, error) { return parseCompare(CmpLte, v) },
		"$cmp":      func(v *anyenc.Value) (Expr, error) { return parseCompare(CmpCmp, v) },
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

// truthy is Mongo's expression boolean coercion: false, 0, null, and missing
// are false; everything else — including "" and empty arrays/objects — is true.
func truthy(v *anyenc.Value) bool {
	if v == nil {
		return false
	}
	switch v.Type() {
	case anyenc.TypeNull, anyenc.TypeFalse:
		return false
	case anyenc.TypeNumber:
		f, _ := v.Float64()
		return f != 0
	}
	return true
}

// CondExpr evaluates $cond in both spellings: [if, then, else] and
// {"if":..., "then":..., "else":...} (all three required). Only the taken
// branch is evaluated (lazy, Mongo semantics).
type CondExpr struct {
	If, Then, Else Expr
}

func parseCond(v *anyenc.Value) (Expr, error) {
	if v.Type() == anyenc.TypeObject {
		return parseCondObject(v)
	}
	args, err := parseOperands("$cond", v, 3, 3)
	if err != nil {
		return nil, err
	}
	return &CondExpr{If: args[0], Then: args[1], Else: args[2]}, nil
}

func parseCondObject(v *anyenc.Value) (Expr, error) {
	e := &CondExpr{}
	slots := map[string]*Expr{"if": &e.If, "then": &e.Then, "else": &e.Else}
	obj, _ := v.Object()
	var perr error
	obj.Visit(func(key []byte, item *anyenc.Value) {
		if perr != nil {
			return
		}
		slot, ok := slots[string(key)]
		if !ok {
			perr = atPath(&query.ParseError{
				Op:     "$cond",
				Reason: "unknown $cond parameter: " + string(key),
			}, string(key))
			return
		}
		if *slot != nil {
			perr = atPath(&query.ParseError{
				Op:     "$cond",
				Reason: "duplicate $cond parameter: " + string(key),
			}, string(key))
			return
		}
		sub, err := ParseExpr(item)
		if err != nil {
			perr = atPath(err, string(key))
			return
		}
		*slot = sub
	})
	if perr != nil {
		return nil, perr
	}
	if e.If == nil || e.Then == nil || e.Else == nil {
		return nil, &query.ParseError{
			Op:     "$cond",
			Reason: "$cond object form requires 'if', 'then' and 'else'",
		}
	}
	return e, nil
}

func (e *CondExpr) Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	c, err := e.If.Eval(a, doc)
	if err != nil {
		return nil, err
	}
	if truthy(c) {
		return e.Then.Eval(a, doc)
	}
	return e.Else.Eval(a, doc)
}

// String renders the canonical array spelling for both parsed forms.
func (e *CondExpr) String() string { return opString("$cond", []Expr{e.If, e.Then, e.Else}) }

// SwitchExpr evaluates $switch: cases run lazily in spec order and the first
// truthy case selects its then branch. No match with no default is a Mongo
// runtime error; with no per-document error channel the result is null
// instead (see docs/aggregation.md).
type SwitchExpr struct {
	Cases   []Expr
	Thens   []Expr
	Default Expr // nil: no default
}

func parseSwitch(v *anyenc.Value) (Expr, error) {
	if v.Type() != anyenc.TypeObject {
		return nil, &query.ParseError{
			Op:     "$switch",
			Reason: "$switch requires an object with a 'branches' array",
		}
	}
	e := &SwitchExpr{}
	obj, _ := v.Object()
	var (
		perr     error
		branches *anyenc.Value
	)
	obj.Visit(func(key []byte, item *anyenc.Value) {
		if perr != nil {
			return
		}
		switch string(key) {
		case "branches":
			branches = item
		case "default":
			sub, err := ParseExpr(item)
			if err != nil {
				perr = atPath(err, "default")
				return
			}
			e.Default = sub
		default:
			perr = atPath(&query.ParseError{
				Op:     "$switch",
				Reason: "unknown $switch parameter: " + string(key),
			}, string(key))
		}
	})
	if perr != nil {
		return nil, perr
	}
	if branches == nil || branches.Type() != anyenc.TypeArray {
		return nil, &query.ParseError{
			Op:     "$switch",
			Reason: "$switch requires a 'branches' array",
		}
	}
	items, _ := branches.Array()
	if len(items) == 0 {
		return nil, &query.ParseError{
			Op:     "$switch",
			Reason: "$switch requires at least one branch",
		}
	}
	e.Cases = make([]Expr, len(items))
	e.Thens = make([]Expr, len(items))
	for i, item := range items {
		caseE, thenE, err := parseSwitchBranch(item)
		if err != nil {
			return nil, atPath(atPath(err, strconv.Itoa(i)), "branches")
		}
		e.Cases[i], e.Thens[i] = caseE, thenE
	}
	return e, nil
}

func parseSwitchBranch(v *anyenc.Value) (caseE, thenE Expr, err error) {
	if v.Type() != anyenc.TypeObject {
		return nil, nil, &query.ParseError{
			Op:     "$switch",
			Reason: "$switch branch must be an object with 'case' and 'then'",
		}
	}
	obj, _ := v.Object()
	var perr error
	obj.Visit(func(key []byte, item *anyenc.Value) {
		if perr != nil {
			return
		}
		var slot *Expr
		switch string(key) {
		case "case":
			slot = &caseE
		case "then":
			slot = &thenE
		default:
			perr = atPath(&query.ParseError{
				Op:     "$switch",
				Reason: "unknown $switch branch parameter: " + string(key),
			}, string(key))
			return
		}
		if *slot != nil {
			perr = atPath(&query.ParseError{
				Op:     "$switch",
				Reason: "duplicate $switch branch parameter: " + string(key),
			}, string(key))
			return
		}
		sub, e := ParseExpr(item)
		if e != nil {
			perr = atPath(e, string(key))
			return
		}
		*slot = sub
	})
	if perr != nil {
		return nil, nil, perr
	}
	if caseE == nil || thenE == nil {
		return nil, nil, &query.ParseError{
			Op:     "$switch",
			Reason: "$switch branch requires 'case' and 'then'",
		}
	}
	return caseE, thenE, nil
}

func (e *SwitchExpr) Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	for i, c := range e.Cases {
		v, err := c.Eval(a, doc)
		if err != nil {
			return nil, err
		}
		if truthy(v) {
			return e.Thens[i].Eval(a, doc)
		}
	}
	if e.Default != nil {
		return e.Default.Eval(a, doc)
	}
	return a.NewNull(), nil
}

func (e *SwitchExpr) String() string {
	var b strings.Builder
	b.WriteString("{$switch:{branches:[")
	for i := range e.Cases {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString("{case:")
		b.WriteString(e.Cases[i].String())
		b.WriteString(",then:")
		b.WriteString(e.Thens[i].String())
		b.WriteByte('}')
	}
	b.WriteByte(']')
	if e.Default != nil {
		b.WriteString(",default:")
		b.WriteString(e.Default.String())
	}
	b.WriteString("}}")
	return b.String()
}

// IfNullExpr evaluates $ifNull (variadic, Mongo 4.4 form, at least 2
// operands): the first operand that is neither null nor missing, else the
// last operand's value verbatim. Evaluation is lazy left-to-right.
type IfNullExpr struct {
	Args []Expr
}

func parseIfNull(v *anyenc.Value) (Expr, error) {
	args, err := parseOperands("$ifNull", v, 2, 0)
	if err != nil {
		return nil, err
	}
	return &IfNullExpr{Args: args}, nil
}

func (e *IfNullExpr) Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	last := len(e.Args) - 1
	for _, arg := range e.Args[:last] {
		v, err := arg.Eval(a, doc)
		if err != nil {
			return nil, err
		}
		if v != nil && v.Type() != anyenc.TypeNull {
			return v, nil
		}
	}
	return e.Args[last].Eval(a, doc)
}

func (e *IfNullExpr) String() string { return opString("$ifNull", e.Args) }

// CompareOp identifies a comparison expression operator.
type CompareOp uint8

const (
	CmpEq CompareOp = iota
	CmpNe
	CmpGt
	CmpGte
	CmpLt
	CmpLte
	CmpCmp // three-way: -1/0/1
)

var compareOpNames = [...]string{"$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$cmp"}

func (op CompareOp) String() string { return compareOpNames[op] }

// CompareExpr evaluates $eq/$ne/$gt/$gte/$lt/$lte ($cmp: -1/0/1) over any two
// values in the engine's canonical cross-type order: bytes.Compare of the
// marshaled anyenc encoding — the exact order $sort and $min/$max use. The
// type tag leads the encoding, so types order by tag (null < number < string
// < false < true < array < object < ... < dateTime; differs from BSON's
// canonical order, see docs/aggregation.md); within a type the encoding is
// order-preserving: numbers via the sortable float encoding (-0 normalized to
// 0), strings bytewise, arrays elementwise, objects by marshaled bytes
// (field-order-sensitive, consistent with $group key equality). $eq is
// c == 0, so all seven operators agree by construction. A missing operand
// marshals as the null tag: missing == null. Operands marshal into two
// reusable per-expr scratch buffers (expressions are per-pipeline,
// single-goroutine): alloc-free after warm-up.
type CompareExpr struct {
	Op   CompareOp
	A, B Expr

	bufA, bufB []byte // scratch, reused across Eval calls
}

func parseCompare(op CompareOp, v *anyenc.Value) (Expr, error) {
	args, err := parseOperands(op.String(), v, 2, 2)
	if err != nil {
		return nil, err
	}
	return &CompareExpr{Op: op, A: args[0], B: args[1]}, nil
}

func (e *CompareExpr) Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	av, err := e.A.Eval(a, doc)
	if err != nil {
		return nil, err
	}
	bv, err := e.B.Eval(a, doc)
	if err != nil {
		return nil, err
	}
	e.bufA = av.MarshalTo(e.bufA[:0]) // nil receiver marshals the null tag
	e.bufB = bv.MarshalTo(e.bufB[:0])
	c := bytes.Compare(e.bufA, e.bufB)
	switch e.Op {
	case CmpEq:
		return a.NewBool(c == 0), nil
	case CmpNe:
		return a.NewBool(c != 0), nil
	case CmpGt:
		return a.NewBool(c > 0), nil
	case CmpGte:
		return a.NewBool(c >= 0), nil
	case CmpLt:
		return a.NewBool(c < 0), nil
	case CmpLte:
		return a.NewBool(c <= 0), nil
	default: // CmpCmp
		return a.NewNumberInt(c), nil
	}
}

func (e *CompareExpr) String() string { return opString(e.Op.String(), []Expr{e.A, e.B}) }
