package aggregate

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

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
		"$add":         func(v *anyenc.Value) (Expr, error) { return parseArith(OpAdd, v) },
		"$subtract":    func(v *anyenc.Value) (Expr, error) { return parseArith(OpSubtract, v) },
		"$multiply":    func(v *anyenc.Value) (Expr, error) { return parseArith(OpMultiply, v) },
		"$divide":      func(v *anyenc.Value) (Expr, error) { return parseArith(OpDivide, v) },
		"$abs":         parseAbs,
		"$round":       parseRound,
		"$concat":      parseConcat,
		"$replaceOne":  parseReplaceOne,
		"$replaceAll":  parseReplaceAll,
		"$split":       parseSplit,
		"$trim":        func(v *anyenc.Value) (Expr, error) { return parseTrim(TrimBoth, v) },
		"$ltrim":       func(v *anyenc.Value) (Expr, error) { return parseTrim(TrimLeft, v) },
		"$rtrim":       func(v *anyenc.Value) (Expr, error) { return parseTrim(TrimRight, v) },
		"$strLenBytes": func(v *anyenc.Value) (Expr, error) { return parseStrLen(false, v) },
		"$strLenCP":    func(v *anyenc.Value) (Expr, error) { return parseStrLen(true, v) },
		"$size":        parseSize,
		"$cond":        parseCond,
		"$switch":      parseSwitch,
		"$ifNull":      parseIfNull,
		"$eq":          func(v *anyenc.Value) (Expr, error) { return parseCompare(CmpEq, v) },
		"$ne":          func(v *anyenc.Value) (Expr, error) { return parseCompare(CmpNe, v) },
		"$gt":          func(v *anyenc.Value) (Expr, error) { return parseCompare(CmpGt, v) },
		"$gte":         func(v *anyenc.Value) (Expr, error) { return parseCompare(CmpGte, v) },
		"$lt":          func(v *anyenc.Value) (Expr, error) { return parseCompare(CmpLt, v) },
		"$lte":         func(v *anyenc.Value) (Expr, error) { return parseCompare(CmpLte, v) },
		"$cmp":         func(v *anyenc.Value) (Expr, error) { return parseCompare(CmpCmp, v) },
		"$dateAdd":     parseDateAdd,
		"$dateDiff":    parseDateDiff,
		"$dateTrunc":   parseDateTrunc,
		"$year":        func(v *anyenc.Value) (Expr, error) { return parseDatePart(partYear, v) },
		"$week":        func(v *anyenc.Value) (Expr, error) { return parseDatePart(partWeek, v) },
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

// parseParamObject fills slots from an object-form operator spec: each key
// parses into its slot, unknown and duplicate keys are structured errors.
// Required-slot checks stay with the caller.
func parseParamObject(op string, v *anyenc.Value, slots map[string]*Expr) error {
	obj, _ := v.Object()
	var perr error
	obj.Visit(func(key []byte, item *anyenc.Value) {
		if perr != nil {
			return
		}
		slot, ok := slots[string(key)]
		if !ok {
			perr = atPath(&query.ParseError{
				Op:     op,
				Reason: "unknown " + op + " parameter: " + string(key),
			}, string(key))
			return
		}
		if *slot != nil {
			perr = atPath(&query.ParseError{
				Op:     op,
				Reason: "duplicate " + op + " parameter: " + string(key),
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
	return perr
}

// evalNumber evaluates sub as a numeric operand. ok=false means the operator
// result is null: the operand is missing, null, or non-numeric. Mongo raises
// a runtime error for non-numeric operands; streaming eval has no
// per-document error channel, so null stands in (see docs/aggregation.md).
// Date arithmetic never comes through here: $add/$subtract classify dateTime
// operands themselves (evalAdd/evalSubtract).
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

// ArithExpr evaluates $add/$subtract/$multiply/$divide over numeric operands,
// plus Mongo's date arithmetic: $add with exactly one dateTime operand shifts
// it by the numeric sum of millis, and $subtract handles [date, date] →
// millis and [date, number] → date. Any other mix, a missing/null/non-numeric
// operand, division by zero, overflow and a non-finite result make the result
// null (Mongo errors for these; see evalNumber). An empty operand list yields
// the operator's identity, Mongo semantics ($add 0, $multiply 1).
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
	switch {
	case len(e.Args) == 0:
		if e.Op == OpMultiply {
			return a.NewNumberFloat64(1), nil
		}
		return a.NewNumberFloat64(0), nil // $add
	case e.Op == OpAdd:
		return e.evalAdd(a, doc)
	case e.Op == OpSubtract:
		return e.evalSubtract(a, doc)
	}
	// $multiply/$divide: numeric operands only. The first operand seeds the
	// accumulator, so one loop serves both the variadic and binary forms.
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
		if e.Op == OpDivide {
			if f == 0 {
				return a.NewNull(), nil
			}
			acc /= f
		} else {
			acc *= f
		}
	}
	return newNumber(a, acc), nil
}

// evalAdd sums numeric operands; at most one dateTime operand shifts by that
// sum of millis (fraction truncated). Two dateTimes, any other non-numeric
// operand and overflow → null.
func (e *ArithExpr) evalAdd(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	var (
		acc    float64
		dateMs int64
		dates  int
	)
	for _, arg := range e.Args {
		v, err := arg.Eval(a, doc)
		if err != nil {
			return nil, err
		}
		if v != nil && v.Type() == anyenc.TypeDateTime {
			if dates++; dates > 1 {
				return a.NewNull(), nil
			}
			dateMs, _ = v.DateTimeMillis()
			continue
		}
		if v == nil || v.Type() != anyenc.TypeNumber {
			return a.NewNull(), nil
		}
		f, _ := v.Float64()
		acc += f
	}
	if dates == 0 {
		return newNumber(a, acc), nil
	}
	res, ok := shiftMillis(dateMs, acc)
	if !ok {
		return a.NewNull(), nil
	}
	return a.NewDateTimeMillis(res), nil
}

// evalSubtract: [num, num] → number, [date, date] → millis number,
// [date, num] → shifted date; [num, date] and any other mix → null (Mongo
// errors).
func (e *ArithExpr) evalSubtract(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	av, err := e.Args[0].Eval(a, doc)
	if err != nil {
		return nil, err
	}
	bv, err := e.Args[1].Eval(a, doc)
	if err != nil {
		return nil, err
	}
	aDate := av != nil && av.Type() == anyenc.TypeDateTime
	switch {
	case aDate && bv != nil && bv.Type() == anyenc.TypeDateTime:
		ams, _ := av.DateTimeMillis()
		bms, _ := bv.DateTimeMillis()
		return newNumber(a, float64(ams)-float64(bms)), nil
	case aDate:
		if bv == nil || bv.Type() != anyenc.TypeNumber {
			return a.NewNull(), nil
		}
		ams, _ := av.DateTimeMillis()
		f, _ := bv.Float64()
		res, ok := shiftMillis(ams, -f)
		if !ok {
			return a.NewNull(), nil
		}
		return a.NewDateTimeMillis(res), nil
	default:
		if av == nil || av.Type() != anyenc.TypeNumber ||
			bv == nil || bv.Type() != anyenc.TypeNumber {
			return a.NewNull(), nil
		}
		af, _ := av.Float64()
		bf, _ := bv.Float64()
		return newNumber(a, af-bf), nil
	}
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

// ReplaceOneExpr evaluates $replaceOne: the first occurrence of find in input
// replaced by replacement; no occurrence leaves input unchanged. Object form
// only, all three parameters required (Mongo). A null or missing operand → null
// (Mongo); a non-string operand → null too (Mongo errors; same rationale as
// evalNumber — regex find is likewise out: no regex value type). An empty find
// matches at position 0, prepending the replacement (Mongo behavior, its docs
// leave the case unstated). The splice goes through a reusable scratch buffer
// (per-pipeline, single-goroutine): alloc-free in steady state.
type ReplaceOneExpr struct {
	Input, Find, Replacement Expr

	buf []byte // scratch, reused across Eval calls
}

// parseReplaceParams parses the shared $replaceOne/$replaceAll object form:
// all three parameters required, unknown/duplicate keys rejected.
func parseReplaceParams(op string, v *anyenc.Value) (input, find, repl Expr, err error) {
	if v.Type() != anyenc.TypeObject {
		return nil, nil, nil, &query.ParseError{
			Op:     op,
			Reason: op + " requires an object with 'input', 'find' and 'replacement'",
		}
	}
	slots := map[string]*Expr{"input": &input, "find": &find, "replacement": &repl}
	if err = parseParamObject(op, v, slots); err != nil {
		return nil, nil, nil, err
	}
	if input == nil || find == nil || repl == nil {
		return nil, nil, nil, &query.ParseError{
			Op:     op,
			Reason: op + " requires 'input', 'find' and 'replacement'",
		}
	}
	return input, find, repl, nil
}

func parseReplaceOne(v *anyenc.Value) (Expr, error) {
	e := &ReplaceOneExpr{}
	var err error
	if e.Input, e.Find, e.Replacement, err = parseReplaceParams("$replaceOne", v); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *ReplaceOneExpr) Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	in, err := e.Input.Eval(a, doc)
	if err != nil {
		return nil, err
	}
	fv, err := e.Find.Eval(a, doc)
	if err != nil {
		return nil, err
	}
	rv, err := e.Replacement.Eval(a, doc)
	if err != nil {
		return nil, err
	}
	if in == nil || in.Type() != anyenc.TypeString ||
		fv == nil || fv.Type() != anyenc.TypeString ||
		rv == nil || rv.Type() != anyenc.TypeString {
		return a.NewNull(), nil
	}
	input, find := in.GetStringBytes(), fv.GetStringBytes()
	idx := bytes.Index(input, find)
	if idx < 0 {
		return in, nil // no occurrence: input unchanged
	}
	e.buf = append(e.buf[:0], input[:idx]...)
	e.buf = append(e.buf, rv.GetStringBytes()...)
	e.buf = append(e.buf, input[idx+len(find):]...)
	return a.NewStringBytes(e.buf), nil // NewStringBytes copies
}

func (e *ReplaceOneExpr) String() string {
	return "{$replaceOne:{input:" + e.Input.String() + ",find:" + e.Find.String() +
		",replacement:" + e.Replacement.String() + "}}"
}

// ReplaceAllExpr evaluates $replaceAll: every occurrence of find in input
// replaced by replacement, left to right, non-overlapping; replaced regions are
// not rescanned. Same contract as ReplaceOneExpr otherwise: object form, all
// three required, null/missing or non-string operand → null. An empty find
// matches at position 0 once, prepending the replacement — same pin as
// $replaceOne (Mongo docs leave the case unstated; a per-position match would
// loop).
type ReplaceAllExpr struct {
	Input, Find, Replacement Expr

	buf []byte // scratch, reused across Eval calls
}

func parseReplaceAll(v *anyenc.Value) (Expr, error) {
	e := &ReplaceAllExpr{}
	var err error
	if e.Input, e.Find, e.Replacement, err = parseReplaceParams("$replaceAll", v); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *ReplaceAllExpr) Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	in, err := e.Input.Eval(a, doc)
	if err != nil {
		return nil, err
	}
	fv, err := e.Find.Eval(a, doc)
	if err != nil {
		return nil, err
	}
	rv, err := e.Replacement.Eval(a, doc)
	if err != nil {
		return nil, err
	}
	if in == nil || in.Type() != anyenc.TypeString ||
		fv == nil || fv.Type() != anyenc.TypeString ||
		rv == nil || rv.Type() != anyenc.TypeString {
		return a.NewNull(), nil
	}
	input, find, rep := in.GetStringBytes(), fv.GetStringBytes(), rv.GetStringBytes()
	if len(find) == 0 {
		e.buf = append(e.buf[:0], rep...)
		e.buf = append(e.buf, input...)
		return a.NewStringBytes(e.buf), nil
	}
	idx := bytes.Index(input, find)
	if idx < 0 {
		return in, nil // no occurrence: input unchanged
	}
	e.buf = e.buf[:0]
	for idx >= 0 {
		e.buf = append(e.buf, input[:idx]...)
		e.buf = append(e.buf, rep...)
		input = input[idx+len(find):]
		idx = bytes.Index(input, find)
	}
	e.buf = append(e.buf, input...)
	return a.NewStringBytes(e.buf), nil // NewStringBytes copies
}

func (e *ReplaceAllExpr) String() string {
	return "{$replaceAll:{input:" + e.Input.String() + ",find:" + e.Find.String() +
		",replacement:" + e.Replacement.String() + "}}"
}

// SplitExpr evaluates $split [string, delimiter]: the array of substrings
// between delimiter occurrences — adjacent delimiters produce empty strings,
// no occurrence yields the whole input as a one-element array. The delimiter
// must be a non-empty string: an empty literal delimiter is a parse error and
// a delimiter expression evaluating to "" → null (Mongo errors at runtime;
// no-error-channel policy — likewise a null/missing or non-string operand,
// where Mongo errors for non-strings; regex delimiters are out: no regex value
// type). Elements and the result array are arena-allocated per eval:
// alloc-free in steady state.
type SplitExpr struct {
	Input, Delim Expr
}

func parseSplit(v *anyenc.Value) (Expr, error) {
	args, err := parseOperands("$split", v, 2, 2)
	if err != nil {
		return nil, err
	}
	if lit, ok := args[1].(*LiteralExpr); ok &&
		lit.v.Type() == anyenc.TypeString && len(lit.v.GetStringBytes()) == 0 {
		return nil, &query.ParseError{
			Op:     "$split",
			Reason: "$split delimiter must be a non-empty string",
		}
	}
	return &SplitExpr{Input: args[0], Delim: args[1]}, nil
}

func (e *SplitExpr) Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	in, err := e.Input.Eval(a, doc)
	if err != nil {
		return nil, err
	}
	dv, err := e.Delim.Eval(a, doc)
	if err != nil {
		return nil, err
	}
	if in == nil || in.Type() != anyenc.TypeString ||
		dv == nil || dv.Type() != anyenc.TypeString {
		return a.NewNull(), nil
	}
	input, sep := in.GetStringBytes(), dv.GetStringBytes()
	if len(sep) == 0 {
		return a.NewNull(), nil // expression-produced empty delimiter
	}
	out := a.NewArray()
	for n := 0; ; n++ {
		idx := bytes.Index(input, sep)
		if idx < 0 {
			out.SetArrayItem(n, a.NewStringBytes(input))
			return out, nil
		}
		out.SetArrayItem(n, a.NewStringBytes(input[:idx]))
		input = input[idx+len(sep):]
	}
}

func (e *SplitExpr) String() string { return opString("$split", []Expr{e.Input, e.Delim}) }

// StrLenExpr evaluates $strLenBytes/$strLenCP: the length of the string
// operand in UTF-8 bytes or code points. A missing, null, or non-string
// operand → null (Mongo errors for non-strings; no-error-channel policy, see
// evalNumber).
type StrLenExpr struct {
	CP  bool // count code points ($strLenCP) instead of bytes ($strLenBytes)
	Arg Expr
}

func parseStrLen(cp bool, v *anyenc.Value) (Expr, error) {
	e := &StrLenExpr{CP: cp}
	args, err := parseOperands(e.op(), v, 1, 1)
	if err != nil {
		return nil, err
	}
	e.Arg = args[0]
	return e, nil
}

func (e *StrLenExpr) op() string {
	if e.CP {
		return "$strLenCP"
	}
	return "$strLenBytes"
}

func (e *StrLenExpr) Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	v, err := e.Arg.Eval(a, doc)
	if err != nil {
		return nil, err
	}
	if v == nil || v.Type() != anyenc.TypeString {
		return a.NewNull(), nil
	}
	b := v.GetStringBytes()
	if e.CP {
		return a.NewNumberInt(utf8.RuneCount(b)), nil
	}
	return a.NewNumberInt(len(b)), nil
}

func (e *StrLenExpr) String() string { return opString(e.op(), []Expr{e.Arg}) }

// SizeExpr evaluates $size: the number of elements in its array operand. A
// missing, null, or non-array operand → null (Mongo errors; no-error-channel
// policy, see evalNumber).
type SizeExpr struct{ Arg Expr }

func parseSize(v *anyenc.Value) (Expr, error) {
	args, err := parseOperands("$size", v, 1, 1)
	if err != nil {
		return nil, err
	}
	return &SizeExpr{Arg: args[0]}, nil
}

func (e *SizeExpr) Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	v, err := e.Arg.Eval(a, doc)
	if err != nil {
		return nil, err
	}
	if v == nil || v.Type() != anyenc.TypeArray {
		return a.NewNull(), nil
	}
	items, _ := v.Array()
	return a.NewNumberInt(len(items)), nil
}

func (e *SizeExpr) String() string { return opString("$size", []Expr{e.Arg}) }

// TrimMode selects which side(s) $trim/$ltrim/$rtrim strip.
type TrimMode uint8

const (
	TrimBoth TrimMode = iota
	TrimLeft
	TrimRight
)

var trimModeNames = [...]string{"$trim", "$ltrim", "$rtrim"}

func (m TrimMode) String() string { return trimModeNames[m] }

// isTrimWhitespace is Mongo's documented default $trim set — exactly the code
// points its manual lists (U+0000, U+0009–U+000D, U+0020, U+00A0, U+1680,
// U+2000–U+200A), not Unicode White_Space: U+2028/2029/202F/205F/3000/FEFF are
// not stripped.
func isTrimWhitespace(r rune) bool {
	switch r {
	case 0x0000, 0x0009, 0x000A, 0x000B, 0x000C, 0x000D, 0x0020, 0x00A0, 0x1680:
		return true
	}
	return r >= 0x2000 && r <= 0x200A
}

// TrimExpr evaluates $trim/$ltrim/$rtrim {input, chars?}: strips the leading
// and/or trailing code points that appear in chars — whole runes, so a
// multibyte member never shreds mid-character. Absent chars means the default
// whitespace set (isTrimWhitespace); chars "" is an empty set and trims
// nothing. A null/missing input, or a non-string input or chars → null (Mongo
// errors for non-strings; no-error-channel policy). The chars set decodes into
// a reusable rune scratch: alloc-free in steady state.
type TrimExpr struct {
	Mode  TrimMode
	Input Expr
	Chars Expr // nil: default whitespace set

	set []rune // chars scratch, reused across Eval calls
}

func parseTrim(mode TrimMode, v *anyenc.Value) (Expr, error) {
	op := mode.String()
	if v.Type() != anyenc.TypeObject {
		return nil, &query.ParseError{
			Op:     op,
			Reason: op + " requires an object with 'input' and optional 'chars'",
		}
	}
	e := &TrimExpr{Mode: mode}
	if err := parseParamObject(op, v, map[string]*Expr{"input": &e.Input, "chars": &e.Chars}); err != nil {
		return nil, err
	}
	if e.Input == nil {
		return nil, &query.ParseError{Op: op, Reason: op + " requires 'input'"}
	}
	return e, nil
}

// trims reports whether r is stripped: default whitespace set without chars,
// the decoded chars set otherwise (linear scan — the set is small).
func (e *TrimExpr) trims(r rune) bool {
	if e.Chars == nil {
		return isTrimWhitespace(r)
	}
	for _, c := range e.set {
		if c == r {
			return true
		}
	}
	return false
}

func (e *TrimExpr) Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	in, err := e.Input.Eval(a, doc)
	if err != nil {
		return nil, err
	}
	if in == nil || in.Type() != anyenc.TypeString {
		return a.NewNull(), nil
	}
	if e.Chars != nil {
		cv, err := e.Chars.Eval(a, doc)
		if err != nil {
			return nil, err
		}
		if cv == nil || cv.Type() != anyenc.TypeString {
			return a.NewNull(), nil
		}
		e.set = e.set[:0]
		for b := cv.GetStringBytes(); len(b) > 0; {
			r, size := utf8.DecodeRune(b)
			e.set = append(e.set, r)
			b = b[size:]
		}
	}
	input := in.GetStringBytes()
	s := input
	if e.Mode != TrimRight {
		for len(s) > 0 {
			r, size := utf8.DecodeRune(s)
			if !e.trims(r) {
				break
			}
			s = s[size:]
		}
	}
	if e.Mode != TrimLeft {
		for len(s) > 0 {
			r, size := utf8.DecodeLastRune(s)
			if !e.trims(r) {
				break
			}
			s = s[:len(s)-size]
		}
	}
	if len(s) == len(input) {
		return in, nil // nothing stripped: input unchanged
	}
	return a.NewStringBytes(s), nil
}

func (e *TrimExpr) String() string {
	s := "{" + e.Mode.String() + ":{input:" + e.Input.String()
	if e.Chars != nil {
		s += ",chars:" + e.Chars.String()
	}
	return s + "}}"
}

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
	if err := parseParamObject("$cond", v, slots); err != nil {
		return nil, err
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
