package query

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/parser"
)

// ErrVectorNotOrderable is returned when an ordering operator ($gt/$gte/$lt/$lte)
// is given a vector operand. Vectors are not points on the scalar order (Rule
// V): the anyenc tag order sorts every vector above every scalar, which made
// such a comparison true for every document — emptying the collection on Delete.
// Equality ($eq/$ne) against a vector remains legal: it is byte equality.
var ErrVectorNotOrderable = errors.New("any-store: a vector is not orderable")

type Operator uint8

const (
	opAnd Operator = iota
	opOr
	opNor
	opText

	_opVal
	opNe
	opEq
	opGt
	opGte
	opLt
	opLte

	opIn
	opNin
	opAll
	opNot
	opExists
	opType
	opRegexp
	opSize

	// opKnn is appended at the END of the block deliberately: it stays > _opVal
	// (field-level for free via isTopLevel) and shifts no existing iota value.
	opKnn
)

var opBytesPrefix = []byte("$")

func MustParseCondition(cond any) Filter {
	f, err := ParseCondition(cond)
	if err != nil {
		panic(err)
	}
	return f
}

func ParseCondition(cond any) (Filter, error) {
	if cond == nil {
		return All{}, nil
	}
	if f, ok := cond.(Filter); ok {
		return f, nil
	}

	v, err := parser.Parse(cond)
	if err != nil {
		return nil, err
	}
	return parseAnd(v)
}

func parseAndArray(v *anyenc.Value) (f Filter, err error) {
	if v.Type() != anyenc.TypeArray {
		return nil, &ParseError{Op: "$and", Reason: "$and must be an array"}
	}
	arr, _ := v.Array()
	var fs And
	if len(arr) > 1 {
		fs = make(And, 0, len(arr))
	}
	for i, el := range arr {
		if f, err = parseAnd(el); err != nil {
			return nil, atPath(err, strconv.Itoa(i))
		}
		if fs != nil {
			fs = append(fs, f)
		}
	}
	if fs != nil {
		return &fs, nil
	}
	return
}

func parseOrArray(v *anyenc.Value) (f Filter, err error) {
	if v.Type() != anyenc.TypeArray {
		return nil, &ParseError{Op: "$or", Reason: "$or must be an array"}
	}
	arr, _ := v.Array()
	var fs Or
	if len(arr) > 1 {
		fs = make(Or, 0, len(arr))
	}
	for i, el := range arr {
		if f, err = parseAnd(el); err != nil {
			return nil, atPath(err, strconv.Itoa(i))
		}
		if fs != nil {
			fs = append(fs, f)
		}
	}
	if fs != nil {
		return fs, nil
	}
	return
}

func parseNorArray(v *anyenc.Value) (f Filter, err error) {
	if v.Type() != anyenc.TypeArray {
		// The message used to say "$or must be an array" — a copy-paste slip
		// that misdirected anyone debugging a bad $nor.
		return nil, &ParseError{Op: "$nor", Reason: "$nor must be an array"}
	}
	arr, _ := v.Array()
	fs := make(Nor, 0, len(arr))
	for i, el := range arr {
		if f, err = parseAnd(el); err != nil {
			return nil, atPath(err, strconv.Itoa(i))
		}
		fs = append(fs, f)
	}
	return fs, nil
}

func parseAnd(val *anyenc.Value) (res Filter, err error) {
	if val.Type() != anyenc.TypeObject {
		return nil, &ParseError{Reason: "query filter must be an object"}
	}
	obj, _ := val.Object()
	var fs And
	var f Filter
	if obj.Len() > 1 {
		fs = make(And, 0, obj.Len())
	}
	var (
		isOp bool
		op   Operator
	)
	obj.Visit(func(key []byte, v *anyenc.Value) {
		if err != nil {
			return
		}
		isOp, op, err = isOperator(key)
		if err != nil {
			err = atPath(err, string(key))
			return
		}
		if isOp {
			if !isTopLevel(op) {
				// isOperator has already recognized the key, so this fires for
				// a KNOWN operator in a position that does not accept it (e.g.
				// {"$eq": 1}) — a distinct fault from an unrecognized token,
				// deliberately not wrapping ErrUnknownOperator.
				err = atPath(&ParseError{
					Op:     string(key),
					Reason: "operator " + string(key) + " is not valid at the top level",
				}, string(key))
				return
			}

			switch op {
			case opAnd:
				if f, err = parseAndArray(v); err != nil {
					err = atPath(err, string(key))
					return
				}
				if fs != nil {
					fs = append(fs, f)
				}
			case opOr:
				if f, err = parseOrArray(v); err != nil {
					err = atPath(err, string(key))
					return
				}
				if fs != nil {
					fs = append(fs, f)
				}
			case opNor:
				if f, err = parseNorArray(v); err != nil {
					err = atPath(err, string(key))
					return
				}
				if fs != nil {
					fs = append(fs, f)
				}
			case opText:
				if f, err = parseText(v); err != nil {
					err = atPath(err, string(key))
					return
				}
				if fs != nil {
					fs = append(fs, f)
				}
			default:
				panic(fmt.Errorf("unexpected top level operator: %v", string(key)))
			}
		} else {
			if f, err = parseComp(string(key), v); err != nil {
				err = atPath(err, string(key))
				return
			}
			if fs != nil {
				fs = append(fs, f)
			}
		}
	})
	if err != nil {
		return nil, err
	}
	if fs != nil {
		return fs, nil
	}
	return f, nil
}

func parseComp(key string, v *anyenc.Value) (f Filter, err error) {
	var fk Key
	if strings.Contains(key, ".") {
		fk.Path = strings.Split(key, ".")
	} else {
		fk.Path = []string{key}
	}
	if v.Type() == anyenc.TypeObject {
		if fk.Filter, err = parseCompObj(v); err != nil {
			return nil, err
		}
	} else {
		eq := &Comp{}
		eq.EqValue = v.MarshalTo(nil)
		eq.notArray = v.Type() != anyenc.TypeArray
		fk.Filter = eq
	}
	return fk, nil
}

func parseCompObj(v *anyenc.Value) (Filter, error) {
	hasCompOp, f, err := parseCompObjOp(v)
	if err != nil {
		return nil, err
	}
	if hasCompOp {
		return f, nil
	} else {
		cmp := &Comp{}
		cmp.EqValue = v.MarshalTo(nil)
		cmp.CompOp = CompOpEq
		cmp.notArray = v.Type() != anyenc.TypeArray
		return cmp, nil
	}
}

func parseCompObjOp(val *anyenc.Value) (ok bool, f Filter, err error) {
	obj, e := val.Object()
	if e != nil {
		return false, nil, &ParseError{Reason: "expected an object of operators"}
	}
	var (
		isOp     bool
		op       Operator
		hasNonOp bool
		hasKnn   bool
	)

	var fs And
	if obj.Len() > 1 {
		fs = make(And, 0, obj.Len())
	}

	obj.Visit(func(key []byte, v *anyenc.Value) {
		if err != nil {
			return
		}
		isOp, op, err = isOperator(key)
		if err != nil {
			err = atPath(err, string(key))
			return
		}
		if isOp {
			if isTopLevel(op) {
				err = atPath(&ParseError{
					Op:     string(key),
					Reason: "operator " + string(key) + " is not valid in a field condition",
				}, string(key))
				return
			}
			if hasNonOp {
				err = atPath(&ParseError{
					Op:     string(key),
					Reason: "mixed operators and values",
				}, string(key))
				return
			}
			ok = true
			if op == opKnn {
				hasKnn = true
			}
			if f, err = makeCompFilter(op, v); err != nil {
				err = atPath(err, string(key))
				return
			}
			if fs != nil {
				fs = append(fs, f)
			}
		} else {
			hasNonOp = true
			if ok {
				err = atPath(&ParseError{
					Reason: "mixed operators and values",
				}, string(key))
				return
			}
		}
	})
	if err != nil {
		return false, nil, err
	}
	// A $knn is a ranked SOURCE, not a predicate: And{Knn, Comp} on one field
	// would leave the Comp as a residual on a field the source already owns,
	// with fail-closed Knn.Ok lurking under any evaluator that sees the pair.
	// One JSON object, one operator. (Programmatic And{Key{f,Knn},Key{f,Comp}}
	// stays legal — the residual builder strips by node identity.)
	if hasKnn && obj.Len() > 1 {
		return false, nil, &ParseError{Op: "$knn", Reason: "$knn must be the only operator on its field"}
	}
	if hasNonOp {
		return false, nil, nil
	}
	if !ok {
		// No $-operator keys (e.g. the empty object {}): treat as an
		// equality match against the whole object value, like MongoDB.
		return false, nil, nil
	}
	if fs != nil {
		return true, fs, nil
	}
	return true, f, nil
}

func makeCompFilter(op Operator, v *anyenc.Value) (f Filter, err error) {
	// Rule V at the door: an ordering op against a vector operand is
	// decidable syntactically, so reject it here rather than silently evaluating
	// to false. This also stops it reflecting through $not, where a
	// never-satisfiable inner predicate becomes match-all. Comp.Ok keeps the
	// eval-time guard for hand-built filters, which never see the parser.
	switch op {
	case opGt, opGte, opLt, opLte:
		if v.Type() == anyenc.TypeVectorF32 {
			return nil, &ParseError{
				Op:      opName(op),
				Reason:  ErrVectorNotOrderable.Error() + ": use $eq for exact match, or a $vector index for similarity search",
				wrapped: ErrVectorNotOrderable,
			}
		}
	}
	switch op {
	case opEq:
		cmp := &Comp{}
		cmp.EqValue = v.MarshalTo(nil)
		cmp.CompOp = CompOpEq
		cmp.notArray = v.Type() != anyenc.TypeArray
		return cmp, nil
	case opNe:
		cmp := &Comp{}
		cmp.EqValue = v.MarshalTo(nil)
		cmp.CompOp = CompOpNe
		cmp.notArray = v.Type() != anyenc.TypeArray
		return cmp, nil
	case opGt:
		cmp := &Comp{}
		cmp.EqValue = v.MarshalTo(nil)
		cmp.CompOp = CompOpGt
		cmp.notArray = v.Type() != anyenc.TypeArray
		return cmp, nil
	case opGte:
		cmp := &Comp{}
		cmp.EqValue = v.MarshalTo(nil)
		cmp.CompOp = CompOpGte
		cmp.notArray = v.Type() != anyenc.TypeArray
		return cmp, nil
	case opLt:
		cmp := &Comp{}
		cmp.EqValue = v.MarshalTo(nil)
		cmp.CompOp = CompOpLt
		cmp.notArray = v.Type() != anyenc.TypeArray
		return cmp, nil
	case opLte:
		cmp := &Comp{}
		cmp.EqValue = v.MarshalTo(nil)
		cmp.CompOp = CompOpLte
		cmp.notArray = v.Type() != anyenc.TypeArray
		return cmp, nil
	case opNot:
		var isOp bool
		not := Not{}
		if isOp, not.Filter, err = parseCompObjOp(v); err != nil {
			// No extra wrapping: the caller's visitor prefixes "$not" onto the
			// ParseError path, which locates the failure better than the old
			// "%w for operator $not" suffix did.
			return nil, err
		}
		if !isOp {
			return nil, &ParseError{Op: "$not", Reason: "no operators found for $not"}
		}
		// A Knn under Not would evaluate !false == match-all (fail-closed Ok
		// reflected). Unrepresentable, at the door.
		if ContainsKnn(not.Filter) {
			return nil, &ParseError{Op: "$knn", Reason: "$knn is not allowed under $not"}
		}
		return not, nil
	case opExists:
		return parseExists(v)
	case opType:
		return parseType(v)
	case opRegexp:
		return parseRegexp(v)
	case opSize:
		return parseSize(v)
	case opKnn:
		// This arm is critical: without it a $knn value would fall through to
		// makeArrComp, whose default arm panics on an unrecognized op.
		return parseKnn(v)
	default:
		return makeArrComp(op, v)
	}
}

// parseKnn parses the {"$knn":{...}} options object. Exactly one form is
// accepted; every rejection is a parse error with a normative message. All of
// these rules are RE-CHECKED at detection time (query build), because the two
// production consumers construct query.Knn programmatically and never pass
// through this parser.
func parseKnn(v *anyenc.Value) (Filter, error) {
	if v.Type() != anyenc.TypeObject {
		// NOTE: a sole-key {"$vector":[…]} object cannot even reach here as an
		// object — anyenc decodes it into a TypeVectorF32 VALUE before the
		// parser runs. That extjson landmine is why the payload key is $query.
		return nil, &ParseError{Op: "$knn", Reason: `$knn must be an object, e.g. {"$knn":{"$query":[...],"$k":10}}`}
	}
	obj, _ := v.Object()
	var (
		kn       Knn
		hasQuery bool
		hasK     bool
		perr     error
	)
	obj.Visit(func(key []byte, val *anyenc.Value) {
		if perr != nil {
			return
		}
		// fail records a structured rejection located at this $knn sub-key.
		fail := func(reason string) {
			perr = atPath(&ParseError{Op: "$knn", Reason: reason}, string(key))
		}
		switch string(key) {
		case "$query":
			var ok bool
			kn.Query, ok = anyenc.AppendFloat32s(val.MarshalTo(nil), nil)
			if !ok {
				fail(`$knn: $query must be an array of numbers or {"$vector":[...]}`)
				return
			}
			hasQuery = true
		case "$k":
			n, e := val.Int()
			if e != nil || val.Type() != anyenc.TypeNumber || float64(n) != val.GetFloat64() || n < 1 || n > KnnMaxK {
				fail(fmt.Sprintf("$knn: $k must be an integer in [1, %d], got %v", KnnMaxK, val))
				return
			}
			kn.K = n
			hasK = true
		case "$ef":
			n, e := val.Int()
			if e != nil || val.Type() != anyenc.TypeNumber || float64(n) != val.GetFloat64() || n > KnnMaxEf {
				fail(fmt.Sprintf("$knn: $ef must be an integer in [$k, %d], got %v", KnnMaxEf, val))
				return
			}
			kn.Ef = n
		case "$index":
			sb, e := val.StringBytes()
			if e != nil {
				fail("$knn: $index must be a string")
				return
			}
			kn.Index = string(sb)
		case "$vector":
			fail(`unknown $knn field: $vector (did you mean "$query"? $vector is the value-type wrapper: {"$query":{"$vector":[...]}})`)
		case "$maxDistance", "$minScore", "$prefilter", "$nprobe":
			// Reserved so adding them later is non-breaking; rejected in v1.
			fail(fmt.Sprintf("$knn: %s is reserved and not supported", string(key)))
		default:
			fail(fmt.Sprintf("unknown $knn field: %s", string(key)))
		}
	})
	if perr != nil {
		return nil, perr
	}
	if !hasQuery {
		return nil, &ParseError{Op: "$knn", Reason: "$knn requires $query"}
	}
	if !hasK {
		return nil, &ParseError{Op: "$knn", Reason: "$knn requires $k (the number of neighbours to select)"}
	}
	// Range/finiteness rules live in ONE place, shared with the executor's
	// detection walk (which validates programmatic NewKnn filters).
	if err := kn.Validate(); err != nil {
		return nil, &ParseError{Op: "$knn", Reason: err.Error(), wrapped: err}
	}
	return kn, nil
}

// parseText parses the $text predicate object into a Text filter:
//
//	{
//	  "$search": "free text \"phrases\" prefix*",  // should/OR (or AND, see below)
//	  "$defaultOperator": "and" | "or",            // default joins $search terms
//	  "$require": "critical" | ["a", "\"b c\""],   // each entry required (AND)
//	  "$exclude": "spam" | ["draft", "x*"]          // each entry excluded
//	}
//
// $require/$exclude take a string or an array of strings; each entry reuses the
// phrase/prefix syntax of $search (ParseTextSearch) with its boolean role fixed
// by the field. This replaces inline +/- in the search string, which is unsafe
// for raw user input (see ParseTextSearch). $language/$caseSensitive/
// $diacriticSensitive are accepted and ignored (the analyzer is language-neutral).
// Note the Text caveat: outside an FTS-driven query its Ok matches everything.
func parseText(v *anyenc.Value) (Filter, error) {
	if v.Type() != anyenc.TypeObject {
		return nil, &ParseError{Op: "$text", Reason: `$text must be an object, e.g. {"$search":"..."}`}
	}
	obj, _ := v.Object()
	var (
		search      string
		hasS        bool
		defaultAnd  bool
		requireRaws []string
		excludeRaws []string
		perr        error
	)
	// fail records a structured rejection located at the given $text sub-key.
	fail := func(field, reason string) {
		perr = atPath(&ParseError{Op: "$text", Reason: reason}, field)
	}
	// appendStrings reads a string or array-of-strings field into dst.
	appendStrings := func(field string, val *anyenc.Value, dst []string) []string {
		switch val.Type() {
		case anyenc.TypeString:
			sb, _ := val.StringBytes()
			return append(dst, string(sb))
		case anyenc.TypeArray:
			arr, _ := val.Array()
			for _, e := range arr {
				sb, er := e.StringBytes()
				if er != nil {
					fail(field, field+" entries must be strings")
					return dst
				}
				dst = append(dst, string(sb))
			}
			return dst
		default:
			fail(field, field+" must be a string or an array of strings")
			return dst
		}
	}
	obj.Visit(func(key []byte, val *anyenc.Value) {
		if perr != nil {
			return
		}
		switch string(key) {
		case "$search":
			sb, e := val.StringBytes()
			if e != nil {
				fail("$search", "$search must be a string")
				return
			}
			search, hasS = string(sb), true
		case "$defaultOperator":
			sb, e := val.StringBytes()
			if e != nil {
				fail("$defaultOperator", `$defaultOperator must be a string ("and" or "or")`)
				return
			}
			switch strings.ToLower(string(sb)) {
			case "and":
				defaultAnd = true
			case "or", "":
				defaultAnd = false
			default:
				fail("$defaultOperator", fmt.Sprintf(`$defaultOperator must be "and" or "or", got %q`, string(sb)))
			}
		case "$require":
			requireRaws = appendStrings("$require", val, requireRaws)
		case "$exclude":
			excludeRaws = appendStrings("$exclude", val, excludeRaws)
		case "$language", "$caseSensitive", "$diacriticSensitive":
			// accepted for Mongo compatibility, ignored in v1
		default:
			fail(string(key), "unknown $text field: "+string(key))
		}
	})
	if perr != nil {
		return nil, perr
	}
	if !hasS {
		return nil, &ParseError{Op: "$text", Reason: "$text requires $search"}
	}

	clauses := ParseTextSearch(search) // all should
	for _, r := range requireRaws {
		clauses = appendTextClauses(clauses, r, TextMust)
	}
	for _, r := range excludeRaws {
		clauses = appendTextClauses(clauses, r, TextMustNot)
	}
	return Text{Search: search, DefaultAnd: defaultAnd, Clauses: clauses}, nil
}

// ParseTextSearch splits a $text $search string into clauses. It is a purely
// syntactic scan — no analysis/tokenization happens here (that is the FTS
// executor's job, using the index's analyzer). Every clause it returns is a
// should (OR) clause; required/excluded clauses come from the typed $require /
// $exclude sub-fields instead (see parseText), not from inline string signs.
//
//   - "double quoted"  → a phrase clause (positional adjacency).
//   - word*            → a prefix clause (vocabulary expansion).
//   - word             → a plain term clause.
//
// Deliberately there is NO inline +/- boolean syntax. Parsing boolean intent out
// of a raw human search string is a footgun for an embedded library whose common
// use is "forward the end-user's search box straight into $search": a user typing
// "-9" (meaning −9°C) or "+1" (a vote) would silently get an exclusion/require.
// The same characters are harmless here — they are punctuation the analyzer drops
// — so "-9" is just the term "9". Boolean require/exclude lives in the structured
// $require / $exclude arrays, which are unambiguous and run through the same
// analyzer. An app that wants single-box power-user syntax can parse it itself and
// populate those fields. A trailing * is still honoured on bare words (prefix
// expansion has negligible false-positive rate in natural text); it is left
// literal inside a phrase, where positional expansion would be combinatorial.
func ParseTextSearch(s string) []TextClause {
	var clauses []TextClause
	rs := []rune(s)
	i, n := 0, len(rs)
	for i < n {
		for i < n && unicode.IsSpace(rs[i]) {
			i++
		}
		if i >= n {
			break
		}
		if rs[i] == '"' {
			i++ // opening quote
			start := i
			for i < n && rs[i] != '"' {
				i++
			}
			raw := strings.TrimSpace(string(rs[start:i]))
			if i < n {
				i++ // closing quote
			}
			if raw != "" {
				clauses = append(clauses, TextClause{Raw: raw, Phrase: true, Op: TextShould})
			}
			continue
		}
		start := i
		for i < n && !unicode.IsSpace(rs[i]) {
			i++
		}
		word := string(rs[start:i])
		prefix := false
		if strings.HasSuffix(word, "*") {
			prefix = true
			word = strings.TrimRight(word, "*")
		}
		if word != "" {
			clauses = append(clauses, TextClause{Raw: word, Prefix: prefix, Op: TextShould})
		}
	}
	return clauses
}

// appendTextClauses parses raw with ParseTextSearch and appends its clauses to
// dst with their boolean role forced to op (used for $require / $exclude entries,
// which reuse the phrase/prefix syntax but fix the clause role by field).
func appendTextClauses(dst []TextClause, raw string, op TextOp) []TextClause {
	for _, c := range ParseTextSearch(raw) {
		c.Op = op
		dst = append(dst, c)
	}
	return dst
}

func parseSize(v *anyenc.Value) (Filter, error) {
	size, err := v.Int()
	if err != nil {
		return nil, &ParseError{Op: "$size", Reason: "$size must be an integer", wrapped: err}
	}
	return Size{Size: int64(size)}, nil
}

func parseRegexp(v *anyenc.Value) (Filter, error) {
	switch v.Type() {
	case anyenc.TypeString:
		exp, err := v.StringBytes()
		if err != nil {
			return nil, &ParseError{Op: "$regex", Reason: "invalid regular expression: " + err.Error(), wrapped: err}
		}
		compiledRegexp, err := regexp.Compile(string(exp))
		if err != nil {
			return nil, &ParseError{Op: "$regex", Reason: "invalid regular expression: " + err.Error(), wrapped: err}
		}
		return Regexp{Regexp: compiledRegexp}, nil
	default:
		return nil, &ParseError{Op: "$regex", Reason: "$regex must be a string, got " + v.String()}
	}
}

func makeArrComp(op Operator, v *anyenc.Value) (Filter, error) {
	if v.Type() != anyenc.TypeArray {
		// The old message rendered op with %v — the raw uint8, e.g. "expected
		// array for 12 operator". Name it instead.
		return nil, &ParseError{Op: opName(op), Reason: opName(op) + " must be an array"}
	}
	switch op {
	case opIn:
		vals, _ := v.Array()
		return NewInValue(vals...), nil
	case opNin:
		return Nor(makeEqArray(v)), nil
	case opAll:
		return And(makeEqArray(v)), nil
	default:
		panic(fmt.Errorf("unexpected operator: %v", op))
	}
}

func makeEqArray(v *anyenc.Value) []Filter {
	vals, _ := v.Array()
	res := make([]Filter, len(vals))
	// All EqValues are marshaled into one shared buffer; offsets are recorded
	// first and slices taken after the final append, so buffer reallocation
	// during marshaling cannot invalidate earlier values.
	var buf []byte
	offs := make([]int, len(vals)+1)
	for i, av := range vals {
		buf = av.MarshalTo(buf)
		offs[i+1] = len(buf)
	}
	for i, av := range vals {
		eq := &Comp{CompOp: CompOpEq}
		eq.EqValue = buf[offs[i]:offs[i+1]:offs[i+1]]
		eq.notArray = av.Type() != anyenc.TypeArray
		res[i] = eq
	}
	return res
}

func parseExists(v *anyenc.Value) (f Filter, err error) {
	switch v.Type() {
	case anyenc.TypeFalse, anyenc.TypeNull:
		return Not{Exists{}}, nil
	case anyenc.TypeNumber:
		if i, _ := v.Int(); i == 0 {
			return Not{Exists{}}, nil
		}
	}
	return Exists{}, nil
}

func parseType(v *anyenc.Value) (f Filter, err error) {
	switch v.Type() {
	case anyenc.TypeNumber:
		n, _ := v.Int()
		tv := Type(n)
		if (tv > TypeObject && tv != TypeObjectID && tv != TypeVectorF32) || tv < 0 {
			return nil, &ParseError{Op: "$type", Reason: fmt.Sprintf("unexpected type: %d", n)}
		}
		return TypeFilter{Type: anyenc.Type(tv)}, err
	case anyenc.TypeString:
		bs, _ := v.StringBytes()
		tv, ok := stringToType[string(bs)]
		if !ok {
			return nil, &ParseError{Op: "$type", Reason: "unexpected type: " + string(bs)}
		}
		return TypeFilter{Type: anyenc.Type(tv)}, err
	default:
		return nil, &ParseError{Op: "$type", Reason: "$type must be a number or a string, got " + v.String()}
	}
}

func isOperator(key []byte) (ok bool, op Operator, err error) {
	if bytes.HasPrefix(key, opBytesPrefix) {
		// The map[string([]byte)] lookup does not allocate; this is the hot
		// recognition path for every '$'-prefixed key.
		if op, ok = operators[string(key)]; ok {
			return true, op, nil
		}
		return true, 0, &ParseError{
			Op:      string(key),
			Reason:  "unknown operator: " + string(key),
			wrapped: ErrUnknownOperator,
		}
	}
	return false, 0, nil
}

func isTopLevel(op Operator) bool {
	return op < _opVal
}
