package query

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/parser"
)

// ErrVectorNotOrderable is returned when an ordering operator ($gt/$gte/$lt/$lte)
// is given a vector operand. Vectors are not points on the scalar order (Rule V,
// BUG-32): the anyenc tag order sorts every vector above every scalar, which made
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

var (
	opBytesPrefix = []byte("$")
	opBytesAnd    = []byte("$and")
	opBytesOr     = []byte("$or")
	opBytesNe     = []byte("$ne")
	opBytesIn     = []byte("$in")
	opBytesNin    = []byte("$nin")
	opBytesAll    = []byte("$all")
	opBytesEq     = []byte("$eq")

	opBytesGt  = []byte("$gt")
	opBytesGte = []byte("$gte")

	opBytesLt  = []byte("$lt")
	opBytesLte = []byte("$lte")
	opBytesNot = []byte("$not")
	opBytesNor = []byte("$nor")

	opBytesExists = []byte("$exists")
	opBytesType   = []byte("$type")
	opBytesRegexp = []byte("$regex")
	opBytesSize   = []byte("$size")
	opBytesText   = []byte("$text")
	opBytesKnn    = []byte("$knn")
)

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
		return nil, fmt.Errorf("$and must be an array")
	}
	arr, _ := v.Array()
	var fs And
	if len(arr) > 1 {
		fs = make(And, 0, len(arr))
	}
	for _, el := range arr {
		if f, err = parseAnd(el); err != nil {
			return nil, err
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
		return nil, fmt.Errorf("$or must be an array")
	}
	arr, _ := v.Array()
	var fs Or
	if len(arr) > 1 {
		fs = make(Or, 0, len(arr))
	}
	for _, el := range arr {
		if f, err = parseAnd(el); err != nil {
			return nil, err
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
		return nil, fmt.Errorf("$or must be an array")
	}
	arr, _ := v.Array()
	fs := make(Nor, 0, len(arr))
	for _, el := range arr {
		if f, err = parseAnd(el); err != nil {
			return nil, err
		}
		fs = append(fs, f)
	}
	return fs, nil
}

func parseAnd(val *anyenc.Value) (res Filter, err error) {
	if val.Type() != anyenc.TypeObject {
		return nil, fmt.Errorf("query filter must be an object")
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
			return
		}
		if isOp {
			if !isTopLevel(op) {
				err = fmt.Errorf("unknow top level operator: %s", string(key))
				return
			}

			switch op {
			case opAnd:
				if f, err = parseAndArray(v); err != nil {
					return
				}
				if fs != nil {
					fs = append(fs, f)
				}
			case opOr:
				if f, err = parseOrArray(v); err != nil {
					return
				}
				if fs != nil {
					fs = append(fs, f)
				}
			case opNor:
				if f, err = parseNorArray(v); err != nil {
					return
				}
				if fs != nil {
					fs = append(fs, f)
				}
			case opText:
				if f, err = parseText(v); err != nil {
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
		return false, nil, fmt.Errorf("expected object")
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
			return
		}
		if isOp {
			if isTopLevel(op) {
				err = fmt.Errorf("unexpected comparsion operator: %v", string(key))
				return
			}
			if hasNonOp {
				err = fmt.Errorf("mixed operators and values")
				return
			}
			ok = true
			if op == opKnn {
				hasKnn = true
			}
			if f, err = makeCompFilter(op, v); err != nil {
				return
			}
			if fs != nil {
				fs = append(fs, f)
			}
		} else {
			hasNonOp = true
			if ok {
				err = fmt.Errorf("unexpected comparsion operator: %v", string(key))
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
		return false, nil, errors.New("$knn must be the only operator on its field")
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
	// Rule V (BUG-32) at the door: an ordering op against a vector operand is
	// decidable syntactically, so reject it here rather than silently evaluating
	// to false. This also stops it reflecting through $not, where a
	// never-satisfiable inner predicate becomes match-all. Comp.Ok keeps the
	// eval-time guard for hand-built filters, which never see the parser.
	switch op {
	case opGt, opGte, opLt, opLte:
		if v.Type() == anyenc.TypeVectorF32 {
			return nil, fmt.Errorf("%w: use $eq for exact match, or a $vector index for similarity search", ErrVectorNotOrderable)
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
			return nil, fmt.Errorf("%w for operator $not", err)
		}
		if !isOp {
			return nil, fmt.Errorf("no operators found for $not")
		}
		// A Knn under Not would evaluate !false == match-all (fail-closed Ok
		// reflected). Unrepresentable, at the door.
		if ContainsKnn(not.Filter) {
			return nil, errors.New("$knn is not allowed under $not")
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
		return nil, errors.New(`$knn must be an object, e.g. {"$knn":{"$query":[...],"$k":10}}`)
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
		switch string(key) {
		case "$query":
			var ok bool
			kn.Query, ok = anyenc.AppendFloat32s(val.MarshalTo(nil), nil)
			if !ok {
				perr = errors.New(`$knn: $query must be an array of numbers or {"$vector":[...]}`)
				return
			}
			hasQuery = true
		case "$k":
			n, e := val.Int()
			if e != nil || val.Type() != anyenc.TypeNumber || float64(n) != val.GetFloat64() || n < 1 || n > KnnMaxK {
				perr = fmt.Errorf("$knn: $k must be an integer in [1, %d], got %v", KnnMaxK, val)
				return
			}
			kn.K = n
			hasK = true
		case "$ef":
			n, e := val.Int()
			if e != nil || val.Type() != anyenc.TypeNumber || float64(n) != val.GetFloat64() || n > KnnMaxEf {
				perr = fmt.Errorf("$knn: $ef must be an integer in [$k, %d], got %v", KnnMaxEf, val)
				return
			}
			kn.Ef = n
		case "$index":
			sb, e := val.StringBytes()
			if e != nil {
				perr = errors.New("$knn: $index must be a string")
				return
			}
			kn.Index = string(sb)
		case "$vector":
			perr = errors.New(`unknown $knn field: $vector (did you mean "$query"? $vector is the value-type wrapper: {"$query":{"$vector":[...]}})`)
		case "$maxDistance", "$minScore", "$prefilter", "$nprobe":
			// Reserved so adding them later is non-breaking; rejected in v1.
			perr = fmt.Errorf("$knn: %s is reserved and not supported", string(key))
		default:
			perr = fmt.Errorf("unknown $knn field: %s", string(key))
		}
	})
	if perr != nil {
		return nil, perr
	}
	if !hasQuery {
		return nil, errors.New("$knn requires $query")
	}
	if !hasK {
		return nil, errors.New("$knn requires $k (the number of neighbours to select)")
	}
	// Range/finiteness rules live in ONE place, shared with the executor's
	// detection walk (which validates programmatic NewKnn filters).
	if err := kn.Validate(); err != nil {
		return nil, err
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
		return nil, fmt.Errorf("$text must be an object, e.g. {\"$search\":\"...\"}")
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
					perr = fmt.Errorf("%s entries must be strings", field)
					return dst
				}
				dst = append(dst, string(sb))
			}
			return dst
		default:
			perr = fmt.Errorf("%s must be a string or an array of strings", field)
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
				perr = fmt.Errorf("$search must be a string")
				return
			}
			search, hasS = string(sb), true
		case "$defaultOperator":
			sb, e := val.StringBytes()
			if e != nil {
				perr = fmt.Errorf("$defaultOperator must be a string (\"and\" or \"or\")")
				return
			}
			switch strings.ToLower(string(sb)) {
			case "and":
				defaultAnd = true
			case "or", "":
				defaultAnd = false
			default:
				perr = fmt.Errorf("$defaultOperator must be \"and\" or \"or\", got %q", string(sb))
			}
		case "$require":
			requireRaws = appendStrings("$require", val, requireRaws)
		case "$exclude":
			excludeRaws = appendStrings("$exclude", val, excludeRaws)
		case "$language", "$caseSensitive", "$diacriticSensitive":
			// accepted for Mongo compatibility, ignored in v1
		default:
			perr = fmt.Errorf("unknown $text field: %s", string(key))
		}
	})
	if perr != nil {
		return nil, perr
	}
	if !hasS {
		return nil, fmt.Errorf("$text requires $search")
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
		return nil, fmt.Errorf("failed to extract size %w", err)
	}
	return Size{Size: int64(size)}, nil
}

func parseRegexp(v *anyenc.Value) (Filter, error) {
	switch v.Type() {
	case anyenc.TypeString:
		exp, err := v.StringBytes()
		if err != nil {
			return nil, fmt.Errorf("failed to parse regular exporession: %w", err)
		}
		compiledRegexp, err := regexp.Compile(string(exp))
		if err != nil {
			return nil, fmt.Errorf("failed to parse regular exporession: %w", err)
		}
		return Regexp{Regexp: compiledRegexp}, nil
	default:
		return nil, fmt.Errorf("unexpetced type: %s", v.String())
	}
}

func makeArrComp(op Operator, v *anyenc.Value) (Filter, error) {
	if v.Type() != anyenc.TypeArray {
		return nil, fmt.Errorf("expected array for %v operator", op)
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
			return nil, fmt.Errorf("unexpected type: %d", n)
		}
		return TypeFilter{Type: anyenc.Type(tv)}, err
	case anyenc.TypeString:
		bs, _ := v.StringBytes()
		tv, ok := stringToType[string(bs)]
		if !ok {
			return nil, fmt.Errorf("unexpected type: %s", string(bs))
		}
		return TypeFilter{Type: anyenc.Type(tv)}, err
	default:
		return nil, fmt.Errorf("unexpetced type: %s", v.String())
	}
}

func isOperator(key []byte) (ok bool, op Operator, err error) {
	if bytes.HasPrefix(key, opBytesPrefix) {
		switch {
		case bytes.Equal(key, opBytesIn):
			return true, opIn, nil
		case bytes.Equal(key, opBytesNin):
			return true, opNin, nil
		case bytes.Equal(key, opBytesOr):
			return true, opOr, nil
		case bytes.Equal(key, opBytesAnd):
			return true, opAnd, nil
		case bytes.Equal(key, opBytesAll):
			return true, opAll, nil
		case bytes.Equal(key, opBytesNe):
			return true, opNe, nil
		case bytes.Equal(key, opBytesNor):
			return true, opNor, nil
		case bytes.Equal(key, opBytesGt):
			return true, opGt, nil
		case bytes.Equal(key, opBytesGte):
			return true, opGte, nil
		case bytes.Equal(key, opBytesLt):
			return true, opLt, nil
		case bytes.Equal(key, opBytesLte):
			return true, opLte, nil
		case bytes.Equal(key, opBytesEq):
			return true, opEq, nil
		case bytes.Equal(key, opBytesNot):
			return true, opNot, nil
		case bytes.Equal(key, opBytesExists):
			return true, opExists, nil
		case bytes.Equal(key, opBytesType):
			return true, opType, nil
		case bytes.Equal(key, opBytesRegexp):
			return true, opRegexp, nil
		case bytes.Equal(key, opBytesSize):
			return true, opSize, nil
		case bytes.Equal(key, opBytesText):
			return true, opText, nil
		case bytes.Equal(key, opBytesKnn):
			return true, opKnn, nil
		default:
			return true, 0, fmt.Errorf("unknow operator: %s", string(key))
		}
	}
	return false, 0, nil
}

func isTopLevel(op Operator) bool {
	return op < _opVal
}
