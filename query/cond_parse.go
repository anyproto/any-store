package query

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/parser"
)

type Operator uint8

const (
	opAnd Operator = iota
	opOr
	opNor

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
)

var (
	opBytesPrefix  = []byte("$")
	opBytesPrefixS = string(opBytesPrefix)
	opBytesAnd     = []byte("$and")
	opBytesOr      = []byte("$or")
	opBytesNe      = []byte("$ne")
	opBytesIn      = []byte("$in")
	opBytesNin     = []byte("$nin")
	opBytesAll     = []byte("$all")
	opBytesEq      = []byte("$eq")

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
	obj.Visit(func(skey string, v *anyenc.Value) {
		if err != nil {
			return
		}
		key := []byte(skey)
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
	fk := Key{
		Path: strings.Split(key, "."),
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
	)

	var fs And
	if obj.Len() > 1 {
		fs = make(And, 0, obj.Len())
	}

	obj.Visit(func(sKey string, v *anyenc.Value) {
		if err != nil {
			return
		}
		key := []byte(sKey)
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
	if hasNonOp {
		return false, nil, nil
	}
	if fs != nil {
		return true, fs, nil
	}
	return true, f, nil
}

func makeCompFilter(op Operator, v *anyenc.Value) (f Filter, err error) {
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
		return not, nil
	case opExists:
		return parseExists(v)
	case opType:
		return parseType(v)
	case opRegexp:
		return parseRegexp(v)
	case opSize:
		return parseSize(v)
	default:
		return makeArrComp(op, v)
	}
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
		return Or(makeEqArray(v)), nil
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
	for i, av := range vals {
		eq := &Comp{CompOp: CompOpEq}
		eq.EqValue = av.MarshalTo(nil)
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
		if tv > TypeObject || tv < 0 {
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
		default:
			return true, 0, fmt.Errorf("unknow operator: %s", string(key))
		}
	}
	return false, 0, nil
}

func isTopLevel(op Operator) bool {
	return op < _opVal
}
