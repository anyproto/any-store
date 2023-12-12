package query

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/encoding"
	"github.com/anyproto/any-store/internal/parser"
)

type Operator uint8

const (
	OpAnd Operator = iota
	OpOr
	OpNor

	_OpVal
	OpNe
	OpEq
	OpGt
	OpGte
	OpLt
	OpLte

	_OpArr
	OpIn
	OpNin
	OpAll
	_OpNot
	OpNot
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
)

var parserPool = &fastjson.ParserPool{}

func ParseCondition(cond any) (Filter, error) {
	p := parserPool.Get()
	defer parserPool.Put(p)

	v, err := parser.AnyToJSON(p, cond)
	if err != nil {
		return nil, err
	}
	return parseAnd(v)
}

func parseAndArray(v *fastjson.Value) (f Filter, err error) {
	if v.Type() != fastjson.TypeArray {
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

func parseOrArray(v *fastjson.Value) (f Filter, err error) {
	if v.Type() != fastjson.TypeArray {
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

func parseNorArray(v *fastjson.Value) (f Filter, err error) {
	if v.Type() != fastjson.TypeArray {
		return nil, fmt.Errorf("$or must be an array")
	}
	arr, _ := v.Array()
	var fs Nor
	if len(arr) > 1 {
		fs = make(Nor, 0, len(arr))
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

func parseAnd(val *fastjson.Value) (res Filter, err error) {
	if val.Type() != fastjson.TypeObject {
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
	obj.Visit(func(key []byte, v *fastjson.Value) {
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
			case OpAnd:
				if f, err = parseAndArray(v); err != nil {
					return
				}
				if fs != nil {
					fs = append(fs, f)
				}
			case OpOr:
				if f, err = parseOrArray(v); err != nil {
					return
				}
				if fs != nil {
					fs = append(fs, f)
				}
			case OpNor:
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

func parseComp(key string, v *fastjson.Value) (f Filter, err error) {
	fk := Key{
		Path: strings.Split(key, "."),
	}
	if v.Type() == fastjson.TypeObject {
		if fk.Filter, err = parseCompObj(v); err != nil {
			return nil, err
		}
	} else {
		eq := &Comp{}
		eq.eqValue = encoding.AppendJSONValue(eq.eqValue, v)
		fk.Filter = eq
	}
	return fk, nil
}

func parseCompObj(v *fastjson.Value) (Filter, error) {
	hasCompOp, f, err := parseCompObjOp(v)
	if err != nil {
		return nil, err
	}
	if hasCompOp {
		return f, nil
	} else {
		cmp := &Comp{}
		cmp.eqValue = encoding.AppendJSONValue(cmp.eqValue, v)
		cmp.CompOp = CompOpEq
		return cmp, nil
	}
}

func parseCompObjOp(val *fastjson.Value) (ok bool, f Filter, err error) {
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

	obj.Visit(func(key []byte, v *fastjson.Value) {
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

func makeCompFilter(op Operator, v *fastjson.Value) (f Filter, err error) {
	switch op {
	case OpEq:
		cmp := &Comp{}
		cmp.eqValue = encoding.AppendJSONValue(cmp.eqValue, v)
		cmp.CompOp = CompOpEq
		return cmp, nil
	case OpNe:
		cmp := &Comp{}
		cmp.eqValue = encoding.AppendJSONValue(cmp.eqValue, v)
		cmp.CompOp = CompOpNe
		return cmp, nil
	case OpGt:
		cmp := &Comp{}
		cmp.eqValue = encoding.AppendJSONValue(cmp.eqValue, v)
		cmp.CompOp = CompOpGt
		return cmp, nil
	case OpGte:
		cmp := &Comp{}
		cmp.eqValue = encoding.AppendJSONValue(cmp.eqValue, v)
		cmp.CompOp = CompOpGte
		return cmp, nil
	case OpLt:
		cmp := &Comp{}
		cmp.eqValue = encoding.AppendJSONValue(cmp.eqValue, v)
		cmp.CompOp = CompOpLt
		return cmp, nil
	case OpLte:
		cmp := &Comp{}
		cmp.eqValue = encoding.AppendJSONValue(cmp.eqValue, v)
		cmp.CompOp = CompOpLte
		return cmp, nil
	case OpNot:
		var isOp bool
		not := Not{}
		if isOp, not.Filter, err = parseCompObjOp(v); err != nil {
			return nil, fmt.Errorf("%w for operator $not", err)
		}
		if !isOp {
			return nil, fmt.Errorf("no operators found for $not")
		}
		return not, nil
	default:
		return makeArrComp(op, v)
	}
}

func makeArrComp(op Operator, v *fastjson.Value) (Filter, error) {
	if v.Type() != fastjson.TypeArray {
		return nil, fmt.Errorf("expected array for %v operator", op)
	}
	switch op {
	case OpIn:
		return Or(makeEqArray(v)), nil
	case OpNin:
		return Nor(makeEqArray(v)), nil
	case OpAll:
		return And(makeEqArray(v)), nil
	default:
		panic(fmt.Errorf("unexpected operator: %v", op))
	}
}

func makeEqArray(v *fastjson.Value) []Filter {
	vals, _ := v.Array()
	res := make([]Filter, len(vals))
	for i, jv := range vals {
		eq := &Comp{CompOp: CompOpEq}
		eq.eqValue = encoding.AppendJSONValue(eq.eqValue, jv)
		res[i] = eq
	}
	return res
}

func isOperator(key []byte) (ok bool, op Operator, err error) {
	if bytes.HasPrefix(key, opBytesPrefix) {
		switch {
		case bytes.Equal(key, opBytesIn):
			return true, OpIn, nil
		case bytes.Equal(key, opBytesNin):
			return true, OpNin, nil
		case bytes.Equal(key, opBytesOr):
			return true, OpOr, nil
		case bytes.Equal(key, opBytesAnd):
			return true, OpAnd, nil
		case bytes.Equal(key, opBytesAll):
			return true, OpAll, nil
		case bytes.Equal(key, opBytesNe):
			return true, OpNe, nil
		case bytes.Equal(key, opBytesNor):
			return true, OpNor, nil
		case bytes.Equal(key, opBytesGt):
			return true, OpGt, nil
		case bytes.Equal(key, opBytesGte):
			return true, OpGte, nil
		case bytes.Equal(key, opBytesLt):
			return true, OpLt, nil
		case bytes.Equal(key, opBytesLte):
			return true, OpLte, nil
		case bytes.Equal(key, opBytesEq):
			return true, OpEq, nil
		case bytes.Equal(key, opBytesNot):
			return true, OpNot, nil
		default:
			return true, 0, fmt.Errorf("unknow operator: %s", string(key))
		}
	}
	return false, 0, nil
}

func isTopLevel(op Operator) bool {
	return op < _OpVal
}
