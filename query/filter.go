package query

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/encoding"
)

type Filter interface {
	Ok(v *fastjson.Value) bool
	fmt.Stringer
}

type CompOp uint8

const (
	CompOpEq CompOp = iota
	CompOpGt
	CompOpGte
	CompOpLt
	CompOpLte
	CompOpNe
)

type Comp struct {
	eqValue []byte
	buf     []byte
	CompOp  CompOp
}

func (e *Comp) Ok(v *fastjson.Value) bool {
	if v == nil {
		return false
	}
	if v.Type() == fastjson.TypeArray {
		vals, _ := v.Array()
		for _, val := range vals {
			e.buf = encoding.AppendJSONValue(e.buf[:0], val)
			if e.comp() {
				return true
			}
		}
		return false
	} else {
		e.buf = encoding.AppendJSONValue(e.buf[:0], v)
		return e.comp()
	}
}

func (e Comp) comp() bool {
	comp := bytes.Compare(e.eqValue, e.buf)
	switch e.CompOp {
	case CompOpEq:
		return comp == 0
	case CompOpGt:
		return comp < 0
	case CompOpGte:
		return comp <= 0
	case CompOpLt:
		return comp > 0
	case CompOpLte:
		return comp >= 0
	case CompOpNe:
		return comp != 0
	default:
		panic(fmt.Errorf("unexpected comp op: %v", e.CompOp))
	}
}

func (e Comp) String() string {
	var op string
	switch e.CompOp {
	case CompOpEq:
		op = string(opBytesEq)
	case CompOpGt:
		op = string(opBytesGt)
	case CompOpGte:
		op = string(opBytesGte)
	case CompOpLt:
		op = string(opBytesLt)
	case CompOpLte:
		op = string(opBytesLte)
	case CompOpNe:
		op = string(opBytesNe)
	}
	p := parserPool.Get()
	defer parserPool.Put(p)
	a := &fastjson.Arena{}
	val, _ := encoding.DecodeToJSON(p, a, e.eqValue)
	return fmt.Sprintf(`{"%s": %s}`, op, val.String())
}

type Key struct {
	Path []string
	Filter
}

func (e Key) Ok(v *fastjson.Value) bool {
	return e.Filter.Ok(v.Get(e.Path...))
}

func (e Key) String() string {
	return fmt.Sprintf(`{"%s": %s}`, strings.Join(e.Path, "."), e.Filter.String())
}

type And []Filter

func (e And) Ok(v *fastjson.Value) bool {
	for _, f := range e {
		if !f.Ok(v) {
			return false
		}
	}
	return true
}

func (e And) String() string {
	var subS []string
	for _, f := range e {
		if f != nil {
			subS = append(subS, f.String())
		}
	}
	return fmt.Sprintf(`{"$and":[%s]}`, strings.Join(subS, ", "))
}

type Or []Filter

func (e Or) Ok(v *fastjson.Value) bool {
	for _, f := range e {
		if f.Ok(v) {
			return true
		}
	}
	return false
}

func (e Or) String() string {
	var subS []string
	for _, f := range e {
		subS = append(subS, f.String())
	}
	return fmt.Sprintf(`{"$or":[%s]}`, strings.Join(subS, ", "))
}

type Nor []Filter

func (e Nor) Ok(v *fastjson.Value) bool {
	for _, f := range e {
		if f.Ok(v) {
			return false
		}
	}
	return true
}

func (e Nor) String() string {
	var subS []string
	for _, f := range e {
		subS = append(subS, f.String())
	}
	return fmt.Sprintf(`{"$nor":[%s]}`, strings.Join(subS, ", "))
}

type Not struct {
	Filter
}

func (e Not) Ok(v *fastjson.Value) bool {
	return !e.Filter.Ok(v)
}

func (e Not) String() string {
	return fmt.Sprintf(`{"$not": %s}`, e.Filter.String())
}

type All struct{}

func (a All) Ok(_ *fastjson.Value) bool {
	return true
}

func (a All) String() string {
	return "null"
}

type Exists struct{}

func (e Exists) Ok(v *fastjson.Value) bool {
	return v != nil
}

func (e Exists) String() string {
	return fmt.Sprintf(`{"$exists": true}`)
}

type TypeFilter struct {
	Type encoding.Type
}

func (e TypeFilter) Ok(v *fastjson.Value) bool {
	if v == nil {
		return false
	}
	return encoding.FastJSONTypeToType(v.Type()) == e.Type
}

func (e TypeFilter) String() string {
	return fmt.Sprintf(`{"$type": "%s"}`, Type(e.Type).String())
}
