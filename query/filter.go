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
	OkBytes(b []byte) bool

	IndexFilter(fieldName string, bs Bounds) (filter Filter, bounds Bounds)

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
	EqValue []byte
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
			if e.comp(e.buf) {
				return true
			}
		}
		return false
	} else {
		e.buf = encoding.AppendJSONValue(e.buf[:0], v)
		return e.comp(e.buf)
	}
}

func (e *Comp) OkBytes(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	return e.comp(b)
}

func (e *Comp) IndexFilter(_ string, bs Bounds) (filter Filter, bounds Bounds) {
	switch e.CompOp {
	case CompOpEq:
		return e, bs.Append(Bound{
			Start:        e.EqValue,
			End:          e.EqValue,
			StartInclude: true,
			EndInclude:   true,
		})
	case CompOpGt:
		return e, bs.Append(Bound{
			Start: e.EqValue,
		})
	case CompOpGte:
		return e, bs.Append(Bound{
			Start:        e.EqValue,
			StartInclude: true,
		})
	case CompOpLt:
		return e, bs.Append(Bound{
			End: e.EqValue,
		})
	case CompOpLte:
		return e, bs.Append(Bound{
			End:        e.EqValue,
			EndInclude: true,
		})
	case CompOpNe:
		return e, bs.Append(Bound{
			End: e.EqValue,
		}).Append(Bound{
			Start: e.EqValue,
		})
	default:
		panic(fmt.Errorf("unexpected comp op: %v", e.CompOp))
	}
}

func (e *Comp) comp(b []byte) bool {
	comp := bytes.Compare(e.EqValue, b)
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

func (e *Comp) String() string {
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
	val, _, _ := encoding.DecodeToJSON(p, a, e.EqValue)
	return fmt.Sprintf(`{"%s": %s}`, op, val.String())
}

type Key struct {
	Path []string
	Filter
}

func (e Key) Ok(v *fastjson.Value) bool {
	return e.Filter.Ok(v.Get(e.Path...))
}

func (e Key) OkBytes(b []byte) bool {
	return e.Filter.OkBytes(b)
}

func (e Key) IndexFilter(fieldName string, bs Bounds) (filter Filter, bounds Bounds) {
	if strings.Join(e.Path, ".") == fieldName {
		return e.Filter.IndexFilter(fieldName, bs)
	}
	return nil, bs
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

func (e And) OkBytes(b []byte) bool {
	for _, f := range e {
		if !f.OkBytes(b) {
			return false
		}
	}
	return true
}

func (e And) IndexFilter(fieldName string, bs Bounds) (filter Filter, bounds Bounds) {
	var af And
	var ff Filter
	for _, f := range e {
		if ff, bs = f.IndexFilter(fieldName, bs); ff != nil {
			af = append(af, ff)
		}
	}
	if len(af) == 0 {
		return nil, bs
	}
	return af, bs
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

func (e Or) OkBytes(b []byte) bool {
	for _, f := range e {
		if f.OkBytes(b) {
			return true
		}
	}
	return false
}

func (e Or) IndexFilter(fieldName string, bs Bounds) (filter Filter, bounds Bounds) {
	var of Or
	var ff Filter
	for _, f := range e {
		if ff, bs = f.IndexFilter(fieldName, bs); ff != nil {
			of = append(of, ff)
		} else {
			return
		}
	}
	if len(of) == 0 {
		return
	}
	return of, bs
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

func (e Nor) OkBytes(b []byte) bool {
	for _, f := range e {
		if f.OkBytes(b) {
			return false
		}
	}
	return true
}

func (e Nor) IndexFilter(fieldName string, bs Bounds) (filter Filter, bounds Bounds) {
	var nf Nor
	var ff Filter
	for _, f := range e {
		if ff, _ = f.IndexFilter(fieldName, bs); ff != nil {
			nf = append(nf, ff)
		} else {
			return nil, bs
		}
	}
	if len(nf) == 0 {
		return nil, bs
	}
	return nf, bs
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

func (e Not) OkBytes(b []byte) bool {
	return !e.Filter.OkBytes(b)
}

func (e Not) IndexFilter(fieldName string, bs Bounds) (filter Filter, bounds Bounds) {
	if ff, _ := e.Filter.IndexFilter(fieldName, nil); ff != nil {
		return e, bs
	}
	return nil, bs
}

func (e Not) String() string {
	return fmt.Sprintf(`{"$not": %s}`, e.Filter.String())
}

type All struct{}

func (a All) Ok(_ *fastjson.Value) bool {
	return true
}

func (a All) OkBytes(_ []byte) bool {
	return true
}

func (a All) IndexFilter(fieldName string, bs Bounds) (filter Filter, bounds Bounds) {
	return nil, bs
}

func (a All) String() string {
	return "null"
}

type Exists struct{}

func (e Exists) Ok(v *fastjson.Value) bool {
	return v != nil
}

func (e Exists) OkBytes(b []byte) bool {
	return len(b) != 0
}

func (a Exists) IndexFilter(fieldName string, bs Bounds) (filter Filter, bounds Bounds) {
	return
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

func (e TypeFilter) OkBytes(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	return b[0] == uint8(e.Type)
}

func (e TypeFilter) IndexFilter(fieldName string, bs Bounds) (filter Filter, bounds Bounds) {
	k := []byte{byte(e.Type)}
	return e, bs.Append(Bound{
		Start:        k,
		End:          k,
		StartInclude: true,
		EndInclude:   true,
	})
}

func (e TypeFilter) String() string {
	return fmt.Sprintf(`{"$type": "%s"}`, Type(e.Type).String())
}
