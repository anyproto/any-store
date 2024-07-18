package jsonutil

import (
	"bytes"

	"github.com/valyala/fastjson"
)

type Value struct {
	Value *fastjson.Value
	p     *fastjson.Parser
	buf   []byte
}

func (v *Value) FillCopy(fv *fastjson.Value) {
	v.buf = fv.MarshalTo(v.buf[:0])
	if v.p == nil {
		v.p = &fastjson.Parser{}
	}
	v.Value, _ = v.p.ParseBytes(v.buf)
	return
}

func (v *Value) Equal(b *fastjson.Value) bool {
	return Equal(v.Value, b)
}

// Equal checks if two JSON values are equal
func Equal(a, b *fastjson.Value) bool {
	if a == b {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.Type() != b.Type() {
		return false
	}
	switch a.Type() {
	case fastjson.TypeArray:
		return equalArray(a, b)
	case fastjson.TypeObject:
		return equalObject(a, b)
	case fastjson.TypeString:
		return bytes.Equal(a.GetStringBytes(), b.GetStringBytes())
	case fastjson.TypeNumber:
		return a.GetFloat64() == b.GetFloat64()
	}
	return true
}

func equalArray(a *fastjson.Value, b *fastjson.Value) bool {
	aa := a.GetArray()
	ba := b.GetArray()
	if len(aa) != len(ba) {
		return false
	}
	for i := range aa {
		if !Equal(aa[i], ba[i]) {
			return false
		}
	}
	return true
}

func equalObject(a *fastjson.Value, b *fastjson.Value) bool {
	aa := a.GetObject()
	ba := b.GetObject()
	eq := true
	var la, lb int
	aa.Visit(func(ka []byte, va *fastjson.Value) {
		la++
		lb = 0
		if !eq {
			return
		}
		var bFound bool
		ba.Visit(func(kb []byte, vb *fastjson.Value) {
			lb++
			if !eq {
				return
			}
			if bytes.Equal(ka, kb) {
				bFound = true
				eq = Equal(va, vb)
			}
		})
		if !bFound {
			eq = false
		}
	})
	if la != lb {
		return false
	}
	return eq
}
