package jsonutil

import (
	"bytes"

	"github.com/anyproto/any-store/anyenc"
)

type Value struct {
	Value *anyenc.Value
	p     *anyenc.Parser
	buf   []byte
}

func (v *Value) FillCopy(fv *anyenc.Value) {
	v.buf = fv.MarshalTo(v.buf[:0])
	if v.p == nil {
		v.p = &anyenc.Parser{}
	}
	v.Value, _ = v.p.Parse(v.buf)
	return
}

func (v *Value) Equal(b *anyenc.Value) bool {
	return Equal(v.Value, b)
}

// Equal checks if two JSON values are equal
func Equal(a, b *anyenc.Value) bool {
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
	case anyenc.TypeArray:
		return equalArray(a, b)
	case anyenc.TypeObject:
		return equalObject(a, b)
	case anyenc.TypeString:
		return bytes.Equal(a.GetStringBytes(), b.GetStringBytes())
	case anyenc.TypeNumber:
		return a.GetFloat64() == b.GetFloat64()
	}
	return true
}

func equalArray(a *anyenc.Value, b *anyenc.Value) bool {
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

func equalObject(a *anyenc.Value, b *anyenc.Value) bool {
	aa := a.GetObject()
	ba := b.GetObject()
	eq := true
	var la, lb int
	aa.Visit(func(ka string, va *anyenc.Value) {
		la++
		lb = 0
		if !eq {
			return
		}
		var bFound bool
		ba.Visit(func(kb string, vb *anyenc.Value) {
			lb++
			if !eq {
				return
			}
			if ka == kb {
				bFound = true
				eq = Equal(va, vb)
			}
		})
		if !bFound || lb < la {
			eq = false
		}
	})
	if la == 0 {
		ba.Visit(func(_ string, _ *anyenc.Value) {
			lb++
		})
	}
	if la != lb {
		return false
	}
	return eq
}
