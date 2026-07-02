package anyenc

import (
	"encoding/base64"

	"github.com/valyala/fastjson"
)

// Extended-JSON type wrappers carry anyenc's non-JSON value types through plain
// JSON, MongoDB-shell style. ParseJson / NewFromFastJson decode them into typed
// values and FastJson / String render them back, so anyenc's JSON API is a
// lossless round-trip for objectID, binary and vectorF32:
//
//	{"$oid":    "<24-hex>"}   <-> TypeObjectID
//	{"$binary": "<base64>"}   <-> TypeBinary
//	{"$vector": [<numbers>]}  <-> TypeVectorF32
//
// Decoding is lenient: a single-key object is read as a typed value only when
// its payload is well-formed (valid hex / base64 / number array); otherwise it
// stays an ordinary object, so a document that merely uses one of these keys is
// never rejected. A plain object of exactly this shape is therefore
// indistinguishable from the typed value — the same trade-off Extended JSON
// makes in MongoDB.
const (
	extTagObjectID = "$oid"
	extTagBinary   = "$binary"
	extTagVector   = "$vector"
)

// extValueFromFastJson decodes a single-key Extended-JSON wrapper object into a
// typed value, or returns nil if o is not a well-formed wrapper (the caller then
// treats o as an ordinary object). o must have exactly one key.
func (a *Arena) extValueFromFastJson(o *fastjson.Object) *Value {
	if jv := o.Get(extTagObjectID); jv != nil {
		if id, err := ObjectIDFromHex(string(jv.GetStringBytes())); err == nil {
			return a.NewObjectID(id)
		}
		return nil
	}
	if jv := o.Get(extTagBinary); jv != nil {
		if b, err := base64.StdEncoding.DecodeString(string(jv.GetStringBytes())); err == nil {
			return a.NewBinary(b)
		}
		return nil
	}
	if jv := o.Get(extTagVector); jv != nil {
		if jv.Type() != fastjson.TypeArray {
			return nil
		}
		arr := jv.GetArray()
		vec := make([]float32, len(arr))
		for i, e := range arr {
			if e.Type() != fastjson.TypeNumber {
				return nil
			}
			vec[i] = float32(e.GetFloat64())
		}
		return a.NewVectorF32(vec)
	}
	return nil
}

// extWrapFastJson builds the single-key wrapper object {tag: val}.
func extWrapFastJson(a *fastjson.Arena, tag string, val *fastjson.Value) *fastjson.Value {
	obj := a.NewObject()
	obj.Set(tag, val)
	return obj
}
