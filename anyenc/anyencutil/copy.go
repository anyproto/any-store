package anyencutil

import (
	"github.com/anyproto/any-store/v2/anyenc"
)

// Copy deep-copies v onto arena a and returns the new value. The result is
// valid until Reset is called on a; it shares no memory with v, so v may come
// from a transient parser/arena that is reused afterwards.
//
// A nil v returns nil.
func Copy(a *anyenc.Arena, v *anyenc.Value) *anyenc.Value {
	if v == nil {
		return nil
	}
	switch v.Type() {
	case anyenc.TypeNull:
		return a.NewNull()
	case anyenc.TypeTrue:
		return a.NewTrue()
	case anyenc.TypeFalse:
		return a.NewFalse()
	case anyenc.TypeNumber:
		return a.NewNumberFloat64(v.GetFloat64())
	case anyenc.TypeString:
		return a.NewStringBytes(v.GetStringBytes())
	case anyenc.TypeBinary:
		return a.NewBinary(v.GetBytes())
	case anyenc.TypeVectorF32:
		vec, _ := v.VectorF32()
		return a.NewVectorF32(vec)
	case anyenc.TypeObjectID:
		id, _ := v.ObjectID()
		return a.NewObjectID(id)
	case anyenc.TypeDateTime:
		ms, _ := v.DateTimeMillis()
		return a.NewDateTimeMillis(ms)
	case anyenc.TypeArray:
		arr := a.NewArray()
		for i, av := range v.GetArray() {
			arr.SetArrayItem(i, Copy(a, av))
		}
		return arr
	case anyenc.TypeObject:
		obj := a.NewObject()
		v.GetObject().Visit(func(key []byte, item *anyenc.Value) {
			obj.Set(string(key), Copy(a, item))
		})
		return obj
	}
	return a.NewNull()
}
