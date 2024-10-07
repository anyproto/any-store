package anyenc

import "github.com/valyala/fastjson"

// Arena may be used for fast creation and re-use of Values.
//
// Typical Arena lifecycle:
//
//  1. Construct Values via the Arena and Value.Set* calls.
//  2. Marshal the constructed Values with Value.MarshalTo call.
//  3. Reset all the constructed Values at once by Arena.Reset call.
//  4. Go to 1 and re-use the Arena.
//
// It is unsafe calling Arena methods from concurrent goroutines.
// Use per-goroutine Arenas or ArenaPool instead.
type Arena struct {
	c cache
}

// Reset resets all the Values allocated by a.
//
// Values previously allocated by a cannot be used after the Reset call.
func (a *Arena) Reset() {
	a.c.reset()
}

// NewObject returns new empty object value.
//
// New entries may be added to the returned object via Set call.
//
// The returned object is valid until Reset is called on a.
func (a *Arena) NewObject() *Value {
	v := a.c.getValue()
	v.t = TypeObject
	v.o.kvs = v.o.kvs[:0]
	return v
}

// NewArray returns new empty array value.
//
// New entries may be added to the returned array via Set* calls.
//
// The returned array is valid until Reset is called on a.
func (a *Arena) NewArray() *Value {
	v := a.c.getValue()
	v.t = TypeArray
	v.a = v.a[:0]
	return v
}

// NewString returns new string value containing s.
//
// The returned string is valid until Reset is called on a.
func (a *Arena) NewString(s string) *Value {
	v := a.c.getValue()
	v.t = TypeString
	v.v = append(v.v[:0], s...)
	return v
}

// NewStringBytes returns new string value containing s.
//
// The returned string is valid until Reset is called on a.
func (a *Arena) NewStringBytes(s []byte) *Value {
	v := a.c.getValue()
	v.t = TypeString
	v.v = append(v.v[:0], s...)
	return v
}

// NewBinary returns new binary value containing b.
//
// The returned value is valid until Reset is called on a.
func (a *Arena) NewBinary(b []byte) *Value {
	v := a.c.getValue()
	v.t = TypeBinary
	v.v = append(v.v[:0], b...)
	return v
}

// NewNumberFloat64 returns new number value containing f.
//
// The returned number is valid until Reset is called on a.
func (a *Arena) NewNumberFloat64(f float64) *Value {
	v := a.c.getValue()
	v.t = TypeNumber
	v.n = f
	return v
}

// NewNumberInt returns new number value containing n.
//
// The returned number is valid until Reset is called on a.
func (a *Arena) NewNumberInt(n int) *Value {
	v := a.c.getValue()
	v.t = TypeNumber
	v.n = float64(n)
	return v
}

// NewNull returns null value.
func (a *Arena) NewNull() *Value {
	return valueNull
}

// NewTrue returns true value.
func (a *Arena) NewTrue() *Value {
	return valueTrue
}

// NewFalse return false value.
func (a *Arena) NewFalse() *Value {
	return valueFalse
}

func (a *Arena) NewFromFastJson(jv *fastjson.Value) *Value {
	switch jv.Type() {
	case fastjson.TypeNull:
		return a.NewNull()
	case fastjson.TypeTrue:
		return a.NewTrue()
	case fastjson.TypeFalse:
		return a.NewFalse()
	case fastjson.TypeNumber:
		return a.NewNumberFloat64(jv.GetFloat64())
	case fastjson.TypeString:
		return a.NewStringBytes(jv.GetStringBytes())
	case fastjson.TypeArray:
		arr := a.NewArray()
		for i, av := range jv.GetArray() {
			arr.SetArrayItem(i, a.NewFromFastJson(av))
		}
		return arr
	case fastjson.TypeObject:
		obj := a.NewObject()
		jv.GetObject().Visit(func(key []byte, v *fastjson.Value) {
			obj.Set(string(key), a.NewFromFastJson(v))
		})
		return obj
	}
	return nil
}
