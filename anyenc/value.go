package anyenc

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/valyala/fastjson"
)

type Value struct {
	o Object
	a []*Value
	v []byte
	n float64
	t Type
}

type keyValue struct {
	key   string
	value *Value
}

// Set sets (key, value) entry in the array or object v.
//
// The value must be unchanged during v lifetime.
func (v *Value) Set(key string, value *Value) {
	if v == nil {
		return
	}
	if v.t == TypeObject {
		v.o.Set(key, value)
		return
	}
	if v.t == TypeArray {
		idx, err := strconv.Atoi(key)
		if err != nil || idx < 0 {
			return
		}
		v.SetArrayItem(idx, value)
	}
}

func (v *Value) Del(key string) {
	if v == nil {
		return
	}
	if v.t == TypeObject {
		v.o.Del(key)
		return
	}
	if v.t == TypeArray {
		idx, err := strconv.Atoi(key)
		if err != nil || idx < 0 {
			return
		}
		n, err := strconv.Atoi(key)
		if err != nil || n < 0 || n >= len(v.a) {
			return
		}
		v.a = append(v.a[:n], v.a[n+1:]...)
	}
}

// SetArrayItem sets the value in the array v at idx position.
//
// The value must be unchanged during v lifetime.
func (v *Value) SetArrayItem(idx int, value *Value) {
	if v == nil || v.t != TypeArray {
		return
	}
	for idx >= len(v.a) {
		v.a = append(v.a, valueNull)
	}
	v.a[idx] = value
}

// Get returns value by the given keys path.
//
// Array indexes may be represented as decimal numbers in keys.
//
// nil is returned for non-existing keys path.
//
// The returned value is valid until Parse is called on the Parser returned v.
func (v *Value) Get(keys ...string) *Value {
	if v == nil {
		return nil
	}
	for _, key := range keys {
		if v.t == TypeObject {
			v = v.o.Get(key)
			if v == nil {
				return nil
			}
		} else if v.t == TypeArray {
			n, err := strconv.Atoi(key)
			if err != nil || n < 0 || n >= len(v.a) {
				return nil
			}
			v = v.a[n]
		} else {
			return nil
		}
	}
	return v
}

func (v *Value) Float64() (float64, error) {
	if v.Type() != TypeNumber {
		return 0, fmt.Errorf("value doesn't contain number; it contains %s", v.Type())
	}
	return v.n, nil
}

func (v *Value) Int() (int, error) {
	n, err := v.Float64()
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

func (v *Value) StringBytes() ([]byte, error) {
	if v.Type() != TypeString {
		return nil, fmt.Errorf("value doesn't contain string; it contains %s", v.Type())
	}
	return v.v, nil
}

func (v *Value) Bytes() ([]byte, error) {
	if v.Type() != TypeBinary {
		return nil, fmt.Errorf("value doesn't contain binary; it contains %s", v.Type())
	}
	return v.v, nil
}

func (v *Value) AppendBytes(b []byte) ([]byte, error) {
	if r, err := v.Bytes(); err != nil {
		return nil, err
	} else {
		return append(b, r...), nil
	}
}

func (v *Value) Array() ([]*Value, error) {
	if v.Type() != TypeArray {
		return nil, fmt.Errorf("value doesn't contain array; it contains %s", v.Type())
	}
	return v.a, nil
}

func (v *Value) Object() (*Object, error) {
	if v.Type() != TypeObject {
		return nil, fmt.Errorf("value doesn't contain object; it contains %s", v.Type())
	}
	return &v.o, nil
}

func (v *Value) Bool() (bool, error) {
	switch v.Type() {
	case TypeTrue:
		return true, nil
	case TypeFalse:
		return false, nil
	default:
		return false, fmt.Errorf("value doesn't contain bool; it contains %s", v.Type())
	}
}

func (v *Value) GetFloat64(keys ...string) float64 {
	vv := v.Get(keys...)
	if vv.Type() != TypeNumber {
		return 0
	}
	return vv.n
}

func (v *Value) GetInt(keys ...string) int {
	vv := v.Get(keys...)
	if vv.Type() != TypeNumber {
		return 0
	}
	return int(vv.n)
}

func (v *Value) GetString(keys ...string) string {
	vv := v.Get(keys...)
	if vv.Type() != TypeString {
		return ""
	}
	return string(v.v)
}

func (v *Value) GetStringBytes(keys ...string) []byte {
	vv := v.Get(keys...)
	if vv.Type() != TypeString {
		return nil
	}
	return v.v
}

func (v *Value) GetBytes(keys ...string) []byte {
	vv := v.Get(keys...)
	if vv.Type() != TypeBinary {
		return nil
	}
	return v.v
}

func (v *Value) GetArray(keys ...string) []*Value {
	vv := v.Get(keys...)
	if vv.Type() != TypeArray {
		return nil
	}
	return v.a
}

func (v *Value) GetObject(keys ...string) *Object {
	vv := v.Get(keys...)
	if vv.Type() != TypeObject {
		return nil
	}
	return &v.o
}

func (v *Value) GetBool(keys ...string) bool {
	vv := v.Get(keys...)
	if vv.Type() == TypeTrue {
		return true
	}
	return false
}

func (v *Value) Type() Type {
	if v == nil {
		return TypeNull
	}
	return v.t
}

func (v *Value) MarshalTo(dst []byte) []byte {
	if v == nil {
		return append(dst, byte(TypeNull))
	}
	switch v.t {
	case TypeObject:
		return v.marshalObject(dst)
	case TypeArray:
		dst = append(dst, byte(TypeArray))
		for _, av := range v.a {
			dst = av.MarshalTo(dst)
		}
		dst = append(dst, EOS)
	case TypeString:
		dst = append(dst, byte(TypeString))
		dst = append(dst, v.v...)
		dst = append(dst, EOS)
	case TypeNumber:
		dst = append(dst, byte(TypeNumber))
		dst = AppendFloat64(dst, v.n)
	case TypeNull:
		dst = append(dst, byte(TypeNull))
	case TypeTrue:
		dst = append(dst, byte(TypeTrue))
	case TypeFalse:
		dst = append(dst, byte(TypeFalse))
	case TypeBinary:
		dst = append(dst, byte(TypeBinary))
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(v.v)))
		return append(dst, v.v...)
	}
	return dst
}

func (v *Value) marshalObject(dst []byte) []byte {
	dst = append(dst, byte(TypeObject))
	for _, kv := range v.o.kvs {
		dst = append(dst, kv.key...)
		dst = append(dst, EOS)
		dst = kv.value.MarshalTo(dst)
	}
	return append(dst, EOS)
}

func (v *Value) FastJson(a *fastjson.Arena) *fastjson.Value {
	switch v.Type() {
	case TypeNumber:
		return a.NewNumberFloat64(v.n)
	case TypeString:
		return a.NewStringBytes(v.v)
	case TypeBinary:
		return a.NewString(base64.StdEncoding.EncodeToString(v.v))
	case TypeArray:
		arr := a.NewArray()
		for i, av := range v.a {
			arr.SetArrayItem(i, av.FastJson(a))
		}
		return arr
	case TypeObject:
		obj := a.NewObject()
		for _, kv := range v.o.kvs {
			obj.Set(kv.key, kv.value.FastJson(a))
		}
		return obj
	case TypeTrue:
		return a.NewTrue()
	case TypeFalse:
		return a.NewFalse()
	case TypeNull:
		return a.NewNull()
	default:
		panic(fmt.Errorf("unexpected type: %s", v.Type()))
	}
}

func (v *Value) String() string {
	return v.FastJson(&fastjson.Arena{}).String()
}

func (v *Value) GoType() any {
	switch v.Type() {
	case TypeNumber:
		return v.n
	case TypeString:
		return string(v.v)
	case TypeBinary:
		return append([]byte{}, v.v...)
	case TypeArray:
		res := make([]any, len(v.a))
		for i, av := range v.a {
			res[i] = av.GoType()
		}
		return res
	case TypeObject:
		obj := make(map[string]any)
		for _, kv := range v.o.kvs {
			obj[kv.key] = kv.value.GoType()
		}
		return obj
	case TypeTrue:
		return true
	case TypeFalse:
		return false
	case TypeNull:
		return nil
	default:
		panic(fmt.Errorf("unexpected type: %s", v.Type()))
	}
}
