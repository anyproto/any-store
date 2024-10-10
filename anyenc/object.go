package anyenc

import (
	"slices"
	"unsafe"
)

// Object represents an anyenc object
type Object struct {
	kvs []keyValue
}

// Visit calls visit func for every key-value pair
func (o *Object) Visit(visit func(k []byte, v *Value)) {
	for _, kv := range o.kvs {
		visit(s2b(kv.key), kv.value)
	}
}

// Get gets a value by key
func (o *Object) Get(key string) *Value {
	for _, kv := range o.kvs {
		if kv.key == key {
			return kv.value
		}
	}
	return nil
}

// Set sets value
func (o *Object) Set(key string, value *Value) {
	for i, kv := range o.kvs {
		if kv.key == key {
			o.kvs[i].value = value
			return
		}
	}
	o.kvs = slices.Grow(o.kvs, 1)[:len(o.kvs)+1]
	o.kvs[len(o.kvs)-1].key = key
	o.kvs[len(o.kvs)-1].value = value
}

// Del deletes value by key
func (o *Object) Del(key string) {
	o.kvs = slices.DeleteFunc(o.kvs, func(kv keyValue) bool {
		return kv.key == key
	})
}

// Len returns object length
func (o *Object) Len() int {
	return len(o.kvs)
}

func s2b(s string) (b []byte) {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
