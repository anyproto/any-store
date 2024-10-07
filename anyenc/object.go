package anyenc

import "slices"

type Object struct {
	kvs []keyValue
}

func (o *Object) Visit(visit func(k string, v *Value)) {
	for _, kv := range o.kvs {
		visit(kv.key, kv.value)
	}
}

func (o *Object) Get(key string) *Value {
	for _, kv := range o.kvs {
		if kv.key == key {
			return kv.value
		}
	}
	return nil
}

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

func (o *Object) Del(key string) {
	o.kvs = slices.DeleteFunc(o.kvs, func(kv keyValue) bool {
		return kv.key == key
	})
}

func (o *Object) Len() int {
	return len(o.kvs)
}
