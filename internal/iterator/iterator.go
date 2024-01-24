package iterator

import "github.com/valyala/fastjson"

type Iterator interface {
	Next() bool
	Valid() bool
	Close() error
}

type IdIterator interface {
	Iterator
	CurrentId() []byte
}

type ValueIterator interface {
	IdIterator
	CurrentValue(onValue func(v *fastjson.Value) error) error
}
