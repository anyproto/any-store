package anystore

import "github.com/valyala/fastjson"

type Doc interface {
	Decode(v any) (err error)
	Value() *fastjson.Value
}
