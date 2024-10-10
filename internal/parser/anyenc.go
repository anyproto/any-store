package parser

import (
	"encoding/json"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/anyenc"
)

func Parse(v any) (*anyenc.Value, error) {
	switch vt := v.(type) {
	case *anyenc.Value:
		return vt, nil
	case *fastjson.Value:
		a := &anyenc.Arena{}
		return a.NewFromFastJson(vt), nil
	case string:
		return anyenc.ParseJson(vt)
	case []byte:
		return anyenc.Parse(vt)
	default:
		jb, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		return anyenc.ParseJson(string(jb))
	}
}
