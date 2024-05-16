package parser

import (
	"encoding/json"

	"github.com/valyala/fastjson"
)

func AnyToJSON(p *fastjson.Parser, doc any) (v *fastjson.Value, err error) {
	switch d := doc.(type) {
	case string:
		return p.Parse(d)
	case []byte:
		return p.ParseBytes(d)
	case *fastjson.Value:
		return d, nil
	default:
		jb, err := json.Marshal(doc)
		if err != nil {
			return nil, err
		}
		return p.ParseBytes(jb)
	}
}
