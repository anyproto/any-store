package key

import (
	"bytes"
	"fmt"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/encoding"
)

const eos = byte(0)

type Key []byte

func (k Key) AppendAny(v any) Key {
	return encoding.AppendAnyValue(k, v)
}

func (k Key) AppendJSON(v *fastjson.Value) Key {
	return encoding.AppendJSONValue(k, v)
}

func (k Key) ReadJSONValue(ns *NS, p *fastjson.Parser, a *fastjson.Arena, f func(v *fastjson.Value) error) (err error) {
	var start = ns.prefixLen
	var v *fastjson.Value
	var k2 = k[ns.prefixLen:]
	for len(k2) > 0 {
		if v, start, err = encoding.DecodeToJSON(p, a, k2); err != nil {
			return err
		}
		if err = f(v); err != nil {
			return
		}
		k2 = k2[start:]
	}
	return
}

func (k Key) ReadAnyValue(ns *NS, f func(v any) error) (err error) {
	var v any
	var start int
	if ns != nil {
		start = ns.prefixLen
	}
	var k2 = k[start:]

	for len(k2) > 0 {
		if v, start, err = encoding.DecodeToAny(k2); err != nil {
			return err
		}
		if err = f(v); err != nil {
			return
		}
		k2 = k2[start:]
		if len(k2) == 1 && k2[0] == 255 {
			return nil
		}
	}
	return
}

func (k Key) ReadByteValues(ns *NS, f func(b []byte) error) (err error) {
	var start int
	if ns != nil {
		start = ns.prefixLen
	}
	var k2 = k[start:]
	var val []byte
	for len(k2) > 0 {
		if val, start, err = encoding.DecodeToByte(k2); err != nil {
			return err
		}
		if err = f(val); err != nil {
			return
		}
		k2 = k2[start:]
	}
	return
}

func (k Key) Equal(k2 Key) bool {
	return bytes.Equal(k, k2)
}

func (k Key) String() string {
	var res string
	var startV int
	if len(k) == 0 {
		return ""
	}
	if string(k[0]) == "/" {
		for i := range k {
			if k[i] == encoding.EOS {
				startV = i + 1
				break
			}
		}
		if startV == 0 {
			return string(k)
		}
		res = string(k[:startV-1])
	} else {
		startV = 0
	}
	err := k.ReadAnyValue(&NS{prefixLen: startV}, func(v any) error {
		if res == "" {
			res += fmt.Sprintf("%v", v)
		} else {
			res += fmt.Sprintf("/%v", v)
		}
		return nil
	})
	if err != nil {
		return res + err.Error()
	}
	return res
}

func (k Key) Copy() Key {
	return bytes.Clone(k)
}

func (k Key) CopyTo(k2 Key) []byte {
	return append(k2, k...)
}
