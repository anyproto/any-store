package key

import (
	"bytes"
	"fmt"

	"github.com/valyala/fastjson"

	encoding2 "github.com/anyproto/any-store/encoding"
)

func New() Key {
	return nil
}

type Key []byte

func (k Key) AppendAny(v any) Key {
	return encoding2.AppendAnyValue(k, v)
}

func (k Key) AppendJSON(v *fastjson.Value) Key {
	return encoding2.AppendJSONValue(k, v)
}

func (k Key) AppendInvertedJSON(v *fastjson.Value) Key {
	return encoding2.AppendInvertedJSON(k, v)
}

func (k Key) Empty() bool {
	return len(k) == 0
}

func (k Key) ReadJSONValue(p *fastjson.Parser, a *fastjson.Arena, f func(v *fastjson.Value) error) (err error) {
	var start int
	var v *fastjson.Value
	var k2 = k
	for len(k2) > 0 {
		if v, start, err = encoding2.DecodeToJSON(p, a, k2); err != nil {
			return err
		}
		if err = f(v); err != nil {
			return
		}
		k2 = k2[start:]
	}
	return
}

func (k Key) ReadAnyValue(f func(v any) error) (err error) {
	var v any
	var start int

	var k2 = k

	for len(k2) > 0 {
		if v, start, err = encoding2.DecodeToAny(k2); err != nil {
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

func (k Key) ReadByteValues(f func(b []byte) error) (err error) {
	var start int

	var k2 = k
	var val []byte
	for len(k2) > 0 {
		if val, start, err = encoding2.DecodeToByte(k2); err != nil {
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

func (k Key) String() (res string) {
	err := k.ReadAnyValue(func(v any) error {
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
