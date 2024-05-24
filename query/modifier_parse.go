package query

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/parser"
)

var (
	opBytesSet      = []byte("$set")
	opBytesUnset    = []byte("$unset")
	opBytesInc      = []byte("$inc")
	opBytesRename   = []byte("$rename")
	opBytesPop      = []byte("$pop")
	opBytesPush     = []byte("$push")
	opBytesPull     = []byte("$pull")
	opBytesPullAll  = []byte("$pullAll")
	opBytesAddToSet = []byte("$addToSet")
)

func MustParseModifier(modifier any) Modifier {
	res, err := ParseModifier(modifier)
	if err != nil {
		panic(err)
	}
	return res
}

func ParseModifier(modifier any) (Modifier, error) {
	if m, ok := modifier.(Modifier); ok {
		return m, nil
	}
	p := parserPool.Get()
	defer parserPool.Put(p)

	v, err := parser.AnyToJSON(p, modifier)
	if err != nil {
		return nil, err
	}
	return parseModRoot(v)
}

func parseModRoot(v *fastjson.Value) (m Modifier, err error) {
	obj, err := v.Object()
	if err != nil {
		return nil, err
	}
	root := modifierRoot{}
	obj.Visit(func(key []byte, v *fastjson.Value) {
		if err != nil {
			return
		}
		switch {
		case bytes.Equal(key, opBytesSet):
			var setMod modifierRoot
			if setMod, err = parseModSet(v); err != nil {
				return
			}
			root = append(root, setMod...)
		case bytes.Equal(key, opBytesUnset):
			var setMod modifierRoot
			if setMod, err = parseModUnset(v); err != nil {
				return
			}
			root = append(root, setMod...)
		default:
			err = fmt.Errorf("unknown modifier '%s'", string(key))
		}
	})
	if err != nil {
		return nil, err
	}
	if len(root) > 0 {
		return root, nil
	}
	return nil, fmt.Errorf("empty modifier")
}

func parseModSet(v *fastjson.Value) (root modifierRoot, err error) {
	obj, err := v.Object()
	if err != nil {
		return nil, err
	}
	obj.Visit(func(key []byte, v *fastjson.Value) {
		if err != nil {
			return
		}
		if bytes.HasPrefix(key, opBytesPrefix) {
			err = fmt.Errorf("unexpect identifier '%s' in $set", string(key))
			return
		}
		root = append(root, newSetModifier(key, v))
	})
	return
}

func newSetModifier(key []byte, v *fastjson.Value) modifierSet {
	return modifierSet{
		fieldPath: strings.Split(string(key), "."),
		val:       v,
	}
}

func parseModUnset(v *fastjson.Value) (root modifierRoot, err error) {
	obj, err := v.Object()
	if err != nil {
		return nil, err
	}
	obj.Visit(func(key []byte, v *fastjson.Value) {
		if err != nil {
			return
		}
		if bytes.HasPrefix(key, opBytesPrefix) {
			err = fmt.Errorf("unexpect identifier '%s' in $unset", string(key))
			return
		}
		root = append(root, newUnsetModifier(key))
	})
	return
}

func newUnsetModifier(key []byte) modifierUnset {
	return modifierUnset{
		fieldPath: strings.Split(string(key), "."),
	}
}
