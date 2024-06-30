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

	v, err := parser.AnyToJSON(&fastjson.Parser{}, modifier)
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
			if setMod, err = parseMod(v, newSetModifier); err != nil {
				return
			}
			root = append(root, setMod...)
		case bytes.Equal(key, opBytesUnset):
			var setMod modifierRoot
			if setMod, err = parseMod(v, newUnsetModifier); err != nil {
				return
			}
			root = append(root, setMod...)
		case bytes.Equal(key, opBytesInc):
			var setMod modifierRoot
			if setMod, err = parseMod(v, newIncModifier); err != nil {
				return
			}
			root = append(root, setMod...)
		case bytes.Equal(key, opBytesRename):
			var setMod modifierRoot
			if setMod, err = parseMod(v, newRenameModifier); err != nil {
				return
			}
			root = append(root, setMod...)
		case bytes.Equal(key, opBytesPop):
			var setMod modifierRoot
			if setMod, err = parseMod(v, newPopModifier); err != nil {
				return
			}
			root = append(root, setMod...)
		case bytes.Equal(key, opBytesPush):
			var setMod modifierRoot
			if setMod, err = parseMod(v, newPushModifier); err != nil {
				return
			}
			root = append(root, setMod...)
		case bytes.Equal(key, opBytesPull):
			var setMod modifierRoot
			if setMod, err = parseMod(v, newPullModifier); err != nil {
				return
			}
			root = append(root, setMod...)
		case bytes.Equal(key, opBytesPullAll):
			var setMod modifierRoot
			if setMod, err = parseMod(v, newPullAllModifier); err != nil {
				return
			}
			root = append(root, setMod...)
		case bytes.Equal(key, opBytesAddToSet):
			var setMod modifierRoot
			if setMod, err = parseMod(v, newAddToSetModifier); err != nil {
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

func parseMod(v *fastjson.Value, create func(key []byte, val *fastjson.Value) (Modifier, error)) (root modifierRoot, err error) {
	obj, err := v.Object()
	if err != nil {
		return nil, err
	}
	obj.Visit(func(key []byte, v *fastjson.Value) {
		if err != nil {
			return
		}

		if bytes.HasPrefix(key, opBytesPrefix) {
			err = fmt.Errorf("unexpect identifier '%s'", string(key))
			return
		}
		var mod Modifier
		if mod, err = create(key, v); err != nil {
			return
		}
		root = append(root, mod)
	})
	return
}

func newSetModifier(key []byte, v *fastjson.Value) (Modifier, error) {
	return modifierSet{
		fieldPath: strings.Split(string(key), "."),
		val:       v,
	}, nil
}

func newUnsetModifier(key []byte, _ *fastjson.Value) (Modifier, error) {
	return modifierUnset{
		fieldPath: strings.Split(string(key), "."),
	}, nil
}

func newIncModifier(key []byte, v *fastjson.Value) (Modifier, error) {
	if v.Type() != fastjson.TypeNumber {
		return nil, fmt.Errorf("not numeric value for $inc in field '%s'", string(key))
	}
	return modifierInc{
		fieldPath: strings.Split(string(key), "."),
		val:       v.GetFloat64(),
	}, nil
}

func newRenameModifier(key []byte, v *fastjson.Value) (Modifier, error) {
	return modifierRename{
		fieldPath: strings.Split(string(key), "."),
		val:       v,
	}, nil
}

func newPopModifier(key []byte, v *fastjson.Value) (Modifier, error) {
	return modifierPop{
		fieldPath: strings.Split(string(key), "."),
		val:       v,
	}, nil
}

func newPushModifier(key []byte, v *fastjson.Value) (Modifier, error) {
	return modifierPush{
		fieldPath: strings.Split(string(key), "."),
		val:       v,
	}, nil
}

func newPullModifier(key []byte, v *fastjson.Value) (Modifier, error) {
	return modifierPull{
		fieldPath: strings.Split(string(key), "."),
		val:       v,
	}, nil
}

func newPullAllModifier(key []byte, v *fastjson.Value) (Modifier, error) {
	return modifierPullAll{
		fieldPath: strings.Split(string(key), "."),
		val:       v,
	}, nil
}

func newAddToSetModifier(key []byte, v *fastjson.Value) (Modifier, error) {
	return modifierAddToSet{
		fieldPath: strings.Split(string(key), "."),
		val:       v,
	}, nil
}
