package query

import (
	"fmt"

	"github.com/valyala/fastjson"
)

type Modifier interface {
	Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error)
}

type modifierRoot []Modifier

func (mRoot modifierRoot) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	result = v
	var ok bool
	for _, m := range mRoot {
		result, ok, err = m.Modify(a, result)
		if err != nil {
			return nil, false, err
		}
		if ok {
			modified = true
		}
	}
	return
}

type modifierSet struct {
	fieldPath []string
	val       *fastjson.Value
	buf       []byte
}

func (m modifierSet) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	prevField := v
	for i, path := range m.fieldPath {
		field := prevField.Get(path)
		if i == len(m.fieldPath)-1 {
			modified = !equal(field, m.val)
			prevField.Set(path, m.val)
			return v, modified, nil
		} else {
			if field == nil {
				field = a.NewObject()
			} else {
				switch field.Type() {
				case fastjson.TypeObject:
				case fastjson.TypeArray:
				default:
					return nil, false, fmt.Errorf("cannot create field '%s' in element %s", path, field.String())
				}
			}
			prevField.Set(path, field)
			prevField = field
		}
	}
	return
}

func equal(v1, v2 *fastjson.Value) bool {
	if v1 == v2 {
		return true
	}
	if v1 == nil || v2 == nil {
		return false
	}
	if v1.Type() != v2.Type() {
		return false
	}
	// TODO: maybe not very fast
	return v1.String() == v2.String()
}
