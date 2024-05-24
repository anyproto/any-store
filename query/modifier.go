package query

import (
	"fmt"
	"strconv"

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
}

func (m modifierSet) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	prevField := v
	for i, path := range m.fieldPath {
		field := prevField.Get(path)
		if i == len(m.fieldPath)-1 {
			modified = !equal(field, m.val)
			if prevField.Type() == fastjson.TypeArray {
				idx, err := strconv.Atoi(path)
				if err != nil || idx < 0 {
					return nil, false, fmt.Errorf("cannot create field '%s' in element %s", path, prevField.String())
				}
				prevField.SetArrayItem(idx, m.val)
			} else {
				prevField.Set(path, m.val)
			}
			return v, modified, nil
		} else {
			if field == nil {
				field = a.NewObject()
			} else {
				switch field.Type() {
				case fastjson.TypeObject:
				case fastjson.TypeArray:
				default:
					return nil, false, fmt.Errorf("cannot create field '%s' in element %s", m.fieldPath[i+1], field.String())
				}
			}
			if prevField.Type() == fastjson.TypeArray {
				idx, err := strconv.Atoi(path)
				if err != nil || idx < 0 {
					return nil, false, fmt.Errorf("cannot create field '%s' in element %s", path, prevField.String())
				}
				prevField.SetArrayItem(idx, field)
			} else {
				prevField.Set(path, field)
			}
			prevField = field
		}
	}
	return
}

type modifierUnset struct {
	fieldPath []string
}

func (m modifierUnset) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	prevField := v
	for i, path := range m.fieldPath {
		if prevField == nil {
			return v, false, nil
		}
		if i == len(m.fieldPath)-1 {
			if prevField.Exists(path) {
				prevField.Del(path)
				modified = true
			}
		} else {
			prevField = prevField.Get(path)
		}
	}
	return v, modified, nil
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
