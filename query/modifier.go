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
	err = walk(a, v, m.fieldPath, true, func(prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
		modified = !equal(value, m.val)
		return m.val, nil
	})
	if err != nil {
		return nil, false, err
	}
	result = v
	return
}

type modifierUnset struct {
	fieldPath []string
}

func (m modifierUnset) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	err = walk(a, v, m.fieldPath, false, func(prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
		modified = value != nil
		return nil, nil
	})
	if err != nil {
		return nil, false, err
	}
	result = v
	return
}

type modifierInc struct {
	fieldPath []string
	val       float64
}

func (m modifierInc) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	err = walk(a, v, m.fieldPath, true, func(prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
		if value == nil {
			modified = true
			return a.NewNumberFloat64(m.val), nil
		}
		if value.Type() != fastjson.TypeNumber {
			return nil, fmt.Errorf("not numeric value '%s'", value.String())
		}
		modified = true
		return a.NewNumberFloat64(value.GetFloat64() + m.val), nil
	})
	if err != nil {
		return nil, false, err
	}
	result = v
	return
}

func walk(
	a *fastjson.Arena,
	v *fastjson.Value,
	fieldPath []string,
	create bool,
	finalize func(prevValue, value *fastjson.Value) (res *fastjson.Value, err error),
) (err error) {
	prevField := v
	for i, path := range fieldPath {
		field := prevField.Get(path)
		if i == len(fieldPath)-1 {
			var newVal *fastjson.Value
			if newVal, err = finalize(prevField, field); err != nil {
				return
			}
			if newVal == nil {
				if field != nil {
					prevField.Del(path)
				}
				return
			}
			if prevField.Type() == fastjson.TypeArray {
				idx, err := strconv.Atoi(path)
				if err != nil || idx < 0 {
					return fmt.Errorf("cannot create field '%s' in element %s", path, prevField.String())
				}
				prevField.SetArrayItem(idx, newVal)
			} else {
				prevField.Set(path, newVal)
			}
			return
		} else {
			if field == nil {
				if create {
					field = a.NewObject()
				} else {
					return nil
				}
			} else {
				switch field.Type() {
				case fastjson.TypeObject:
				case fastjson.TypeArray:
				default:
					return fmt.Errorf("cannot create field '%s' in element %s", fieldPath[i+1], field.String())
				}
			}
			if prevField.Type() == fastjson.TypeArray {
				idx, err := strconv.Atoi(path)
				if err != nil || idx < 0 {
					return fmt.Errorf("cannot create field '%s' in element %s", path, prevField.String())
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
