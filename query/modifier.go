package query

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/valyala/fastjson"
)

type Modifier interface {
	Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error)
}

type ModifyFunc func(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error)

func (m ModifyFunc) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	if m == nil {
		return nil, false, fmt.Errorf("modify func is nil")
	}
	return m(a, v)
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
	err = walk(a, v, m.fieldPath, true, func(path string, prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
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
	err = walk(a, v, m.fieldPath, false, func(path string, prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
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
	err = walk(a, v, m.fieldPath, true, func(path string, prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
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

type modifierRename struct {
	fieldPath []string
	val       string
}

func (m modifierRename) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	err = walk(a, v, m.fieldPath, true, func(path string, prevValue, oldFieldValue *fastjson.Value) (res *fastjson.Value, err error) {
		if path == m.val {
			return oldFieldValue, nil
		}
		modified = oldFieldValue != nil
		if modified {
			err = walk(a, v, []string{m.val}, true, func(path string, prevValue, newFieldValue *fastjson.Value) (res *fastjson.Value, err error) {
				modified = !equal(newFieldValue, oldFieldValue)
				return oldFieldValue, nil
			})
			if err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	result = v
	return
}

type modifierPop struct {
	fieldPath []string
	val       int
}

func (m modifierPop) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	err = walk(a, v, m.fieldPath, true, func(path string, prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
		if value == nil {
			return nil, nil
		}
		arrayOfValues, err := value.Array()
		if err != nil {
			return nil, fmt.Errorf("failed to pop item, %w", err)
		}
		if len(arrayOfValues) == 0 {
			return value, nil
		}
		arrayOfValues, err = m.getResultArray(arrayOfValues)
		if err != nil {
			return nil, err
		}
		modified = true
		newValue := a.NewArray()
		for i, val := range arrayOfValues {
			newValue.SetArrayItem(i, val)
		}
		return newValue, nil
	})
	if err != nil {
		return nil, false, err
	}
	result = v
	return
}

func (m modifierPop) getResultArray(arrayOfValues []*fastjson.Value) ([]*fastjson.Value, error) {
	if m.val == 1 {
		arrayOfValues = arrayOfValues[:len(arrayOfValues)-1]
	} else if m.val == -1 {
		arrayOfValues = arrayOfValues[1:]
	} else {
		return nil, fmt.Errorf("failed to pop item: wrong argument")
	}
	return arrayOfValues, nil
}

type modifierPush struct {
	fieldPath []string
	val       *fastjson.Value
}

func (m modifierPush) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	err = walk(a, v, m.fieldPath, true, func(path string, prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
		if value == nil {
			return nil, nil
		}
		arrayOfValues, err := value.Array()
		if err != nil {
			return nil, fmt.Errorf("failed to pop item, %w", err)
		}
		arrayOfValues = append(arrayOfValues, m.val)
		for i, val := range arrayOfValues {
			value.SetArrayItem(i, val)
		}
		modified = true
		return value, nil
	})
	if err != nil {
		return nil, false, err
	}
	result = v
	return
}

type modifierPull struct {
	fieldPath []string
	filter    Filter
	val       *fastjson.Value
}

func (m modifierPull) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	err = walk(a, v, m.fieldPath, true, func(path string, prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
		if value == nil {
			return nil, nil
		}
		arrayOfValues, err := value.Array()
		if err != nil {
			return nil, fmt.Errorf("failed to pop item, %w", err)
		}
		if len(arrayOfValues) == 0 {
			return value, nil
		}
		array := a.NewArray()
		if m.filter != nil {
			modified = removeElements(arrayOfValues, array, func(value *fastjson.Value) bool {
				return m.filter.Ok(value)
			})
		} else {
			modified = removeElements(arrayOfValues, array, func(value *fastjson.Value) bool {
				return equal(value, m.val)
			})
		}
		return array, nil
	})
	if err != nil {
		return nil, false, err
	}
	result = v
	return
}

type modifierPullAll struct {
	fieldPath     []string
	removedValues []*fastjson.Value
}

func (m modifierPullAll) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	err = walk(a, v, m.fieldPath, true, func(path string, prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
		if value == nil {
			return nil, nil
		}
		arrayOfValues, err := value.Array()
		if err != nil {
			return nil, fmt.Errorf("failed to pop item, %w", err)
		}
		array := a.NewArray()
		modified = removeElements(arrayOfValues, array, func(value *fastjson.Value) bool {
			return slices.ContainsFunc(m.removedValues, func(removedValue *fastjson.Value) bool {
				return equal(value, removedValue)
			})
		})
		return array, nil
	})
	if err != nil {
		return nil, false, err
	}
	result = v
	return
}

func removeElements(arrayOfValues []*fastjson.Value, newArray *fastjson.Value, shouldRemove func(*fastjson.Value) bool) bool {
	n := 0
	var modified bool
	for _, val := range arrayOfValues {
		if !shouldRemove(val) {
			arrayOfValues[n] = val
			n++
		} else {
			modified = true
		}
	}
	arrayOfValues = arrayOfValues[:n]
	for i, val := range arrayOfValues {
		newArray.SetArrayItem(i, val)
	}
	return modified
}

type modifierAddToSet struct {
	fieldPath []string
	val       *fastjson.Value
}

func (m modifierAddToSet) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	err = walk(a, v, m.fieldPath, true, func(path string, prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
		if value == nil {
			return nil, nil
		}
		arrayOfValues, err := value.Array()
		if err != nil {
			return nil, fmt.Errorf("failed to pop item, %w", err)
		}
		arrayOfValues, modified = m.addElements(arrayOfValues, m.val)
		for i, val := range arrayOfValues {
			value.SetArrayItem(i, val)
		}
		return value, nil
	})
	if err != nil {
		return nil, false, err
	}
	result = v
	return
}

func (m modifierAddToSet) addElements(arrayOfValues []*fastjson.Value, addElem *fastjson.Value) ([]*fastjson.Value, bool) {
	if slices.ContainsFunc(arrayOfValues, func(val *fastjson.Value) bool {
		return equal(addElem, val)
	}) {
		return arrayOfValues, false
	}
	arrayOfValues = append(arrayOfValues, addElem)
	return arrayOfValues, true
}

func walk(a *fastjson.Arena, v *fastjson.Value, fieldPath []string, create bool, finalize func(path string, prevValue *fastjson.Value, value *fastjson.Value) (res *fastjson.Value, err error)) (err error) {
	prevField := v
	for i, path := range fieldPath {
		field := prevField.Get(path)
		if i == len(fieldPath)-1 {
			var newVal *fastjson.Value
			if newVal, err = finalize(path, prevField, field); err != nil {
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
