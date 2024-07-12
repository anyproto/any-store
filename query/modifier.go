package query

import (
	"bytes"
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

type ModifierChain []Modifier

func (mRoot ModifierChain) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
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
	equalBuf  *equalBuf
}

func (m modifierSet) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	err = walk(a, v, m.fieldPath, true, func(path string, prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
		modified = !m.equalBuf.equal(value, m.val)
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
	val       []string
	equalBuf  *equalBuf
}

func (m modifierRename) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	var oldFieldValue *fastjson.Value
	err = walk(a, v, m.fieldPath, true, func(path string, prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
		if value != nil {
			modified = true
			oldFieldValue = value
		}
		return nil, nil
	})
	if modified {
		err = walk(a, v, m.val, true, func(path string, prevValue, newFieldValue *fastjson.Value) (res *fastjson.Value, err error) {
			modified = !m.equalBuf.equal(newFieldValue, oldFieldValue)
			return oldFieldValue, nil
		})
		if err != nil {
			return nil, false, err
		}
	}
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
	} else {
		arrayOfValues = arrayOfValues[1:]
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
		value.SetArrayItem(len(arrayOfValues), m.val)
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
	equalBuf  *equalBuf
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
		if m.filter != nil {
			modified = removeElements(arrayOfValues, value, func(value *fastjson.Value) bool {
				return m.filter.Ok(value)
			})
		} else {
			modified = removeElements(arrayOfValues, value, func(value *fastjson.Value) bool {
				return m.equalBuf.equal(value, m.val)
			})
		}
		return value, nil
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
	equalBuf      *equalBuf
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
		modified = removeElements(arrayOfValues, value, func(value *fastjson.Value) bool {
			return slices.ContainsFunc(m.removedValues, func(removedValue *fastjson.Value) bool {
				return m.equalBuf.equal(value, removedValue)
			})
		})
		return value, nil
	})
	if err != nil {
		return nil, false, err
	}
	result = v
	return
}

func removeElements(arrayOfValues []*fastjson.Value, value *fastjson.Value, shouldRemove func(*fastjson.Value) bool) bool {
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
	for i := len(arrayOfValues) - 1; i >= n; i-- {
		key := strconv.FormatInt(int64(i), 10)
		value.Del(key)
	}
	return modified
}

type modifierAddToSet struct {
	fieldPath []string
	val       *fastjson.Value
	equalBuf  *equalBuf
}

func (m modifierAddToSet) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	err = walk(a, v, m.fieldPath, true, func(path string, prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
		if value == nil {
			return nil, nil
		}
		_, err = value.Array()
		if err != nil {
			return nil, fmt.Errorf("failed to pop item, %w", err)
		}
		modified = m.addElements(value, m.val)
		return value, nil
	})
	if err != nil {
		return nil, false, err
	}
	result = v
	return
}

func (m modifierAddToSet) addElements(value *fastjson.Value, addElem *fastjson.Value) bool {
	arrayOfValues := value.GetArray()
	if slices.ContainsFunc(arrayOfValues, func(val *fastjson.Value) bool {
		return m.equalBuf.equal(addElem, val)
	}) {
		return false
	}
	value.SetArrayItem(len(arrayOfValues), addElem)
	return true
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

type equalBuf struct {
	left, right []byte
}

func (eb *equalBuf) equal(v1, v2 *fastjson.Value) bool {
	if v1 == v2 {
		return true
	}
	if v1 == nil || v2 == nil {
		return false
	}
	if v1.Type() != v2.Type() {
		return false
	}
	eb.left = v1.MarshalTo(eb.left[:0])
	eb.right = v2.MarshalTo(eb.right[:0])
	return bytes.Equal(eb.left, eb.right)
}
