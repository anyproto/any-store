package query

import (
	"bytes"
	"fmt"
	"slices"
	"strconv"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/encoding"
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

type modifierRename struct {
	fieldPath []string
	val       *fastjson.Value
}

func (m modifierRename) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	err = walk(a, v, m.fieldPath, true, func(prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
		if value == nil {
			return nil, nil
		}
		stringBytes, err := m.val.StringBytes()
		if err != nil {
			return nil, fmt.Errorf("failed to rename field: %w", err)
		}
		set := &modifierSet{fieldPath: []string{string(stringBytes)}, val: value}
		result, modified, err = set.Modify(a, prevValue)
		if err != nil {
			return nil, fmt.Errorf("failed to rename field: %w", err)
		}
		if !modified {
			return value, nil
		}
		return
	})
	if err != nil {
		return nil, false, err
	}
	result = v
	return
}

type modifierPop struct {
	fieldPath []string
	val       *fastjson.Value
}

func (m modifierPop) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	err = walk(a, v, m.fieldPath, true, func(prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
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
		return prepareNewArrayValue(a, arrayOfValues), nil
	})
	if err != nil {
		return nil, false, err
	}
	result = v
	return
}

func (m modifierPop) getResultArray(arrayOfValues []*fastjson.Value) ([]*fastjson.Value, error) {
	elem, err := m.val.Int()
	if err != nil {
		return nil, fmt.Errorf("failed to pop item, %w", err)
	}
	if elem == 1 {
		arrayOfValues = arrayOfValues[:len(arrayOfValues)-1]
	} else if elem == -1 {
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
	err = walk(a, v, m.fieldPath, true, func(prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
		if value == nil {
			return nil, nil
		}
		arrayOfValues, err := value.Array()
		if err != nil {
			return nil, fmt.Errorf("failed to pop item, %w", err)
		}
		arrayOfValues = append(arrayOfValues, m.val)
		modified = true
		return prepareNewArrayValue(a, arrayOfValues), nil
	})
	if err != nil {
		return nil, false, err
	}
	result = v
	return
}

type modifierPull struct {
	fieldPath []string
	val       *fastjson.Value
}

func (m modifierPull) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	err = walk(a, v, m.fieldPath, true, func(prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
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
		var newArray []*fastjson.Value
		if m.val.Type() == fastjson.TypeObject {
			newArray, modified = m.handleObjectValue(arrayOfValues, newArray)
		} else {
			newArray, modified = m.handleNonObjectValue(arrayOfValues, newArray)
		}
		return prepareNewArrayValue(a, newArray), nil
	})
	if err != nil {
		return nil, false, err
	}
	result = v
	return
}

func (m modifierPull) handleNonObjectValue(arrayOfValues, newArray []*fastjson.Value) ([]*fastjson.Value, bool) {
	var modified bool
	for _, val := range arrayOfValues {
		if !compare(val, m.val) {
			newArray = append(newArray, val)
		} else {
			modified = true
		}
	}
	return newArray, modified
}

func (m modifierPull) handleObjectValue(arrayOfValues, newArray []*fastjson.Value) ([]*fastjson.Value, bool) {
	var modified bool
	condition, err := ParseCompObj(m.val)
	if err == nil {
		for _, val := range arrayOfValues {
			if condition.Ok(val) {
				modified = true
				continue
			}
			newArray = append(newArray, val)
		}
	}
	return newArray, modified
}

func compare(left *fastjson.Value, right *fastjson.Value) bool {
	if left.Type() != right.Type() {
		return false
	}
	var leftBytes []byte
	leftBytes = encoding.AppendJSONValue(leftBytes, left)
	var rightBytes []byte
	rightBytes = encoding.AppendJSONValue(rightBytes, right)

	return bytes.Compare(leftBytes, rightBytes) == 0
}

func prepareNewArrayValue(a *fastjson.Arena, arrayOfValues []*fastjson.Value) *fastjson.Value {
	newValue := a.NewArray()
	for i, val := range arrayOfValues {
		newValue.SetArrayItem(i, val)
	}
	return newValue
}

type modifierPullAll struct {
	fieldPath []string
	val       *fastjson.Value
}

func (m modifierPullAll) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	err = walk(a, v, m.fieldPath, true, func(prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
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
		removedElems, err := m.val.Array()
		if err != nil {
			return nil, fmt.Errorf("failed to pop item, %w", err)
		}
		var newArray []*fastjson.Value
		newArray, modified = m.removedElements(arrayOfValues, removedElems)
		return prepareNewArrayValue(a, newArray), nil
	})
	if err != nil {
		return nil, false, err
	}
	result = v
	return
}

func (m modifierPullAll) removedElements(arrayOfValues []*fastjson.Value, removedElems []*fastjson.Value) ([]*fastjson.Value, bool) {
	var (
		newArray []*fastjson.Value
		modified bool
	)
	for _, val := range arrayOfValues {
		if slices.ContainsFunc(removedElems, func(removedValue *fastjson.Value) bool {
			return compare(val, removedValue)
		}) {
			modified = true
			continue
		}
		newArray = append(newArray, val)
	}
	return newArray, modified
}

type modifierAddToSet struct {
	fieldPath []string
	val       *fastjson.Value
}

func (m modifierAddToSet) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	err = walk(a, v, m.fieldPath, true, func(prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
		if value == nil {
			return nil, nil
		}
		arrayOfValues, err := value.Array()
		if err != nil {
			return nil, fmt.Errorf("failed to pop item, %w", err)
		}
		arrayOfValues, modified = m.addElements(arrayOfValues, m.val)
		return prepareNewArrayValue(a, arrayOfValues), nil
	})
	if err != nil {
		return nil, false, err
	}
	result = v
	return
}

func (m modifierAddToSet) addElements(arrayOfValues []*fastjson.Value, addElem *fastjson.Value) ([]*fastjson.Value, bool) {
	if slices.ContainsFunc(arrayOfValues, func(val *fastjson.Value) bool {
		return compare(addElem, val)
	}) {
		return arrayOfValues, false
	}
	arrayOfValues = append(arrayOfValues, addElem)
	return arrayOfValues, true
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
