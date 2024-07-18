package query

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/jsonutil"
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
}

func (m modifierSet) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	err = jsonutil.Walk(a, v, m.fieldPath, true, func(prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
		modified = !jsonutil.Equal(value, m.val)
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
	err = jsonutil.Walk(a, v, m.fieldPath, false, func(prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
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
	err = jsonutil.Walk(a, v, m.fieldPath, true, func(prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
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
}

func (m modifierRename) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	var oldFieldValue *fastjson.Value
	err = jsonutil.Walk(a, v, m.fieldPath, true, func(prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
		if value != nil {
			modified = true
			oldFieldValue = value
		}
		return nil, nil
	})
	if modified {
		err = jsonutil.Walk(a, v, m.val, true, func(prevValue, newFieldValue *fastjson.Value) (res *fastjson.Value, err error) {
			modified = !jsonutil.Equal(newFieldValue, oldFieldValue)
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
	err = jsonutil.Walk(a, v, m.fieldPath, true, func(prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
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
	err = jsonutil.Walk(a, v, m.fieldPath, true, func(prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
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
}

func (m modifierPull) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	err = jsonutil.Walk(a, v, m.fieldPath, true, func(prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
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
				return jsonutil.Equal(value, m.val)
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
}

func (m modifierPullAll) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	err = jsonutil.Walk(a, v, m.fieldPath, true, func(prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
		if value == nil {
			return nil, nil
		}
		arrayOfValues, err := value.Array()
		if err != nil {
			return nil, fmt.Errorf("failed to pop item, %w", err)
		}
		modified = removeElements(arrayOfValues, value, func(value *fastjson.Value) bool {
			return slices.ContainsFunc(m.removedValues, func(removedValue *fastjson.Value) bool {
				return jsonutil.Equal(value, removedValue)
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
}

func (m modifierAddToSet) Modify(a *fastjson.Arena, v *fastjson.Value) (result *fastjson.Value, modified bool, err error) {
	err = jsonutil.Walk(a, v, m.fieldPath, true, func(prevValue, value *fastjson.Value) (res *fastjson.Value, err error) {
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
		return jsonutil.Equal(addElem, val)
	}) {
		return false
	}
	value.SetArrayItem(len(arrayOfValues), addElem)
	return true
}
