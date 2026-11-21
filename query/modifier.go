package query

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/anyenc/anyencutil"
)

type Modifier interface {
	Modify(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error)
}

type ModifyFunc func(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error)

func (m ModifyFunc) Modify(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
	if m == nil {
		return nil, false, fmt.Errorf("modify func is nil")
	}
	return m(a, v)
}

type ModifierChain []Modifier

func (mRoot ModifierChain) Modify(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
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

// paddingStaged is a parsed modifier with a path segment that names an array
// index past anyencutil.MaxArrayPadding. On an array that segment is refused,
// and the operations run in order: on the caller's document, a refusal in the
// middle would leave the operations before it applied, and a $rename whose
// destination is refused would already have removed its source. Modify
// therefore applies the modifier once, to an in-memory copy of the document
// (anyenc.Arena.Copy), and the copy is the result: on success it replaces the
// document, and on a refusal it is dropped and the document is as it was.
// MongoDB applies an update the same way, to a mutable copy that is stored
// only when every operation succeeds. A modifier without such a segment is
// never refused by the padding limit and runs on the document directly.
//
// The chain stores copies of its operands (ownOperands), as MongoDB copies an
// operand from the immutable update spec into the document
// (update/set_node.cpp: setValueBSONElement(val)). The copy the chain
// runs on then shares no memory with the modifier or with the document
// passed in, so no operation reaches that document, even when it is an
// earlier result of the same modifier, and the operands stay as parsed.
type paddingStaged struct {
	ModifierChain
}

// ownOperands returns chain with its $set, $push and $addToSet set to store a
// copy of their operand on every application.
func ownOperands(chain ModifierChain) ModifierChain {
	owned := make(ModifierChain, len(chain))
	for i, m := range chain {
		switch mt := m.(type) {
		case modifierSet:
			mt.copyVal = true
			m = mt
		case modifierPush:
			mt.copyVal = true
			m = mt
		case modifierAddToSet:
			mt.copyVal = true
			m = mt
		case ModifierChain:
			m = ownOperands(mt)
		}
		owned[i] = m
	}
	return owned
}

// stored is the value an operation stores for operand val: val itself, or a
// copy of it in a when the chain owns its operands (ownOperands).
func stored(a *anyenc.Arena, val *anyenc.Value, copyVal bool) *anyenc.Value {
	if copyVal {
		return a.Copy(val)
	}
	return val
}

func (m paddingStaged) Modify(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
	if v == nil {
		return m.ModifierChain.Modify(a, v)
	}
	return m.ModifierChain.Modify(a, a.Copy(v))
}

// mayExceedPadding reports whether a path of the chain has a segment naming
// an array index the padding limit can refuse: past
// anyencutil.MaxArrayPadding, or too large for an int. A segment naming a
// smaller index needs at most MaxArrayPadding nulls whatever the array.
func mayExceedPadding(chain ModifierChain) bool {
	for _, m := range chain {
		var paths [][]string
		switch mt := m.(type) {
		case modifierSet:
			paths = [][]string{mt.fieldPath}
		case modifierUnset:
			paths = [][]string{mt.fieldPath}
		case modifierInc:
			paths = [][]string{mt.fieldPath}
		case modifierRename:
			paths = [][]string{mt.fieldPath, mt.val}
		case modifierPop:
			paths = [][]string{mt.fieldPath}
		case modifierPush:
			paths = [][]string{mt.fieldPath}
		case modifierPull:
			paths = [][]string{mt.fieldPath}
		case modifierPullAll:
			paths = [][]string{mt.fieldPath}
		case modifierAddToSet:
			paths = [][]string{mt.fieldPath}
		case ModifierChain:
			if mayExceedPadding(mt) {
				return true
			}
		}
		for _, path := range paths {
			for _, seg := range path {
				// ParseIndexSegment reports an index too large for an int as
				// n = -1, a negative one too; only the latter starts with '-'.
				n, ok := anyenc.ParseIndexSegment(seg)
				if ok && seg[0] != '-' && (n < 0 || n > anyencutil.MaxArrayPadding) {
					return true
				}
			}
		}
	}
	return false
}

type modifierSet struct {
	fieldPath []string
	val       *anyenc.Value
	copyVal   bool // store a copy of val (ownOperands)
}

func (m modifierSet) Modify(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
	err = anyencutil.Walk(a, v, m.fieldPath, true, func(prevValue, value *anyenc.Value) (res *anyenc.Value, err error) {
		modified = !anyencutil.Equal(value, m.val)
		return stored(a, m.val, m.copyVal), nil
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

func (m modifierUnset) Modify(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
	err = anyencutil.Walk(a, v, m.fieldPath, false, func(prevValue, value *anyenc.Value) (res *anyenc.Value, err error) {
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

func (m modifierInc) Modify(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
	err = anyencutil.Walk(a, v, m.fieldPath, true, func(prevValue, value *anyenc.Value) (res *anyenc.Value, err error) {
		if value == nil {
			modified = true
			return a.NewNumberFloat64(m.val), nil
		}
		if value.Type() != anyenc.TypeNumber {
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

func (m modifierRename) Modify(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
	var oldFieldValue *anyenc.Value
	err = anyencutil.Walk(a, v, m.fieldPath, true, func(prevValue, value *anyenc.Value) (res *anyenc.Value, err error) {
		if value != nil {
			modified = true
			oldFieldValue = value
		}
		return nil, nil
	})
	if modified {
		err = anyencutil.Walk(a, v, m.val, true, func(prevValue, newFieldValue *anyenc.Value) (res *anyenc.Value, err error) {
			modified = modified || !anyencutil.Equal(newFieldValue, oldFieldValue)
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

func (m modifierPop) Modify(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
	err = anyencutil.Walk(a, v, m.fieldPath, true, func(prevValue, value *anyenc.Value) (res *anyenc.Value, err error) {
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

func (m modifierPop) getResultArray(arrayOfValues []*anyenc.Value) ([]*anyenc.Value, error) {
	if m.val == 1 {
		arrayOfValues = arrayOfValues[:len(arrayOfValues)-1]
	} else {
		arrayOfValues = arrayOfValues[1:]
	}
	return arrayOfValues, nil
}

type modifierPush struct {
	fieldPath []string
	val       *anyenc.Value
	copyVal   bool // store a copy of val (ownOperands)
}

func (m modifierPush) Modify(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
	err = anyencutil.Walk(a, v, m.fieldPath, true, func(prevValue, value *anyenc.Value) (res *anyenc.Value, err error) {
		if value == nil {
			return nil, nil
		}
		arrayOfValues, err := value.Array()
		if err != nil {
			return nil, fmt.Errorf("failed to pop item, %w", err)
		}
		value.SetArrayItem(len(arrayOfValues), stored(a, m.val, m.copyVal))
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
	val       *anyenc.Value
}

func (m modifierPull) Modify(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
	err = anyencutil.Walk(a, v, m.fieldPath, true, func(prevValue, value *anyenc.Value) (res *anyenc.Value, err error) {
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
			modified = removeElements(arrayOfValues, value, func(value *anyenc.Value) bool {
				return m.filter.Ok(value, nil)
			})
		} else {
			modified = removeElements(arrayOfValues, value, func(value *anyenc.Value) bool {
				return anyencutil.Equal(value, m.val)
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
	removedValues []*anyenc.Value
}

func (m modifierPullAll) Modify(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
	err = anyencutil.Walk(a, v, m.fieldPath, true, func(prevValue, value *anyenc.Value) (res *anyenc.Value, err error) {
		if value == nil {
			return nil, nil
		}
		arrayOfValues, err := value.Array()
		if err != nil {
			return nil, fmt.Errorf("failed to pop item, %w", err)
		}
		modified = removeElements(arrayOfValues, value, func(value *anyenc.Value) bool {
			return slices.ContainsFunc(m.removedValues, func(removedValue *anyenc.Value) bool {
				return anyencutil.Equal(value, removedValue)
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

func removeElements(arrayOfValues []*anyenc.Value, value *anyenc.Value, shouldRemove func(*anyenc.Value) bool) bool {
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
	val       *anyenc.Value
	copyVal   bool // store a copy of val (ownOperands)
}

func (m modifierAddToSet) Modify(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
	err = anyencutil.Walk(a, v, m.fieldPath, true, func(prevValue, value *anyenc.Value) (res *anyenc.Value, err error) {
		if value == nil {
			value = a.NewArray()
		}
		_, err = value.Array()
		if err != nil {
			return nil, fmt.Errorf("failed to pop item, %w", err)
		}
		modified = m.addElements(a, value)
		return value, nil
	})
	if err != nil {
		return nil, false, err
	}
	result = v
	return
}

func (m modifierAddToSet) addElements(a *anyenc.Arena, value *anyenc.Value) bool {
	arrayOfValues := value.GetArray()
	if slices.ContainsFunc(arrayOfValues, func(val *anyenc.Value) bool {
		return anyencutil.Equal(m.val, val)
	}) {
		return false
	}
	value.SetArrayItem(len(arrayOfValues), stored(a, m.val, m.copyVal))
	return true
}
