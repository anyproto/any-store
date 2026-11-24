package query

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

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
type paddingStaged struct {
	ModifierChain
}

// stored is the value $set, $push and $addToSet store for their operand val:
// a copy of it in a, or val itself when nothing can change it in place.
// MongoDB copies an operand from the immutable update spec into the document
// (update/set_node.cpp: setValueBSONElement(val)). Stored as itself, an
// operand would be shared by every document the modifier is applied to — the
// matches of a filtered Update, or the documents of a caller that reuses the
// parsed modifier — and a change to it in place would change the modifier,
// and through it the next document: a later operation of the same modifier
// ($inc into the object $push appended, $push into the array $set stored), or
// the caller writing through the slice a result's GetStringBytes, GetBytes
// or GetVectorF32 returns. A number, a bool, null, an object id and a date
// hand out no storage of their own, and are stored as themselves.
func stored(a *anyenc.Arena, val *anyenc.Value) *anyenc.Value {
	switch val.Type() {
	case anyenc.TypeNumber, anyenc.TypeTrue, anyenc.TypeFalse, anyenc.TypeNull,
		anyenc.TypeObjectID, anyenc.TypeDateTime:
		return val
	}
	return a.Copy(val)
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
}

func (m modifierSet) Modify(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
	err = anyencutil.Walk(a, v, m.fieldPath, true, func(prevValue, value *anyenc.Value) (res *anyenc.Value, err error) {
		modified = !anyencutil.Equal(value, m.val)
		return stored(a, m.val), nil
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
	err = walkExisting(v, m.fieldPath, "$unset", true, func(value *anyenc.Value) (*anyenc.Value, error) {
		modified = true
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

// walkExisting is the walk of the operators that remove: $unset, $pop, $pull,
// $pullAll and the source of $rename. It walks v to the field path names,
// creating nothing, and when the field is there calls finalize with it: a
// nil result removes the field, another value takes its place. A document
// without the field is left as it was, as MongoDB does; walked with
// creation, a missing parent was set to {}, kept, and stored once another
// operation of the modifier changed the document.
//
// Where the walk stops matters as in MongoDB. A path that stops at an object,
// or at an array whose next part is an index past its end, could be created,
// and is a no-op. One whose next part is a field of a scalar or a name in an
// array could not: allowNonViable ($unset) makes it a no-op too
// (UnsetNode::allowNonViablePath), and every other remover refuses it, as
// MongoDB's PathNotViable does (UpdateLeafNode::checkViability for $pop,
// $pull and $pullAll; pathsupport::findLongestPrefix for the source of
// $rename).
func walkExisting(v *anyenc.Value, path []string, op string, allowNonViable bool, finalize func(value *anyenc.Value) (*anyenc.Value, error)) error {
	parent := v
	for i, part := range path {
		field := parent.Get(part)
		if field == nil {
			if allowNonViable || canHoldField(parent, part) {
				return nil
			}
			return fmt.Errorf("%s: cannot use the part '%s' of '%s' to traverse the element %s",
				op, part, strings.Join(path, "."), parent.String())
		}
		if i < len(path)-1 {
			parent = field
			continue
		}
		newVal, err := finalize(field)
		if err != nil {
			return err
		}
		if newVal == nil {
			parent.Del(part)
		} else if newVal != field {
			parent.Set(part, newVal)
		}
	}
	return nil
}

// canHoldField reports whether part could name a field of v that v does not
// have: v is an object, or an array and part an index past its end. A scalar
// holds no field, and an array none by name. An index is what MongoDB's
// UpdateLeafNode::checkViability takes for one (str::parseUnsignedBase10Integer
// at r8.0.0): decimal digits, no sign, that an unsigned 32-bit int holds. The
// host's int plays no part, so an update's outcome does not depend on the word
// size: items.2147483648 is an index past the end on a 32-bit target too,
// though no int there holds it.
func canHoldField(v *anyenc.Value, part string) bool {
	switch v.Type() {
	case anyenc.TypeObject:
		return true
	case anyenc.TypeArray:
		_, err := strconv.ParseUint(part, 10, 32)
		return err == nil
	}
	return false
}

func (m modifierRename) Modify(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
	var oldFieldValue *anyenc.Value
	err = walkExisting(v, m.fieldPath, "$rename", false, func(value *anyenc.Value) (*anyenc.Value, error) {
		modified = true
		oldFieldValue = value
		return nil, nil
	})
	if err != nil {
		return nil, false, err
	}
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
	err = walkExisting(v, m.fieldPath, "$pop", false, func(value *anyenc.Value) (*anyenc.Value, error) {
		arrayOfValues, err := value.Array()
		if err != nil {
			return nil, notArrayError("$pop", m.fieldPath, err)
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
}

func (m modifierPush) Modify(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
	err = anyencutil.Walk(a, v, m.fieldPath, true, func(prevValue, value *anyenc.Value) (res *anyenc.Value, err error) {
		if value == nil {
			// A missing field becomes an array holding the value, as in
			// MongoDB; through an upsert, so does the new document's field.
			value = a.NewArray()
		}
		arrayOfValues, err := value.Array()
		if err != nil {
			return nil, notArrayError("$push", m.fieldPath, err)
		}
		value.SetArrayItem(len(arrayOfValues), stored(a, m.val))
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
	err = walkExisting(v, m.fieldPath, "$pull", false, func(value *anyenc.Value) (*anyenc.Value, error) {
		arrayOfValues, err := value.Array()
		if err != nil {
			return nil, notArrayError("$pull", m.fieldPath, err)
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
	err = walkExisting(v, m.fieldPath, "$pullAll", false, func(value *anyenc.Value) (*anyenc.Value, error) {
		arrayOfValues, err := value.Array()
		if err != nil {
			return nil, notArrayError("$pullAll", m.fieldPath, err)
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

// notArrayError is the error of an array operator applied to a field that
// holds something other than an array, which MongoDB refuses as well ("The
// field 'a' must be an array but is of type string").
func notArrayError(op string, fieldPath []string, err error) error {
	return fmt.Errorf("%s: field '%s' must be an array: %w", op, strings.Join(fieldPath, "."), err)
}

type modifierAddToSet struct {
	fieldPath []string
	val       *anyenc.Value
}

func (m modifierAddToSet) Modify(a *anyenc.Arena, v *anyenc.Value) (result *anyenc.Value, modified bool, err error) {
	err = anyencutil.Walk(a, v, m.fieldPath, true, func(prevValue, value *anyenc.Value) (res *anyenc.Value, err error) {
		if value == nil {
			value = a.NewArray()
		}
		_, err = value.Array()
		if err != nil {
			return nil, notArrayError("$addToSet", m.fieldPath, err)
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
	value.SetArrayItem(len(arrayOfValues), stored(a, m.val))
	return true
}
