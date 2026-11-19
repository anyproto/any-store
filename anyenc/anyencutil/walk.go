package anyencutil

import (
	"fmt"
	"strconv"

	"github.com/anyproto/any-store/v2/anyenc"
)

// maxArrayPadding is the largest number of null elements a numeric path
// segment may add to an array to reach its index: {"$set": {"arr.N": v}} on
// an array of length L appends N-L nulls before v. The limit and the error are
// MongoDB's (kMaxPaddingAllowed), and stop one path segment from asking for an
// allocation proportional to a number the caller wrote.
const maxArrayPadding = 1_500_000

// setArrayItem stores value at the index the path segment names, padding arr
// with nulls up to it. It refuses, before anything is allocated, a segment
// that is not a non-negative index or that needs more than maxArrayPadding
// nulls.
func setArrayItem(arr *anyenc.Value, path string, value *anyenc.Value) error {
	idx, err := strconv.Atoi(path) // an index that overflows int is an error here
	if err != nil || idx < 0 {
		return fmt.Errorf("cannot create field '%s' in element %s", path, arr.String())
	}
	if elems, _ := arr.Array(); idx-len(elems) > maxArrayPadding {
		return fmt.Errorf("cannot create field '%s': can't backfill array to larger than %d elements", path, maxArrayPadding)
	}
	arr.SetArrayItem(idx, value)
	return nil
}

// Walk walks to the given path and calls the "finalize" function that can replace the value.
// If create == true path will be created.
//
// A numeric segment into an array sets that element, padding the array with
// nulls when the index is past its end; padding by more than maxArrayPadding
// elements is an error and leaves the array as it was.
func Walk(a *anyenc.Arena, v *anyenc.Value, fieldPath []string, create bool, finalize func(prevValue *anyenc.Value, value *anyenc.Value) (res *anyenc.Value, err error)) (err error) {
	prevField := v
	for i, path := range fieldPath {
		field := prevField.Get(path)
		if i == len(fieldPath)-1 {
			var newVal *anyenc.Value
			if newVal, err = finalize(prevField, field); err != nil {
				return
			}
			if newVal == nil {
				if field != nil {
					prevField.Del(path)
				}
				return
			}
			if prevField.Type() == anyenc.TypeArray {
				return setArrayItem(prevField, path, newVal)
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
				case anyenc.TypeObject:
				case anyenc.TypeArray:
				default:
					return fmt.Errorf("cannot create field '%s' in element %s", fieldPath[i+1], field.String())
				}
			}
			if prevField.Type() == anyenc.TypeArray {
				if err = setArrayItem(prevField, path, field); err != nil {
					return err
				}
			} else {
				prevField.Set(path, field)
			}
			prevField = field
		}
	}
	return
}
