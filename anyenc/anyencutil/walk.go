package anyencutil

import (
	"fmt"
	"strconv"

	"github.com/anyproto/any-store/anyenc"
)

// Walk walks to the given path and calls the "finalize" function that can replace the value.
// If create == true path will be created
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
				case anyenc.TypeObject:
				case anyenc.TypeArray:
				default:
					return fmt.Errorf("cannot create field '%s' in element %s", fieldPath[i+1], field.String())
				}
			}
			if prevField.Type() == anyenc.TypeArray {
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
