package anyenc

import "fmt"

// Tuple represents an encoded sequence of values as a byte slice.
type Tuple []byte

// Append adds a new encoded value to the tuple.
func (t Tuple) Append(v *Value) Tuple {
	return v.MarshalTo(t)
}

// AppendInverted adds a new encoded value and inverts bytes
func (t Tuple) AppendInverted(v *Value) Tuple {
	var prevLen = len(t)
	t = v.MarshalTo(t)
	for i := range t[prevLen:] {
		t[i+prevLen] = ^t[i+prevLen]
	}
	return t
}

// ReadValues decodes and reads all values from the start of the tuple.
// The provided function `f` is called for each value.
func (t Tuple) ReadValues(p *Parser, f func(v *Value) error) error {
	return t.ReadBytes(func(b []byte) error {
		if v, err := p.Parse(b); err != nil {
			return err
		} else {
			if err = f(v); err != nil {
				return err
			}
		}
		return nil
	})
}

// ReadBytes iterates over every value in the tuple and passes the raw bytes
// to the provided function `f`. It continues until all values are processed.
func (t Tuple) ReadBytes(f func(b []byte) error) (err error) {
	var tail = t
	var nextTail []byte
	for len(tail) > 0 {
		if _, nextTail, err = parseValue(tail, nil); err != nil {
			return
		}
		if err = f(tail[:len(tail)-len(nextTail)]); err != nil {
			return
		}
		tail = nextTail
	}
	return nil
}

// String returns a string representation of the tuple.
func (t Tuple) String() string {
	var p = &Parser{}
	var res string
	err := t.ReadValues(p, func(v *Value) error {
		if res != "" {
			res += "/"
		}
		res += v.String()
		return nil
	})
	if err != nil {
		res += fmt.Sprintf("err:%v", err)
	}
	return res
}

// Copy creates and returns a copy of the tuple.
func (t Tuple) Copy() Tuple {
	c := make(Tuple, len(t))
	copy(c, t)
	return c
}
