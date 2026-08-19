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
		if _, nextTail, err = parseValue(tail, nil, 0); err != nil {
			return
		}
		if err = f(tail[:len(tail)-len(nextTail)]); err != nil {
			return
		}
		tail = nextTail
	}
	return nil
}

// OffsetAfter returns the byte offset right after the first n values in the tuple.
// If n <= 0, it returns 0. If n is greater than the number of values, it returns
// len(t). Returns an error if tuple encoding is corrupted.
func (t Tuple) OffsetAfter(n int) (int, error) {
	if n <= 0 {
		return 0, nil
	}
	tail := t
	off := 0
	for i := 0; i < n && len(tail) > 0; i++ {
		_, nextTail, err := parseValue(tail, nil, 0)
		if err != nil {
			return 0, err
		}
		consumed := len(tail) - len(nextTail)
		off += consumed
		tail = nextTail
	}
	if off > len(t) {
		off = len(t)
	}
	return off, nil
}

// FieldBytes returns the raw encoded bytes of the nth field (0-based).
// Single pass through the tuple up to field n.
func (t Tuple) FieldBytes(n int) ([]byte, error) {
	tail := []byte(t)
	for i := 0; i <= n; i++ {
		if len(tail) == 0 {
			return nil, fmt.Errorf("tuple: field %d out of range", n)
		}
		_, nextTail, err := parseValue(tail, nil, 0)
		if err != nil {
			return nil, err
		}
		if i == n {
			return tail[:len(tail)-len(nextTail)], nil
		}
		tail = nextTail
	}
	return nil, fmt.Errorf("tuple: field %d out of range", n)
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
