package anyenc

import "fmt"

type Tuple []byte

func (t Tuple) Append(v *Value) Tuple {
	return v.MarshalTo(t)
}

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

func (t Tuple) Copy() Tuple {
	c := make(Tuple, len(t))
	copy(c, t)
	return c
}
