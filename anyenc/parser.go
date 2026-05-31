package anyenc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"unsafe"

	"github.com/klauspost/compress/s2"
	"github.com/valyala/fastjson"
)

var (
	valueTrue  = &Value{t: TypeTrue}
	valueFalse = &Value{t: TypeFalse}
	valueNull  = &Value{t: TypeNull}
)

// MustParse parses bytes to Value. Panics in case of error
func MustParse(b []byte) *Value {
	if v, err := Parse(b); err == nil {
		return v
	} else {
		panic(err)
	}
}

// Parse parses bytes to Value
func Parse(b []byte) (*Value, error) {
	p := &Parser{}
	return p.Parse(b)
}

// MustParseJson parses Value from json string. Panics in case of error
func MustParseJson(jsonString string) *Value {
	if v, err := ParseJson(jsonString); err == nil {
		return v
	} else {
		panic(err)
	}
}

// ParseJson parses Value from json string
func ParseJson(jsonString string) (*Value, error) {
	jv, err := fastjson.Parse(jsonString)
	if err != nil {
		return nil, err
	}
	a := &Arena{}
	return a.NewFromFastJson(jv), nil
}

// Parser parses encoded bytes.
//
// Parser may be re-used for subsequent parsing.
//
// Parser cannot be used from concurrent goroutines.
// Use per-goroutine parsers or ParserPool instead.
type Parser struct {
	b []byte
	c cache
}

// Parse parses encoded bytes, copying them into an internal buffer.
//
// The returned value is valid until the next call to Parse*.
func (p *Parser) Parse(b []byte) (v *Value, err error) {
	p.c.reset()
	p.b = slices.Grow(p.b[:0], len(b))[:len(b)]
	copy(p.b, b)
	var tail []byte
	v, tail, err = parseValue(p.b, &p.c)
	if len(tail) != 0 {
		return nil, fmt.Errorf("unexpected tail")
	}
	return
}

// ParseOwned parses encoded bytes without copying them. The caller must
// guarantee that b will not be modified until the returned Value is no
// longer used. This avoids the allocation and copy overhead of Parse.
//
// The returned value is valid until the next call to Parse*/ParseOwned,
// or until b is modified — whichever comes first.
func (p *Parser) ParseOwned(b []byte) (v *Value, err error) {
	p.c.reset()
	var tail []byte
	v, tail, err = parseValue(b, &p.c)
	if err != nil {
		return nil, err
	}
	if len(tail) != 0 {
		return nil, fmt.Errorf("unexpected tail")
	}
	return
}

// ApproxSize returns approximate size of parser cache.
// Excludes reusable scratch buffers (decompBuf, input copy) that are
// deliberately kept to avoid re-allocation on subsequent calls.
func (p *Parser) ApproxSize() int {
	return p.c.approxSizeValues()
}

func parseValue(b []byte, c *cache) (v *Value, tail []byte, err error) {
	if len(b) == 0 {
		return nil, nil, fmt.Errorf("expected value, but got 0 byte")
	}
	// Index keys store reverse-flagged fields bitwise-inverted (see
	// index.go writeValues / anyenc.Tuple.AppendInverted). An inverted
	// field's leading tag byte is ^Type(n) (e.g. iTypeNumber=0xFD), which
	// the normal switch below treats as an unknown type. The readers that
	// skip over such fields (Tuple.OffsetAfter/FieldBytes/ReadBytes, which
	// call parseValue with c==nil) only need the correct field LENGTH, never
	// a decoded *Value. So we normalize the inverted tag to its base type and
	// run the same length computation, scanning for the inverted terminator
	// (^EOS == 0xFF) where the normal path uses EOS. Decoding inverted bytes
	// into a *Value (c != nil) is intentionally unsupported: it keeps the hot
	// document-parse path's "unknown type" guard for corrupt input intact, and
	// no production reader decodes an inverted index-key field (only the cosmetic
	// Explain Bound.String() path would, where a decode error renders harmlessly).
	t0 := b[0]
	inverted := t0 >= byte(iTypeCompressedObjectS2) && t0 <= byte(iTypeNull) // 0xF6..0xFE
	nt := Type(t0)
	eos := byte(EOS)
	if inverted {
		if c != nil {
			return nil, nil, fmt.Errorf("unknown type %d", Type(t0))
		}
		nt = ^Type(t0)
		eos = ^byte(EOS) // 0xFF
	}
	switch nt {
	case TypeString:
		// Search from index 1 so the leading tag byte is never mistaken for
		// the terminator (the inverted tag 0xFD..0xFE differs from 0xFF, but
		// the normal tag 0x03 also differs from 0x00 — searching from 1 keeps
		// both paths correct and symmetric).
		rel := bytes.IndexByte(b[1:], eos)
		if rel < 0 {
			return nil, nil, fmt.Errorf("end of string not found")
		}
		eosIdx := rel + 1
		if c != nil {
			v = c.getValue()
			v.t = TypeString
			v.v = slices.Grow(v.v[:0], eosIdx-1)[:eosIdx-1]
			copy(v.v, b[1:eosIdx])
		}
		return v, b[eosIdx+1:], nil
	case TypeNumber:
		if len(b[1:]) < 8 {
			return nil, nil, fmt.Errorf("expected 8 bytes to read number but got %d", len(b[1:]))
		}
		if c != nil {
			v = c.getValue()
			v.t = TypeNumber
			v.n = BytesToFloat64(b[1:])
		}
		return v, b[9:], nil
	case TypeBinary:
		return parseBinary(b[1:], c, inverted)
	case TypeObject:
		return parseObject(b[1:], c, eos)
	case TypeArray:
		return parseArray(b[1:], c, eos)
	case TypeTrue:
		return valueTrue, b[1:], nil
	case TypeFalse:
		return valueFalse, b[1:], nil
	case TypeNull:
		return valueNull, b[1:], nil
	case TypeCompressedObjectS2:
		// Compression is whole-document only; a compressed object never
		// appears inside an index key, so its inverted form is unreachable.
		// The non-inverted form is handled normally below.
		if inverted {
			return nil, nil, fmt.Errorf("unknown type %d", Type(t0))
		}
		return parseCompressedObjectS2(b, c)
	default:
		return nil, nil, fmt.Errorf("unknown type %d", Type(t0))
	}
}

// parseObject parses an object body (after the leading object tag). eos is the
// terminator/separator byte: EOS (0x00) for a normal object, ^EOS (0xFF) for an
// inverted (reverse index-key) object. When the object is inverted, c is always
// nil (decode of inverted bytes is unsupported); only length matters, so the
// inverted key bytes are skipped via the inverted terminator without decoding.
func parseObject(b []byte, c *cache, eos byte) (*Value, []byte, error) {
	var o *Value
	if c != nil {
		o = c.getValue()
		o.t = TypeObject
		o.o.kvs = o.o.kvs[:0]
	}
	var i int
	var err error
	for {
		if len(b) == 0 {
			return nil, nil, fmt.Errorf("parse object: unexpected end")
		}
		if b[0] == eos {
			return o, b[1:], nil
		}
		eosI := bytes.IndexByte(b, eos)
		if eosI < 0 {
			return nil, nil, fmt.Errorf("parse object key: end of string not found")
		}
		if c != nil {
			o.o.kvs = slices.Grow(o.o.kvs, 1)[:i+1]
			if len(b[:eosI]) == 1 && b[0] == emptyKey {
				o.o.kvs[i].key = ""
			} else {
				o.o.kvs[i].key = unsafe.String(unsafe.SliceData(b[:eosI]), len(b[:eosI]))
			}
			if o.o.kvs[i].value, b, err = parseValue(b[eosI+1:], c); err != nil {
				return nil, nil, err
			}
		} else {
			if _, b, err = parseValue(b[eosI+1:], c); err != nil {
				return nil, nil, err
			}
		}
		i++
	}
}

// parseArray parses an array body (after the leading array tag). eos is the
// element terminator: EOS (0x00) for a normal array, ^EOS (0xFF) for an inverted
// (reverse index-key) array. Each element is parsed by parseValue, which
// self-normalizes inverted element tags; only the array's own terminator needs
// the eos byte. An inverted element's leading tag (0xF6..0xFE) never collides
// with the inverted terminator 0xFF, so the b[0]==eos check is unambiguous.
func parseArray(b []byte, c *cache, eos byte) (*Value, []byte, error) {
	var a *Value
	if c != nil {
		a = c.getValue()
		a.t = TypeArray
		a.a = a.a[:0]
	}
	var i int
	var err error
	var val *Value
	for {
		if len(b) == 0 {
			return nil, nil, fmt.Errorf("parse array: unexpected end")
		}
		if b[0] == eos {
			return a, b[1:], nil
		}
		if val, b, err = parseValue(b, c); err != nil {
			return nil, nil, err
		}
		if c != nil {
			a.a = slices.Grow(a.a, 1)[:i+1]
			a.a[i] = val
		}
		i++
	}
}

// parseBinary parses a binary body (after the leading binary tag). When
// inverted (reverse index-key, c always nil), the 4-byte big-endian length
// header is itself bitwise-inverted; un-invert it to recover the payload length
// so the field can be skipped. The payload bytes are not decoded on this path.
func parseBinary(b []byte, c *cache, inverted bool) (*Value, []byte, error) {
	if len(b) < 4 {
		return nil, nil, fmt.Errorf("expected minimum 4 byte for binary header, but got %d", len(b))
	}
	var l uint32
	if inverted {
		var hdr [4]byte
		hdr[0], hdr[1], hdr[2], hdr[3] = ^b[0], ^b[1], ^b[2], ^b[3]
		l = binary.BigEndian.Uint32(hdr[:])
	} else {
		l = binary.BigEndian.Uint32(b)
	}
	if len(b[4:]) < int(l) {
		return nil, nil, fmt.Errorf("expected %d bytes to read binary, but got %d", l, len(b)-4)
	}
	if c != nil {
		a := c.getValue()
		a.t = TypeBinary
		a.v = slices.Grow(a.v[:0], int(l))[:l]
		copy(a.v, b[4:l+4])
		return a, b[l+4:], nil
	}
	return nil, b[l+4:], nil
}

type cache struct {
	vs        []Value
	decompBuf []byte
}

func (c *cache) reset() {
	c.vs = c.vs[:0]
}

func (c *cache) getValue() *Value {
	c.vs = slices.Grow(c.vs, 1)[:len(c.vs)+1]
	return &c.vs[len(c.vs)-1]
}

func (c *cache) approxSize() (size int) {
	size = c.approxSizeValues()
	size += cap(c.decompBuf)
	return
}

// approxSizeValues returns approximate size of cached Value structs only,
// excluding reusable scratch buffers (decompBuf).
func (c *cache) approxSizeValues() (size int) {
	for _, v := range c.vs[:cap(c.vs)] {
		size += len(v.v) + int(valueSize)
	}
	return
}

func parseCompressedObjectS2(b []byte, c *cache) (*Value, []byte, error) {
	if len(b) < 5 {
		return nil, nil, fmt.Errorf("compressed object: expected at least 5 bytes, got %d", len(b))
	}
	compLen := binary.BigEndian.Uint32(b[1:5])
	if uint32(len(b)-5) < compLen {
		return nil, nil, fmt.Errorf("compressed object: expected %d compressed bytes, got %d", compLen, len(b)-5)
	}
	compressed := b[5 : 5+compLen]
	tail := b[5+compLen:]

	var (
		decompressed []byte
		err          error
	)
	if c != nil {
		c.decompBuf, err = s2.Decode(c.decompBuf[:cap(c.decompBuf)], compressed)
		if err != nil {
			return nil, nil, fmt.Errorf("compressed object: s2 decode: %w", err)
		}
		decompressed = c.decompBuf
	} else {
		decompressed, err = s2.Decode(nil, compressed)
		if err != nil {
			return nil, nil, fmt.Errorf("compressed object: s2 decode: %w", err)
		}
	}

	v, _, err := parseValue(decompressed, c)
	if err != nil {
		return nil, nil, fmt.Errorf("compressed object: parse inner: %w", err)
	}
	return v, tail, nil
}
