package query

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/syncpool"
)

type Filter interface {
	Ok(v *anyenc.Value, buf *syncpool.DocBuffer) bool

	IndexBounds(fieldName string, bs Bounds) (bounds Bounds)

	fmt.Stringer
}

type CompOp uint8

const (
	CompOpEq CompOp = iota
	CompOpGt
	CompOpGte
	CompOpLt
	CompOpLte
	CompOpNe
)

func NewComp(op CompOp, value any) *Comp {
	return &Comp{
		EqValue: anyenc.AppendAnyValue(nil, value),
		CompOp:  op,
	}
}

func NewCompValue(op CompOp, value *anyenc.Value) *Comp {
	return &Comp{
		EqValue: value.MarshalTo(nil),
		CompOp:  op,
	}
}

type Comp struct {
	EqValue  []byte
	CompOp   CompOp
	notArray bool
}

func (e *Comp) Ok(v *anyenc.Value, docBuf *syncpool.DocBuffer) bool {
	if v == nil {
		if e.CompOp == CompOpNe {
			return true
		} else {
			return false
		}
	}
	var buf []byte
	if docBuf != nil {
		buf = docBuf.SmallBuf[:0]
		defer func() { docBuf.SmallBuf = buf }()
	}

	if v.Type() == anyenc.TypeArray {
		vals, _ := v.Array()
		if e.CompOp == CompOpNe {
			if !e.notArray {
				buf = v.MarshalTo(buf[:0])
				if !e.comp(buf) {
					return false
				}
			}
			for _, val := range vals {
				buf = val.MarshalTo(buf[:0])
				if !e.comp(buf) {
					return false
				}
			}

			return true
		} else {
			if !e.notArray {
				buf = v.MarshalTo(buf[:0])
				if e.comp(buf) {
					return true
				}
			}
			for _, val := range vals {
				buf = val.MarshalTo(buf[:0])
				if e.comp(buf) {
					return true
				}
			}
			return false
		}
	} else {
		buf = v.MarshalTo(buf[:0])
		return e.comp(buf)
	}
}

func (e *Comp) IndexBounds(fieldName string, bs Bounds) (bounds Bounds) {
	switch e.CompOp {
	case CompOpEq:
		return bs.Append(Bound{
			Start:        e.EqValue,
			End:          e.EqValue,
			StartInclude: true,
			EndInclude:   true,
		})
	case CompOpGt:
		return bs.Append(Bound{
			Start: e.EqValue,
		})
	case CompOpGte:
		return bs.Append(Bound{
			Start:        e.EqValue,
			StartInclude: true,
		})
	case CompOpLt:
		return bs.Append(Bound{
			End: e.EqValue,
		})
	case CompOpLte:
		return bs.Append(Bound{
			End:        e.EqValue,
			EndInclude: true,
		})
	case CompOpNe:
		return bs.Append(Bound{
			End: e.EqValue,
		}).Append(Bound{
			Start: e.EqValue,
		})
	default:
		panic(fmt.Errorf("unexpected comp op: %v", e.CompOp))
	}
}

func (e *Comp) comp(b []byte) bool {
	comp := bytes.Compare(e.EqValue, b)
	switch e.CompOp {
	case CompOpEq:
		return comp == 0
	case CompOpGt:
		return comp < 0
	case CompOpGte:
		return comp <= 0
	case CompOpLt:
		return comp > 0
	case CompOpLte:
		return comp >= 0
	case CompOpNe:
		return comp != 0
	default:
		panic(fmt.Errorf("unexpected comp op: %v", e.CompOp))
	}
}

func (e *Comp) String() string {
	var op string
	switch e.CompOp {
	case CompOpEq:
		op = string(opBytesEq)
	case CompOpGt:
		op = string(opBytesGt)
	case CompOpGte:
		op = string(opBytesGte)
	case CompOpLt:
		op = string(opBytesLt)
	case CompOpLte:
		op = string(opBytesLte)
	case CompOpNe:
		op = string(opBytesNe)
	}
	a := &fastjson.Arena{}
	p := parserPool.Get()
	defer parserPool.Put(p)
	av, _ := p.Parse(e.EqValue)
	val := av.FastJson(a)
	return fmt.Sprintf(`{"%s": %s}`, op, val.String())
}

type Key struct {
	Path []string
	Filter
}

func (e Key) Ok(v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	return e.Filter.Ok(v.Get(e.Path...), buf)
}

func (e Key) IndexBounds(fieldName string, bs Bounds) (bounds Bounds) {
	if strings.Join(e.Path, ".") == fieldName {
		return e.Filter.IndexBounds(fieldName, bs)
	}
	return bs
}

func (e Key) String() string {
	return fmt.Sprintf(`{"%s": %s}`, strings.Join(e.Path, "."), e.Filter.String())
}

type And []Filter

func (e And) Ok(v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	for _, f := range e {
		if !f.Ok(v, buf) {
			return false
		}
	}
	return true
}

func (e And) IndexBounds(fieldName string, bs Bounds) (bounds Bounds) {
	for _, f := range e {
		if bounds = f.IndexBounds(fieldName, bs); len(bounds) != len(bs) {
			return
		}
	}
	return bs
}

func (e And) String() string {
	var subS []string
	for _, f := range e {
		if f != nil {
			subS = append(subS, f.String())
		}
	}
	return fmt.Sprintf(`{"$and":[%s]}`, strings.Join(subS, ", "))
}

type Or []Filter

func (e Or) Ok(v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	for _, f := range e {
		if f.Ok(v, buf) {
			return true
		}
	}
	return false
}

func (e Or) IndexBounds(fieldName string, bs Bounds) (bounds Bounds) {
	for _, f := range e {
		beforeBounds := len(bs)
		if bs = f.IndexBounds(fieldName, bs); len(bs) == beforeBounds {
			return
		}
	}
	return bs
}

func (e Or) String() string {
	var subS []string
	for _, f := range e {
		subS = append(subS, f.String())
	}
	return fmt.Sprintf(`{"$or":[%s]}`, strings.Join(subS, ", "))
}

type Nor []Filter

func (e Nor) Ok(v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	for _, f := range e {
		if f.Ok(v, buf) {
			return false
		}
	}
	return true
}

func (e Nor) IndexBounds(fieldName string, bs Bounds) (bounds Bounds) {
	return bs
}

func (e Nor) String() string {
	var subS []string
	for _, f := range e {
		subS = append(subS, f.String())
	}
	return fmt.Sprintf(`{"$nor":[%s]}`, strings.Join(subS, ", "))
}

type Not struct {
	Filter
}

func (e Not) Ok(v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	return !e.Filter.Ok(v, buf)
}

func (e Not) IndexBounds(fieldName string, bs Bounds) (bounds Bounds) {
	return bs
}

func (e Not) String() string {
	return fmt.Sprintf(`{"$not": %s}`, e.Filter.String())
}

type All struct{}

func (a All) Ok(_ *anyenc.Value, buf *syncpool.DocBuffer) bool {
	return true
}

func (a All) IndexBounds(fieldName string, bs Bounds) (bounds Bounds) {
	return bs
}

func (a All) String() string {
	return "null"
}

type Exists struct{}

func (e Exists) Ok(v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	return v != nil
}

func (e Exists) IndexBounds(fieldName string, bs Bounds) (bounds Bounds) {
	return bs
}

func (e Exists) String() string {
	return fmt.Sprintf(`{"$exists": true}`)
}

type TypeFilter struct {
	Type anyenc.Type
}

func (e TypeFilter) Ok(v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	if v == nil {
		return false
	}
	return v.Type() == e.Type
}

func (e TypeFilter) IndexBounds(fieldName string, bs Bounds) (bounds Bounds) {
	k := []byte{byte(e.Type), 255}
	return bs.Append(Bound{
		Start:        k[:1],
		End:          k,
		StartInclude: true,
		EndInclude:   true,
	})
}

func (e TypeFilter) String() string {
	return fmt.Sprintf(`{"$type": "%s"}`, Type(e.Type).String())
}

type Regexp struct {
	Regexp *regexp.Regexp
}

func (r Regexp) Ok(v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	if v == nil {
		return false
	}
	if v.Type() != anyenc.TypeString && v.Type() != anyenc.TypeArray {
		return false
	}
	if v.Type() == anyenc.TypeArray {
		vals, _ := v.Array()
		for _, val := range vals {
			exp, err := val.StringBytes()
			if err != nil {
				return false
			}
			if r.Regexp.Match(exp) {
				return true
			}
		}
		return false
	}
	exp, err := v.StringBytes()
	if err != nil {
		return false
	}
	return r.Regexp.Match(exp)
}

func (r Regexp) IndexBounds(_ string, bs Bounds) (bounds Bounds) {
	prefix := extractPrefix(r.Regexp.String())
	if prefix == "" {
		return
	}
	var (
		prefixBuf     = make([]byte, 0, len(prefix)+2)
		prefixEncoded = anyenc.AppendAnyValue(prefixBuf, prefix)
	)
	// strip the 'eof' byte
	prefixEncoded = prefixEncoded[:len(prefixEncoded)-1]
	bound := Bound{
		Start:        prefixEncoded,
		End:          append(prefixEncoded, 255),
		StartInclude: true,
		EndInclude:   true,
	}
	return bs.Append(bound)
}

func findPrefix(pattern string) string {
	var result []rune
	specialChars := `^$|*+?(){}[]\.`
	escaped := false

	for i := 0; i < len(pattern); i++ {
		char := pattern[i]

		if escaped {
			escaped = false
			result = append(result, rune(char))
			continue
		}

		if char == '\\' {
			escaped = true
			continue
		}

		if !isSpecialChar(char, specialChars) {
			result = append(result, rune(char))
		} else {
			break
		}
	}
	var resultString string
	for _, v := range result {
		resultString += string(v)
	}
	return resultString
}

func isSpecialChar(char byte, specialChars string) bool {
	for i := 0; i < len(specialChars); i++ {
		if char == specialChars[i] {
			return true
		}
	}
	return false
}

func extractPrefix(pattern string) string {
	if !strings.HasPrefix(pattern, "^") || strings.HasPrefix(pattern, "^(?i)") {
		return ""
	}
	pattern = strings.TrimPrefix(pattern, "^")
	return findPrefix(pattern)
}

func (r Regexp) String() string {
	return fmt.Sprintf(`{"$regex": "%s"}`, r.Regexp.String())
}

type Size struct {
	Size int64
}

func (s Size) Ok(v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	if v == nil {
		return false
	}
	array, err := v.Array()
	if err != nil {
		return false
	}
	return int64(len(array)) == s.Size
}

func (s Size) IndexBounds(_ string, bs Bounds) (bounds Bounds) {
	return bs
}

func (s Size) String() string {
	return fmt.Sprintf(`{"$size": %d}`, s.Size)
}
