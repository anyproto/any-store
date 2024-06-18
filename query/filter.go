package query

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/encoding"
)

type Filter interface {
	Ok(v *fastjson.Value) bool
	OkBytes(b []byte) bool

	IndexFilter(fieldName string, bs Bounds) (filter Filter, bounds Bounds)

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

type Comp struct {
	EqValue []byte
	buf     []byte
	CompOp  CompOp
}

func (e *Comp) Ok(v *fastjson.Value) bool {
	if v == nil {
		return false
	}
	if v.Type() == fastjson.TypeArray {
		vals, _ := v.Array()
		if e.CompOp == CompOpNe {
			for _, val := range vals {
				e.buf = encoding.AppendJSONValue(e.buf[:0], val)
				if !e.comp(e.buf) {
					return false
				}
			}
			return true
		} else {
			for _, val := range vals {
				e.buf = encoding.AppendJSONValue(e.buf[:0], val)
				if e.comp(e.buf) {
					return true
				}
			}
			return false
		}
	} else {
		e.buf = encoding.AppendJSONValue(e.buf[:0], v)
		return e.comp(e.buf)
	}
}

func (e *Comp) OkBytes(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	return e.comp(b)
}

func (e *Comp) IndexFilter(_ string, bs Bounds) (filter Filter, bounds Bounds) {
	switch e.CompOp {
	case CompOpEq:
		return e, bs.Append(Bound{
			Start:        e.EqValue,
			End:          e.EqValue,
			StartInclude: true,
			EndInclude:   true,
		})
	case CompOpGt:
		return e, bs.Append(Bound{
			Start: e.EqValue,
		})
	case CompOpGte:
		return e, bs.Append(Bound{
			Start:        e.EqValue,
			StartInclude: true,
		})
	case CompOpLt:
		return e, bs.Append(Bound{
			End: e.EqValue,
		})
	case CompOpLte:
		return e, bs.Append(Bound{
			End:        e.EqValue,
			EndInclude: true,
		})
	case CompOpNe:
		return e, bs.Append(Bound{
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
	val, _, _ := encoding.DecodeToJSON(&fastjson.Parser{}, a, e.EqValue)
	return fmt.Sprintf(`{"%s": %s}`, op, val.String())
}

type Key struct {
	Path []string
	Filter
}

func (e Key) Ok(v *fastjson.Value) bool {
	return e.Filter.Ok(v.Get(e.Path...))
}

func (e Key) OkBytes(b []byte) bool {
	return e.Filter.OkBytes(b)
}

func (e Key) IndexFilter(fieldName string, bs Bounds) (filter Filter, bounds Bounds) {
	if strings.Join(e.Path, ".") == fieldName {
		return e.Filter.IndexFilter(fieldName, bs)
	}
	return nil, bs
}

func (e Key) String() string {
	return fmt.Sprintf(`{"%s": %s}`, strings.Join(e.Path, "."), e.Filter.String())
}

type And []Filter

func (e And) Ok(v *fastjson.Value) bool {
	for _, f := range e {
		if !f.Ok(v) {
			return false
		}
	}
	return true
}

func (e And) OkBytes(b []byte) bool {
	for _, f := range e {
		if !f.OkBytes(b) {
			return false
		}
	}
	return true
}

func (e And) IndexFilter(fieldName string, bs Bounds) (filter Filter, bounds Bounds) {
	for _, f := range e {
		if filter, bounds = f.IndexFilter(fieldName, bs); filter != nil {
			return
		}
	}
	return nil, bs
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

func (e Or) Ok(v *fastjson.Value) bool {
	for _, f := range e {
		if f.Ok(v) {
			return true
		}
	}
	return false
}

func (e Or) OkBytes(b []byte) bool {
	for _, f := range e {
		if f.OkBytes(b) {
			return true
		}
	}
	return false
}

func (e Or) IndexFilter(fieldName string, bs Bounds) (filter Filter, bounds Bounds) {
	var of Or
	var ff Filter
	for _, f := range e {
		if ff, bs = f.IndexFilter(fieldName, bs); ff != nil {
			of = append(of, ff)
		} else {
			return
		}
	}
	if len(of) == 0 {
		return
	}
	return of, bs
}

func (e Or) String() string {
	var subS []string
	for _, f := range e {
		subS = append(subS, f.String())
	}
	return fmt.Sprintf(`{"$or":[%s]}`, strings.Join(subS, ", "))
}

type Nor []Filter

func (e Nor) Ok(v *fastjson.Value) bool {
	for _, f := range e {
		if f.Ok(v) {
			return false
		}
	}
	return true
}

func (e Nor) OkBytes(b []byte) bool {
	for _, f := range e {
		if f.OkBytes(b) {
			return false
		}
	}
	return true
}

func (e Nor) IndexFilter(fieldName string, bs Bounds) (filter Filter, bounds Bounds) {
	var nf Nor
	var ff Filter
	for _, f := range e {
		if ff, _ = f.IndexFilter(fieldName, bs); ff != nil {
			nf = append(nf, ff)
		} else {
			return nil, bs
		}
	}
	if len(nf) == 0 {
		return nil, bs
	}
	return nf, bs
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

func (e Not) Ok(v *fastjson.Value) bool {
	return !e.Filter.Ok(v)
}

func (e Not) OkBytes(b []byte) bool {
	return !e.Filter.OkBytes(b)
}

func (e Not) IndexFilter(fieldName string, bs Bounds) (filter Filter, bounds Bounds) {
	if ff, _ := e.Filter.IndexFilter(fieldName, nil); ff != nil {
		return e, bs
	}
	return nil, bs
}

func (e Not) String() string {
	return fmt.Sprintf(`{"$not": %s}`, e.Filter.String())
}

type All struct{}

func (a All) Ok(_ *fastjson.Value) bool {
	return true
}

func (a All) OkBytes(_ []byte) bool {
	return true
}

func (a All) IndexFilter(fieldName string, bs Bounds) (filter Filter, bounds Bounds) {
	return nil, bs
}

func (a All) String() string {
	return "null"
}

type Exists struct{}

func (e Exists) Ok(v *fastjson.Value) bool {
	return v != nil
}

func (e Exists) OkBytes(b []byte) bool {
	return len(b) != 0
}

func (a Exists) IndexFilter(fieldName string, bs Bounds) (filter Filter, bounds Bounds) {
	return
}

func (e Exists) String() string {
	return fmt.Sprintf(`{"$exists": true}`)
}

type TypeFilter struct {
	Type encoding.Type
}

func (e TypeFilter) Ok(v *fastjson.Value) bool {
	if v == nil {
		return false
	}
	return encoding.FastJSONTypeToType(v.Type()) == e.Type
}

func (e TypeFilter) OkBytes(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	return b[0] == uint8(e.Type)
}

func (e TypeFilter) IndexFilter(fieldName string, bs Bounds) (filter Filter, bounds Bounds) {
	k := []byte{byte(e.Type), 255}
	return e, bs.Append(Bound{
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

func (r Regexp) Ok(v *fastjson.Value) bool {
	if v == nil {
		return false
	}
	if v.Type() != fastjson.TypeString && v.Type() != fastjson.TypeArray {
		return false
	}
	if v.Type() == fastjson.TypeArray {
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

func (r Regexp) OkBytes(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	return r.Regexp.Match(b)
}

func (r Regexp) IndexFilter(_ string, bs Bounds) (filter Filter, bounds Bounds) {
	prefix := extractPrefix(r.Regexp.String())
	if prefix == "" {
		return
	}
	var (
		prefixBuf     = make([]byte, 0, len(prefix)+2)
		prefixEncoded = encoding.AppendAnyValue(prefixBuf, prefix)
	)
	// strip the 'eof' byte
	prefixEncoded = prefixEncoded[:len(prefixEncoded)-1]
	bound := Bound{
		Start:        prefixEncoded,
		End:          append(prefixEncoded, 255),
		StartInclude: true,
		EndInclude:   true,
	}
	return r, bs.Append(bound)
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
