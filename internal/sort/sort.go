package sort

import (
	"fmt"
	"strings"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/encoding"
	"github.com/anyproto/any-store/internal/key"
)

func MustParseSort(sorts ...any) Sort {
	s, err := ParseSort(sorts...)
	if err != nil {
		panic(err)
	}
	return s
}

func ParseSort(sorts ...any) (Sort, error) {
	var result = make(Sorts, 0, len(sorts))
	for _, s := range sorts {
		switch v := s.(type) {
		case string:
			sf, err := parseSortString(v)
			if err != nil {
				return nil, err
			}
			result = append(result, sf)
		case Sort:
			result = append(result, v)
		default:
			return nil, fmt.Errorf("unexpected sort argument type: %T", s)
		}
	}
	if len(result) == 1 {
		return result[0], nil
	}
	return result, nil
}

func parseSortString(ss string) (Sort, error) {
	res := &SortField{}
	if strings.HasPrefix(ss, "-") {
		res.Field = ss[1:]
		res.Reverse = true
		res.Path = strings.Split(ss[1:], ".")
	} else {
		res.Field = ss
		res.Path = strings.Split(ss, ".")
	}
	if len(res.Path) == 0 {
		return nil, fmt.Errorf("empty sort condition")
	}
	return res, nil
}

type Sort interface {
	Fields() []SortField
	AppendKey(k key.Key, v *fastjson.Value) key.Key
}

type Sorts []Sort

func (ss Sorts) AppendKey(k key.Key, v *fastjson.Value) key.Key {
	for _, s := range ss {
		k = s.AppendKey(k, v)
	}
	return k
}

func (ss Sorts) Fields() []SortField {
	if len(ss) == 0 {
		return nil
	}
	res := make([]SortField, 0, len(ss))
	for _, s := range ss {
		res = append(res, s.Fields()...)
	}
	return res
}

type SortField struct {
	Field   string
	Path    []string
	Reverse bool
}

func (s *SortField) AppendKey(k key.Key, v *fastjson.Value) key.Key {
	if !s.Reverse {
		return encoding.AppendJSONValue(k, v.Get(s.Path...))
	} else {
		return encoding.AppendInvertedJSON(k, v.Get(s.Path...))
	}
}

func (s *SortField) Fields() []SortField {
	return []SortField{*s}
}
