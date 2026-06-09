package query

import (
	"fmt"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"
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
			if len(v.Fields()) == 0 {
				return nil, fmt.Errorf("sort interface must provide some fields")
			}
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
	if t := strings.TrimLeft(ss, " \t\n\r"); strings.HasPrefix(t, "{") {
		return parseSortJSON(t)
	}
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

// parseSortJSON parses an object-form sort. Currently only the relevance
// projection {"<field>":{"$meta":"textScore"}} is supported, which orders
// full-text results by descending BM25 score. The field name is cosmetic.
func parseSortJSON(ss string) (Sort, error) {
	v, err := anyenc.ParseJson(ss)
	if err != nil {
		return nil, fmt.Errorf("invalid sort object: %w", err)
	}
	obj, err := v.Object()
	if err != nil {
		return nil, fmt.Errorf("sort object must be a JSON object")
	}
	var (
		ts    *TextScoreSort
		perr  error
		count int
	)
	obj.Visit(func(key []byte, val *anyenc.Value) {
		if perr != nil {
			return
		}
		count++
		mo, e := val.Object()
		if e != nil {
			perr = fmt.Errorf("unsupported sort spec for %q", string(key))
			return
		}
		meta := mo.Get("$meta")
		if meta == nil {
			perr = fmt.Errorf("unsupported sort spec for %q", string(key))
			return
		}
		mb, _ := meta.StringBytes()
		if string(mb) != "textScore" {
			perr = fmt.Errorf("unsupported $meta: %s", string(mb))
			return
		}
		ts = &TextScoreSort{Field: string(key)}
	})
	if perr != nil {
		return nil, perr
	}
	if ts == nil || count != 1 {
		return nil, fmt.Errorf("invalid sort object")
	}
	return ts, nil
}

// TextScoreSort marks a request to order full-text results by descending
// relevance ({"<field>":{"$meta":"textScore"}}). It carries no document field —
// the full-text scan already yields documents in score order — so AppendKey and
// Fields are inert; the query layer recognises the type and keeps scan order.
type TextScoreSort struct {
	Field string
}

func (TextScoreSort) Fields() []SortField { return nil }

func (TextScoreSort) AppendKey(k anyenc.Tuple, _ *anyenc.Value) anyenc.Tuple { return k }

func (TextScoreSort) String() string { return `{"$meta":"textScore"}` }

// IsTextScoreSort reports whether s requests relevance ordering (directly or as
// the sole element of a Sorts list).
func IsTextScoreSort(s Sort) bool {
	switch v := s.(type) {
	case TextScoreSort, *TextScoreSort:
		return true
	case Sorts:
		return len(v) == 1 && IsTextScoreSort(v[0])
	}
	return false
}

type Sort interface {
	Fields() []SortField
	AppendKey(k anyenc.Tuple, v *anyenc.Value) anyenc.Tuple
}

type Sorts []Sort

func (ss Sorts) AppendKey(k anyenc.Tuple, v *anyenc.Value) anyenc.Tuple {
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

func (s *SortField) AppendKey(tuple anyenc.Tuple, v *anyenc.Value) anyenc.Tuple {
	if !s.Reverse {
		return tuple.Append(v.Get(s.Path...))
	} else {
		return tuple.AppendInverted(v.Get(s.Path...))
	}
}

func (s *SortField) Fields() []SortField {
	return []SortField{*s}
}
