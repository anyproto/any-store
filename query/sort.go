package query

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/key"
)

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
		res.Reverse = true
		res.Path = strings.Split(ss[1:], ".")
	} else {
		res.Path = strings.Split(ss, ".")
	}
	if len(res.Path) == 0 {
		return nil, fmt.Errorf("empty sort condition")
	}
	return res, nil
}

type Sort interface {
	Fields() []string
	Compare(v1, v2 *fastjson.Value) int
}

type Sorts []Sort

func (ss Sorts) Compare(v1, v2 *fastjson.Value) (cmp int) {
	for _, s := range ss {
		cmp = s.Compare(v1, v2)
		if cmp == 0 {
			continue
		} else {
			return
		}
	}
	return
}

func (ss Sorts) Fields() []string {
	if len(ss) == 0 {
		return nil
	}
	res := make([]string, 0, len(ss))
	for _, s := range ss {
		res = append(res, s.Fields()...)
	}
	return res
}

type SortField struct {
	Path       []string
	Reverse    bool
	bufA, bufB key.Key
}

func (s *SortField) Compare(v1, v2 *fastjson.Value) int {
	s.bufA = s.bufA[:0].AppendJSON(v1.Get(s.Path...))
	s.bufB = s.bufB[:0].AppendJSON(v2.Get(s.Path...))
	cmp := bytes.Compare(s.bufA, s.bufB)
	if s.Reverse && cmp != 0 {
		return -cmp
	}
	return cmp
}

func (s *SortField) Fields() []string {
	return []string{strings.Join(s.Path, ".")}
}

type SortDocs struct {
	Sort Sort
	Docs []*fastjson.Value
}

func (s *SortDocs) Len() int {
	return len(s.Docs)
}

func (s *SortDocs) Less(i, j int) bool {
	return s.Sort.Compare(s.Docs[i], s.Docs[j]) == -1
}

func (s *SortDocs) Swap(i, j int) {
	s.Docs[i], s.Docs[j] = s.Docs[j], s.Docs[i]
}
