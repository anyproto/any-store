package query

import (
	"bytes"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/key"
)

type Sort interface {
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
