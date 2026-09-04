package query

import (
	"bytes"
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

// AppendKey appends the document's sort key for this field under Mongo
// array-sort semantics. The path resolves to its leaf set
// (anyenc.Value.WalkLeaves) and every non-empty, non-positional array leaf
// contributes its elements; the key is the MINIMUM candidate ascending /
// MAXIMUM descending — chosen from all candidates, independent of any query
// predicate (MongoDB >= 4.4, SERVER-19402). This is exactly the entry an
// ordered index scan encounters first for the doc (min forward, max reverse
// — see qplanner.CanonicalKeyDedupIter), so plan choice cannot change the
// order. A missing leaf is null. An EMPTY array leaf keeps its whole-array
// key: the index stores an empty array only under that key, so both plans
// agree; this deliberately diverges from Mongo (which sorts [] before null)
// — matching that would need a key encoding below TypeNull on disk.
//
// Zero-alloc: candidates are marshaled into the tuple's spare tail capacity
// and the loser is truncated away (copy handles the overlapping move), so the
// running best lives in the caller's buffer and this method needs no state —
// a Sort, like a Filter, may be shared by concurrent queries.
func (s *SortField) AppendKey(tuple anyenc.Tuple, v *anyenc.Value) anyenc.Tuple {
	if leaf, single := v.GetLeaf(s.Path...); single {
		return s.appendElementKey(tuple, leaf)
	}
	base := len(tuple)
	v.WalkLeaves(s.Path, func(l anyenc.Leaf) { tuple = s.considerLeaf(tuple, base, l) })
	return s.finishKey(tuple, base)
}

// appendElementKey is AppendKey for one already-resolved, non-positional
// leaf (the raw fast path, whose walk never crosses an array container).
func (s *SortField) appendElementKey(tuple anyenc.Tuple, fv *anyenc.Value) anyenc.Tuple {
	base := len(tuple)
	tuple = s.considerLeaf(tuple, base, anyenc.Leaf{Value: fv})
	return s.finishKey(tuple, base)
}

func (s *SortField) considerLeaf(tuple anyenc.Tuple, base int, l anyenc.Leaf) anyenc.Tuple {
	if l.Value != nil && l.Value.Type() == anyenc.TypeArray && !l.Positional {
		if items, _ := l.Value.Array(); len(items) > 0 {
			for _, item := range items {
				tuple = s.consider(tuple, base, item)
			}
			return tuple
		}
	}
	return s.consider(tuple, base, l.Value)
}

// consider marshals fv after the running best at tuple[base:] and keeps the
// better of the two.
func (s *SortField) consider(tuple anyenc.Tuple, base int, fv *anyenc.Value) anyenc.Tuple {
	candOff := len(tuple)
	tuple = fv.MarshalTo(tuple)
	if candOff == base {
		return tuple // first candidate is the initial best
	}
	cand, best := tuple[candOff:], tuple[base:candOff]
	cmp := bytes.Compare(cand, best)
	if (!s.Reverse && cmp < 0) || (s.Reverse && cmp > 0) {
		n := copy(tuple[base:], cand) // overlapping move: new best down
		return tuple[:base+n]
	}
	return tuple[:candOff] // loser: drop the tail bytes
}

// finishKey inverts the chosen key for a descending field.
func (s *SortField) finishKey(tuple anyenc.Tuple, base int) anyenc.Tuple {
	if s.Reverse {
		for i := base; i < len(tuple); i++ {
			tuple[i] = ^tuple[i]
		}
	}
	return tuple
}

func (s *SortField) Fields() []SortField {
	return []SortField{*s}
}
