package aggregate

import (
	"github.com/anyproto/any-store/v2/anyenc"
)

// UnwindStage emits one row per element of an array field, replacing the
// array with the current element by mutating the document in place: the
// element values stay alive because they belong to the original (detached)
// array value, while the document's field slot is repointed per emission.
// No arena allocations, no document rebuild.
//
// Before pulling the next source row the original array is restored into the
// field slot. An upstream stage may hold and re-emit the same document
// (another $unwind multiplying rows does), so leaving the field pointing at
// the last element would corrupt every later row of that document.
//
// Mongo semantics: missing / null / empty array drop the document (unless
// PreserveNullAndEmptyArrays, which emits it as-is — with the field removed
// for an empty array); a non-array value passes through unchanged.
type UnwindStage struct {
	Src                        Stage
	Field                      string
	Path                       []string
	PreserveNullAndEmptyArrays bool

	cur *anyenc.Value // current source document, nil when exhausted
	arr []*anyenc.Value
	idx int

	// mutated/mutatedVal track the document whose field slot this stage
	// repointed (or deleted), and the original array to put back.
	mutated    *anyenc.Value
	mutatedVal *anyenc.Value
}

func (s *UnwindStage) Next(ctx *Ctx) (*anyenc.Value, error) {
	for {
		if s.cur == nil {
			s.undoMutation()
			v, err := s.Src.Next(ctx)
			if v == nil || err != nil {
				return nil, err
			}
			av := v.Get(s.Path...)
			if av == nil || av.Type() == anyenc.TypeNull {
				if s.PreserveNullAndEmptyArrays {
					return v, nil
				}
				continue
			}
			if av.Type() != anyenc.TypeArray {
				return v, nil
			}
			arr := av.GetArray()
			if len(arr) == 0 {
				if s.PreserveNullAndEmptyArrays {
					s.mutated, s.mutatedVal = v, av
					s.parent(v).Del(s.last())
					return v, nil
				}
				continue
			}
			s.cur, s.arr, s.idx = v, arr, 0
			s.mutated, s.mutatedVal = v, av
		}
		if s.idx >= len(s.arr) {
			s.cur, s.arr = nil, nil
			continue
		}
		elem := s.arr[s.idx]
		s.idx++
		s.parent(s.cur).Set(s.last(), elem)
		return s.cur, nil
	}
}

// undoMutation restores the original array into the previous document's field
// slot before a new row is pulled (the consumer of the previous row is done
// with it by then — rows are valid only until the next Next call).
func (s *UnwindStage) undoMutation() {
	if s.mutated == nil {
		return
	}
	s.parent(s.mutated).Set(s.last(), s.mutatedVal)
	s.mutated, s.mutatedVal = nil, nil
}

// parent returns the object holding the unwound field (the document itself
// for a top-level path). The path is known to exist: Next only gets here
// after Get(Path...) succeeded.
func (s *UnwindStage) parent(doc *anyenc.Value) *anyenc.Value {
	if len(s.Path) == 1 {
		return doc
	}
	return doc.Get(s.Path[:len(s.Path)-1]...)
}

func (s *UnwindStage) last() string { return s.Path[len(s.Path)-1] }

func (s *UnwindStage) Close() { s.Src.Close() }

func (s *UnwindStage) String() string {
	return UnwindSpec{Field: s.Field, PreserveNullAndEmptyArrays: s.PreserveNullAndEmptyArrays}.String()
}
