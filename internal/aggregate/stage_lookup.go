package aggregate

import (
	"bytes"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/syncpool"
)

// LookupFunc serves one $lookup point read: key is the anyenc-marshaled
// primary-key value, buf is stage-owned scratch (DocBuf + Parser) the
// implementation fetches and parses the stored document with — the returned
// value stays valid until the same buf is reused. (nil, nil) means no document
// has that key. Injected by the root package at Build time: the reads must run
// against the same snapshot the pipeline streams from, and internal/aggregate
// cannot import the root package to reach it.
type LookupFunc func(key []byte, buf *syncpool.DocBuffer) (*anyenc.Value, error)

// Env carries execution-environment hooks injected at Build time.
type Env struct {
	// Lookup resolves $lookup point reads. Required when the pipeline
	// contains a LookupSpec; Build rejects the spec without it.
	Lookup LookupFunc
}

// LookupStage implements the self-join $lookup: for each row it resolves the
// localField value(s) as primary keys of the aggregated collection and
// overlays the matched full documents onto the row as the "as" field — always
// an array (Mongo semantics), empty when nothing matches, replacing any
// existing field.
//
// A missing or null local value yields an empty array: the primary key is
// never null, so Mongo's null-matching join cannot apply here. An array local
// value is set membership: elements are deduplicated by first occurrence and
// the output keeps first-occurrence order (Mongo leaves the order
// unspecified). An element of a type no stored key has simply doesn't match.
//
// Fetched documents live in stage-owned per-slot buffers reused every row —
// zero steady-state allocations once the slot count reaches the row's match
// count high-water mark.
type LookupStage struct {
	Src    Stage
	Spec   LookupSpec
	Lookup LookupFunc

	// resetArena: see ProjectStage.resetArena.
	resetArena bool
	// undoOverlay: restore the previous row's "as" slot before pulling the
	// next one (upstream $unwind re-emits the same document).
	undoOverlay bool

	bufs []*syncpool.DocBuffer // one per matched doc of the current row

	keyScratch []byte
	keys       []byte          // marshaled ids seen in the current row's array
	keyOffs    []int           // start offsets into keys
	leaves     []anyenc.Leaf   // scratch: the local path's leaf set
	vals       []*anyenc.Value // scratch: its lookup values

	prev    *anyenc.Value // previous overlaid row, nil when nothing to undo
	prevVal *anyenc.Value // its original "as" value (nil = was missing)
}

func (s *LookupStage) Next(ctx *Ctx) (*anyenc.Value, error) {
	if s.resetArena {
		ctx.RowArena.Reset()
	}
	// Undo strictly before Src.Next: the previous document is still intact at
	// this point (see AddFieldsStage).
	s.undo()
	v, err := s.Src.Next(ctx)
	if v == nil || err != nil {
		return nil, err
	}
	out := ctx.RowArena.NewArray()
	n := 0
	// The local path resolves with field-path semantics (a path through an
	// array of objects collects every element's value, as Mongo's localField
	// does) and a leaf array — positional or not — contributes its elements,
	// one level deep. Null, missing and array values never name a pk: skipped.
	s.leaves = v.AppendLeaves(s.leaves[:0], s.Spec.LocalPath...)
	s.vals = s.vals[:0]
	for _, l := range s.leaves {
		if l.Value != nil && l.Value.Type() == anyenc.TypeArray {
			arr, _ := l.Value.Array()
			s.vals = append(s.vals, arr...)
			continue
		}
		s.vals = append(s.vals, l.Value)
	}
	if len(s.vals) == 1 {
		if lv := s.vals[0]; lv != nil && lv.Type() != anyenc.TypeNull && lv.Type() != anyenc.TypeArray {
			s.keyScratch = lv.MarshalTo(s.keyScratch[:0])
			doc, lerr := s.fetch(0)
			if lerr != nil {
				return nil, lerr
			}
			if doc != nil {
				out.SetArrayItem(0, doc)
			}
		}
	} else {
		s.keys, s.keyOffs = s.keys[:0], s.keyOffs[:0]
		for _, el := range s.vals {
			if el == nil || el.Type() == anyenc.TypeNull || el.Type() == anyenc.TypeArray {
				continue
			}
			s.keyScratch = el.MarshalTo(s.keyScratch[:0])
			if s.seen(s.keyScratch) {
				continue // set membership: first occurrence wins
			}
			s.keyOffs = append(s.keyOffs, len(s.keys))
			s.keys = append(s.keys, s.keyScratch...)
			doc, lerr := s.fetch(n)
			if lerr != nil {
				return nil, lerr
			}
			if doc != nil {
				out.SetArrayItem(n, doc)
				n++
			}
		}
	}
	if s.undoOverlay {
		s.prev, s.prevVal = v, v.Get(s.Spec.As)
	}
	v.Set(s.Spec.As, out)
	return v, nil
}

// seen reports whether key was already resolved for the current row. Linear
// scan: local id arrays are small, and this keeps dedup alloc-free.
func (s *LookupStage) seen(key []byte) bool {
	for i, start := range s.keyOffs {
		end := len(s.keys)
		if i+1 < len(s.keyOffs) {
			end = s.keyOffs[i+1]
		}
		if bytes.Equal(s.keys[start:end], key) {
			return true
		}
	}
	return false
}

// fetch resolves the key in keyScratch through slot's buffer, growing the slot
// set on demand (the only per-row allocation, amortized to zero once the
// high-water mark is reached).
func (s *LookupStage) fetch(slot int) (*anyenc.Value, error) {
	if slot >= len(s.bufs) {
		s.bufs = append(s.bufs, &syncpool.DocBuffer{Arena: &anyenc.Arena{}, Parser: &anyenc.Parser{}})
	}
	return s.Lookup(s.keyScratch, s.bufs[slot])
}

// undo restores the previously emitted row's original "as" slot.
func (s *LookupStage) undo() {
	if s.prev == nil {
		return
	}
	if s.prevVal != nil {
		s.prev.Set(s.Spec.As, s.prevVal)
	} else {
		s.prev.Del(s.Spec.As)
	}
	s.prev, s.prevVal = nil, nil
}

func (s *LookupStage) Close() { s.Src.Close() }

func (s *LookupStage) String() string { return s.Spec.String() }
