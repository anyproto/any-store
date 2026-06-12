package aggregate

import (
	"github.com/anyproto/any-store/v2/anyenc"
)

// ProjectStage replaces each upstream document with a new object containing
// only the specified fields. The output is built on RowArena; field values
// alias subtrees of the input document (zero-copy), which is safe because the
// input stays valid for the whole row lifetime.
type ProjectStage struct {
	Src    Stage
	Fields []ProjectField

	// resetArena is set by Build when no stage below keeps arena-allocated
	// data alive across pulls. It lets the stage free the previous row before
	// pulling the next one, keeping arena footprint O(row) even when an
	// upstream $unwind multiplies rows without touching the source.
	resetArena bool
}

func (s *ProjectStage) Next(ctx *Ctx) (*anyenc.Value, error) {
	if s.resetArena {
		ctx.RowArena.Reset()
	}
	v, err := s.Src.Next(ctx)
	if v == nil || err != nil {
		return nil, err
	}
	out := ctx.RowArena.NewObject()
	for i := range s.Fields {
		fv, err := s.Fields[i].Expr.Eval(ctx.RowArena, v)
		if err != nil {
			return nil, err
		}
		if fv == nil {
			continue // missing fields are omitted, Mongo semantics
		}
		out.Set(s.Fields[i].Name, fv)
	}
	return out, nil
}

func (s *ProjectStage) Close() { s.Src.Close() }

func (s *ProjectStage) String() string { return projectFieldsString("$project", s.Fields) }

// AddFieldsStage overlays computed fields onto each upstream document by
// mutating it in place (the same pattern FtsIter uses to inject _score):
// zero-copy, no output object rebuild. Mutation is safe because rows are
// transient — the underlying parse buffers are rewritten on the next row.
//
// When an upstream $unwind may emit the same underlying document for several
// consecutive rows (undoOverlay, set by Build), the original value of every
// overlaid field is captured and rolled back before the next row is pulled:
// otherwise a later row of the same document would evaluate its expressions
// against the previous row's overlays instead of the stored fields.
type AddFieldsStage struct {
	Src    Stage
	Fields []ProjectField

	// resetArena: see ProjectStage.resetArena.
	resetArena bool
	// undoOverlay: roll back the previous row's overlays before pulling the
	// next one (upstream $unwind re-emits the same document).
	undoOverlay bool

	prev  *anyenc.Value   // previous overlaid row, nil when nothing to undo
	saved []*anyenc.Value // per-field original values (nil = was missing)
	evals []*anyenc.Value // per-row expression results, reused across rows
}

func (s *AddFieldsStage) Next(ctx *Ctx) (*anyenc.Value, error) {
	if s.resetArena {
		ctx.RowArena.Reset()
	}
	// Undo strictly before Src.Next: the previous document is still intact at
	// this point (upstream stages re-parse / re-point only when pulled).
	s.undo()
	v, err := s.Src.Next(ctx)
	if v == nil || err != nil {
		return nil, err
	}
	// Evaluate every expression against the unmodified input document first
	// (Mongo semantics: $addFields expressions see the stage input, never the
	// overlays of the same stage), then apply the overlays.
	s.evals = s.evals[:0]
	for i := range s.Fields {
		fv, err := s.Fields[i].Expr.Eval(ctx.RowArena, v)
		if err != nil {
			return nil, err
		}
		s.evals = append(s.evals, fv)
	}
	for i := range s.Fields {
		if s.undoOverlay {
			s.saved = append(s.saved, v.Get(s.Fields[i].Name))
		}
		if fv := s.evals[i]; fv == nil {
			v.Del(s.Fields[i].Name) // missing result removes the field, Mongo semantics
		} else {
			v.Set(s.Fields[i].Name, fv)
		}
	}
	if s.undoOverlay {
		s.prev = v
	}
	return v, nil
}

// undo restores the original field slots of the previously emitted row, in
// reverse application order.
func (s *AddFieldsStage) undo() {
	if s.prev == nil {
		return
	}
	for i := len(s.Fields) - 1; i >= 0; i-- {
		if old := s.saved[i]; old != nil {
			s.prev.Set(s.Fields[i].Name, old)
		} else {
			s.prev.Del(s.Fields[i].Name)
		}
	}
	s.prev, s.saved = nil, s.saved[:0]
}

func (s *AddFieldsStage) Close() { s.Src.Close() }

func (s *AddFieldsStage) String() string { return projectFieldsString("$addFields", s.Fields) }
