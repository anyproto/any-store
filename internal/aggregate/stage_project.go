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
type AddFieldsStage struct {
	Src    Stage
	Fields []ProjectField

	// resetArena: see ProjectStage.resetArena.
	resetArena bool
}

func (s *AddFieldsStage) Next(ctx *Ctx) (*anyenc.Value, error) {
	if s.resetArena {
		ctx.RowArena.Reset()
	}
	v, err := s.Src.Next(ctx)
	if v == nil || err != nil {
		return nil, err
	}
	for i := range s.Fields {
		fv, err := s.Fields[i].Expr.Eval(ctx.RowArena, v)
		if err != nil {
			return nil, err
		}
		if fv == nil {
			v.Del(s.Fields[i].Name) // missing result removes the field, Mongo semantics
			continue
		}
		v.Set(s.Fields[i].Name, fv)
	}
	return v, nil
}

func (s *AddFieldsStage) Close() { s.Src.Close() }

func (s *AddFieldsStage) String() string { return projectFieldsString("$addFields", s.Fields) }
