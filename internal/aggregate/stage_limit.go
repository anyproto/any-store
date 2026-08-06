package aggregate

import (
	"github.com/anyproto/any-store/v2/anyenc"
)

// SkipStage drops the first N upstream rows.
type SkipStage struct {
	Src     Stage
	N       int
	skipped int
}

func (s *SkipStage) Next(ctx *Ctx) (*anyenc.Value, error) {
	for s.skipped < s.N {
		v, err := s.Src.Next(ctx)
		if v == nil || err != nil {
			return nil, err
		}
		s.skipped++
	}
	return s.Src.Next(ctx)
}

func (s *SkipStage) Close() { s.Src.Close() }

func (s *SkipStage) String() string { return SkipSpec{N: s.N}.String() }

// LimitStage emits at most N rows and stops pulling upstream afterwards.
type LimitStage struct {
	Src     Stage
	N       int
	emitted int
}

func (s *LimitStage) Next(ctx *Ctx) (*anyenc.Value, error) {
	if s.emitted >= s.N {
		return nil, nil
	}
	v, err := s.Src.Next(ctx)
	if v == nil || err != nil {
		return nil, err
	}
	s.emitted++
	return v, nil
}

func (s *LimitStage) Close() { s.Src.Close() }

func (s *LimitStage) String() string { return LimitSpec{N: s.N}.String() }

// CountStage drains the upstream stream and emits a single {"<field>": N}
// document.
type CountStage struct {
	Src   Stage
	Field string
	n     int // field, not a local: the count survives a facet-feed pause
	done  bool
}

func (s *CountStage) Next(ctx *Ctx) (*anyenc.Value, error) {
	if s.done {
		return nil, nil
	}
	for {
		v, err := s.Src.Next(ctx)
		if err != nil {
			return nil, err
		}
		if v == nil {
			break
		}
		s.n++
		if s.n%ctxCheckEvery == 0 {
			if cerr := ctx.Context.Err(); cerr != nil {
				return nil, cerr
			}
		}
	}
	s.done = true
	// The drain is over: the count doc is the only live row, so the stage
	// becomes the row owner for its single emission.
	ctx.RowArena.Reset()
	out := ctx.RowArena.NewObject()
	out.Set(s.Field, ctx.RowArena.NewNumberInt(s.n))
	return out, nil
}

func (s *CountStage) Close() { s.Src.Close() }

func (s *CountStage) String() string { return CountSpec{Field: s.Field}.String() }
