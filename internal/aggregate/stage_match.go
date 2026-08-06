package aggregate

import (
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
)

// MatchStage filters the upstream stream with a query filter and/or $expr
// predicates (AND-ed, $cond truthiness: false/0/null/missing are false).
// Matched rows are passed through untouched (1:1); discarded rows are freed
// by the row owner's per-row arena reset on the following pull. Expression
// temporaries are allocated on RowArena and die with the row.
type MatchStage struct {
	Src    Stage
	Filter query.Filter // nil: no ordinary-filter part
	Exprs  []Expr       // nil: no $expr part

	// resetArena is set by Build when the stage evaluates expressions and no
	// stage below keeps arena-allocated data alive across pulls: freeing the
	// previous row's expression temporaries before each pull keeps the arena
	// O(row) even when the upstream row owner (e.g. an in-pipeline $sort)
	// never resets it.
	resetArena bool
}

func (s *MatchStage) Next(ctx *Ctx) (*anyenc.Value, error) {
	for {
		if s.resetArena {
			ctx.RowArena.Reset()
		}
		v, err := s.Src.Next(ctx)
		if v == nil || err != nil {
			return nil, err
		}
		if s.Filter != nil && !s.Filter.Ok(v, ctx.Buf) {
			continue
		}
		matched := true
		for _, e := range s.Exprs {
			ev, eerr := e.Eval(ctx.RowArena, v)
			if eerr != nil {
				return nil, eerr
			}
			if !truthy(ev) {
				matched = false
				break
			}
		}
		if matched {
			return v, nil
		}
	}
}

func (s *MatchStage) Close() { s.Src.Close() }

func (s *MatchStage) String() string { return MatchSpec{Filter: s.Filter, Exprs: s.Exprs}.String() }
