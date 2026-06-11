package aggregate

import (
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
)

// MatchStage filters the upstream stream with a query filter. Matched rows
// are passed through untouched (1:1, no arena allocations); discarded rows
// are freed by the source's per-row arena reset on the following pull.
type MatchStage struct {
	Src    Stage
	Filter query.Filter
}

func (s *MatchStage) Next(ctx *Ctx) (*anyenc.Value, error) {
	for {
		v, err := s.Src.Next(ctx)
		if v == nil || err != nil {
			return nil, err
		}
		if s.Filter.Ok(v, ctx.Buf) {
			return v, nil
		}
	}
}

func (s *MatchStage) Close() { s.Src.Close() }

func (s *MatchStage) String() string { return MatchSpec{Filter: s.Filter}.String() }
