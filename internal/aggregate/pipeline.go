package aggregate

import (
	"fmt"
)

// Build compiles parsed stage specs into an executable stage chain on top of
// source. The source must reset Ctx.RowArena once per row it yields (it is
// the default row owner).
//
// Build also performs the arena-ownership analysis: $project / $addFields get
// permission to reset RowArena before pulling (freeing the previous row) when
// no stage below them keeps arena-allocated data alive across pulls. This
// keeps the arena footprint O(row) under $unwind row multiplication.
func Build(source Stage, specs Pipeline, limits Limits) (Stage, error) {
	limits = limits.WithDefaults()
	cur := source
	// rowOnArena: rows at this point of the chain may be (or contain)
	// RowArena-allocated values.
	// heldOnArena: some stage below holds such a row across its Next calls
	// (only $unwind does), so nobody above may reset the arena mid-stream.
	var rowOnArena, heldOnArena bool
	for i := 0; i < len(specs); i++ {
		switch sp := specs[i].(type) {
		case MatchSpec:
			cur = &MatchStage{Src: cur, Filter: sp.Filter}
		case SkipSpec:
			cur = &SkipStage{Src: cur, N: sp.N}
		case LimitSpec:
			cur = &LimitStage{Src: cur, N: sp.N}
		case CountSpec:
			cur = &CountStage{Src: cur, Field: sp.Field}
			// The single count row is built after the drain; nothing below is
			// live anymore.
			rowOnArena, heldOnArena = true, false
		case ProjectSpec:
			cur = &ProjectStage{Src: cur, Fields: sp.Fields, resetArena: !heldOnArena}
			rowOnArena = true
		case AddFieldsSpec:
			cur = &AddFieldsStage{Src: cur, Fields: sp.Fields, resetArena: !heldOnArena}
			rowOnArena = true
		case UnwindSpec:
			cur = &UnwindStage{
				Src:                        cur,
				Field:                      sp.Field,
				Path:                       sp.Path,
				PreserveNullAndEmptyArrays: sp.PreserveNullAndEmptyArrays,
			}
			if rowOnArena {
				heldOnArena = true
			}
		case GroupSpec:
			cur = newGroupStage(cur, sp, limits)
			// $group drains its upstream completely before emitting; emitted
			// rows are built fresh on RowArena with a per-emit reset, and
			// nothing below stays live afterwards.
			rowOnArena, heldOnArena = true, false
		case SortSpec:
			// A directly following $skip/$limit bounds the sort's retained
			// set (top-K); the skip/limit stages themselves stay in the chain.
			cur = &SortStage{Src: cur, Spec: sp, TopK: foldTopK(specs[i+1:])}
			// Emitted rows are owned by the stage parser; nothing below stays
			// live after the drain.
			rowOnArena, heldOnArena = false, false
		default:
			return nil, fmt.Errorf("aggregate: unsupported stage: %T", sp)
		}
	}
	return cur, nil
}

// foldTopK returns skip+limit when an in-pipeline $sort is immediately
// followed by $skip/$limit stages (in that combination), bounding how many
// rows the sort must retain. 0 means a full sort.
func foldTopK(rest Pipeline) int {
	skip := 0
	for _, spec := range rest {
		switch sp := spec.(type) {
		case SkipSpec:
			skip += sp.N
		case LimitSpec:
			return skip + sp.N
		default:
			return 0
		}
	}
	return 0
}
