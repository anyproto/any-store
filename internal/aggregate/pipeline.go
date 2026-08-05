package aggregate

import (
	"errors"
	"fmt"
	"strings"

	"github.com/anyproto/any-store/v2/query"
)

// Prefix is the leading pipeline part that can be pushed down into the access
// plan (the regular Find machinery: index/CBO selection, FTS/vector sources,
// index-order sorts, cursor-level offset skips).
type Prefix struct {
	Filter query.Filter // folded leading $match chain; nil if none
	Sort   query.Sort   // nil if no $sort was pushed
	Skip   int
	Limit  int // 0 = none
}

// SplitPrefix consumes the longest pushable pipeline prefix in canonical
// order — $match* → $sort? → $skip? → $limit? — and returns it together with
// the remaining stages (which run as in-pipeline operators). Pushdown stops
// at the first stage that synthesizes documents ($group/$project/$addFields/
// $unwind/$count) and at any stage out of canonical order: a $match after
// $skip/$limit does not commute, and out-of-order combinations are rare
// enough that running them in-pipeline (correct, just unoptimized) beats
// rewrite complexity.
//
// $expr predicates are never pushed into the access plan: a leading $match's
// ordinary filter part enters the prefix (index-eligible) while its
// expressions come back as a synthesized residual MatchSpec at the head of
// the remaining stages. A following $sort may still be pushed — filtering a
// sorted stream preserves its order — but $skip/$limit may not: they would
// truncate before the residual predicate runs.
func SplitPrefix(p Pipeline) (Prefix, Pipeline) {
	var (
		pre     Prefix
		filters []query.Filter
		exprs   []Expr
		phase   int // 0: $match, 1: $sort, 2: $skip, 3: $limit
		i       int
	)
loop:
	for ; i < len(p); i++ {
		switch sp := p[i].(type) {
		case MatchSpec:
			if phase > 0 {
				break loop
			}
			if sp.Filter != nil {
				filters = append(filters, sp.Filter)
			}
			exprs = append(exprs, sp.Exprs...)
		case SortSpec:
			if phase >= 1 {
				break loop
			}
			pre.Sort = sp.Sort
			phase = 1
		case SkipSpec:
			if phase >= 2 || len(exprs) > 0 {
				break loop
			}
			pre.Skip = sp.N
			phase = 2
		case LimitSpec:
			if phase >= 3 || len(exprs) > 0 {
				break loop
			}
			pre.Limit = sp.N
			phase = 3
		default:
			break loop
		}
	}
	switch len(filters) {
	case 0:
	case 1:
		pre.Filter = filters[0]
	default:
		pre.Filter = query.And(filters)
	}
	rest := p[i:]
	if len(exprs) > 0 {
		rest = append(Pipeline{MatchSpec{Exprs: exprs}}, rest...)
	}
	return pre, rest
}

// ExplainStages renders the in-pipeline stage list for Explain output,
// including the top-K annotation a following $skip/$limit folds into $sort.
func ExplainStages(p Pipeline) string {
	var b strings.Builder
	for i, spec := range p {
		s := spec.String()
		if ss, ok := spec.(SortSpec); ok {
			if k := foldTopK(p[i+1:]); k > 0 {
				s = (&SortStage{Spec: ss, TopK: k}).String()
			}
		}
		fmt.Fprintf(&b, "  %d. %s\n", i+1, s)
	}
	return b.String()
}

// Build compiles parsed stage specs into an executable stage chain on top of
// source. The source must reset Ctx.RowArena once per row it yields (it is
// the default row owner). env carries the execution hooks the root package
// injects (the $lookup point-read function).
//
// Build also performs the arena-ownership analysis: $project / $addFields /
// $lookup get permission to reset RowArena before pulling (freeing the
// previous row) when no stage below them keeps arena-allocated data alive
// across pulls. This keeps the arena footprint O(row) under $unwind row
// multiplication.
func Build(source Stage, specs Pipeline, limits Limits, env Env) (Stage, error) {
	limits = limits.WithDefaults()
	cur := source
	// rowOnArena: rows at this point of the chain may be (or contain)
	// RowArena-allocated values.
	// heldOnArena: some stage below holds such a row across its Next calls
	// (only $unwind does), so nobody above may reset the arena mid-stream.
	// multiEmit: an upstream $unwind may emit the same underlying document for
	// several consecutive rows, so in-place mutations must be rolled back
	// between rows.
	var rowOnArena, heldOnArena, multiEmit bool
	for i := 0; i < len(specs); i++ {
		switch sp := specs[i].(type) {
		case MatchSpec:
			cur = &MatchStage{
				Src:    cur,
				Filter: sp.Filter,
				Exprs:  sp.Exprs,
				// Expression temporaries live on RowArena; free the previous
				// row's before pulling the next one whenever nothing below
				// holds arena data across pulls (same rule as $project).
				resetArena: len(sp.Exprs) > 0 && !heldOnArena,
			}
		case SkipSpec:
			cur = &SkipStage{Src: cur, N: sp.N}
		case LimitSpec:
			cur = &LimitStage{Src: cur, N: sp.N}
		case CountSpec:
			cur = &CountStage{Src: cur, Field: sp.Field}
			// The single count row is built after the drain; nothing below is
			// live anymore.
			rowOnArena, heldOnArena, multiEmit = true, false, false
		case ProjectSpec:
			// $project builds a fresh output object per pull, so rows above it
			// are distinct objects even under an upstream $unwind.
			cur = &ProjectStage{Src: cur, Fields: sp.Fields, resetArena: !heldOnArena}
			rowOnArena, multiEmit = true, false
		case AddFieldsSpec:
			cur = &AddFieldsStage{Src: cur, Fields: sp.Fields, resetArena: !heldOnArena, undoOverlay: multiEmit}
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
			multiEmit = true
		case LookupSpec:
			if env.Lookup == nil {
				return nil, errors.New("aggregate: $lookup requires an injected LookupFunc")
			}
			// $lookup overlays the "as" array (arena-allocated container) onto
			// the document in place, like $addFields overlays computed fields:
			// same arena-reset permission, same undo under upstream $unwind
			// multi-emit (a later row of the same document must read localField
			// from the stored fields, not the previous row's overlay).
			cur = &LookupStage{Src: cur, Spec: sp, Lookup: env.Lookup, resetArena: !heldOnArena, undoOverlay: multiEmit}
			rowOnArena = true
		case GroupSpec:
			cur = newGroupStage(cur, sp, limits)
			// $group drains its upstream completely before emitting; emitted
			// rows are built fresh on RowArena with a per-emit reset, and
			// nothing below stays live afterwards.
			rowOnArena, heldOnArena, multiEmit = true, false, false
		case SortSpec:
			// A directly following $skip/$limit bounds the sort's retained
			// set (top-K); the skip/limit stages themselves stay in the chain.
			cur = &SortStage{Src: cur, Spec: sp, TopK: foldTopK(specs[i+1:])}
			// Emitted rows are owned by the stage parser; nothing below stays
			// live after the drain.
			rowOnArena, heldOnArena, multiEmit = false, false, false
		default:
			return nil, fmt.Errorf("aggregate: unsupported stage: %T", sp)
		}
	}
	return cur, nil
}

// HasLookup reports whether the pipeline contains a $lookup spec (the root
// package injects Env.Lookup — and keeps a read tx for it — only then).
func HasLookup(p Pipeline) bool {
	for _, spec := range p {
		if _, ok := spec.(LookupSpec); ok {
			return true
		}
	}
	return false
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
