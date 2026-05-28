package anystore

import (
	"github.com/anyproto/any-store/v2/internal/qplanner"
)

// SetKWayMergeMax overrides the upper bound on k for the multi-bound
// $in / $or k-way docId-merge dispatch in the query planner. Above the
// returned value, multi-bound multi-key Count routes through the
// pre-sized seen-set walk instead. Pass 0 to disable the merge entirely
// — the documented kill switch for an unforeseen regression. Returns
// the previous value, suitable for the restore-on-defer idiom:
//
//	prev := anystore.SetKWayMergeMax(0)
//	defer anystore.SetKWayMergeMax(prev)
//
// Process-global; takes effect immediately for all in-flight queries
// (atomic). Default 64.
func SetKWayMergeMax(n int) int {
	return qplanner.SetKWayMergeMax(n)
}

// SetKWayMergeMinEntries overrides the minimum sum-of-sketch-estimates
// across the query's bounds below which the merge dispatch is bypassed
// in favor of the pre-sized seen-set walk (cursor setup cost dominates
// the merge body at very small N). Returns the previous value.
// Process-global, atomic, default 200. See SetKWayMergeMax for the
// restore-on-defer idiom.
func SetKWayMergeMinEntries(n int) int {
	return qplanner.SetKWayMergeMinEntries(n)
}
