package qplanner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
)

// TestFilterIter_RejectsNonMatching covers filter_iter.go:80-86 — when
// Filter.Ok returns false, the iterator advances without yielding and clears
// DocParsed cache. Also covers the ErrKeyNotFound swallow at 55-57.
func TestFilterIter_RejectsNonMatching(t *testing.T) {
	db, ns := coverageBtree(t, "filter_reject", []string{"a", "b", "missing", "c"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	// Feed IDs a, b, nonexistent, c. "nonexistent" hits the ErrKeyNotFound
	// swallow; the filter {"id":"a"} rejects b and c.
	//
	// In production, an upstream FetchIter populates Plan.DocParsed per-iteration,
	// so FilterIter's reuse branch at filter_iter.go:48 is safe. Here we pass
	// nil Plan to force a fresh fetch on every iteration — this exercises the
	// non-reuse branch at filter_iter.go:51-68 and the ErrKeyNotFound swallow.
	source := &fakeIter{
		hits: []fakeHit{
			{docId: anyenc.AppendAnyValue(nil, "a")},
			{docId: anyenc.AppendAnyValue(nil, "b")},
			{docId: anyenc.AppendAnyValue(nil, "nonexistent")},
			{docId: anyenc.AppendAnyValue(nil, "c")},
		},
	}
	it := &FilterIter{
		Source: source,
		Data:   &CursorSource{Tx: rtx, Ns: ns},
		Filter: query.MustParseCondition(`{"id":"a"}`),
		Buf:    buf,
		// Plan: nil → filter-reject branch must NOT try to cache DocParsed
	}

	var matched int
	for {
		_, docId, _, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		matched++
	}
	assert.Equal(t, 1, matched, "only the 'a' doc must pass the filter")
}

// TestFilterIter_NoPlan covers the `if it.Plan != nil` branch at
// filter_iter.go:84 — the reject path. With Plan nil, the filter must be
// able to REJECT the doc and continue iteration without panicking (because
// the DocParsed reset is gated behind the nil check). The no-rejection
// path doesn't exercise line 84 at all, so we must force a rejection.
func TestFilterIter_NoPlan(t *testing.T) {
	db, ns := coverageBtree(t, "filter_no_plan", []string{"x"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	source := &fakeIter{hits: []fakeHit{{docId: anyenc.AppendAnyValue(nil, "x")}}}
	it := &FilterIter{
		Source: source,
		Data:   &CursorSource{Tx: rtx, Ns: ns},
		// Filter REJECTS: doc has id=="x" but we ask for id=="nope".
		// Rejection forces filter_iter.go:84 (the nil-Plan-guarded reset) to
		// execute — this is the branch that is meaningfully nil-Plan-sensitive.
		Filter: query.MustParseCondition(`{"id":"nope"}`),
		Buf:    buf,
		// Plan: nil → line 84 must NOT attempt it.Plan.DocParsed = nil (nil deref).
	}
	// Iteration must silently complete with docId==nil (no matches) and no error.
	_, docId, _, err := it.Next()
	require.NoError(t, err, "nil-Plan reset branch must be skipped, not crash")
	assert.Nil(t, docId, "filter rejects the only doc, so iteration ends clean")
}

// TestFilterIter_PerfBranches exercises filter_iter.go perf guards and
// asserts counters moved to reflect the actual iteration outcome.
func TestFilterIter_PerfBranches(t *testing.T) {
	resetPerfCounters()
	setPerfCountersEnabled(true)
	defer func() {
		setPerfCountersEnabled(false)
		resetPerfCounters()
	}()

	db, ns := coverageBtree(t, "filter_perf", []string{"a", "b"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	source := &fakeIter{hits: []fakeHit{
		{docId: anyenc.AppendAnyValue(nil, "a")},
		{docId: anyenc.AppendAnyValue(nil, "b")},
	}}
	it := &FilterIter{
		Source: source,
		Data:   &CursorSource{Tx: rtx, Ns: ns},
		Filter: query.MustParseCondition(`{"id":"a"}`),
		Buf:    buf,
	}
	for {
		_, docId, _, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
	}

	s := snapshotPerfCounters()
	assert.Greater(t, s.FilterNextCalls, uint64(0), "FilterNextCalls must be incremented")
	assert.Equal(t, uint64(1), s.FilterYields, "only 'a' matches the filter")
	assert.Greater(t, s.FilterNextNs, uint64(0), "FilterNextNs must accumulate timing")
}
