package qplanner

import "sync/atomic"

// PerfCounters provides coarse-grained hot-path counters for planner iterators.
// Intended for local profiling and regression diagnosis.
type PerfCounters struct {
	IndexNextCalls uint64
	IndexYields    uint64
	IndexNextNs    uint64

	FetchNextCalls uint64
	FetchYields    uint64
	FetchNextNs    uint64
	FetchLookupNs  uint64
	FetchParseNs   uint64

	FilterNextCalls uint64
	FilterYields    uint64
	FilterNextNs    uint64
	FilterEvalNs    uint64

	// MergeDispatched counts k-way docId-merge invocations (Task 4).
	// Incremented once per kWayDocIdMergeIter construction. Tests pin
	// path selection through this counter instead of inspecting internal
	// state.
	MergeDispatched uint64
}

var qpPerf struct {
	enabled atomic.Bool

	indexNextCalls atomic.Uint64
	indexYields    atomic.Uint64
	indexNextNs    atomic.Uint64

	fetchNextCalls atomic.Uint64
	fetchYields    atomic.Uint64
	fetchNextNs    atomic.Uint64
	fetchLookupNs  atomic.Uint64
	fetchParseNs   atomic.Uint64

	filterNextCalls atomic.Uint64
	filterYields    atomic.Uint64
	filterNextNs    atomic.Uint64
	filterEvalNs    atomic.Uint64

	mergeDispatched atomic.Uint64
}

func setPerfCountersEnabled(enabled bool) {
	qpPerf.enabled.Store(enabled)
}

func perfCountersEnabled() bool {
	return qpPerf.enabled.Load()
}

func resetPerfCounters() {
	qpPerf.indexNextCalls.Store(0)
	qpPerf.indexYields.Store(0)
	qpPerf.indexNextNs.Store(0)
	qpPerf.fetchNextCalls.Store(0)
	qpPerf.fetchYields.Store(0)
	qpPerf.fetchNextNs.Store(0)
	qpPerf.fetchLookupNs.Store(0)
	qpPerf.fetchParseNs.Store(0)
	qpPerf.filterNextCalls.Store(0)
	qpPerf.filterYields.Store(0)
	qpPerf.filterNextNs.Store(0)
	qpPerf.filterEvalNs.Store(0)
	qpPerf.mergeDispatched.Store(0)
}

func snapshotPerfCounters() PerfCounters {
	return PerfCounters{
		IndexNextCalls: qpPerf.indexNextCalls.Load(),
		IndexYields:    qpPerf.indexYields.Load(),
		IndexNextNs:    qpPerf.indexNextNs.Load(),

		FetchNextCalls: qpPerf.fetchNextCalls.Load(),
		FetchYields:    qpPerf.fetchYields.Load(),
		FetchNextNs:    qpPerf.fetchNextNs.Load(),
		FetchLookupNs:  qpPerf.fetchLookupNs.Load(),
		FetchParseNs:   qpPerf.fetchParseNs.Load(),

		FilterNextCalls: qpPerf.filterNextCalls.Load(),
		FilterYields:    qpPerf.filterYields.Load(),
		FilterNextNs:    qpPerf.filterNextNs.Load(),
		FilterEvalNs:    qpPerf.filterEvalNs.Load(),

		MergeDispatched: qpPerf.mergeDispatched.Load(),
	}
}
