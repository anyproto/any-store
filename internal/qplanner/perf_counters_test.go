package qplanner

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPerfCounters_ControlAndSnapshot(t *testing.T) {
	setPerfCountersEnabled(true)
	assert.True(t, perfCountersEnabled())

	qpPerf.indexNextCalls.Store(11)
	qpPerf.indexYields.Store(7)
	qpPerf.indexNextNs.Store(101)
	qpPerf.fetchNextCalls.Store(13)
	qpPerf.fetchYields.Store(9)
	qpPerf.fetchNextNs.Store(202)
	qpPerf.fetchLookupNs.Store(303)
	qpPerf.fetchParseNs.Store(404)
	qpPerf.filterNextCalls.Store(17)
	qpPerf.filterYields.Store(10)
	qpPerf.filterNextNs.Store(505)
	qpPerf.filterEvalNs.Store(606)

	s := snapshotPerfCounters()
	assert.Equal(t, uint64(11), s.IndexNextCalls)
	assert.Equal(t, uint64(7), s.IndexYields)
	assert.Equal(t, uint64(101), s.IndexNextNs)
	assert.Equal(t, uint64(13), s.FetchNextCalls)
	assert.Equal(t, uint64(9), s.FetchYields)
	assert.Equal(t, uint64(202), s.FetchNextNs)
	assert.Equal(t, uint64(303), s.FetchLookupNs)
	assert.Equal(t, uint64(404), s.FetchParseNs)
	assert.Equal(t, uint64(17), s.FilterNextCalls)
	assert.Equal(t, uint64(10), s.FilterYields)
	assert.Equal(t, uint64(505), s.FilterNextNs)
	assert.Equal(t, uint64(606), s.FilterEvalNs)

	resetPerfCounters()
	s = snapshotPerfCounters()
	assert.Equal(t, PerfCounters{}, s)

	setPerfCountersEnabled(false)
	assert.False(t, perfCountersEnabled())
}

func TestPerfCounters_Exports(t *testing.T) {
	EnablePerfCounters(true)
	assert.True(t, perfCountersEnabled())

	qpPerf.fetchNextCalls.Store(42)
	assert.Equal(t, uint64(42), SnapshotPerfCounters().FetchNextCalls)

	ResetPerfCounters()
	assert.Equal(t, PerfCounters{}, SnapshotPerfCounters())
}
