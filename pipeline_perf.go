package anystore

import (
	"sync/atomic"
	"time"

	"github.com/anyproto/any-store/v2/internal/qplanner"
)

// PipelinePerfCounters aggregates iterator and Doc() fallback profiling counters.
type PipelinePerfCounters struct {
	Planner qplanner.PerfCounters

	DocCalls          uint64
	DocParsedHits     uint64
	DocFallbacks      uint64
	DocFallbackSeekNs uint64
	DocFallbackParseNs uint64
}

var pipePerf struct {
	enabled            atomic.Bool
	docCalls           atomic.Uint64
	docParsedHits      atomic.Uint64
	docFallbacks       atomic.Uint64
	docFallbackSeekNs  atomic.Uint64
	docFallbackParseNs atomic.Uint64
}

func resetPipelinePerfCounters() {
	pipePerf.docCalls.Store(0)
	pipePerf.docParsedHits.Store(0)
	pipePerf.docFallbacks.Store(0)
	pipePerf.docFallbackSeekNs.Store(0)
	pipePerf.docFallbackParseNs.Store(0)
	qplanner.ResetPerfCounters()
}

func setPipelinePerfEnabled(enabled bool) {
	pipePerf.enabled.Store(enabled)
	qplanner.EnablePerfCounters(enabled)
}

func pipelinePerfEnabled() bool {
	return pipePerf.enabled.Load()
}

func snapshotPipelinePerfCounters() PipelinePerfCounters {
	return PipelinePerfCounters{
		Planner:            qplanner.SnapshotPerfCounters(),
		DocCalls:           pipePerf.docCalls.Load(),
		DocParsedHits:      pipePerf.docParsedHits.Load(),
		DocFallbacks:       pipePerf.docFallbacks.Load(),
		DocFallbackSeekNs:  pipePerf.docFallbackSeekNs.Load(),
		DocFallbackParseNs: pipePerf.docFallbackParseNs.Load(),
	}
}

func perfSinceNs(start time.Time) uint64 {
	return uint64(time.Since(start).Nanoseconds())
}
