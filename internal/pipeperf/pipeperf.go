// Package pipeperf holds the query pipeline's profiling counters: how an
// iterator's Doc calls found their documents (already parsed by the plan, or
// read again from the collection), with the planner's own counters
// (qplanner.PerfCounters). They are off by default, when they cost an atomic
// load per Doc call. The tests of the module turn them on to see what a plan
// did; they are not part of the public API.
package pipeperf

import (
	"sync/atomic"
	"time"

	"github.com/anyproto/any-store/v2/internal/qplanner"
)

// Counters is a snapshot of the pipeline's counters.
type Counters struct {
	Planner qplanner.PerfCounters

	DocCalls           uint64
	DocParsedHits      uint64
	DocFallbacks       uint64
	DocFallbackSeekNs  uint64
	DocFallbackParseNs uint64
}

var enabled atomic.Bool

// The Doc counters, added to only while Enabled reports true.
var (
	DocCalls           atomic.Uint64
	DocParsedHits      atomic.Uint64
	DocFallbacks       atomic.Uint64
	DocFallbackSeekNs  atomic.Uint64
	DocFallbackParseNs atomic.Uint64
)

// Enabled reports whether the counters count.
func Enabled() bool {
	return enabled.Load()
}

// Enable turns the counters, the planner's included, on or off.
func Enable(on bool) {
	enabled.Store(on)
	qplanner.EnablePerfCounters(on)
}

// Reset sets every counter, the planner's included, to zero.
func Reset() {
	DocCalls.Store(0)
	DocParsedHits.Store(0)
	DocFallbacks.Store(0)
	DocFallbackSeekNs.Store(0)
	DocFallbackParseNs.Store(0)
	qplanner.ResetPerfCounters()
}

// Snapshot returns the counters' current values.
func Snapshot() Counters {
	return Counters{
		Planner:            qplanner.SnapshotPerfCounters(),
		DocCalls:           DocCalls.Load(),
		DocParsedHits:      DocParsedHits.Load(),
		DocFallbacks:       DocFallbacks.Load(),
		DocFallbackSeekNs:  DocFallbackSeekNs.Load(),
		DocFallbackParseNs: DocFallbackParseNs.Load(),
	}
}

// SinceNs returns the nanoseconds since start.
func SinceNs(start time.Time) uint64 {
	return uint64(time.Since(start).Nanoseconds())
}
