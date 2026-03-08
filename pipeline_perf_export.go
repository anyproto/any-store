package anystore

// ResetPipelinePerfCounters clears internal iterator/doc profiling counters.
func ResetPipelinePerfCounters() {
	resetPipelinePerfCounters()
}

// EnablePipelinePerfCounters toggles pipeline profiling counters.
func EnablePipelinePerfCounters(enabled bool) {
	setPipelinePerfEnabled(enabled)
}

// SnapshotPipelinePerfCounters returns current profiling counters.
func SnapshotPipelinePerfCounters() PipelinePerfCounters {
	return snapshotPipelinePerfCounters()
}
