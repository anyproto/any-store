package qplanner

// ResetPerfCounters clears planner iterator profiling counters.
func ResetPerfCounters() {
	resetPerfCounters()
}

// EnablePerfCounters toggles planner iterator profiling counters.
func EnablePerfCounters(enabled bool) {
	setPerfCountersEnabled(enabled)
}

// SnapshotPerfCounters returns current planner iterator profiling counters.
func SnapshotPerfCounters() PerfCounters {
	return snapshotPerfCounters()
}
