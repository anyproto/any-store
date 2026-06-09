package vector

import (
	"testing"
	"time"
)

// TestTombstoneLatencyReport quantifies the research finding that tombstoned
// nodes are still traversed during search, so query latency and recall drift as
// the deleted fraction grows — until Compact removes them from navigation.
func TestTombstoneLatencyReport(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	const (
		n   = 20000
		dim = 64 // representative embedding-like regime; uniform-random 128d is a pathological HNSW case
		k   = 10
	)
	vecs, keys := randVectors(n, dim, 2024)
	queries, _ := randVectors(2000, dim, 7)

	g := NewFlatHNSW(dim, L2, 1)
	g.EfSearch = 64
	for i := range vecs {
		g.Add(keys[i], vecs[i])
	}

	timeSearch := func() (nsPerOp float64) {
		// warmup
		for i := 0; i < 200; i++ {
			g.Search(queries[i%len(queries)], k)
		}
		start := time.Now()
		const iters = 4000
		for i := 0; i < iters; i++ {
			g.Search(queries[i%len(queries)], k)
		}
		return float64(time.Since(start).Nanoseconds()) / iters
	}
	recallVs := func(live map[uint64]bool) float64 {
		brute := NewBrute(dim, L2)
		for i := range vecs {
			if live == nil || live[keys[i]] {
				brute.Add(keys[i], vecs[i])
			}
		}
		var r float64
		qn := 200
		for i := 0; i < qn; i++ {
			r += recallAt(g.Search(queries[i], k), brute.Search(queries[i], k))
		}
		return r / float64(qn)
	}

	t.Logf("%-26s %10s %10s", "state", "search ns/op", "recall@10")
	t.Logf("%-26s %10.0f %10.3f", "0% deleted", timeSearch(), recallVs(nil))

	live := make(map[uint64]bool, n)
	for _, kk := range keys {
		live[kk] = true
	}
	deleted := 0
	for _, frac := range []float64{0.10, 0.20, 0.30, 0.50} {
		target := int(frac * n)
		for ; deleted < target; deleted++ {
			g.Delete(keys[deleted])
			delete(live, keys[deleted])
		}
		t.Logf("%-26s %10.0f %10.3f", fmtPct(frac)+" tombstoned", timeSearch(), recallVs(live))
	}

	// compact and re-measure
	ct := time.Now()
	g.Compact()
	compactMs := float64(time.Since(ct).Microseconds()) / 1000
	t.Logf("%-26s %10.0f %10.3f   (compact took %.1f ms)", "after compact", timeSearch(), recallVs(live), compactMs)
}

// TestHardDeleteCost contrasts the O(1) tombstone with the O(N) full-arena scan
// a hard delete needs (no reverse index), explaining why production HNSW defers.
func TestHardDeleteCost(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	const (
		n   = 20000
		dim = 128
	)
	build := func() (*FlatHNSW, []uint64) {
		vecs, keys := randVectors(n, dim, 5)
		g := NewFlatHNSW(dim, L2, 1)
		for i := range vecs {
			g.Add(keys[i], vecs[i])
		}
		return g, keys
	}

	g1, keys := build()
	const ops = 1000
	start := time.Now()
	for i := 0; i < ops; i++ {
		g1.Delete(keys[i])
	}
	softNs := float64(time.Since(start).Nanoseconds()) / ops

	g2, keys2 := build()
	start = time.Now()
	for i := 0; i < ops; i++ {
		g2.DeleteHardRepair(keys2[i], true)
	}
	hardNs := float64(time.Since(start).Nanoseconds()) / ops

	t.Logf("tombstone Delete:        %10.0f ns/op", softNs)
	t.Logf("DeleteHardRepair (scan): %10.0f ns/op  (%.0fx slower; scales with index size)", hardNs, hardNs/softNs)
}

func fmtPct(f float64) string {
	switch f {
	case 0.10:
		return "10%"
	case 0.20:
		return "20%"
	case 0.30:
		return "30%"
	case 0.50:
		return "50%"
	}
	return ""
}
