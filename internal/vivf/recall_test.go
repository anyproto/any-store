package vivf

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/viterin/vek/vek32"
)

// readF32 / readI32 mirror the ASV_VBENCH export format used by
// internal/vindex/bulk_real_test.go: LE uint32 n, uint32 width, then row-major data.
func readF32(path string) ([][]float32, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	n := int(binary.LittleEndian.Uint32(b[0:]))
	d := int(binary.LittleEndian.Uint32(b[4:]))
	out := make([][]float32, n)
	off := 8
	for i := 0; i < n; i++ {
		row := make([]float32, d)
		for j := 0; j < d; j++ {
			row[j] = math.Float32frombits(binary.LittleEndian.Uint32(b[off:]))
			off += 4
		}
		out[i] = row
	}
	return out, nil
}

func readI32(path string) ([][]int32, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	n := int(binary.LittleEndian.Uint32(b[0:]))
	w := int(binary.LittleEndian.Uint32(b[4:]))
	out := make([][]int32, n)
	off := 8
	for i := 0; i < n; i++ {
		row := make([]int32, w)
		for j := 0; j < w; j++ {
			row[j] = int32(binary.LittleEndian.Uint32(b[off:]))
			off += 4
		}
		out[i] = row
	}
	return out, nil
}

func vbenchDir() string {
	if d := os.Getenv("ASV_VBENCH"); d != "" {
		return d
	}
	return "/tmp/vbench"
}

// recallVsGT is the fraction of the true top-k (gtRow) recovered in got.
func recallVsGT(got []int, gtRow []int32, k int) float64 {
	truth := make(map[int]bool, k)
	for i := 0; i < k && i < len(gtRow); i++ {
		truth[int(gtRow[i])] = true
	}
	hit := 0
	for _, g := range got {
		if truth[g] {
			hit++
		}
	}
	return float64(hit) / float64(k)
}

// TestVivfRecallReal validates IVF-PQ + re-rank recall against the SAME real
// dataset + cosine ground truth that the current HNSW index scores 0.970 on
// (see /tmp/vbench/results_anystore.csv). Skipped unless that export is present.
func TestVivfRecallReal(t *testing.T) {
	if testing.Short() {
		t.Skip("recall diagnostic")
	}
	dir := vbenchDir()
	base, err := readF32(filepath.Join(dir, "base.f32"))
	if err != nil {
		t.Skipf("no ASV_VBENCH export at %s: %v", dir, err)
	}
	queries, err := readF32(filepath.Join(dir, "query.f32"))
	if err != nil {
		t.Skipf("no query.f32: %v", err)
	}
	gt, err := readI32(filepath.Join(dir, "gt.i32"))
	if err != nil {
		t.Skipf("no gt.i32: %v", err)
	}
	// Queries are themselves base rows; qidx[i] is query i's own base index, which
	// every engine excludes from its results before scoring (see bench.py).
	qidx, err := readI32(filepath.Join(dir, "qidx.i32"))
	if err != nil {
		t.Skipf("no qidx.i32: %v", err)
	}
	// qidx is a single row of n self-indices (stored n=1,w=nq; python reshapes -1).
	self := func(i int) int { return int(qidx[0][i]) }
	const k = 10
	dim := len(base[0])
	t.Logf("dataset: %d base × %dd, %d queries, gt width %d", len(base), dim, len(queries), len(gt[0]))

	// Sanity: a brute-force cosine top-k must reproduce the ground truth. If this
	// isn't ~1.0, our normalization/metric disagrees with how gt was built and the
	// IVF-PQ numbers below would be meaningless.
	{
		var sum float64
		qn := make([][]float32, len(queries))
		bn := make([][]float32, len(base))
		for i, v := range base {
			bn[i] = normalize(v)
		}
		for i, q := range queries {
			qn[i] = normalize(q)
		}
		for i, q := range qn {
			got := exclude(bruteTopK(bn, q, k+1), self(i), k)
			sum += recallVsGT(got, gt[i], k)
		}
		bf := sum / float64(len(queries))
		t.Logf("sanity brute-force cosine recall@%d vs gt = %.4f (want ~1.0)", k, bf)
		// ~0.99, not exactly 1.0: gt was built in float64 numpy; our float32 L2
		// reorders genuine near-ties. This precision ceiling caps IVF-PQ equally.
		if bf < 0.98 {
			t.Fatalf("brute-force recall %.4f != ~1.0: metric/normalization mismatch with gt", bf)
		}
	}

	// Isolate the recall-knee levers at a fixed code budget (M=96, nlist=256,
	// kFactor=10): random vs k-means++ init, and closure factor (multi-assignment).
	// Goal: reach HNSW parity (~0.97) at LOW nprobe.
	const (
		m       = 96
		nlist   = 256
		kFactor = 10
	)
	type cfg struct {
		name   string
		kpp    bool
		assign int
	}
	cfgs := []cfg{
		{"random init, assign=1", false, 1},
		{"kmeans++, assign=1", true, 1},
		{"kmeans++, assign=2", true, 2},
		{"kmeans++, assign=4", true, 4},
	}
	nprobes := []int{4, 8, 16, 32, 64}

	var best float64
	var bestDesc string
	for _, c := range cfgs {
		t0 := time.Now()
		ix := Train(base, Params{Dim: dim, NList: nlist, M: m, Seed: 42, KMeansPP: c.kpp, Assign: c.assign})
		buildMs := time.Since(t0).Milliseconds()
		t.Logf("--- %s (code=%dB, build=%dms, %.1f entries/vec) ---", c.name, m, buildMs, ix.avgReplication())
		for _, np := range nprobes {
			var sum float64
			for i, q := range queries {
				got := exclude(ix.Search(q, k+1, np, kFactor), self(i), k)
				sum += recallVsGT(got, gt[i], k)
			}
			r := sum / float64(len(queries))
			t.Logf("  nprobe=%-3d recall@%d = %.4f", np, k, r)
			if r > best {
				best = r
				bestDesc = fmt.Sprintf("%s nprobe=%d", c.name, np)
			}
		}
	}
	t.Logf("BEST IVF-PQ recall@%d = %.4f (%s) — HNSW baseline on this set = 0.970", k, best, bestDesc)

	// Gate: IVF-PQ + re-rank must reach HNSW-class recall (~0.97) on real
	// embeddings, or the btree storage work in later phases isn't worth pursuing.
	const target = 0.96
	if best < target {
		t.Fatalf("best IVF-PQ recall@%d = %.4f < target %.2f", k, best, target)
	}
}

// exclude drops self from got and truncates to k (engines never return the query
// vector itself as its own neighbour).
func exclude(got []int, self, k int) []int {
	out := make([]int, 0, k)
	for _, g := range got {
		if g == self {
			continue
		}
		out = append(out, g)
		if len(out) == k {
			break
		}
	}
	return out
}

// bruteTopK returns the k nearest labels (cosine == L2 on unit vectors) of q.
func bruteTopK(vecs [][]float32, q []float32, k int) []int {
	type p struct {
		i int
		d float32
	}
	all := make([]p, len(vecs))
	for i := range vecs {
		all[i] = p{i, vek32.Distance(q, vecs[i])}
	}
	partialSortByDist(all, func(x p) float32 { return x.d }, k)
	out := make([]int, 0, k)
	for i := 0; i < k && i < len(all); i++ {
		out = append(out, all[i].i)
	}
	return out
}
