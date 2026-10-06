package vivf

import (
	"encoding/binary"
	"math"
	"os"
	"slices"

	"github.com/anyproto/any-store/v2/internal/simd"
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

// bruteTopK returns the k nearest labels of q by exact distance.
func bruteTopK(vecs [][]float32, q []float32, k int) []int {
	type p struct {
		i int
		d float32
	}
	all := make([]p, len(vecs))
	for i := range vecs {
		all[i] = p{i, simd.Distance(q, vecs[i])}
	}
	slices.SortFunc(all, func(a, b p) int { return cmpF32(a.d, b.d) })
	out := make([]int, 0, k)
	for i := 0; i < k && i < len(all); i++ {
		out = append(out, all[i].i)
	}
	return out
}
