package vivf

import (
	"math/rand"
	"testing"
)

// These micro-benchmarks isolate the ADC *scoring kernel* — the per-candidate LUT
// gather that profiling found to be the IVF-PQ search bottleneck (a scattered read
// of a 96 KB, L2-resident table for M=96) — to decide whether PQ fast-scan is worth
// a full implementation (4-bit codes + transposed layout + an AVX2 pshufb kernel,
// i.e. hand-written assembly in Go).
//
// They compare, on N synthetic candidates at dim 768:
//   - adc8  : the current 8-bit kernel (M=96 sub-quantizers, 256-entry LUTs → 96 KB)
//   - adc4  : a scalar 4-bit kernel    (M'=192, 16-entry LUTs → 12 KB, fits L1)
//
// adc4 has 2× the lookups but from L1; adc8 has half the lookups but from L2. The
// real fast-scan win is the SIMD shuffle (32 candidates per pshufb), which needs
// assembly — but if even the scalar 4-bit kernel is competitive, the L1-resident
// LUT alone helps and the SIMD version would be a clear win.

const (
	fsN    = 8192 // candidates scanned per query (≈ a probed-cell sweep)
	fsM8   = 96   // 8-bit sub-quantizers (dim 768, dsub 8)
	fsM4   = 192  // 4-bit sub-quantizers (dim 768, dsub 4)
	fsPQK8 = 256
	fsPQK4 = 16
)

func fsMake8() (lut []float32, codes []byte) {
	rng := rand.New(rand.NewSource(1))
	lut = make([]float32, fsM8*fsPQK8)
	for i := range lut {
		lut[i] = rng.Float32()
	}
	codes = make([]byte, fsN*fsM8) // one M-byte code per candidate
	for i := range codes {
		codes[i] = byte(rng.Intn(256))
	}
	return
}

func fsMake4() (lut []float32, codes []byte) {
	rng := rand.New(rand.NewSource(2))
	lut = make([]float32, fsM4*fsPQK4)
	for i := range lut {
		lut[i] = rng.Float32()
	}
	codes = make([]byte, fsN*fsM4/2) // two 4-bit codes packed per byte
	for i := range codes {
		codes[i] = byte(rng.Intn(256))
	}
	return
}

func BenchmarkADC8(b *testing.B) {
	lut, codes := fsMake8()
	b.ReportAllocs()
	b.ResetTimer()
	var sink float32
	for i := 0; i < b.N; i++ {
		var acc float32
		for c := 0; c < fsN; c++ {
			code := codes[c*fsM8 : c*fsM8+fsM8]
			var s float32
			for m := 0; m < fsM8; m++ {
				s += lut[m*fsPQK8+int(code[m])]
			}
			acc += s
		}
		sink += acc
	}
	_ = sink
}

func BenchmarkADC4(b *testing.B) {
	lut, codes := fsMake4()
	b.ReportAllocs()
	b.ResetTimer()
	var sink float32
	for i := 0; i < b.N; i++ {
		var acc float32
		const cbytes = fsM4 / 2
		for c := 0; c < fsN; c++ {
			code := codes[c*cbytes : c*cbytes+cbytes]
			var s float32
			for mb := 0; mb < cbytes; mb++ {
				bb := code[mb]
				s += lut[(2*mb)*fsPQK4+int(bb&0x0f)]
				s += lut[(2*mb+1)*fsPQK4+int(bb>>4)]
			}
			acc += s
		}
		sink += acc
	}
	_ = sink
}
