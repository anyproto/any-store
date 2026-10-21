//go:build !noasm && arm64

package simd

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/anyproto/any-store/v2/internal/simd/asm"
)

// floatByteLengths covers every length 0..70 (each shape of the kernel's 32-,
// 16-, 4- and 1-element stages) plus the block edges around embedding dims.
func floatByteLengths() []int {
	var ns []int
	for n := 0; n <= 70; n++ {
		ns = append(ns, n)
	}
	return append(ns, 127, 128, 129, 383, 384, 385, 767, 768, 769, 1536, 3072)
}

// floatByteTol bounds the float32 re-association error of any summation order
// used here (8×4 or 4 lane chains, then a reduction): ~chain·ε·Σ|qᵢ·uᵢ|, plus
// an absolute floor for subnormal products.
func floatByteTol(q []float32, u []byte) float64 {
	var abs float64
	for i := range q {
		abs += math.Abs(float64(q[i]) * float64(u[i]))
	}
	return 1e-5*abs + float64(len(q)+1)*0x1p-149
}

// floatByteCases fill q and u with the value shapes the kernel must survive:
// both ends of the byte range, large, tiny and subnormal floats, negatives.
var floatByteCases = []struct {
	name string
	q    func(r *rand.Rand) float32
	u    func(r *rand.Rand) byte
}{
	{"uniform", func(r *rand.Rand) float32 { return r.Float32()*2 - 1 }, func(r *rand.Rand) byte { return byte(r.Intn(256)) }},
	{"bytes0", func(r *rand.Rand) float32 { return r.Float32()*2 - 1 }, func(*rand.Rand) byte { return 0 }},
	{"bytes255", func(r *rand.Rand) float32 { return r.Float32()*2 - 1 }, func(*rand.Rand) byte { return 255 }},
	{"bytes0or255", func(r *rand.Rand) float32 { return r.Float32()*2 - 1 }, func(r *rand.Rand) byte { return byte(r.Intn(2) * 255) }},
	{"negative", func(r *rand.Rand) float32 { return -r.Float32() * 100 }, func(r *rand.Rand) byte { return byte(128 + r.Intn(128)) }},
	{"large", func(r *rand.Rand) float32 { return (r.Float32()*2 - 1) * 1e30 }, func(r *rand.Rand) byte { return byte(r.Intn(256)) }},
	{"small", func(r *rand.Rand) float32 { return (r.Float32()*2 - 1) * 1e-30 }, func(r *rand.Rand) byte { return byte(r.Intn(256)) }},
	{"subnormal", func(r *rand.Rand) float32 { return (r.Float32()*2 - 1) * 1e-40 }, func(r *rand.Rand) byte { return byte(r.Intn(256)) }},
	{"mixed-magnitude", func(r *rand.Rand) float32 {
		v := float32(math.Ldexp(1+r.Float64(), r.Intn(61)-30))
		if r.Intn(2) == 0 {
			v = -v
		}
		return v
	}, func(r *rand.Rand) byte { return byte(r.Intn(256)) }},
}

// TestDotFloatByteNEONMatchesFallback checks the NEON kernel against the
// float64 reference and the pure-Go fallback for every length, value shape and
// slice start offset 0..15 (q and u misaligned independently).
func TestDotFloatByteNEONMatchesFallback(t *testing.T) {
	r := rand.New(rand.NewSource(7590))
	for _, c := range floatByteCases {
		for _, n := range floatByteLengths() {
			qb := make([]float32, n+16)
			ub := make([]byte, n+16)
			for i := range qb {
				qb[i] = c.q(r)
			}
			for i := range ub {
				ub[i] = c.u(r)
			}
			for off := 0; off < 16; off++ {
				q, u := qb[off:off+n], ub[(off*7)%16:(off*7)%16+n]
				got := float64(dotFloatByteNEON(q, u))
				want := refDotFloatByte(q, u)
				fb := float64(dotFloatByteScalar(q, u))
				tol := floatByteTol(q, u)
				if !(math.Abs(got-want) <= tol && math.Abs(got-fb) <= 2*tol) { // NaN fails
					t.Fatalf("%s n=%d off=%d: neon=%v ref=%v fallback=%v (tol %v)",
						c.name, n, off, got, want, fb, tol)
				}
			}
		}
	}
}

// TestDotFloatByteNEONExact uses small integer floats, where every partial sum
// is exact in float32 whatever the order, so any lane-mapping, tail or unsigned
// widening slip shows up as an inequality rather than hiding in a tolerance.
func TestDotFloatByteNEONExact(t *testing.T) {
	r := rand.New(rand.NewSource(7591))
	for _, n := range floatByteLengths() {
		qb := make([]float32, n+16)
		ub := make([]byte, n+16)
		for off := 0; off < 16; off++ {
			for i := range qb {
				qb[i] = float32(r.Intn(17) - 8) // |Σ| ≤ 3072·8·255 < 2²⁴
			}
			for i := range ub {
				ub[i] = byte(r.Intn(256))
			}
			q, u := qb[off:off+n], ub[15-off:15-off+n]
			var want int64
			for i := range q {
				want += int64(q[i]) * int64(u[i])
			}
			if got := dotFloatByteNEON(q, u); got != float32(want) {
				t.Fatalf("n=%d off=%d: got %v want %d", n, off, got, want)
			}
		}
	}
}

// TestDotFloatByteNEONLengthContract pins the API contract shared with the
// fallback: the length is len(q), extra bytes in u are ignored, and a u
// shorter than q panics instead of reading past it.
func TestDotFloatByteNEONLengthContract(t *testing.T) {
	r := rand.New(rand.NewSource(7592))
	for _, n := range []int{0, 3, 16, 33, 768} {
		q := randVec(r, n)
		u := make([]byte, n+5)
		for i := range u {
			u[i] = byte(r.Intn(256))
		}
		if got, want := dotFloatByteNEON(q, u), dotFloatByteNEON(q, u[:n]); got != want {
			t.Errorf("n=%d: extra bytes changed the result: %v vs %v", n, got, want)
		}
		if n == 0 {
			continue
		}
		for _, k := range []struct {
			name string
			fn   func([]float32, []byte) float32
		}{{"neon", dotFloatByteNEON}, {"fallback", dotFloatByteScalar}} {
			func() {
				defer func() {
					if recover() == nil {
						t.Errorf("%s n=%d: short u did not panic", k.name, n)
					}
				}()
				k.fn(q, u[:n-1])
			}()
		}
	}
}

// TestDotFloatByteNEONNonFinite checks Inf and NaN propagate like the fallback.
func TestDotFloatByteNEONNonFinite(t *testing.T) {
	for _, n := range []int{1, 5, 17, 40, 768} {
		for _, at := range []int{0, n / 2, n - 1} {
			q := make([]float32, n)
			u := make([]byte, n)
			for i := range q {
				q[i], u[i] = 0.5, 200
			}
			q[at] = float32(math.Inf(1))
			if got := dotFloatByteNEON(q, u); !math.IsInf(float64(got), 1) {
				t.Errorf("n=%d at=%d: +Inf gave %v", n, at, got)
			}
			q[at] = float32(math.NaN())
			if got := dotFloatByteNEON(q, u); !math.IsNaN(float64(got)) {
				t.Errorf("n=%d at=%d: NaN gave %v", n, at, got)
			}
		}
	}
}

var floatByteSink float32

// BenchmarkDotFloatByte compares the any-store NEON float×byte kernel with the
// vendored one it replaces and with the f32 NEON Dot, hot cache, direct calls.
func BenchmarkDotFloatByte(b *testing.B) {
	for _, dim := range []int{128, 384, 768, 1536} {
		r := rand.New(rand.NewSource(int64(dim)))
		q, v := randVec(r, dim), randVec(r, dim)
		u := make([]byte, dim)
		for i := range u {
			u[i] = byte(r.Intn(256))
		}
		b.Run(fmt.Sprintf("dim=%d/neon", dim), func(b *testing.B) {
			var sink float32
			for i := 0; i < b.N; i++ {
				sink += dotFloatByteNEON(q, u)
			}
			floatByteSink = sink
		})
		b.Run(fmt.Sprintf("dim=%d/vendored", dim), func(b *testing.B) {
			var sink float32
			for i := 0; i < b.N; i++ {
				sink += asm.DotFloatByte_Neon(q, u)
			}
			floatByteSink = sink
		})
		b.Run(fmt.Sprintf("dim=%d/f32-dot", dim), func(b *testing.B) {
			var sink float32
			for i := 0; i < b.N; i++ {
				sink += asm.Dot_Neon(q, v)
			}
			floatByteSink = sink
		})
	}
}
