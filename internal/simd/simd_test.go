package simd

import (
	"math"
	"math/rand"
	"testing"
)

// naive references — trivially correct, used to validate the active kernels.
func refDot(a, b []float32) float64 {
	var s float64
	for i := range a {
		s += float64(a[i]) * float64(b[i])
	}
	return s
}

func refL2sq(a, b []float32) float64 {
	var s float64
	for i := range a {
		d := float64(a[i]) - float64(b[i])
		s += d * d
	}
	return s
}

func randVec(r *rand.Rand, n int) []float32 {
	v := make([]float32, n)
	for i := range v {
		v[i] = r.Float32()*2 - 1
	}
	return v
}

// closeTo allows for float32 accumulation error, scaled by magnitude.
func closeTo(got, want float64) bool {
	tol := 1e-3 * (1 + math.Abs(want))
	return math.Abs(got-want) <= tol
}

func TestKernelsMatchReference(t *testing.T) {
	t.Logf("Accelerated()=%v", Accelerated())
	r := rand.New(rand.NewSource(1))
	// cover SIMD block boundaries, tails, odd and short lengths
	lengths := []int{1, 2, 3, 4, 5, 7, 8, 13, 15, 16, 17, 31, 32, 33, 63, 64, 96, 100, 127, 128, 256, 768, 1000}
	for _, n := range lengths {
		for trial := 0; trial < 8; trial++ {
			a, b := randVec(r, n), randVec(r, n)

			if got, want := float64(Dot(a, b)), refDot(a, b); !closeTo(got, want) {
				t.Errorf("Dot n=%d: got %v want %v", n, got, want)
			}
			if got, want := float64(L2Squared(a, b)), refL2sq(a, b); !closeTo(got, want) {
				t.Errorf("L2Squared n=%d: got %v want %v", n, got, want)
			}
			if got, want := float64(Distance(a, b)), math.Sqrt(refL2sq(a, b)); !closeTo(got, want) {
				t.Errorf("Distance n=%d: got %v want %v", n, got, want)
			}
			if got, want := float64(Norm(a)), math.Sqrt(refDot(a, a)); !closeTo(got, want) {
				t.Errorf("Norm n=%d: got %v want %v", n, got, want)
			}
			da, db := refDot(a, a), refDot(b, b)
			wantCos := refDot(a, b) / (math.Sqrt(da) * math.Sqrt(db))
			if got := float64(CosineSimilarity(a, b)); !closeTo(got, wantCos) {
				t.Errorf("CosineSimilarity n=%d: got %v want %v", n, got, wantCos)
			}
		}
	}
}

// TestFallbackMatchesReference validates the pure-Go kernels directly (the path
// taken on wasm / 386 / no-AVX2 x86), independent of whatever the host selected.
func TestFallbackMatchesReference(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	for _, n := range []int{1, 3, 7, 8, 15, 16, 33, 768} {
		a, b := randVec(r, n), randVec(r, n)
		if got, want := float64(dotScalar(a, b)), refDot(a, b); !closeTo(got, want) {
			t.Errorf("dotScalar n=%d: got %v want %v", n, got, want)
		}
		if got, want := float64(l2Scalar(a, b)), refL2sq(a, b); !closeTo(got, want) {
			t.Errorf("l2Scalar n=%d: got %v want %v", n, got, want)
		}
	}
}

func refDotFloatByte(q []float32, u []byte) float64 {
	var s float64
	for i := range q {
		s += float64(q[i]) * float64(u[i]) // unsigned byte
	}
	return s
}

func TestDotFloatByteMatchesReference(t *testing.T) {
	t.Logf("AcceleratedFloatByte()=%v", AcceleratedFloatByte())
	r := rand.New(rand.NewSource(11))
	lengths := []int{1, 2, 3, 4, 5, 7, 8, 12, 13, 16, 17, 31, 32, 33, 64, 100, 768, 1000}
	for _, n := range lengths {
		for trial := 0; trial < 8; trial++ {
			q := randVec(r, n)
			u := make([]byte, n)
			for i := range u {
				u[i] = byte(r.Intn(256)) // full unsigned range incl. >127
			}
			got := float64(DotFloatByte(q, u))
			want := refDotFloatByte(q, u)
			if !closeTo(got, want) {
				t.Errorf("DotFloatByte n=%d: got %v want %v", n, got, want)
			}
			// also validate the pure-Go fallback directly (wasm/no-SIMD path)
			if gf := float64(dotFloatByteScalar(q, u)); !closeTo(gf, want) {
				t.Errorf("dotFloatByteScalar n=%d: got %v want %v", n, gf, want)
			}
		}
	}
}

// kernelEntryPoints are the exported functions that reach a dispatched kernel,
// each reduced to a float32 result.
var kernelEntryPoints = []struct {
	name string
	f    func(v []float32, u []byte) float32
}{
	{"Dot", func(v []float32, _ []byte) float32 { return Dot(v, v) }},
	{"L2Squared", func(v []float32, _ []byte) float32 { return L2Squared(v, v) }},
	{"Distance", func(v []float32, _ []byte) float32 { return Distance(v, v) }},
	{"Norm", func(v []float32, _ []byte) float32 { return Norm(v) }},
	{"CosineSimilarity", func(v []float32, _ []byte) float32 { return CosineSimilarity(v, v) }},
	{"NormalizeInto", func(v []float32, _ []byte) float32 { return float32(len(NormalizeInto(nil, v))) }},
	{"DotFloatByte", func(v []float32, u []byte) float32 { return DotFloatByte(v, u) }},
}

// useKernels runs f with the given kernels dispatched, as if init had selected
// them, and restores the previous ones afterwards.
func useKernels(dot, l2 func(a, b []float32) float32, dotFB func([]float32, []byte) float32, f func()) {
	d, l, fb := dotImpl, l2Impl, dotFBImpl
	defer func() { dotImpl, l2Impl, dotFBImpl = d, l, fb }()
	dotImpl, l2Impl, dotFBImpl = dot, l2, dotFB
	f()
}

// checkEmptyVectors checks that every kernel entry point gives the pure-Go
// fallback's result for nil and empty vectors with the currently dispatched
// kernels.
func checkEmptyVectors(t *testing.T) {
	t.Helper()
	for _, in := range []struct {
		name string
		v    []float32
		u    []byte
	}{
		{"nil", nil, nil},
		{"empty", []float32{}, []byte{}},
	} {
		for _, e := range kernelEntryPoints {
			got := e.f(in.v, in.u)
			var want float32
			useKernels(dotScalar, l2Scalar, dotFloatByteScalar, func() { want = e.f(in.v, in.u) })
			if got != want {
				t.Errorf("%s on %s vectors: got %v, fallback gives %v", e.name, in.name, got, want)
			}
		}
	}
}

func TestEmptyVectors(t *testing.T) {
	t.Logf("Accelerated()=%v AcceleratedFloatByte()=%v", Accelerated(), AcceleratedFloatByte())
	checkEmptyVectors(t)
}

// TestInt8OffsetBinaryDot proves the offset-binary identity used by vindex:
// signed dot q·s == scale·(DotFloatByte(q,u) − 128·Σq) where u = s+128.
func TestInt8OffsetBinaryDot(t *testing.T) {
	r := rand.New(rand.NewSource(12))
	n := 768
	const scale = float32(0.0123)
	q := randVec(r, n)
	s8 := make([]int8, n)
	u := make([]byte, n)
	for i := range s8 {
		s8[i] = int8(r.Intn(255) - 127) // [-127,127]
		u[i] = byte(int(s8[i]) + 128)   // offset-binary
	}
	var qsum float64
	var wantSigned float64
	for i := range q {
		qsum += float64(q[i])
		wantSigned += float64(q[i]) * float64(s8[i]) * float64(scale)
	}
	gotSigned := float64(scale) * (float64(DotFloatByte(q, u)) - 128*qsum)
	if !closeTo(gotSigned, wantSigned) {
		t.Fatalf("offset-binary dot: got %v want %v", gotSigned, wantSigned)
	}
}

func TestNormalizeIntoUnit(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	for _, n := range []int{1, 8, 17, 768} {
		v := randVec(r, n)
		out := NormalizeInto(nil, v)
		if got := float64(Norm(out)); !closeTo(got, 1) {
			t.Errorf("NormalizeInto n=%d: norm=%v want 1", n, got)
		}
	}
	// zero vector passes through as zeros (norm 0)
	zero := make([]float32, 8)
	out := NormalizeInto(nil, zero)
	if Norm(out) != 0 {
		t.Errorf("zero vector should stay zero, norm=%v", Norm(out))
	}
}

func TestSubAndMul(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	n := 17
	a, b := randVec(r, n), randVec(r, n)
	sub := make([]float32, n)
	Sub_Into(sub, a, b)
	for i := range sub {
		if sub[i] != a[i]-b[i] {
			t.Fatalf("Sub_Into[%d]=%v want %v", i, sub[i], a[i]-b[i])
		}
	}
	mul := make([]float32, n)
	MulNumber_Into(mul, a, 2.5)
	for i := range mul {
		if mul[i] != a[i]*2.5 {
			t.Fatalf("MulNumber_Into[%d]=%v want %v", i, mul[i], a[i]*2.5)
		}
	}
}
