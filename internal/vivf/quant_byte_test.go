package vivf

import (
	"math"
	"math/rand"
	"testing"

	"github.com/anyproto/any-store/v2/internal/simd"
)

// TestDistBytesMatchesDecode verifies the byte-kernel distance equals the
// decode-then-exactDist path for both metrics, across dims and the encode
// round-trip (offset-binary + stored sqnorm for L2).
func TestDistBytesMatchesDecode(t *testing.T) {
	t.Logf("AcceleratedFloatByte()=%v", simd.AcceleratedFloatByte())
	r := rand.New(rand.NewSource(7))
	for _, normalize := range []bool{true, false} {
		for _, dim := range []int{64, 100, 128, 768} {
			ix := &StoreIndex{dim: dim, normalize: normalize}
			l2 := !normalize
			scratch := make([]float32, dim)
			for trial := 0; trial < 20; trial++ {
				v := make([]float32, dim)
				for i := range v {
					v[i] = r.Float32()*2 - 1
				}
				q := make([]float32, dim)
				for i := range q {
					q[i] = r.Float32()*2 - 1
				}
				qn := q
				if normalize {
					qn = normalizeInto(nil, q)
					v = normalizeInto(nil, v) // stored vectors are unit length for cosine
				}
				rec := encodeVecInt8(nil, v, l2)

				// reference: dequant then exactDist
				want := ix.exactDist(qn, decodeVecInt8(rec, dim, l2, scratch))

				// byte path
				var qsum, qnorm2 float32
				for _, x := range qn {
					qsum += x
				}
				if l2 {
					qnorm2 = simd.Dot(qn, qn)
				}
				scale, sqnorm, comps, ok := int8Split(rec, dim, l2)
				if !ok {
					t.Fatalf("int8Split failed dim=%d l2=%v", dim, l2)
				}
				got := ix.distBytes(qn, scale, sqnorm, comps, qsum, qnorm2)

				tol := 1e-3 * (1 + float32(math.Abs(float64(want))))
				if math.Abs(float64(got-want)) > float64(tol) {
					t.Errorf("normalize=%v dim=%d: distBytes=%v want %v (Δ=%v)",
						normalize, dim, got, want, got-want)
				}
			}
		}
	}
}

// TestEncodeInt8RoundTrip checks the offset-binary round-trip and the stored L2
// squared-norm.
func TestEncodeInt8RoundTrip(t *testing.T) {
	r := rand.New(rand.NewSource(9))
	dim := 256
	for _, l2 := range []bool{false, true} {
		v := make([]float32, dim)
		for i := range v {
			v[i] = r.Float32()*4 - 2
		}
		rec := encodeVecInt8(nil, v, l2)
		if got := int8RecordSize(dim, l2); got != len(rec) {
			t.Fatalf("l2=%v: record size %d, want %d", l2, len(rec), got)
		}
		dec := decodeVecInt8(rec, dim, l2, make([]float32, dim))
		// each component within one quant step (scale) of the original
		scale, sqnorm, _, _ := int8Split(rec, dim, l2)
		for i := range v {
			if math.Abs(float64(dec[i]-v[i])) > float64(scale)*1.01 {
				t.Fatalf("l2=%v comp %d: dec=%v v=%v scale=%v", l2, i, dec[i], v[i], scale)
			}
		}
		if l2 {
			want := simd.Dot(dec, dec) // ‖x‖² of the dequantized vector
			if math.Abs(float64(sqnorm-want)) > 1e-3*float64(1+want) {
				t.Fatalf("stored sqnorm=%v want ‖x‖²=%v", sqnorm, want)
			}
		}
	}
}
