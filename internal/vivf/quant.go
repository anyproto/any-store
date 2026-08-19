package vivf

import (
	"encoding/binary"
	"math"

	"github.com/anyproto/any-store/v2/internal/vecf"
)

// The re-rank store (:vec) and IVF-SQ cell store hold int8-quantized vectors
// instead of raw f32 — ~4× smaller on disk and in the page cache, which is the
// win on the random per-candidate re-rank reads and the SQ cell scan. The format
// is per-vector symmetric scalar quantization with OFFSET-BINARY components, so
// the unsigned SIMD float×byte kernel can score a candidate straight from the
// bytes (no dequant) via the identity in StoreIndex.distBytes:
//
//	dot(q, x) = scale·(DotFloatByte(q, u) − 128·Σq),  where xᵢ = scale·(uᵢ − 128).
//
// Layout depends on the metric:
//
//	cosine (normalize):  [scale f32 LE][dim uint8]                 (4 + dim)
//	L2     (!normalize): [scale f32 LE][sqnorm f32 LE][dim uint8]  (8 + dim)
//
// L2 additionally stores sqnorm = ‖x‖² so ‖q−x‖² = ‖q‖² + sqnorm − 2·dot(q,x) can
// be computed from the same byte-dot kernel (cosine needs only the dot, since the
// stored vectors are unit-normalized). Offset-binary encode is the sign-bit flip
// `byte(q) ^ 0x80` (== q+128 mod 256); decode subtracts the bias back. This
// mirrors — and for L2 goes beyond — internal/vindex/quant.go.

// int8Bias is the offset-binary bias (see vecf.Int8Bias).
const int8Bias = vecf.Int8Bias

// int8RecordSize is the encoded size of one int8 record for the metric (L2
// carries an extra f32 squared-norm).
func int8RecordSize(dim int, l2 bool) int {
	if l2 {
		return 8 + dim
	}
	return 4 + dim
}

// encodeVecInt8 appends v's offset-binary int8 record to dst (reusing it) and
// returns it. When l2 is set the per-vector squared norm ‖x‖² is stored too.
func encodeVecInt8(dst []byte, v []float32, l2 bool) []byte {
	dst = append(dst[:0], 0, 0, 0, 0) // scale slot, back-patched below
	var scale float32
	if l2 {
		normSlot := len(dst)
		dst = append(dst, 0, 0, 0, 0) // sqnorm placeholder, back-patched below
		var sumSq float32
		dst, scale, sumSq = vecf.QuantizeInt8Norm(dst, v)
		sqnorm := scale * scale * sumSq // ‖x‖² of the dequantized vector
		binary.LittleEndian.PutUint32(dst[normSlot:normSlot+4], math.Float32bits(sqnorm))
	} else {
		dst, scale = vecf.QuantizeInt8(dst, v)
	}
	binary.LittleEndian.PutUint32(dst[:4], math.Float32bits(scale))
	return dst
}

// decodeVecInt8 dequantizes an int8 record into dst (length dim). l2 selects the
// layout (whether a stored squared-norm precedes the components).
func decodeVecInt8(data []byte, dim int, l2 bool, dst []float32) []float32 {
	off := 4
	if l2 {
		off = 8
	}
	scale := math.Float32frombits(binary.LittleEndian.Uint32(data[:4]))
	vecf.DequantizeInt8(dst[:dim], data[off:off+dim], scale)
	return dst
}

// int8Split returns the scale, stored squared-norm (0 for cosine), and the
// offset-binary component bytes of an int8 record without dequantizing — the
// input to the byte-kernel distance path.
func int8Split(data []byte, dim int, l2 bool) (scale, sqnorm float32, comps []byte, ok bool) {
	off := 4
	if l2 {
		off = 8
	}
	if len(data) < off+dim {
		return 0, 0, nil, false
	}
	scale = math.Float32frombits(binary.LittleEndian.Uint32(data[:4]))
	if l2 {
		sqnorm = math.Float32frombits(binary.LittleEndian.Uint32(data[4:8]))
	}
	comps = data[off : off+dim]
	return scale, sqnorm, comps, true
}
