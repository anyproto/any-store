package anyenc

import (
	"fmt"
	"unsafe"
)

// TypeVectorF32 stores an embedding as a packed little-endian []float32 blob.
// On little-endian hosts (amd64/arm64) encode and decode are a single bulk copy
// and reads are zero-copy. NOTE (prototype): the packed bytes use host byte
// order; a production version should encode/decode explicit little-endian and
// guarantee 4-byte alignment of the decode buffer.

// appendVectorF32LE appends vec as packed float32 bytes (host order) to dst.
func appendVectorF32LE(dst []byte, vec []float32) []byte {
	if len(vec) == 0 {
		return dst
	}
	src := unsafe.Slice((*byte)(unsafe.Pointer(&vec[0])), len(vec)*4)
	return append(dst, src...)
}

// bytesAsF32 reinterprets packed float32 bytes as a []float32 (zero-copy).
func bytesAsF32(b []byte) []float32 {
	if len(b) < 4 {
		return nil
	}
	return unsafe.Slice((*float32)(unsafe.Pointer(&b[0])), len(b)/4)
}

// VectorF32 returns the embedding for a TypeVectorF32 value, or an error if the
// value is not a vector. The result is a zero-copy view valid until the parser
// is reused.
func (v *Value) VectorF32() ([]float32, error) {
	if v.Type() != TypeVectorF32 {
		return nil, fmt.Errorf("value doesn't contain vectorF32; it contains %s", v.Type())
	}
	return bytesAsF32(v.v), nil
}

// GetVectorF32 returns the embedding at the given path, or nil if it is not a
// TypeVectorF32 value. The result is a zero-copy view valid until the parser is
// reused.
func (v *Value) GetVectorF32(keys ...string) []float32 {
	vv := v.Get(keys...)
	if vv.Type() != TypeVectorF32 {
		return nil
	}
	return bytesAsF32(vv.v)
}

// VectorBytes returns the raw packed little-endian bytes of a TypeVectorF32
// value (zero-copy), or nil if v is not a vector. Use for fast, alloc-free
// byte-wise equality (bit-identical vectors are equal — the right semantic for
// change detection, and free of NaN / signed-zero float comparison quirks).
func (v *Value) VectorBytes() []byte {
	if v.Type() != TypeVectorF32 {
		return nil
	}
	return v.v
}
