package bitmap

import "math/bits"

// Bitmap256 is a bitmap of 256 elements.
type Bitmap256 [4]uint64

// Set sets the bit at position pos to true.
func (v Bitmap256) Set(pos uint8) Bitmap256 {
	v[pos>>6] |= 1 << (pos & 63)
	return v
}

// Clear sets the bit at position pos to false.
func (v Bitmap256) Clear(pos uint8) Bitmap256 {
	v[pos>>6] &= ^(1 << (pos & 63))
	return v
}

// Get returns the bit value at position pos.
func (v Bitmap256) Get(pos uint8) bool {
	return ((v[pos>>6] >> (pos & 63)) & 1) == 1
}

// Or returns a new Bitmap256 that is the bitwise OR of the current bitmap and the given bitmap.
func (v Bitmap256) Or(b Bitmap256) Bitmap256 {
	return Bitmap256{
		v[0] | b[0],
		v[1] | b[1],
		v[2] | b[2],
		v[3] | b[3],
	}
}

// CountLeadingOnes returns the count of consecutive 1s starting from the beginning.
func (v Bitmap256) CountLeadingOnes() int {
	for i := 0; i < 256; i++ {
		if !v.Get(uint8(i)) {
			return i
		}
	}
	return 256
}

// Count returns the total number of 1s in the bitmap.
func (v Bitmap256) Count() int {
	return bits.OnesCount64(v[0]) +
		bits.OnesCount64(v[1]) +
		bits.OnesCount64(v[2]) +
		bits.OnesCount64(v[3])
}

// Iterate calls the given function f with the index of each set bit.
func (v Bitmap256) Iterate(f func(i int)) {
	for wordIdx, word := range v {
		bitIdx := wordIdx * 64
		for word != 0 {
			lsb := bits.TrailingZeros64(word)
			f(bitIdx + lsb)
			word &= word - 1
		}
	}
}
