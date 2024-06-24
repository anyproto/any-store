package bitmap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSet(t *testing.T) {
	var bitmap Bitmap256
	positions := []uint8{5, 65, 130, 195} // Positions that use all elements of the array

	for _, pos := range positions {
		bitmap = bitmap.Set(pos)
		assert.True(t, bitmap.Get(pos), "Expected bit %d to be set", pos)
	}
}

func TestClear(t *testing.T) {
	var bitmap Bitmap256
	positions := []uint8{5, 65, 130, 195} // Positions that use all elements of the array

	for _, pos := range positions {
		bitmap = bitmap.Set(pos)
	}
	for _, pos := range positions {
		bitmap = bitmap.Clear(pos)
		assert.False(t, bitmap.Get(pos), "Expected bit %d to be cleared", pos)
	}
}

func TestOr(t *testing.T) {
	var bitmap1 Bitmap256
	var bitmap2 Bitmap256

	positions1 := []uint8{5, 130}  // Use elements 0 and 2 of the array
	positions2 := []uint8{65, 195} // Use elements 1 and 3 of the array

	for _, pos := range positions1 {
		bitmap1 = bitmap1.Set(pos)
	}
	for _, pos := range positions2 {
		bitmap2 = bitmap2.Set(pos)
	}

	result := bitmap1.Or(bitmap2)

	for _, pos := range append(positions1, positions2...) {
		assert.True(t, result.Get(pos), "Expected bit %d to be set in result", pos)
	}
}

func TestGet(t *testing.T) {
	var bitmap Bitmap256
	positions := []uint8{5, 65, 130, 195} // Positions that use all elements of the array

	for _, pos := range positions {
		bitmap = bitmap.Set(pos)
		assert.True(t, bitmap.Get(pos), "Expected bit %d to be set", pos)
		assert.False(t, bitmap.Get(pos+1), "Expected bit %d to be not set", pos+1)
	}
}

func TestCountLeadingOnes(t *testing.T) {
	var bitmap Bitmap256

	// Test with no leading ones
	assert.Equal(t, 0, bitmap.CountLeadingOnes(), "Expected 0 leading ones")

	// Test with some leading ones
	positions := []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8}
	for _, pos := range positions {
		bitmap = bitmap.Set(pos)
	}
	assert.Equal(t, len(positions), bitmap.CountLeadingOnes(), "Expected %d leading ones", len(positions))

	// Test with a mix of ones and zeros at the start
	bitmap = Bitmap256{}
	positions = []uint8{0, 1, 2, 64, 65, 66}
	for _, pos := range positions {
		bitmap = bitmap.Set(pos)
	}
	assert.Equal(t, 3, bitmap.CountLeadingOnes(), "Expected 3 leading ones")
}

func TestCount(t *testing.T) {
	var bitmap Bitmap256

	// Test with no bits set
	assert.Equal(t, 0, bitmap.Count(), "Expected 0 ones")

	// Test with some bits set
	positions := []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 64, 65, 66, 130, 195}
	for _, pos := range positions {
		bitmap = bitmap.Set(pos)
	}
	assert.Equal(t, len(positions), bitmap.Count(), "Expected %d ones", len(positions))
}

func TestIterate(t *testing.T) {
	var bitmap Bitmap256
	positions := []uint8{5, 65, 130, 195} // Positions that use all elements of the array

	for _, pos := range positions {
		bitmap = bitmap.Set(pos)
	}

	found := make(map[int]bool)
	bitmap.Iterate(func(i int) {
		found[i] = true
	})

	for _, pos := range positions {
		assert.True(t, found[int(pos)], "Expected to find bit %d set", pos)
	}

	assert.Equal(t, len(positions), len(found), "Expected to find %d bits set", len(positions))
}

func TestSubtract(t *testing.T) {
	var bitmap1 Bitmap256
	var bitmap2 Bitmap256

	positions1 := []uint8{1, 2, 5}
	positions2 := []uint8{1, 2, 6}

	for _, pos := range positions1 {
		bitmap1 = bitmap1.Set(pos)
	}
	for _, pos := range positions2 {
		bitmap2 = bitmap2.Set(pos)
	}

	expectedPositions := []uint8{6}
	result := bitmap1.Subtract(bitmap2)

	for _, pos := range expectedPositions {
		assert.True(t, result.Get(pos), "Expected bit %d to be set in the result", pos)
	}

	unexpectedPositions := positions1
	for _, pos := range unexpectedPositions {
		assert.False(t, result.Get(pos), "Expected bit %d to not be set in the result", pos)
	}
}
