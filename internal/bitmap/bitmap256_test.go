package bitmap

import "testing"

// TestSet tests the Set method of Bitmap256.
func TestSet(t *testing.T) {
	var bitmap Bitmap256
	positions := []uint8{5, 65, 130, 195} // Positions that use all elements of the array
	for _, pos := range positions {
		bitmap = bitmap.Set(pos)
		if !bitmap.Get(pos) {
			t.Errorf("Expected bit %d to be set", pos)
		}
	}
}

// TestClear tests the Clear method of Bitmap256.
func TestClear(t *testing.T) {
	var bitmap Bitmap256
	positions := []uint8{5, 65, 130, 195} // Positions that use all elements of the array
	for _, pos := range positions {
		bitmap = bitmap.Set(pos)
	}
	for _, pos := range positions {
		bitmap = bitmap.Clear(pos)
		if bitmap.Get(pos) {
			t.Errorf("Expected bit %d to be cleared", pos)
		}
	}
}

// TestOr tests the Or method of Bitmap256.
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
		if !result.Get(pos) {
			t.Errorf("Expected bit %d to be set in result", pos)
		}
	}
}

// TestGet tests the Get method of Bitmap256.
func TestGet(t *testing.T) {
	var bitmap Bitmap256
	positions := []uint8{5, 65, 130, 195} // Positions that use all elements of the array
	for _, pos := range positions {
		bitmap = bitmap.Set(pos)
		if !bitmap.Get(pos) {
			t.Errorf("Expected bit %d to be set", pos)
		}
		if bitmap.Get(pos + 1) { // Check adjacent bit to ensure it's not set
			t.Errorf("Expected bit %d to be not set", pos+1)
		}
	}
}

// TestCountLeadingOnes tests the CountLeadingOnes method of Bitmap256.
func TestCountLeadingOnes(t *testing.T) {
	var bitmap Bitmap256

	// Test with no leading ones
	if result := bitmap.CountLeadingOnes(); result != 0 {
		t.Errorf("Expected 0 leading ones, got %d", result)
	}

	// Test with some leading ones
	positions := []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8}
	for _, pos := range positions {
		bitmap = bitmap.Set(pos)
	}

	if result := bitmap.CountLeadingOnes(); result != len(positions) {
		t.Errorf("Expected %d leading ones, got %d", len(positions), result)
	}

	// Test with a mix of ones and zeros at the start
	bitmap = Bitmap256{}
	positions = []uint8{0, 1, 2, 64, 65, 66}
	for _, pos := range positions {
		bitmap = bitmap.Set(pos)
	}

	if result := bitmap.CountLeadingOnes(); result != 3 {
		t.Errorf("Expected 3 leading ones, got %d", result)
	}
}

// TestCount tests the Count method of Bitmap256.
func TestCount(t *testing.T) {
	var bitmap Bitmap256

	// Test with no bits set
	if result := bitmap.Count(); result != 0 {
		t.Errorf("Expected 0 ones, got %d", result)
	}

	// Test with some bits set
	positions := []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 64, 65, 66, 130, 195}
	for _, pos := range positions {
		bitmap = bitmap.Set(pos)
	}

	if result := bitmap.Count(); result != len(positions) {
		t.Errorf("Expected %d ones, got %d", len(positions), result)
	}
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
		if !found[int(pos)] {
			t.Errorf("Expected to find bit %d set", pos)
		}
	}

	if len(found) != len(positions) {
		t.Errorf("Expected to find %d bits set, found %d", len(positions), len(found))
	}
}
