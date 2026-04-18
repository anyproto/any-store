package qplanner

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestIndexSketch_Increment_DecrementEstimate covers Increment, Decrement,
// Estimate, and the Decrement underflow-clamp (returning early when old==0).
func TestIndexSketch_Increment_DecrementEstimate(t *testing.T) {
	s := NewIndexSketch(DefaultSketchSize)
	k := []byte("key-a")

	assert.Equal(t, uint64(0), s.Estimate(k))
	s.Increment(k)
	s.Increment(k)
	assert.Equal(t, uint64(2), s.Estimate(k))

	s.Decrement(k)
	assert.Equal(t, uint64(1), s.Estimate(k))
	// Two more decrements drain the bucket; a third is a no-op (clamped at 0).
	s.Decrement(k)
	s.Decrement(k) // hits the `if old == 0 { return }` early exit
	assert.Equal(t, uint64(0), s.Estimate(k))
}

// TestIndexSketch_DocCount covers Increment/DecrementDocCount + GetDocCount
// including the underflow-clamp at old==0.
func TestIndexSketch_DocCount(t *testing.T) {
	s := NewIndexSketch(DefaultSketchSize)
	assert.Equal(t, uint64(0), s.GetDocCount())
	s.IncrementDocCount()
	s.IncrementDocCount()
	assert.Equal(t, uint64(2), s.GetDocCount())
	s.DecrementDocCount()
	assert.Equal(t, uint64(1), s.GetDocCount())
	s.DecrementDocCount()
	s.DecrementDocCount() // underflow → clamped
	assert.Equal(t, uint64(0), s.GetDocCount())
}

// TestIndexSketch_MarshalUnmarshal covers MarshalBinary / UnmarshalBinary
// round-trip and the backward-compat branch (data WITHOUT trailing docCount).
func TestIndexSketch_MarshalUnmarshal(t *testing.T) {
	s := NewIndexSketch(4)
	s.Increment([]byte("a"))
	s.Increment([]byte("b"))
	s.Increment([]byte("b"))
	s.IncrementDocCount()
	s.IncrementDocCount()

	data := s.MarshalBinary(nil)

	// Round-trip into a fresh sketch.
	s2 := NewIndexSketch(4)
	s2.UnmarshalBinary(data)
	assert.Equal(t, s.Estimate([]byte("a")), s2.Estimate([]byte("a")))
	assert.Equal(t, s.Estimate([]byte("b")), s2.Estimate([]byte("b")))
	assert.Equal(t, uint64(2), s2.GetDocCount())

	// Backward compat: trim off the trailing 8-byte docCount.
	oldData := data[:len(data)-8]
	s3 := NewIndexSketch(4)
	// Pre-set docCount to a sentinel so we can distinguish
	// "branch left it alone" from "branch zeroed it".
	s3.IncrementDocCount()
	s3.IncrementDocCount()
	s3.IncrementDocCount() // sentinel = 3
	s3.UnmarshalBinary(oldData)
	assert.Equal(t, uint64(3), s3.GetDocCount(),
		"data missing docCount must LEAVE the pre-existing value untouched, not zero it")
	assert.Equal(t, s.Estimate([]byte("a")), s3.Estimate([]byte("a")))
}

// TestIndexSketch_Reset pins Reset — all buckets and docCount go to zero.
func TestIndexSketch_Reset(t *testing.T) {
	s := NewIndexSketch(8)
	s.Increment([]byte("a"))
	s.IncrementDocCount()
	s.Reset()
	assert.Equal(t, uint64(0), s.Estimate([]byte("a")))
	assert.Equal(t, uint64(0), s.GetDocCount())
}

// TestNewIndexSketch_DefaultsBadSize covers the `if size <= 0` branch at
// sketch.go:23-25 — non-positive sizes fall back to DefaultSketchSize.
func TestNewIndexSketch_DefaultsBadSize(t *testing.T) {
	s := NewIndexSketch(0)
	assert.Equal(t, DefaultSketchSize, s.Size)
	s2 := NewIndexSketch(-5)
	assert.Equal(t, DefaultSketchSize, s2.Size)
}
