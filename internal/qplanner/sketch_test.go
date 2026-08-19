package qplanner

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestIndexSketch_Increment_DecrementEstimate covers Increment, Decrement,
// Estimate, EntryCount, and the Decrement underflow-clamp (returning early when
// old==0) at a single level.
func TestIndexSketch_Increment_DecrementEstimate(t *testing.T) {
	s := NewIndexSketch(DefaultSketchSize, 1)
	k := []byte("key-a")

	assert.Equal(t, uint64(0), s.Estimate(0, k))
	assert.Equal(t, uint64(0), s.EntryCount(0))
	s.Increment(0, k)
	s.Increment(0, k)
	assert.Equal(t, uint64(2), s.Estimate(0, k))
	assert.Equal(t, uint64(2), s.EntryCount(0))

	s.Decrement(0, k)
	assert.Equal(t, uint64(1), s.Estimate(0, k))
	assert.Equal(t, uint64(1), s.EntryCount(0))
	// Two more decrements drain the bucket; a third is a no-op (clamped at 0).
	s.Decrement(0, k)
	s.Decrement(0, k) // hits the `if old == 0 { return }` early exit
	assert.Equal(t, uint64(0), s.Estimate(0, k))
	assert.Equal(t, uint64(0), s.EntryCount(0))
}

// TestIndexSketch_LevelIsolation verifies that levels are collision-isolated: a
// flood of distinct values at the deep level never changes a shallow level's
// estimate for a value that was only inserted at the shallow level.
func TestIndexSketch_LevelIsolation(t *testing.T) {
	s := NewIndexSketch(64, 3)
	shallow := []byte("status=active")
	s.Increment(0, shallow)

	// Flood the deepest level with many distinct keys (far exceeding 64 buckets).
	for i := 0; i < 100000; i++ {
		s.Increment(2, []byte{byte(i), byte(i >> 8), byte(i >> 16)})
	}

	assert.Equal(t, uint64(1), s.Estimate(0, shallow),
		"deep-level flood must not pollute the shallow level")
	assert.Equal(t, uint64(1), s.EntryCount(0))
	assert.Equal(t, uint64(100000), s.EntryCount(2))
}

// TestIndexSketch_OutOfRangeLevel covers the validLevel guards.
func TestIndexSketch_OutOfRangeLevel(t *testing.T) {
	s := NewIndexSketch(16, 2)
	k := []byte("x")
	s.Increment(5, k) // ignored
	s.Decrement(5, k) // ignored
	assert.Equal(t, uint64(0), s.Estimate(5, k))
	assert.Equal(t, uint64(0), s.EntryCount(5))
	assert.Equal(t, uint64(0), s.Estimate(-1, k))
}

// TestIndexSketch_DocCount covers Increment/DecrementDocCount + GetDocCount
// including the underflow-clamp at old==0.
func TestIndexSketch_DocCount(t *testing.T) {
	s := NewIndexSketch(DefaultSketchSize, 2)
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

// TestIndexSketch_MarshalUnmarshal covers V2 MarshalBinary / UnmarshalBinary
// round-trip across multiple levels, including levelTotals and docCount.
func TestIndexSketch_MarshalUnmarshal(t *testing.T) {
	s := NewIndexSketch(4, 3)
	s.Increment(0, []byte("a"))
	s.Increment(1, []byte("ab"))
	s.Increment(1, []byte("ab"))
	s.Increment(2, []byte("abc"))
	s.IncrementDocCount()
	s.IncrementDocCount()

	data := s.MarshalBinary(nil)

	// Round-trip into a fresh same-shape sketch.
	s2 := NewIndexSketch(4, 3)
	s2.UnmarshalBinary(data)
	assert.Equal(t, s.Estimate(0, []byte("a")), s2.Estimate(0, []byte("a")))
	assert.Equal(t, s.Estimate(1, []byte("ab")), s2.Estimate(1, []byte("ab")))
	assert.Equal(t, s.Estimate(2, []byte("abc")), s2.Estimate(2, []byte("abc")))
	assert.Equal(t, uint64(1), s2.EntryCount(0))
	assert.Equal(t, uint64(2), s2.EntryCount(1))
	assert.Equal(t, uint64(1), s2.EntryCount(2))
	assert.Equal(t, uint64(2), s2.GetDocCount())
	assert.False(t, s2.NeedsRebuild())

	// Round-trip into a differently-shaped sketch: UnmarshalBinary adopts the
	// blob's shape (the inspect path).
	s3 := NewIndexSketch(DefaultSketchSize, 1)
	s3.UnmarshalBinary(data)
	assert.Equal(t, 4, s3.Size)
	assert.Equal(t, 3, s3.NumLevels())
	assert.Equal(t, s.Estimate(1, []byte("ab")), s3.Estimate(1, []byte("ab")))
}

// TestIndexSketch_LegacyMigration covers loading an old single-row (V1) blob into
// a multi-level sketch: it lands in the full-key level, shallow levels stay empty,
// and the sketch is flagged for rebuild.
func TestIndexSketch_LegacyMigration(t *testing.T) {
	// Build a genuine V1 blob: [Size*8 buckets][docCount 8], no magic.
	const size = DefaultSketchSize
	legacy := make([]byte, size*8+8)
	// Pick a value, write its count into the V1 bucket it would hash to.
	v1 := NewIndexSketch(size, 1)
	v1.Increment(0, []byte("full-key"))
	v1.Increment(0, []byte("full-key"))
	v1Data := v1.MarshalBinary(nil) // V2 format
	// Re-encode as legacy V1 (strip header + levelTotals): just buckets+docCount.
	// Reconstruct from v1's level-0 buckets.
	for i := 0; i < size; i++ {
		off := i * 8
		// little-endian copy of bucket i
		b := v1.Buckets[i]
		legacy[off+0] = byte(b)
		legacy[off+1] = byte(b >> 8)
		legacy[off+2] = byte(b >> 16)
		legacy[off+3] = byte(b >> 24)
		legacy[off+4] = byte(b >> 32)
		legacy[off+5] = byte(b >> 40)
		legacy[off+6] = byte(b >> 48)
		legacy[off+7] = byte(b >> 56)
	}
	// docCount = 7 in the trailing 8 bytes.
	legacy[size*8] = 7
	_ = v1Data

	// Load into a 3-level sketch.
	s := NewIndexSketch(size, 3)
	s.UnmarshalBinary(legacy)

	full := s.NumLevels() - 1
	assert.Equal(t, uint64(2), s.Estimate(full, []byte("full-key")),
		"legacy combined-key data must load into the full-key level")
	assert.Equal(t, uint64(0), s.Estimate(0, []byte("full-key")),
		"shallow levels have no historical data")
	assert.Equal(t, uint64(7), s.GetDocCount())
	assert.Equal(t, uint64(2), s.EntryCount(full),
		"full-key level total = sum of loaded buckets")
	assert.True(t, s.NeedsRebuild(), "compound sketch with empty shallow levels needs rebuild")

	// A single-level sketch loading legacy is complete (level 0 == full key).
	s1 := NewIndexSketch(size, 1)
	s1.UnmarshalBinary(legacy)
	assert.Equal(t, uint64(2), s1.Estimate(0, []byte("full-key")))
	assert.False(t, s1.NeedsRebuild())
}

// TestIndexSketch_LegacyMigration_NoDocCount covers the even-older docCount-less
// format: a missing trailing docCount must leave the pre-existing value untouched.
func TestIndexSketch_LegacyMigration_NoDocCount(t *testing.T) {
	const size = 4
	v1 := NewIndexSketch(size, 1)
	v1.Increment(0, []byte("k"))
	// Legacy buckets only (no docCount): size*8 bytes.
	legacy := make([]byte, size*8)
	for i := 0; i < size; i++ {
		b := v1.Buckets[i]
		for j := 0; j < 8; j++ {
			legacy[i*8+j] = byte(b >> (8 * j))
		}
	}

	s := NewIndexSketch(size, 2)
	s.IncrementDocCount()
	s.IncrementDocCount()
	s.IncrementDocCount() // sentinel docCount = 3
	s.UnmarshalBinary(legacy)
	assert.Equal(t, uint64(3), s.GetDocCount(),
		"data missing docCount must LEAVE the pre-existing value untouched")
}

// TestIndexSketch_Reset pins Reset — all buckets, level totals and docCount go to
// zero.
func TestIndexSketch_Reset(t *testing.T) {
	s := NewIndexSketch(8, 2)
	s.Increment(0, []byte("a"))
	s.Increment(1, []byte("ab"))
	s.IncrementDocCount()
	s.Reset()
	assert.Equal(t, uint64(0), s.Estimate(0, []byte("a")))
	assert.Equal(t, uint64(0), s.Estimate(1, []byte("ab")))
	assert.Equal(t, uint64(0), s.EntryCount(0))
	assert.Equal(t, uint64(0), s.EntryCount(1))
	assert.Equal(t, uint64(0), s.GetDocCount())
}

// TestNewIndexSketch_Defaults covers the size<=0 and levels<1 clamps.
func TestNewIndexSketch_Defaults(t *testing.T) {
	s := NewIndexSketch(0, 1)
	assert.Equal(t, DefaultSketchSize, s.Size)
	s2 := NewIndexSketch(-5, 1)
	assert.Equal(t, DefaultSketchSize, s2.Size)
	s3 := NewIndexSketch(16, 0)
	assert.Equal(t, 1, s3.NumLevels())
	s4 := NewIndexSketch(16, -2)
	assert.Equal(t, 1, s4.NumLevels())
}

// TestIndexSketch_Distribution covers Distribution across the empty, uniform
// and skewed cases plus the percentile helper, on a chosen level.
func TestIndexSketch_Distribution(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		s := NewIndexSketch(64, 1)
		d := s.Distribution(0)
		assert.Equal(t, SketchDistribution{}, d)
	})

	t.Run("out-of-range level", func(t *testing.T) {
		s := NewIndexSketch(64, 1)
		assert.Equal(t, SketchDistribution{}, s.Distribution(3))
	})

	t.Run("uniform", func(t *testing.T) {
		// One increment into every bucket of level 1: fully filled, no skew.
		s := NewIndexSketch(64, 2)
		base := 1 * s.Size
		for i := 0; i < s.Size; i++ {
			s.Buckets[base+i] = 1
		}
		d := s.Distribution(1)
		assert.Equal(t, 64, d.NonEmptyBuckets)
		assert.Equal(t, 1.0, d.Fill)
		assert.Equal(t, uint64(1), d.MinNonEmpty)
		assert.Equal(t, uint64(1), d.MaxBucket)
		assert.Equal(t, 1.0, d.MeanNonEmpty)
		assert.Equal(t, uint64(1), d.P50)
		assert.Equal(t, uint64(1), d.P99)
		assert.Equal(t, 1.0, d.Skew)
		// Level 0 (untouched) is empty.
		assert.Equal(t, SketchDistribution{}, s.Distribution(0))
	})

	t.Run("skewed", func(t *testing.T) {
		// Many light buckets and one very hot bucket on level 0.
		s := NewIndexSketch(100, 1)
		for i := 0; i < 50; i++ {
			s.Buckets[i] = 2
		}
		s.Buckets[99] = 200
		d := s.Distribution(0)
		assert.Equal(t, 51, d.NonEmptyBuckets)
		assert.InDelta(t, 0.51, d.Fill, 1e-9)
		assert.Equal(t, uint64(2), d.MinNonEmpty)
		assert.Equal(t, uint64(200), d.MaxBucket)
		assert.Greater(t, d.Skew, 1.0)
		assert.Equal(t, uint64(2), d.P50)
		// The hot bucket sits in the top percentile.
		assert.Equal(t, uint64(200), d.P99)
	})
}
