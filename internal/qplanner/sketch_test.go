package qplanner

import (
	"encoding/binary"
	"fmt"
	"slices"
	"testing"

	"github.com/klauspost/compress/s2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// A tagged blob cut off inside its header or body leaves the sketch as it was:
// the legacy path would zero the shallow level and flag a rebuild.
func TestIndexSketch_UnmarshalTruncatedV2(t *testing.T) {
	s := NewIndexSketch(8, 2)
	s.Increment(0, []byte("a"))
	s.Increment(1, []byte("ab"))
	s.IncrementDocCount()
	want := s.Snapshot(nil)
	blob := s.MarshalBinary(nil)

	for n := len(sketchMagic); n < len(blob); n++ {
		assert.False(t, s.UnmarshalBinary(blob[:n]), "prefix of %d bytes", n)
		assert.Equal(t, want, s.Snapshot(nil), "prefix of %d bytes", n)
		assert.False(t, s.NeedsRebuild(), "prefix of %d bytes", n)
	}
}

// A V2 header may claim any shape. One whose size in bytes overflows is
// refused like any other shape the blob is too short for.
func TestIndexSketch_UnmarshalV2HugeShape(t *testing.T) {
	s := NewIndexSketch(8, 2)
	s.Increment(0, []byte("a"))
	s.IncrementDocCount()
	want := s.Snapshot(nil)

	for _, shape := range [][2]uint32{
		{0x30303030, 0x58303030}, // 8*levels*size wraps to a negative int
		{1 << 31, 1 << 31},
		{1<<32 - 1, 1<<32 - 1},
		{1, 1<<32 - 1},
		{1<<32 - 1, 1},
	} {
		blob := append([]byte(nil), sketchMagic[:]...)
		blob = binary.LittleEndian.AppendUint32(blob, shape[0])
		blob = binary.LittleEndian.AppendUint32(blob, shape[1])
		assert.True(t, s.SameShape(blob), "%v", shape)
		assert.False(t, s.UnmarshalBinary(blob), "%v", shape)
		assert.Equal(t, want, s.Snapshot(nil), "%v", shape)
	}
}

func TestIndexSketch_SameShape(t *testing.T) {
	s := NewIndexSketch(64, 2)
	same := NewIndexSketch(64, 2).MarshalBinary(nil)
	otherLevels := NewIndexSketch(64, 1).MarshalBinary(nil)
	otherSize := NewIndexSketch(32, 2).MarshalBinary(nil)
	assert.True(t, s.SameShape(same))
	assert.False(t, s.SameShape(otherLevels))
	assert.False(t, s.SameShape(otherSize))
	// Bytes UnmarshalBinary loads into the existing shape or leaves alone.
	assert.True(t, s.SameShape(make([]byte, 8*64)), "legacy")
	assert.True(t, s.SameShape(nil))
	assert.True(t, s.SameShape(otherSize[:len(otherSize)-1]), "truncated")
	corrupt := append([]byte(nil), otherLevels...)
	binary.LittleEndian.PutUint32(corrupt[4:], 0)
	assert.True(t, s.SameShape(corrupt), "corrupt header")

	// A compressed blob counts by its header, as a V2 one does.
	same = NewIndexSketch(64, 2).MarshalCompressed(nil)
	otherLevels = NewIndexSketch(64, 1).MarshalCompressed(nil)
	otherSize = NewIndexSketch(32, 2).MarshalCompressed(nil)
	assert.True(t, s.SameShape(same))
	assert.False(t, s.SameShape(otherLevels))
	assert.False(t, s.SameShape(otherSize))
	assert.True(t, s.SameShape(otherSize[:sketchHeaderLen+1]), "truncated header")
	corrupt = append([]byte(nil), otherLevels...)
	binary.LittleEndian.PutUint32(corrupt[4:], 0)
	assert.True(t, s.SameShape(corrupt), "corrupt header")
	corrupt = append([]byte(nil), otherLevels...)
	corrupt[sketchHeaderLen] = 1 // a plane the block does not hold
	assert.True(t, s.SameShape(corrupt), "block of another length")
}

// sketchWith returns a sketch of the given shape whose counters are set by
// hand: bucket i of level l holds counts[l][i].
func sketchWith(size int, counts ...[]uint64) *IndexSketch {
	s := NewIndexSketch(size, len(counts))
	for l, level := range counts {
		var total uint64
		for i, c := range level {
			s.Buckets[l*size+i] = c
			total += c
		}
		s.levelTotals[l] = total
	}
	s.docCount.Store(uint64(len(counts)) * 1000003)
	return s
}

// The compressed format keeps every counter, whatever its width, and stores
// per level only the planes its widest count needs.
func TestIndexSketch_MarshalCompressed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		s      *IndexSketch
		planes []byte
	}{
		{"empty", NewIndexSketch(DefaultSketchSize, 2), []byte{0, 0}},
		{"one byte", sketchWith(8, []uint64{0, 1, 255, 0, 7}), []byte{1}},
		{"two bytes", sketchWith(8, []uint64{256, 1, 0, 65535}), []byte{2}},
		{"three bytes", sketchWith(8, []uint64{1 << 16, 0, 0x123456}), []byte{3}},
		{"eight bytes", sketchWith(8, []uint64{1<<63 | 0x0102030405060708, 3}), []byte{8}},
		{"width per level", sketchWith(4, []uint64{70000, 0, 2}, nil, []uint64{0, 9}, []uint64{1 << 40}), []byte{3, 0, 1, 6}},
		{"counted", filledSketch(5000, 2, 0), []byte{2, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blob := tc.s.MarshalCompressed(nil)
			assert.Equal(t, sketchMagicS2[:], blob[:4])
			assert.Equal(t, tc.planes, blob[sketchHeaderLen:sketchHeaderLen+tc.s.Levels])

			// into a sketch of the same shape, in place
			got := NewIndexSketch(tc.s.Size, tc.s.Levels)
			got.Buckets[0], got.levelTotals[0] = 99, 99 // overwritten by the load
			got.docCount.Store(99)
			got.needsRebuild.Store(true)
			buckets := &got.Buckets[0]
			require.True(t, got.SameShape(blob))
			require.True(t, got.UnmarshalBinary(blob))
			assert.Equal(t, tc.s.Snapshot(nil), got.Snapshot(nil))
			assert.Same(t, buckets, &got.Buckets[0], "loaded in place")
			assert.False(t, got.NeedsRebuild())

			// into a sketch of another shape, which adopts the blob's
			other := NewIndexSketch(tc.s.Size+1, tc.s.Levels+1)
			require.False(t, other.SameShape(blob))
			other.UnmarshalBinary(blob)
			assert.Equal(t, tc.s.Size, other.Size)
			assert.Equal(t, tc.s.Levels, other.NumLevels())
			assert.Equal(t, tc.s.Snapshot(nil), other.Snapshot(nil))

			// the buffer is reused, and a second encoding is the same bytes
			again := tc.s.MarshalCompressed(blob[:0])
			assert.Same(t, &blob[0], &again[0])
			assert.Equal(t, blob, again)

			// the V2 format carries the same counters
			v2 := NewIndexSketch(tc.s.Size, tc.s.Levels)
			v2.UnmarshalBinary(tc.s.MarshalBinary(nil))
			assert.Equal(t, got.Snapshot(nil), v2.Snapshot(nil))
		})
	}
}

// The sizes the compressed format is for: a near-empty sketch is a small row
// and a saturated one stays well under a page per level.
func TestIndexSketch_MarshalCompressedSizes(t *testing.T) {
	assert.Less(t, len(NewIndexSketch(DefaultSketchSize, 1).MarshalCompressed(nil)), 40)
	assert.Less(t, len(filledSketch(20, 1, 0).MarshalCompressed(nil)), 200)
	assert.Less(t, len(filledSketch(20, 2, 0).MarshalCompressed(nil)), 400)
	assert.Less(t, len(filledSketch(100_000, 1, 0).MarshalCompressed(nil)), 1200)
	assert.Less(t, len(filledSketch(100_000, 2, 0).MarshalCompressed(nil)), 1500)
}

// A sketch too large for the compressed format's bound is written in the V2
// format, so that what MarshalCompressed returns always loads.
func TestIndexSketch_MarshalCompressedBound(t *testing.T) {
	atBound := NewIndexSketch(maxCompressedBuckets/2, 2)
	atBound.Increment(1, []byte("k"))
	blob := atBound.MarshalCompressed(nil)
	assert.Equal(t, sketchMagicS2[:], blob[:4])
	got := NewIndexSketch(maxCompressedBuckets/2, 2)
	got.UnmarshalBinary(blob)
	assert.Equal(t, uint64(1), got.Estimate(1, []byte("k")))

	over := NewIndexSketch(maxCompressedBuckets/2+1, 2)
	over.Increment(1, []byte("k"))
	blob = over.MarshalCompressed(nil)
	assert.Equal(t, over.MarshalBinary(nil), blob)
	got = NewIndexSketch(maxCompressedBuckets/2+1, 2)
	got.UnmarshalBinary(blob)
	assert.Equal(t, uint64(1), got.Estimate(1, []byte("k")))

	// a header that claims more is refused before its block is looked at
	blob = atBound.MarshalCompressed(nil)
	binary.LittleEndian.PutUint32(blob[8:], maxCompressedBuckets/2+1)
	_, _, _, _, _, ok := s2Shape(blob)
	assert.False(t, ok)
}

// A compressed blob that does not parse leaves the sketch as it was: its
// counters stay, and it is never read as legacy buckets, which would zero the
// shallow level and flag a rebuild.
func TestIndexSketch_UnmarshalCorruptCompressed(t *testing.T) {
	src := sketchWith(8, []uint64{300, 0, 7}, []uint64{1, 2, 3, 70000})
	blob := src.MarshalCompressed(nil)
	block := sketchHeaderLen + src.Levels

	s := sketchWith(8, []uint64{5, 5}, []uint64{0, 0, 9})
	want := s.Snapshot(nil)
	check := func(name string, data []byte) {
		t.Helper()
		assert.False(t, s.UnmarshalBinary(data), name)
		assert.Equal(t, want, s.Snapshot(nil), name)
		assert.False(t, s.NeedsRebuild(), name)
	}
	mutate := func(f func(b []byte)) []byte {
		b := append([]byte(nil), blob...)
		f(b)
		return b
	}

	// cut off anywhere after the tag: inside the header, the planes, the block
	for n := len(sketchMagicS2); n < len(blob); n++ {
		check(fmt.Sprintf("prefix of %d bytes", n), blob[:n])
	}
	check("trailing byte", append(append([]byte(nil), blob...), 0))
	check("no levels", mutate(func(b []byte) { binary.LittleEndian.PutUint32(b[4:], 0) }))
	check("no buckets", mutate(func(b []byte) { binary.LittleEndian.PutUint32(b[8:], 0) }))
	check("more levels", mutate(func(b []byte) { binary.LittleEndian.PutUint32(b[4:], 3) }))
	check("another size", mutate(func(b []byte) { binary.LittleEndian.PutUint32(b[8:], 9) }))
	check("huge shape", mutate(func(b []byte) {
		binary.LittleEndian.PutUint32(b[4:], 1<<31)
		binary.LittleEndian.PutUint32(b[8:], 1<<31)
	}))
	check("nine planes", mutate(func(b []byte) { b[sketchHeaderLen] = 9 }))
	check("a plane less", mutate(func(b []byte) { b[sketchHeaderLen]-- }))
	check("a plane more", mutate(func(b []byte) { b[sketchHeaderLen+1]++ }))
	check("another block length", mutate(func(b []byte) { b[block]++ }))

	// a block of the declared length whose content does not decode
	bad := mutate(func(b []byte) {
		for i := block + 1; i < len(b); i++ {
			b[i] = 0xff
		}
	})
	_, err := s2.Decode(nil, bad[block:])
	require.Error(t, err)
	_, _, _, _, _, ok := s2Shape(bad)
	require.True(t, ok, "the header and the declared length are intact")
	check("corrupt block", bad)

	// the same bytes into a sketch of another shape: not reshaped either
	fresh := NewIndexSketch(DefaultSketchSize, 1)
	fresh.UnmarshalBinary(bad)
	assert.Equal(t, DefaultSketchSize, fresh.Size)
	assert.Equal(t, 1, fresh.NumLevels())
	assert.Equal(t, uint64(0), fresh.GetDocCount())
}

// A blob under a sketch tag this release does not know is left unread: a later
// release's encoding must not be taken for legacy buckets, which would zero the
// shallow level, flag a rebuild and be saved back.
func TestIndexSketch_UnmarshalUnknownTag(t *testing.T) {
	s := sketchWith(8, []uint64{5, 5}, []uint64{0, 0, 9})
	want := s.Snapshot(nil)
	body := make([]byte, 8*8+8)
	for i := range body {
		body[i] = byte(i + 1)
	}
	for _, tag := range []string{"SK04", "SK10", "SK99", "SK00", "SK01"} {
		for _, blob := range [][]byte{[]byte(tag), append([]byte(tag), body...)} {
			assert.True(t, s.SameShape(blob), tag)
			assert.False(t, s.UnmarshalBinary(blob), tag)
			assert.Equal(t, want, s.Snapshot(nil), tag)
			assert.False(t, s.NeedsRebuild(), tag)
		}
	}
	// Bytes that are not a tag are legacy buckets, as before.
	for _, tag := range []string{"SKxx", "SK0", "sk04", "XK04"} {
		legacy := sketchWith(8, []uint64{5, 5}, []uint64{0, 0, 9})
		assert.True(t, legacy.UnmarshalBinary(append([]byte(tag), body...)), tag)
		assert.True(t, legacy.NeedsRebuild(), tag)
	}
}

// packLevel starts from the width the counters had when they were scanned. A
// counter that has grown past it since is still stored whole.
func TestIndexSketch_PackLevelWidens(t *testing.T) {
	level := []uint64{3, 0x1234, 0, 0x01020304}
	for estimate := 1; estimate <= 4; estimate++ {
		out := make([]byte, 8*len(level))
		planes := packLevel(out, level, estimate)
		require.Equal(t, 4, planes, "estimate %d", estimate)
		got := make([]uint64, len(level))
		readPlanes(got, out[:planes*len(level)], planes)
		assert.Equal(t, level, got, "estimate %d", estimate)
	}
	// wider than needed is stored as asked
	out := make([]byte, 8*len(level))
	assert.Equal(t, 6, packLevel(out, level, 6))
	// an all-zero scan writes nothing
	assert.Equal(t, 0, packLevel(nil, level, 0))

	// what is left above the planes written, over all counters
	assert.Equal(t, uint64(0x12|0x010203), writePlanes(make([]byte, 4), level, 1))
	assert.Equal(t, uint64(0x0102), writePlanes(make([]byte, 8), level, 2))
	assert.Equal(t, uint64(0x01), writePlanes(make([]byte, 12), level, 3))
	assert.Equal(t, uint64(0), writePlanes(make([]byte, 32), level, 8))
}

// A marshal beside the writer never stores a counter cut short: every loaded
// count is one the counter held, so the counts of a counter that only grows
// never go down from one blob to the next.
func TestIndexSketch_MarshalCompressedBesideWriter(t *testing.T) {
	const incs = 70_000 // crosses one and two bytes
	s := NewIndexSketch(4, 1)
	key := []byte("k")
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < incs; i++ {
			s.Increment(0, key)
		}
	}()
	got := NewIndexSketch(4, 1)
	var blob []byte
	var last uint64
	for running := true; running; {
		select {
		case <-done:
			running = false
		default:
		}
		blob = s.MarshalCompressed(blob)
		got.UnmarshalBinary(blob)
		v := got.Estimate(0, key)
		require.GreaterOrEqual(t, v, last)
		last = v
	}
	assert.Equal(t, uint64(incs), last)
}

func FuzzIndexSketch_UnmarshalBinary(f *testing.F) {
	src := sketchWith(8, []uint64{300, 0, 7}, []uint64{1, 2, 3, 70000})
	f.Add(src.MarshalBinary(nil))
	f.Add(src.MarshalCompressed(nil))
	f.Add(NewIndexSketch(8, 2).MarshalCompressed(nil))
	f.Add(make([]byte, 8*8+8))
	f.Add([]byte("SK03"))
	f.Add([]byte("SK020000000X"))
	f.Add(append([]byte("SK04"), make([]byte, 8*8+8)...))
	f.Fuzz(func(t *testing.T, data []byte) {
		s := sketchWith(8, []uint64{5, 5}, []uint64{0, 0, 9})
		before := s.Snapshot(nil)
		same := s.SameShape(data)
		s.UnmarshalBinary(data)
		if len(s.Buckets) != s.Size*s.Levels || len(s.levelTotals) != s.Levels {
			t.Fatalf("shape %dx%d with %d buckets, %d totals", s.Levels, s.Size, len(s.Buckets), len(s.levelTotals))
		}
		if same && (s.Size != 8 || s.Levels != 2) {
			t.Fatalf("reshaped to %dx%d by bytes SameShape accepted", s.Levels, s.Size)
		}
		if len(data) >= 4 && sketchTagged([4]byte(data[:4])) && [4]byte(data[:4]) != sketchMagic {
			if s.NeedsRebuild() {
				t.Fatal("a tagged blob loaded as legacy buckets")
			}
			// loaded, or left alone
			if _, _, _, block, _, ok := s2Shape(data); ok {
				if _, err := s2.Decode(nil, block); err == nil {
					return
				}
			}
			if got := s.Snapshot(nil); !slices.Equal(before, got) {
				t.Fatal("a blob that does not parse changed the sketch")
			}
		}
	})
}

func FuzzIndexSketch_CompressedRoundTrip(f *testing.F) {
	f.Add([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff}, uint8(2), uint8(3))
	f.Add([]byte{}, uint8(1), uint8(1))
	f.Fuzz(func(t *testing.T, counters []byte, levels, size uint8) {
		s := NewIndexSketch(int(size%32)+1, int(levels%4)+1)
		for i := 0; i+8 <= len(counters) && i/8 < len(s.Buckets); i += 8 {
			s.Buckets[i/8] = binary.LittleEndian.Uint64(counters[i:])
		}
		for l := range s.levelTotals {
			s.levelTotals[l] = uint64(len(counters)) << (8 * l)
		}
		s.docCount.Store(uint64(len(counters)))
		got := NewIndexSketch(s.Size, s.Levels)
		got.UnmarshalBinary(s.MarshalCompressed(nil))
		if want, have := s.Snapshot(nil), got.Snapshot(nil); !slices.Equal(want, have) {
			t.Fatalf("round trip: want %v, have %v", want, have)
		}
	})
}

// sketchFixtures are the counter distributions the stored formats are
// measured on: docs unique values in one field, or a compound index whose
// first field has few values.
var sketchFixtures = []struct {
	name string
	make func() *IndexSketch
}{
	{"empty", func() *IndexSketch { return NewIndexSketch(DefaultSketchSize, 1) }},
	{"docs=20", func() *IndexSketch { return filledSketch(20, 1, 0) }},
	{"docs=5k", func() *IndexSketch { return filledSketch(5_000, 1, 0) }},
	{"docs=100k", func() *IndexSketch { return filledSketch(100_000, 1, 0) }},
	{"docs=1M", func() *IndexSketch { return filledSketch(1_000_000, 1, 0) }},
	{"docs=100k,hot=50", func() *IndexSketch { return filledSketch(100_000, 1, 50) }},
	{"docs=20,levels=2", func() *IndexSketch { return filledSketch(20, 2, 0) }},
	{"docs=100k,levels=2", func() *IndexSketch { return filledSketch(100_000, 2, 0) }},
}

// filledSketch counts docs documents into a sketch of the given levels. The
// deepest level sees a value per document, or one of hot values when hot is
// set; a shallower level sees ten values.
func filledSketch(docs, levels, hot int) *IndexSketch {
	s := NewIndexSketch(DefaultSketchSize, levels)
	var key [16]byte
	for i := 0; i < docs; i++ {
		for l := 0; l < levels; l++ {
			v := uint64(i)
			if l < levels-1 {
				v = uint64(i % 10)
			} else if hot > 0 {
				// squared: the low values take most of the documents
				v = uint64(i%hot) * uint64(i%hot) / uint64(hot)
			}
			binary.LittleEndian.PutUint64(key[:], v)
			binary.LittleEndian.PutUint64(key[8:], uint64(l))
			s.Increment(l, key[:])
		}
		s.IncrementDocCount()
	}
	return s
}

// plainS2 is the V2 body under S2 with no byte planes: what the compressed
// format is measured against.
func plainS2(s *IndexSketch, dst, scratch []byte) (out, v2 []byte) {
	v2 = s.MarshalBinary(scratch)
	return s2.Encode(dst[:cap(dst)], v2[sketchHeaderLen:]), v2
}

func BenchmarkIndexSketch_Marshal(b *testing.B) {
	for _, fx := range sketchFixtures {
		s := fx.make()
		b.Run("V2/"+fx.name, func(b *testing.B) {
			var buf []byte
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				buf = s.MarshalBinary(buf)
			}
			b.ReportMetric(float64(len(buf)), "B/blob")
		})
		b.Run("PlainS2/"+fx.name, func(b *testing.B) {
			var buf, scratch []byte
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				buf, scratch = plainS2(s, buf, scratch)
			}
			b.ReportMetric(float64(len(buf)), "B/blob")
		})
		b.Run("Compressed/"+fx.name, func(b *testing.B) {
			var buf []byte
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				buf = s.MarshalCompressed(buf)
			}
			b.ReportMetric(float64(len(buf)), "B/blob")
		})
	}
}

// BenchmarkIndexSketch_Unmarshal loads a blob into a sketch of its shape.
// "same" loads it over the counters it already holds, as a reload that finds
// nothing changed; "changed" alternates it with an empty sketch's blob, so
// every non-zero counter is stored on each load.
func BenchmarkIndexSketch_Unmarshal(b *testing.B) {
	for _, fx := range sketchFixtures {
		s := fx.make()
		empty := NewIndexSketch(s.Size, s.Levels)
		for _, enc := range []struct {
			name        string
			blob, blank []byte
		}{
			{"V2", s.MarshalBinary(nil), empty.MarshalBinary(nil)},
			{"Compressed", s.MarshalCompressed(nil), empty.MarshalCompressed(nil)},
		} {
			b.Run(enc.name+"/same/"+fx.name, func(b *testing.B) {
				dst := NewIndexSketch(s.Size, s.Levels)
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					dst.UnmarshalBinary(enc.blob)
				}
			})
			b.Run(enc.name+"/changed/"+fx.name, func(b *testing.B) {
				dst := NewIndexSketch(s.Size, s.Levels)
				blobs := [2][]byte{enc.blob, enc.blank}
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					dst.UnmarshalBinary(blobs[i&1])
				}
			})
		}
	}
}
