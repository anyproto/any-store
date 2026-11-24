package vindex

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// An arena block holds 256 vectors, fewer when that passes
// vecCacheMaxBlockBytes, and none when one vector alone does: its size is
// never computed past what an int holds. At dim 2^23 on a 32-bit target,
// 256 vectors were a negative length and a panic in make.
func TestVecCacheBlockShiftFor(t *testing.T) {
	for _, tc := range []struct {
		dim   int
		shift uint
		ok    bool
	}{
		{1, 8, true},
		{768, 8, true},
		{1 << 16, 8, true},    // 256 × 256 KiB = 64 MiB
		{1<<16 + 1, 7, true},  // a vector past 256 KiB: 128 a block
		{1 << 23, 1, true},    // 32 MiB vectors: two a block
		{1<<23 + 1, 0, true},  // one a block
		{1 << 24, 0, true},    // a 64 MiB vector fills a block
		{1<<24 + 1, 0, false}, // larger than a block: not cached
		{math.MaxInt, 0, false},
		{0, 0, false},
	} {
		shift, ok := vecCacheBlockShiftFor(tc.dim)
		require.Equal(t, tc.ok, ok, "dim %d", tc.dim)
		if !ok {
			continue
		}
		require.Equal(t, tc.shift, shift, "dim %d", tc.dim)
		require.LessOrEqual(t, (1<<shift)*tc.dim*4, vecCacheMaxBlockBytes, "dim %d", tc.dim)
	}
}

// With vectors too large for a block the cache reads every vector uncached:
// reserve hands out no slot, so no block is allocated. With smaller vectors
// it caches, and the vectors of a later block resolve through the shift and
// mask of the block size.
func TestVecCacheReserve(t *testing.T) {
	var c vecCache
	c.reset(1<<24+1, 16)
	dst, hit := c.reserve(7)
	require.Nil(t, dst)
	require.False(t, hit)
	require.Empty(t, c.blocks)

	for _, tc := range []struct {
		dim, labels, blocks int
	}{
		{4, 600, 3},       // 256 vectors a block
		{1<<23 + 1, 2, 2}, // one vector a block
	} {
		c.reset(tc.dim, tc.labels)
		for label := range uint32(tc.labels) {
			dst, hit := c.reserve(label)
			require.False(t, hit)
			require.Len(t, dst, tc.dim)
			dst[0], dst[tc.dim-1] = float32(label), float32(label)+0.5
		}
		require.Len(t, c.blocks, tc.blocks, "dim %d", tc.dim)
		for label := range uint32(tc.labels) {
			v, hit := c.reserve(label)
			require.True(t, hit)
			require.Equal(t, float32(label), v[0], "dim %d label %d", tc.dim, label)
			require.Equal(t, float32(label)+0.5, v[tc.dim-1], "dim %d label %d", tc.dim, label)
		}
	}
}
