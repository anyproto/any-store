package vivf

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

// legacyMetaHex is an IVF-SQ meta record as the last release with IVF-PQ
// encoded it (dim 8, nlist 4, m 4, assign 2, nprobe 3, cosine, int8, sq,
// count 10, nextLabel 12, reconBase 0.5, driftSum 1.5, driftN 3, buildCount
// 10, churn 2, build 42). The record this code writes for the same meta must
// be these bytes: earlier releases keep reading what this one writes.
const legacyMetaHex = "010000000800000004000000040000000200000003000000010a000000000000000c000000000000000000e03f000000000000f83f03000000000000000a0000000000000002000000000000000000000001012a00000000000000"

func TestMetaRoundTrip(t *testing.T) {
	in := &meta{dim: 8, nlist: 4, pqM: 4, assign: 2, nprobe: 3, normalize: true,
		count: 10, nextLabel: 12, reconBase: 0.5, driftSum: 1.5, driftN: 3, buildCount: 10, churn: 2, build: 42}
	b := encodeMeta(in)
	require.Equal(t, legacyMetaHex, hex.EncodeToString(b))
	out, err := decodeMeta(b)
	require.NoError(t, err)
	require.Equal(t, in, out)
}

// The reserved subquantizer slot is never written as 0 — earlier releases
// divide by it at open — and a fresh build fills it with their default.
func TestMetaLegacySubquantizers(t *testing.T) {
	legacy, err := hex.DecodeString(legacyMetaHex)
	require.NoError(t, err)
	mt, err := decodeMeta(legacy)
	require.NoError(t, err)
	require.Equal(t, 4, mt.pqM)
	require.Equal(t, legacy, encodeMeta(mt), "a decoded record re-encodes to the same bytes")
	for dim, want := range map[int]int{768: 96, 128: 64, 24: 8, 8: 4, 3: 3, 1: 1} {
		require.Equal(t, want, legacySubquantizers(dim), "dim %d", dim)
		require.NotZero(t, legacySubquantizers(dim))
	}
}

// A record without the IVF-SQ layout flag names the removed IVF-PQ layout
// (:cell held PQ codes); decoding refuses it rather than scanning codes as int8
// vectors — whether the flag is clear or the record predates it.
func TestMetaRejectsPQ(t *testing.T) {
	b := encodeMeta(&meta{dim: 8, nlist: 4, pqM: 4, build: 1})
	flag := len(b) - 9 // the layout flag precedes the 8-byte build tail
	b[flag] = 0
	_, err := decodeMeta(b)
	require.ErrorIs(t, err, ErrUnsupportedPQ)
	_, err = decodeMeta(b[:flag])
	require.ErrorIs(t, err, ErrUnsupportedPQ)
}
