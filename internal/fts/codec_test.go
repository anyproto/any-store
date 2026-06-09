package fts

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChunk_RoundTrip(t *testing.T) {
	in := []Posting{
		{DocID: 3, Positions: []uint32{0, 5, 17}},
		{DocID: 4, Positions: []uint32{2}},
		{DocID: 130, Positions: []uint32{0, 1, 2, 3}},
		{DocID: 9001, Positions: []uint32{42}},
	}
	blob := AppendChunk(nil, in)
	assert.Equal(t, byte(PostingsVersion), blob[0])

	out, err := DecodeChunk(nil, blob)
	require.NoError(t, err)
	assert.Equal(t, in, out)
}

func TestChunk_SingleDoc(t *testing.T) {
	in := []Posting{{DocID: 0, Positions: []uint32{0}}}
	out, err := DecodeChunk(nil, AppendChunk(nil, in))
	require.NoError(t, err)
	assert.Equal(t, in, out)
}

func TestChunk_Empty(t *testing.T) {
	blob := AppendChunk(nil, nil)
	require.Len(t, blob, 1) // just the version byte
	out, err := DecodeChunk(nil, blob)
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestChunkReader_SkipPositions(t *testing.T) {
	// Advancing without reading positions must still land on correct DocIDs/TF.
	in := []Posting{
		{DocID: 1, Positions: []uint32{0, 1, 2}},
		{DocID: 7, Positions: []uint32{9}},
		{DocID: 12, Positions: []uint32{3, 8}},
	}
	r, err := NewChunkReader(AppendChunk(nil, in))
	require.NoError(t, err)

	var gotDocs []uint64
	var gotTF []uint32
	for r.Next() {
		gotDocs = append(gotDocs, r.DocID())
		gotTF = append(gotTF, r.TF())
		// deliberately do NOT call AppendPositions for most docs
	}
	require.NoError(t, r.Err())
	assert.Equal(t, []uint64{1, 7, 12}, gotDocs)
	assert.Equal(t, []uint32{3, 1, 2}, gotTF)
}

func TestChunkReader_PositionsOnDemand(t *testing.T) {
	in := []Posting{
		{DocID: 1, Positions: []uint32{0, 4}},
		{DocID: 2, Positions: []uint32{7, 9, 11}},
	}
	r, err := NewChunkReader(AppendChunk(nil, in))
	require.NoError(t, err)

	require.True(t, r.Next())
	assert.Equal(t, uint64(1), r.DocID())
	// skip positions of doc 1 by not reading them
	require.True(t, r.Next())
	assert.Equal(t, uint64(2), r.DocID())
	assert.Equal(t, []uint32{7, 9, 11}, r.AppendPositions(nil))
	// idempotent: second call returns nothing more (already consumed)
	assert.Empty(t, r.AppendPositions(nil))
	require.False(t, r.Next())
	require.NoError(t, r.Err())
}

func TestChunkReader_BadVersion(t *testing.T) {
	_, err := NewChunkReader([]byte{0xFF, 0x00})
	assert.ErrorIs(t, err, ErrUnknownVersion)
}

func TestChunkReader_EmptyBlob(t *testing.T) {
	_, err := NewChunkReader(nil)
	assert.ErrorIs(t, err, ErrCorruptChunk)
}

func TestChunkReader_TruncatedIsDetected(t *testing.T) {
	in := []Posting{{DocID: 5, Positions: []uint32{1, 2, 3}}}
	blob := AppendChunk(nil, in)
	// Chop the last byte to truncate a position varint stream.
	r, err := NewChunkReader(blob[:len(blob)-1])
	require.NoError(t, err)
	for r.Next() {
		r.AppendPositions(nil)
	}
	assert.ErrorIs(t, r.Err(), ErrCorruptChunk)
}

func TestChunkID(t *testing.T) {
	assert.Equal(t, uint64(0), ChunkID(0))
	assert.Equal(t, uint64(0), ChunkID(127))
	assert.Equal(t, uint64(1), ChunkID(128))
	assert.Equal(t, uint64(70), ChunkID(9001))
}

// buildFullChunk makes a realistic full (128-doc) chunk: a common term with a
// few positions per doc.
func buildFullChunk() []byte {
	postings := make([]Posting, ChunkSize)
	for i := 0; i < ChunkSize; i++ {
		postings[i] = Posting{
			DocID:     uint64(i * 3), // gaps, ascending
			Positions: []uint32{uint32(i), uint32(i + 10), uint32(i + 25)},
		}
	}
	return AppendChunk(nil, postings)
}

func BenchmarkDecodeChunk_Full(b *testing.B) {
	blob := buildFullChunk()
	b.ReportAllocs()
	b.SetBytes(int64(len(blob)))
	dst := make([]Posting, 0, ChunkSize)
	pos := make([]uint32, 0, 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ := NewChunkReader(blob)
		dst = dst[:0]
		for r.Next() {
			pos = r.AppendPositions(pos[:0])
			_ = r.DocID()
			_ = pos
		}
	}
}

func BenchmarkDecodeChunk_DocIDsOnly(b *testing.B) {
	// Scoring/merge that only needs DocIDs+TF and skips positions.
	blob := buildFullChunk()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ := NewChunkReader(blob)
		var sum uint64
		for r.Next() {
			sum += r.DocID() + uint64(r.TF())
		}
		_ = sum
	}
}
