package fts

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sf builds a single-field posting (field 0), the common case: FieldMask = 1,
// one TF = len(positions).
func sf(docID uint64, pos ...uint32) Posting {
	return Posting{DocID: docID, Fields: 1, FieldTF: []uint32{uint32(len(pos))}, Positions: pos}
}

func TestChunk_RoundTrip(t *testing.T) {
	in := []Posting{
		sf(3, 0, 5, 17),
		sf(4, 2),
		sf(130, 0, 1, 2, 3),
		sf(9001, 42),
	}
	blob := AppendChunk(nil, in)
	assert.Equal(t, byte(PostingsVersion), blob[0])

	out, err := DecodeChunk(nil, blob)
	require.NoError(t, err)
	assert.Equal(t, in, out)
}

func TestChunk_MultiField_RoundTrip(t *testing.T) {
	// Field 0 (title) + field 2 (tag): mask = 0b101, TFs in ascending field order.
	in := []Posting{
		{DocID: 1, Fields: 0b101, FieldTF: []uint32{1, 2}, Positions: []uint32{0, 200, 201}},
		{DocID: 2, Fields: 0b010, FieldTF: []uint32{3}, Positions: []uint32{100, 101, 102}},
	}
	out, err := DecodeChunk(nil, AppendChunk(nil, in))
	require.NoError(t, err)
	assert.Equal(t, in, out)
}

func TestChunkReader_FieldTFAccessor(t *testing.T) {
	in := []Posting{{DocID: 7, Fields: 0b1010, FieldTF: []uint32{4, 9}, Positions: make([]uint32, 13)}}
	// fill positions ascending so decode is valid
	for i := range in[0].Positions {
		in[0].Positions[i] = uint32(i)
	}
	r, err := NewChunkReader(AppendChunk(nil, in))
	require.NoError(t, err)
	require.True(t, r.Next())
	assert.Equal(t, uint64(0b1010), r.FieldMask())
	assert.Equal(t, uint32(0), r.FieldTF(0))
	assert.Equal(t, uint32(4), r.FieldTF(1)) // first set bit
	assert.Equal(t, uint32(0), r.FieldTF(2))
	assert.Equal(t, uint32(9), r.FieldTF(3)) // second set bit
	assert.Equal(t, uint32(13), r.TF())      // total
	assert.Equal(t, []uint32{4, 9}, r.AppendFieldTFs(nil))
}

func TestChunk_SingleDoc(t *testing.T) {
	in := []Posting{sf(0, 0)}
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
	in := []Posting{sf(1, 0, 1, 2), sf(7, 9), sf(12, 3, 8)}
	r, err := NewChunkReader(AppendChunk(nil, in))
	require.NoError(t, err)

	var gotDocs []uint64
	var gotTF []uint32
	for r.Next() {
		gotDocs = append(gotDocs, r.DocID())
		gotTF = append(gotTF, r.TF())
	}
	require.NoError(t, r.Err())
	assert.Equal(t, []uint64{1, 7, 12}, gotDocs)
	assert.Equal(t, []uint32{3, 1, 2}, gotTF)
}

func TestChunkReader_PositionsOnDemand(t *testing.T) {
	in := []Posting{sf(1, 0, 4), sf(2, 7, 9, 11)}
	r, err := NewChunkReader(AppendChunk(nil, in))
	require.NoError(t, err)

	require.True(t, r.Next())
	assert.Equal(t, uint64(1), r.DocID())
	require.True(t, r.Next())
	assert.Equal(t, uint64(2), r.DocID())
	assert.Equal(t, []uint32{7, 9, 11}, r.AppendPositions(nil))
	assert.Empty(t, r.AppendPositions(nil))
	require.False(t, r.Next())
	require.NoError(t, r.Err())
}

func TestChunkReader_Reset(t *testing.T) {
	a := AppendChunk(nil, []Posting{sf(1, 0), sf(2, 1)})
	b := AppendChunk(nil, []Posting{sf(5, 0, 1)})
	r, err := NewChunkReader(a)
	require.NoError(t, err)
	require.True(t, r.Next())
	require.NoError(t, r.Reset(b))
	require.True(t, r.Next())
	assert.Equal(t, uint64(5), r.DocID())
	assert.Equal(t, uint32(2), r.TF())
	require.False(t, r.Next())
}

func TestChunkReader_BadVersion(t *testing.T) {
	_, err := NewChunkReader([]byte{0xFF, 0x00})
	assert.ErrorIs(t, err, ErrUnknownVersion)
	// A v1 blob (version byte 1) is rejected — it must be migrated, not decoded.
	_, err = NewChunkReader([]byte{0x01, 0x00})
	assert.ErrorIs(t, err, ErrUnknownVersion)
}

func TestChunkReader_EmptyBlob(t *testing.T) {
	_, err := NewChunkReader(nil)
	assert.ErrorIs(t, err, ErrCorruptChunk)
}

func TestChunkReader_TruncatedIsDetected(t *testing.T) {
	blob := AppendChunk(nil, []Posting{sf(5, 1, 2, 3)})
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

// buildFullChunk makes a realistic full (128-doc) chunk: a common single-field
// term with a few positions per doc.
func buildFullChunk() []byte {
	postings := make([]Posting, ChunkSize)
	for i := 0; i < ChunkSize; i++ {
		postings[i] = sf(uint64(i*3), uint32(i), uint32(i+10), uint32(i+25))
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
