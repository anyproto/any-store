package qplanner

import (
	"encoding/binary"

	"github.com/cespare/xxhash/v2"
)

// IndexSketch is a frequency sketch for cardinality estimation.
// Each index maintains a sketch to estimate how many documents match a specific value.
// DocCount tracks the total number of documents in the collection, avoiding expensive tree traversals.
type IndexSketch struct {
	Buckets  []uint64
	Size     int
	DocCount uint64
}

// NewIndexSketch creates a new sketch with the given number of buckets.
func NewIndexSketch(size int) *IndexSketch {
	if size <= 0 {
		size = DefaultSketchSize
	}
	return &IndexSketch{
		Buckets: make([]uint64, size),
		Size:    size,
	}
}

// bucket returns the bucket index for the given encoded value.
func (s *IndexSketch) bucket(value []byte) uint64 {
	return xxhash.Sum64(value) % uint64(s.Size)
}

// Increment increments the count for the given encoded value.
func (s *IndexSketch) Increment(value []byte) {
	s.Buckets[s.bucket(value)]++
}

// Decrement decrements the count for the given encoded value.
// The count is clamped to 0 to avoid underflow.
func (s *IndexSketch) Decrement(value []byte) {
	b := s.bucket(value)
	if s.Buckets[b] > 0 {
		s.Buckets[b]--
	}
}

// Estimate returns the estimated count for the given encoded value.
func (s *IndexSketch) Estimate(value []byte) uint64 {
	return s.Buckets[s.bucket(value)]
}

// MarshalBinary serializes the sketch to a byte slice.
// Format: [buckets (8*size bytes)] [docCount (8 bytes)]
func (s *IndexSketch) MarshalBinary() []byte {
	data := make([]byte, 8*s.Size+8)
	for i, v := range s.Buckets {
		binary.LittleEndian.PutUint64(data[i*8:], v)
	}
	binary.LittleEndian.PutUint64(data[8*s.Size:], s.DocCount)
	return data
}

// UnmarshalBinary deserializes the sketch from a byte slice.
// Format: [buckets (8*size bytes)] [docCount (8 bytes)]
// Backward compatible: if data has no trailing docCount, DocCount remains 0.
func (s *IndexSketch) UnmarshalBinary(data []byte) {
	n := len(data) / 8
	hasDocCount := n > s.Size
	if n > s.Size {
		n = s.Size
	}
	for i := range n {
		s.Buckets[i] = binary.LittleEndian.Uint64(data[i*8:])
	}
	if hasDocCount && len(data) >= 8*s.Size+8 {
		s.DocCount = binary.LittleEndian.Uint64(data[8*s.Size:])
	}
}

// Reset zeroes all buckets and the document count.
func (s *IndexSketch) Reset() {
	for i := range s.Buckets {
		s.Buckets[i] = 0
	}
	s.DocCount = 0
}
