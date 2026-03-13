package qplanner

import (
	"encoding/binary"
	"slices"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
)

// IndexSketch is a frequency sketch for cardinality estimation.
// Each index maintains a sketch to estimate how many documents match a specific value.
// DocCount tracks the total number of documents in the collection, avoiding expensive tree traversals.
// All methods are safe for concurrent use via atomic operations.
type IndexSketch struct {
	Buckets  []uint64
	Size     int
	docCount atomic.Uint64
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
	atomic.AddUint64(&s.Buckets[s.bucket(value)], 1)
}

// Decrement decrements the count for the given encoded value.
// The count is clamped to 0 to avoid underflow.
func (s *IndexSketch) Decrement(value []byte) {
	b := s.bucket(value)
	for {
		old := atomic.LoadUint64(&s.Buckets[b])
		if old == 0 {
			return
		}
		if atomic.CompareAndSwapUint64(&s.Buckets[b], old, old-1) {
			return
		}
	}
}

// Estimate returns the estimated count for the given encoded value.
func (s *IndexSketch) Estimate(value []byte) uint64 {
	return atomic.LoadUint64(&s.Buckets[s.bucket(value)])
}

// IncrementDocCount atomically increments the document count.
func (s *IndexSketch) IncrementDocCount() {
	s.docCount.Add(1)
}

// DecrementDocCount atomically decrements the document count, clamped to 0.
func (s *IndexSketch) DecrementDocCount() {
	for {
		old := s.docCount.Load()
		if old == 0 {
			return
		}
		if s.docCount.CompareAndSwap(old, old-1) {
			return
		}
	}
}

// GetDocCount atomically returns the document count.
func (s *IndexSketch) GetDocCount() uint64 {
	return s.docCount.Load()
}

// MarshalBinary serializes the sketch into dst, reusing its capacity when possible.
// Format: [buckets (8*size bytes)] [docCount (8 bytes)]
func (s *IndexSketch) MarshalBinary(dst []byte) []byte {
	need := 8*s.Size + 8
	dst = slices.Grow(dst[:0], need)[:need]
	for i := range s.Size {
		binary.LittleEndian.PutUint64(dst[i*8:], atomic.LoadUint64(&s.Buckets[i]))
	}
	binary.LittleEndian.PutUint64(dst[8*s.Size:], s.docCount.Load())
	return dst
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
		atomic.StoreUint64(&s.Buckets[i], binary.LittleEndian.Uint64(data[i*8:]))
	}
	if hasDocCount && len(data) >= 8*s.Size+8 {
		s.docCount.Store(binary.LittleEndian.Uint64(data[8*s.Size:]))
	}
}

// Reset zeroes all buckets and the document count.
func (s *IndexSketch) Reset() {
	for i := range s.Buckets {
		atomic.StoreUint64(&s.Buckets[i], 0)
	}
	s.docCount.Store(0)
}
