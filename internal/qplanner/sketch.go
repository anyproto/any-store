package qplanner

import (
	"encoding/binary"
	"math"
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

// DistinctEstimate returns an estimate of the number of distinct values indexed
// by the sketch, in a single atomic-load pass over the buckets. It powers the
// per-column "indexed equality" selectivity heuristic used when a per-value
// frequency cannot be read directly — e.g. the leading column of a compound
// index, whose sketch is keyed on the full composite tuple.
//
// It uses the occupancy (coupon-collector) estimator: if k of Size buckets are
// non-empty, the expected number of distinct hashed values is
// D = -Size*ln(1 - k/Size). The result is capped at the document count (a hard
// upper bound on distinct values) — mandatory because a saturated sketch (k==Size,
// e.g. a unique compound index) drives the ln term to infinity. An empty/cold
// sketch returns 1, so the caller clamps back to the default selectivity.
func (s *IndexSketch) DistinctEstimate() float64 {
	dc := float64(s.docCount.Load())
	if dc <= 0 || s.Size == 0 {
		return 1
	}
	nonEmpty := 0
	for i := range s.Size {
		if atomic.LoadUint64(&s.Buckets[i]) != 0 {
			nonEmpty++
		}
	}
	if nonEmpty == 0 {
		return 1
	}
	if nonEmpty >= s.Size {
		// Fully saturated: the occupancy estimator diverges (distinct >> Size).
		// The tightest sound bound we have is the document count.
		return dc
	}
	fill := float64(nonEmpty) / float64(s.Size)
	d := -float64(s.Size) * math.Log(1-fill)
	if d > dc {
		d = dc
	}
	if d < 1 {
		d = 1
	}
	return d
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
//
// Atomically stores into Buckets and docCount, so concurrent readers
// (GetDocCount/Estimate) see consistent values without locking. Trailing
// buckets beyond what data covers are zeroed atomically so that loading
// a smaller-sized payload into a larger live sketch can't leak stale
// counts (defends a future change where Size becomes configurable —
// currently Size is the constant DefaultSketchSize across all calls).
func (s *IndexSketch) UnmarshalBinary(data []byte) {
	n := len(data) / 8
	hasDocCount := n > s.Size
	if n > s.Size {
		n = s.Size
	}
	for i := range n {
		atomic.StoreUint64(&s.Buckets[i], binary.LittleEndian.Uint64(data[i*8:]))
	}
	for i := n; i < s.Size; i++ {
		// Defensively zero any trailing buckets the payload didn't cover.
		atomic.StoreUint64(&s.Buckets[i], 0)
	}
	if hasDocCount && len(data) >= 8*s.Size+8 {
		s.docCount.Store(binary.LittleEndian.Uint64(data[8*s.Size:]))
	}
}

// SketchDistribution summarizes the frequency distribution held in a sketch's
// bucket array. Because values are hashed into a fixed number of buckets, it
// describes per-bucket load rather than exact per-value cardinality; it is
// still a good indicator of skew and of how saturated the sketch is.
type SketchDistribution struct {
	// NonEmptyBuckets is the number of buckets with a count greater than zero.
	NonEmptyBuckets int
	// Fill is NonEmptyBuckets/Size — the sketch's saturation. A value near 1.0
	// means distinct values far exceed Size and per-value estimates degrade.
	Fill float64
	// MinNonEmpty is the smallest non-zero bucket count.
	MinNonEmpty uint64
	// MaxBucket is the largest bucket count — the heaviest indexed value(s).
	MaxBucket uint64
	// MeanNonEmpty is the mean count over non-empty buckets.
	MeanNonEmpty float64
	// P50, P90 and P99 are percentile bucket counts over non-empty buckets.
	P50 uint64
	P90 uint64
	P99 uint64
	// Skew is MaxBucket/MeanNonEmpty — values well above 1 flag a hot value.
	Skew float64
}

// Distribution computes a summary of the sketch's bucket frequency
// distribution in a single atomic-load pass. An empty sketch yields the zero
// value.
func (s *IndexSketch) Distribution() SketchDistribution {
	var d SketchDistribution
	if s.Size == 0 {
		return d
	}
	counts := make([]uint64, 0, s.Size)
	var sum uint64
	for i := range s.Size {
		c := atomic.LoadUint64(&s.Buckets[i])
		if c == 0 {
			continue
		}
		counts = append(counts, c)
		sum += c
		if c > d.MaxBucket {
			d.MaxBucket = c
		}
		if d.MinNonEmpty == 0 || c < d.MinNonEmpty {
			d.MinNonEmpty = c
		}
	}
	d.NonEmptyBuckets = len(counts)
	d.Fill = float64(len(counts)) / float64(s.Size)
	if len(counts) == 0 {
		return d
	}
	d.MeanNonEmpty = float64(sum) / float64(len(counts))
	if d.MeanNonEmpty > 0 {
		d.Skew = float64(d.MaxBucket) / d.MeanNonEmpty
	}
	slices.Sort(counts)
	d.P50 = percentile(counts, 0.50)
	d.P90 = percentile(counts, 0.90)
	d.P99 = percentile(counts, 0.99)
	return d
}

// percentile returns the p-quantile of an ascending-sorted slice using the
// nearest-rank method. sorted must be non-empty.
func percentile(sorted []uint64, p float64) uint64 {
	idx := int(p * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// Reset zeroes all buckets and the document count.
func (s *IndexSketch) Reset() {
	for i := range s.Buckets {
		atomic.StoreUint64(&s.Buckets[i], 0)
	}
	s.docCount.Store(0)
}
