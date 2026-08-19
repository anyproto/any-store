package qplanner

import (
	"encoding/binary"
	"slices"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
)

// IndexSketch is a multi-level frequency sketch for cardinality estimation.
//
// A compound index (a, b, c) keeps one isolated count-row per prefix length:
// level 0 counts values of `a`, level 1 counts `(a, b)`, level 2 counts the full
// `(a, b, c)`. Each level owns its own Size-bucket window of the flat Buckets
// array (level L → Buckets[L*Size : (L+1)*Size]); a value hashes only within its
// level, so a high-cardinality deep level cannot pollute the shallow levels the
// CBO relies on for prefix-scan selectivity. A single-field index has Levels==1
// and behaves exactly like the original single-row sketch.
//
// Two distinct totals are tracked and must not be conflated:
//   - docCount   — the collection document count, incremented ONCE per indexed
//     document regardless of how many entries it produces. This is the
//     query.docCount() proxy and is independent of sparsity / multikey fan-out.
//   - levelTotals[L] — the number of live index ENTRIES at level L. Sparse
//     documents contribute none; an array-valued field contributes one per
//     distinct element. So levelTotals[L] != docCount in general, and differs
//     between levels. Exposed via EntryCount(L).
//
// All methods are safe for concurrent use via atomic operations.
//
// # Plan-time only
//
// A sketch read may decide a PLAN — which index to pick, what cost to assign,
// what size to preallocate. It may NOT decide an ANSWER: no caller may treat a
// zero Estimate as "no rows match" and skip the scan, or return GetDocCount as
// a query result.
//
// The sketch is not snapshot-isolated, so it can disagree with the caller's
// snapshot in either direction. A write tx mutates the live sketch before it
// commits, and a rolled-back tx's deltas survive until the next write tx begins
// (db.resetUncommittedSketches). A peer process's commits arrive only via the
// advisory reload in db.checkStale, which is fail-soft: a decode error keeps
// the stale copy. A plan-time read tolerates all of this — a wrong cost means a
// slower query, and execution still runs against the snapshot. An answer-time
// read does not: a wrong estimate of zero is a wrong answer.
//
// Making an answer-time read safe requires storing sketches in the btree under
// a versioned key and consulting them through the snapshot. Freshness checks or
// retries at the call site do not substitute — they only narrow the race.
type IndexSketch struct {
	// Buckets holds Levels*Size counts; level L occupies [L*Size:(L+1)*Size].
	Buckets []uint64
	// Size is the number of buckets per level.
	Size int
	// Levels is the number of prefix levels (>=1), equal to the number of
	// fields in the index.
	Levels int

	// levelTotals[L] is the live entry count at level L (see type doc).
	levelTotals []uint64
	// docCount is the collection document count (see type doc).
	docCount atomic.Uint64
	// needsRebuild is set when a legacy single-level blob was loaded into a
	// multi-level sketch: the deep (full-key) level is populated but the shallow
	// levels are empty and must be re-accrued by a background rescan. Not
	// serialized.
	needsRebuild atomic.Bool
}

// sketchMagic tags the V2 multi-level on-disk format. Legacy V1 blobs (a single
// flat bucket row, optionally followed by an 8-byte docCount) carry no magic and
// are detected by its absence, then loaded into the full-key level.
var sketchMagic = [4]byte{'S', 'K', '0', '2'}

// sketchHeaderLen is the V2 header size: magic(4) + levels(4) + size(4).
const sketchHeaderLen = 12

// NewIndexSketch creates a sketch with the given per-level bucket count and
// number of prefix levels. A non-positive size falls back to DefaultSketchSize;
// levels below 1 are clamped to 1.
func NewIndexSketch(size, levels int) *IndexSketch {
	if size <= 0 {
		size = DefaultSketchSize
	}
	if levels < 1 {
		levels = 1
	}
	return &IndexSketch{
		Buckets:     make([]uint64, size*levels),
		Size:        size,
		Levels:      levels,
		levelTotals: make([]uint64, levels),
	}
}

// NumLevels returns the number of prefix levels.
func (s *IndexSketch) NumLevels() int { return s.Levels }

// NeedsRebuild reports whether shallow levels were left empty by a legacy load
// and should be re-accrued by a full rescan.
func (s *IndexSketch) NeedsRebuild() bool { return s.needsRebuild.Load() }

// MarkRebuilt clears the rebuild flag after a rescan has repopulated the
// shallow levels.
func (s *IndexSketch) MarkRebuilt() { s.needsRebuild.Store(false) }

func (s *IndexSketch) validLevel(level int) bool {
	return level >= 0 && level < s.Levels
}

// bucket returns the absolute bucket index for the given encoded prefix value at
// the given level.
func (s *IndexSketch) bucket(level int, value []byte) uint64 {
	return uint64(level*s.Size) + xxhash.Sum64(value)%uint64(s.Size)
}

// Increment increments the count for the given encoded prefix value at the given
// level and bumps that level's entry total. Out-of-range levels are ignored.
func (s *IndexSketch) Increment(level int, value []byte) {
	if !s.validLevel(level) {
		return
	}
	atomic.AddUint64(&s.Buckets[s.bucket(level, value)], 1)
	atomic.AddUint64(&s.levelTotals[level], 1)
}

// Decrement decrements the count for the given encoded prefix value at the given
// level and drops that level's entry total. Both are clamped to 0 to avoid
// underflow. Out-of-range levels are ignored.
func (s *IndexSketch) Decrement(level int, value []byte) {
	if !s.validLevel(level) {
		return
	}
	b := s.bucket(level, value)
	for {
		old := atomic.LoadUint64(&s.Buckets[b])
		if old == 0 {
			break
		}
		if atomic.CompareAndSwapUint64(&s.Buckets[b], old, old-1) {
			break
		}
	}
	for {
		old := atomic.LoadUint64(&s.levelTotals[level])
		if old == 0 {
			return
		}
		if atomic.CompareAndSwapUint64(&s.levelTotals[level], old, old-1) {
			return
		}
	}
}

// Estimate returns the estimated count for the given encoded prefix value at the
// given level. Out-of-range levels return 0.
//
// The result is a cost-model hint, never an answer — a zero is not proof that
// no row matches. See the "Plan-time only" section of the IndexSketch doc.
func (s *IndexSketch) Estimate(level int, value []byte) uint64 {
	if !s.validLevel(level) {
		return 0
	}
	return atomic.LoadUint64(&s.Buckets[s.bucket(level, value)])
}

// EntryCount returns the total live entry count at the given level — the honest
// denominator for that level's fraction-of-entries. Out-of-range levels return
// 0. This is NOT the collection document count (see GetDocCount and the type
// doc): sparse and multikey indexes make per-level entry counts diverge from it.
func (s *IndexSketch) EntryCount(level int) uint64 {
	if !s.validLevel(level) {
		return 0
	}
	return atomic.LoadUint64(&s.levelTotals[level])
}

// IncrementDocCount atomically increments the collection document count.
func (s *IndexSketch) IncrementDocCount() {
	s.docCount.Add(1)
}

// DecrementDocCount atomically decrements the collection document count, clamped
// to 0.
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

// GetDocCount atomically returns the collection document count.
//
// The result is a cost-model hint (or a statistic to report), never a query
// answer. See the "Plan-time only" section of the IndexSketch doc.
func (s *IndexSketch) GetDocCount() uint64 {
	return s.docCount.Load()
}

// MarshalBinary serializes the sketch into dst (V2 format), reusing its capacity
// when possible.
// Format: [magic 4][levels u32][size u32][buckets levels*size*8][levelTotals levels*8][docCount 8]
func (s *IndexSketch) MarshalBinary(dst []byte) []byte {
	nb := s.Size * s.Levels
	need := sketchHeaderLen + 8*nb + 8*s.Levels + 8
	dst = slices.Grow(dst[:0], need)[:need]
	copy(dst[0:4], sketchMagic[:])
	binary.LittleEndian.PutUint32(dst[4:], uint32(s.Levels))
	binary.LittleEndian.PutUint32(dst[8:], uint32(s.Size))
	off := sketchHeaderLen
	for i := 0; i < nb; i++ {
		binary.LittleEndian.PutUint64(dst[off+i*8:], atomic.LoadUint64(&s.Buckets[i]))
	}
	off += 8 * nb
	for l := 0; l < s.Levels; l++ {
		binary.LittleEndian.PutUint64(dst[off+l*8:], atomic.LoadUint64(&s.levelTotals[l]))
	}
	off += 8 * s.Levels
	binary.LittleEndian.PutUint64(dst[off:], s.docCount.Load())
	return dst
}

// UnmarshalBinary deserializes the sketch. A V2 blob (magic-tagged) is loaded
// directly, adopting its shape; a legacy V1 blob (no magic) is loaded into the
// full-key level with the shallow levels left empty and flagged for rebuild.
//
// Atomic stores into Buckets/levelTotals/docCount let concurrent readers see
// consistent values without locking. Reshaping (reallocating the backing slices
// to match a V2 blob's declared shape) only happens for a not-yet-published
// sketch — construction or InspectIndexSketch. A live, published sketch always
// carries the index's fixed (Size, Levels), so the reshape branch never
// reallocates a slice a concurrent reader might hold.
func (s *IndexSketch) UnmarshalBinary(data []byte) {
	if len(data) >= sketchHeaderLen && [4]byte{data[0], data[1], data[2], data[3]} == sketchMagic {
		s.unmarshalV2(data)
		return
	}
	s.unmarshalLegacy(data)
}

func (s *IndexSketch) unmarshalV2(data []byte) {
	levels := int(binary.LittleEndian.Uint32(data[4:]))
	size := int(binary.LittleEndian.Uint32(data[8:]))
	if levels < 1 || size < 1 {
		return // corrupt header: preserve current state
	}
	nb := size * levels
	if len(data) < sketchHeaderLen+8*nb+8*levels+8 {
		return // truncated: preserve current state
	}
	if size != s.Size || levels != s.Levels {
		s.Buckets = make([]uint64, nb)
		s.levelTotals = make([]uint64, levels)
		s.Size = size
		s.Levels = levels
	}
	off := sketchHeaderLen
	for i := 0; i < nb; i++ {
		atomic.StoreUint64(&s.Buckets[i], binary.LittleEndian.Uint64(data[off+i*8:]))
	}
	off += 8 * nb
	for l := 0; l < levels; l++ {
		atomic.StoreUint64(&s.levelTotals[l], binary.LittleEndian.Uint64(data[off+l*8:]))
	}
	off += 8 * levels
	s.docCount.Store(binary.LittleEndian.Uint64(data[off:]))
	s.needsRebuild.Store(false)
}

// unmarshalLegacy loads an old single-row blob (the combined-key sketch) into the
// full-key level. The shallow prefix levels have no historical data, so they are
// zeroed and the sketch is flagged for rebuild (unless Levels==1, where the
// single level already IS the full key). Backward compatible with the even older
// docCount-less format: when the trailing docCount is absent the existing
// docCount is left untouched.
func (s *IndexSketch) unmarshalLegacy(data []byte) {
	nWords := len(data) / 8
	hasDocCount := nWords > s.Size
	nBuckets := min(nWords, s.Size)
	full := s.Levels - 1
	base := full * s.Size
	var sum uint64
	for i := 0; i < nBuckets; i++ {
		v := binary.LittleEndian.Uint64(data[i*8:])
		atomic.StoreUint64(&s.Buckets[base+i], v)
		sum += v
	}
	for i := nBuckets; i < s.Size; i++ {
		atomic.StoreUint64(&s.Buckets[base+i], 0)
	}
	// Zero shallow levels — no historical data exists for them.
	for l := 0; l < full; l++ {
		b := l * s.Size
		for i := 0; i < s.Size; i++ {
			atomic.StoreUint64(&s.Buckets[b+i], 0)
		}
		atomic.StoreUint64(&s.levelTotals[l], 0)
	}
	atomic.StoreUint64(&s.levelTotals[full], sum)
	if hasDocCount && len(data) >= 8*s.Size+8 {
		s.docCount.Store(binary.LittleEndian.Uint64(data[8*s.Size:]))
	}
	s.needsRebuild.Store(s.Levels > 1)
}

// SketchDistribution summarizes the frequency distribution held in one level of
// a sketch's bucket array. Because values are hashed into a fixed number of
// buckets, it describes per-bucket load rather than exact per-value cardinality;
// it is still a good indicator of skew and of how saturated that level is.
type SketchDistribution struct {
	// NonEmptyBuckets is the number of buckets with a count greater than zero.
	NonEmptyBuckets int
	// Fill is NonEmptyBuckets/Size — the level's saturation. A value near 1.0
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

// Distribution computes a summary of the given level's bucket frequency
// distribution in a single atomic-load pass. An empty level or out-of-range
// level yields the zero value.
func (s *IndexSketch) Distribution(level int) SketchDistribution {
	var d SketchDistribution
	if s.Size == 0 || !s.validLevel(level) {
		return d
	}
	base := level * s.Size
	counts := make([]uint64, 0, s.Size)
	var sum uint64
	for i := 0; i < s.Size; i++ {
		c := atomic.LoadUint64(&s.Buckets[base+i])
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

// Reset zeroes all buckets, level totals, and the document count.
func (s *IndexSketch) Reset() {
	for i := range s.Buckets {
		atomic.StoreUint64(&s.Buckets[i], 0)
	}
	for i := range s.levelTotals {
		atomic.StoreUint64(&s.levelTotals[i], 0)
	}
	s.docCount.Store(0)
	s.needsRebuild.Store(false)
}
