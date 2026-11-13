package qplanner

import (
	"encoding/binary"
	"math/bits"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
	"github.com/klauspost/compress/s2"
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
// All methods are safe for concurrent use via atomic operations, except
// Snapshot: the writer's.
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
// commits; a savepoint's rollback restores the sketches the savepoint wrote
// (Snapshot, Restore), and the deltas of a tx that rolled back or whose
// commit failed survive until the next write tx begins
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

// sketchMagicS2 tags the compressed on-disk format (MarshalCompressed).
var sketchMagicS2 = [4]byte{'S', 'K', '0', '3'}

// sketchTagged reports whether tag has the form of a sketch tag: the letters
// SK and two digits. One this release does not know is an encoding of a later
// release.
func sketchTagged(tag [4]byte) bool {
	return tag[0] == 'S' && tag[1] == 'K' && tag[2] >= '0' && tag[2] <= '9' && tag[3] >= '0' && tag[3] <= '9'
}

// sketchHeaderLen is the V2 header size: magic(4) + levels(4) + size(4). The
// compressed format has one more byte per level after it.
const sketchHeaderLen = 12

// maxCompressedBuckets bounds levels*size of a compressed blob: what a header
// may claim before its block is decoded, and so what MarshalCompressed writes.
// 128 levels of DefaultSketchSize.
const maxCompressedBuckets = 1 << 17

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

// MarshalCompressed serializes the sketch into dst in the compressed format,
// reusing its capacity when possible.
//
// The counters of a level are written as byte planes: byte 0 of every bucket,
// then byte 1, and so on up to the widest count of the level. The zero high
// bytes of the counters are not stored, and the low ones lie together for S2:
// a near-empty sketch takes about 100 bytes and a saturated level 1 to 1.5 KiB,
// where the V2 format takes 8 KiB per level.
//
// Format: [magic 4][levels u32][size u32][planes u8 per level][S2 block]. The
// block holds, per level, planes[l] planes of size bytes each, then
// [levelTotals levels*8][docCount 8].
//
// A sketch above maxCompressedBuckets is written in the V2 format, which
// UnmarshalBinary reads wherever it reads this one.
//
// Counters may move while it runs, as under MarshalBinary: each is loaded
// atomically, and a count that outgrows the planes chosen for its level
// widens them.
func (s *IndexSketch) MarshalCompressed(dst []byte) []byte {
	nb := s.Size * s.Levels
	if nb > maxCompressedBuckets {
		return s.MarshalBinary(dst)
	}
	hdr := sketchHeaderLen + s.Levels
	dst = slices.Grow(dst[:0], hdr)[:hdr]
	copy(dst[0:4], sketchMagicS2[:])
	binary.LittleEndian.PutUint32(dst[4:], uint32(s.Levels))
	binary.LittleEndian.PutUint32(dst[8:], uint32(s.Size))

	// The block's content is laid out in one scratch buffer and encoded into
	// another, each sized for what it holds.
	scratch := getSketchScratch(8*nb + 8*s.Levels + 8)
	body := *scratch
	n := 0
	for l := 0; l < s.Levels; l++ {
		level := s.Buckets[l*s.Size : (l+1)*s.Size]
		var all uint64
		for i := range level {
			all |= atomic.LoadUint64(&level[i])
		}
		planes := packLevel(body[n:], level, byteWidth(all))
		dst[sketchHeaderLen+l] = byte(planes)
		n += planes * s.Size
	}
	for l := 0; l < s.Levels; l++ {
		binary.LittleEndian.PutUint64(body[n:], atomic.LoadUint64(&s.levelTotals[l]))
		n += 8
	}
	binary.LittleEndian.PutUint64(body[n:], s.docCount.Load())
	n += 8

	enc := getSketchScratch(s2.MaxEncodedLen(n))
	dst = append(dst, s2.Encode(*enc, body[:n])...)
	putSketchScratch(enc)
	putSketchScratch(scratch)
	return dst
}

// byteWidth returns the number of bytes v needs: 0 for zero.
func byteWidth(v uint64) int {
	return (bits.Len64(v) + 7) / 8
}

// packLevel writes the counters of level into out as byte planes and returns
// how many it wrote. planes is the width the counters had a moment ago: a
// counter that has outgrown it since widens the planes, so none is stored cut
// short. A level that was all zeros stays so, as if marshaled a moment earlier.
func packLevel(out []byte, level []uint64, planes int) int {
	for planes > 0 {
		over := writePlanes(out[:planes*len(level)], level, planes)
		if over == 0 {
			break
		}
		planes += byteWidth(over)
	}
	return planes
}

// writePlanes writes the low planes bytes of every counter of level into
// out, plane by plane, and returns the bits the counters have above them:
// zero unless one is wider than planes bytes. Each counter is loaded once, so
// its bytes come from one value.
func writePlanes(out []byte, level []uint64, planes int) (over uint64) {
	size := len(level)
	switch planes {
	case 1:
		p0 := out[:size]
		for i := range level {
			v := atomic.LoadUint64(&level[i])
			p0[i] = byte(v)
			over |= v >> 8
		}
	case 2:
		p0, p1 := out[:size], out[size:2*size]
		for i := range level {
			v := atomic.LoadUint64(&level[i])
			p0[i] = byte(v)
			p1[i] = byte(v >> 8)
			over |= v >> 16
		}
	default:
		for i := range level {
			v := atomic.LoadUint64(&level[i])
			for j := i; j < planes*size; j += size {
				out[j] = byte(v)
				v >>= 8
			}
			over |= v
		}
	}
	return over
}

// readPlanes sets the counters of level from src, which holds their low
// planes bytes plane by plane; the bytes above them are zero.
func readPlanes(level []uint64, src []byte, planes int) {
	size := len(level)
	switch planes {
	case 0:
		for i := range level {
			setCounter(&level[i], 0)
		}
	case 1:
		p0 := src[:size]
		for i := range level {
			setCounter(&level[i], uint64(p0[i]))
		}
	case 2:
		p0, p1 := src[:size], src[size:2*size]
		for i := range level {
			setCounter(&level[i], uint64(p0[i])|uint64(p1[i])<<8)
		}
	default:
		for i := range level {
			var v uint64
			for j := i + (planes-1)*size; j >= 0; j -= size {
				v = v<<8 | uint64(src[j])
			}
			setCounter(&level[i], v)
		}
	}
}

// setCounter stores v unless the counter holds it already: a load costs a
// fraction of an atomic store, and a reload finds most counters unchanged.
// The counter has one mutator while a blob is loaded into it.
func setCounter(p *uint64, v uint64) {
	if atomic.LoadUint64(p) != v {
		atomic.StoreUint64(p, v)
	}
}

// sketchScratch pools the buffers MarshalCompressed and the compressed load
// work in: a sketch has no buffer of its own for either. A buffer is as large
// as a block's content or its encoding, which maxCompressedBuckets bounds.
var sketchScratch sync.Pool

// getSketchScratch returns a buffer of n bytes.
func getSketchScratch(n int) *[]byte {
	p, _ := sketchScratch.Get().(*[]byte)
	if p == nil {
		p = new([]byte)
	}
	if cap(*p) < n {
		*p = make([]byte, n)
	}
	*p = (*p)[:n]
	return p
}

func putSketchScratch(p *[]byte) {
	sketchScratch.Put(p)
}

// UnmarshalBinary deserializes the sketch. A V2 or compressed blob
// (magic-tagged) is loaded directly, adopting its shape; a legacy V1 blob (no
// magic) is loaded into the full-key level with the shallow levels left empty
// and flagged for rebuild. The magic alone decides which: a tagged blob that
// does not parse, down to a cut-off header, leaves the sketch as it was and is
// never read as legacy buckets. Nor is a blob under a tag of a later release:
// it gives this one no statistics, not wrong ones. The result reports whether
// data was loaded.
//
// Atomic stores into Buckets/levelTotals/docCount let concurrent readers see
// consistent values without locking. Reshaping (reallocating the backing slices
// to match a tagged blob's declared shape) only happens for a not-yet-published
// sketch — construction or InspectIndexSketch. A live, published sketch always
// carries the index's fixed (Size, Levels), so the reshape branch never
// reallocates a slice a concurrent reader might hold.
func (s *IndexSketch) UnmarshalBinary(data []byte) bool {
	if len(data) >= len(sketchMagic) {
		switch [4]byte(data[:4]) {
		case sketchMagic:
			return s.unmarshalV2(data)
		case sketchMagicS2:
			return s.unmarshalS2(data)
		}
		if sketchTagged([4]byte(data[:4])) {
			return false
		}
	}
	s.unmarshalLegacy(data)
	return true
}

// v2Shape parses a V2 blob's header into its levels and size. ok is false
// for a blob without the magic, a corrupt header or a truncated body: none of
// these loads into a sketch with a reshape.
func v2Shape(data []byte) (levels, size int, ok bool) {
	if len(data) < sketchHeaderLen || [4]byte{data[0], data[1], data[2], data[3]} != sketchMagic {
		return 0, 0, false
	}
	l := uint64(binary.LittleEndian.Uint32(data[4:]))
	sz := uint64(binary.LittleEndian.Uint32(data[8:]))
	n := uint64(len(data))
	// Bounded by the blob before any arithmetic in bytes: a header may claim
	// a shape whose byte size does not fit an int.
	if l < 1 || sz < 1 || l*sz > n/8 || n < sketchHeaderLen+8*l*sz+8*l+8 {
		return 0, 0, false
	}
	return int(l), int(sz), true
}

// s2Shape parses a compressed blob's header: its levels and size, the planes
// stored per level and the S2 block, with the length the block decodes to.
// ok is false for a blob without the magic, a corrupt header, or a block that
// does not declare the length the header implies: none of these loads into a
// sketch.
func s2Shape(data []byte) (levels, size int, planes, block []byte, bodyLen int, ok bool) {
	if len(data) < sketchHeaderLen || [4]byte(data[:4]) != sketchMagicS2 {
		return 0, 0, nil, nil, 0, false
	}
	l := binary.LittleEndian.Uint32(data[4:])
	sz := binary.LittleEndian.Uint32(data[8:])
	if l < 1 || sz < 1 || uint64(l)*uint64(sz) > maxCompressedBuckets || len(data) < sketchHeaderLen+int(l) {
		return 0, 0, nil, nil, 0, false
	}
	levels, size = int(l), int(sz)
	planes = data[sketchHeaderLen : sketchHeaderLen+levels]
	block = data[sketchHeaderLen+levels:]
	bodyLen = 8*levels + 8
	for _, p := range planes {
		if p > 8 {
			return 0, 0, nil, nil, 0, false
		}
		bodyLen += int(p) * size
	}
	if n, err := s2.DecodedLen(block); err != nil || n != bodyLen {
		return 0, 0, nil, nil, 0, false
	}
	return levels, size, planes, block, bodyLen, true
}

// SameShape reports whether UnmarshalBinary would load data without
// reshaping the sketch: a V2 or compressed blob of its size and levels, or
// bytes that load into the existing shape or leave it alone (legacy, corrupt,
// truncated). A published sketch may be reloaded in place only when this
// holds, since a reshape replaces slices a concurrent reader holds.
func (s *IndexSketch) SameShape(data []byte) bool {
	levels, size, ok := v2Shape(data)
	if !ok {
		levels, size, _, _, _, ok = s2Shape(data)
	}
	return !ok || (levels == s.Levels && size == s.Size)
}

func (s *IndexSketch) unmarshalV2(data []byte) bool {
	levels, size, ok := v2Shape(data)
	if !ok {
		return false // corrupt header or truncated body: preserve current state
	}
	nb := size * levels
	if size != s.Size || levels != s.Levels {
		s.Buckets = make([]uint64, nb)
		s.levelTotals = make([]uint64, levels)
		s.Size = size
		s.Levels = levels
	}
	off := sketchHeaderLen
	for i := 0; i < nb; i++ {
		setCounter(&s.Buckets[i], binary.LittleEndian.Uint64(data[off+i*8:]))
	}
	off += 8 * nb
	for l := 0; l < levels; l++ {
		atomic.StoreUint64(&s.levelTotals[l], binary.LittleEndian.Uint64(data[off+l*8:]))
	}
	off += 8 * levels
	s.docCount.Store(binary.LittleEndian.Uint64(data[off:]))
	s.needsRebuild.Store(false)
	return true
}

func (s *IndexSketch) unmarshalS2(data []byte) bool {
	levels, size, planes, block, bodyLen, ok := s2Shape(data)
	if !ok {
		return false // corrupt header or block length: preserve current state
	}
	// Decoded in full before the first store: a block that fails partway
	// leaves the counters alone.
	scratch := getSketchScratch(bodyLen)
	defer putSketchScratch(scratch)
	body, err := s2.Decode(*scratch, block)
	if err != nil {
		return false // corrupt block: preserve current state
	}
	if size != s.Size || levels != s.Levels {
		s.Buckets = make([]uint64, size*levels)
		s.levelTotals = make([]uint64, levels)
		s.Size = size
		s.Levels = levels
	}
	off := 0
	for l, p := range planes {
		n := int(p) * size
		readPlanes(s.Buckets[l*size:(l+1)*size], body[off:off+n], int(p))
		off += n
	}
	for l := 0; l < levels; l++ {
		atomic.StoreUint64(&s.levelTotals[l], binary.LittleEndian.Uint64(body[off+l*8:]))
	}
	off += 8 * levels
	s.docCount.Store(binary.LittleEndian.Uint64(body[off:]))
	s.needsRebuild.Store(false)
	return true
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

// Snapshot copies the counters of s — buckets, level totals, document
// count — into dst, reusing its capacity: the pre-image a savepoint's
// rollback puts back with Restore. Writer only: s has a single mutator, so
// plain loads see its counts.
func (s *IndexSketch) Snapshot(dst []uint64) []uint64 {
	dst = append(dst[:0], s.Buckets...)
	dst = append(dst, s.levelTotals...)
	return append(dst, s.docCount.Load())
}

// Restore sets the counters from a Snapshot of s, with atomic stores for
// the readers loading them concurrently, and reports whether it did: a
// snapshot of another shape is ignored, as a live sketch keeps its index's
// shape.
func (s *IndexSketch) Restore(src []uint64) bool {
	nb := len(s.Buckets)
	if len(src) != nb+len(s.levelTotals)+1 {
		return false
	}
	for i := range s.Buckets {
		setCounter(&s.Buckets[i], src[i])
	}
	for l := range s.levelTotals {
		atomic.StoreUint64(&s.levelTotals[l], src[nb+l])
	}
	s.docCount.Store(src[nb+len(s.levelTotals)])
	return true
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
