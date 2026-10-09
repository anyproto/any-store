package vivf

import (
	"encoding/binary"
	"errors"
	"math"
	"math/rand/v2"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/vecf"
)

// On-disk record formats for the IVF-SQ namespaces (little-endian). Mirrors the
// conventions of internal/vindex/codec.go. Namespaces (prefix + suffix):
//
//	:meta  fixed key "m"            -> index parameters + counters
//	:cb    "coarse"                 -> coarse centroids (raw float32 blob)
//	:cell  listID(be32)‖label(be32) -> int8 record of the vector (the inverted lists)
//	:vec   (empty)                  -> kept so earlier releases, which require it, open the index
//	:lbl   label(be32)              -> docID
//	:doc   docID                    -> label(le32) + cells
//
// The layout is byte-for-byte the one every earlier release wrote for IVF-SQ
// (the removed IVF-PQ layout used :vec as its re-rank store): a file this code
// writes to stays readable by them (multiprocess, mixed versions).
const (
	nsMeta = ":meta"
	nsCB   = ":cb"
	nsCell = ":cell"
	nsVec  = ":vec"
	nsLbl  = ":lbl"
	nsDoc  = ":doc"
)

var (
	metaKey   = []byte("m")
	coarseKey = []byte("coarse")
)

const metaVersion = 1

// meta is the single :meta record. The record keeps the slots of the removed
// IVF-PQ layout (subquantizer count, precomputed-table budget, int8 flag) so
// the build id stays at its offset and earlier releases keep reading it; the
// layout flag that follows them is what tells the two layouts apart (see
// decodeMeta).
type meta struct {
	dim       int
	nlist     int
	pqM       int  // reserved subquantizer count: earlier releases divide by it at open, so never 0
	assign    int  // closure factor used at build (informational)
	nprobe    int  // default cells to scan at search
	normalize bool // cosine: vectors stored/queried unit-normalized
	nextLabel uint32
	count     int64

	// Drift tracking (cheap, maintained incrementally): centroid quality decays as
	// the data distribution shifts away from the build-time centroids. reconBase is
	// the mean squared residual norm (‖x−nearestCentroid‖²) over the build set;
	// driftSum/driftN accumulate the same for inserts since the last build, so
	// (driftSum/driftN)/reconBase is how much worse new data fits the centroids.
	// buildCount is the live count at the last build and churn counts writes since,
	// for a churn-ratio backstop. See StoreIndex.DriftScore.
	reconBase  float64
	driftSum   float64
	driftN     int64
	buildCount int64
	churn      int64
	// build identifies the build of the index: a random value taken when the
	// index is built or rebuilt, never by a write. An open object serves a
	// view only for the build the view's meta names (BuildOf): a rebuild can
	// land every namespace back on its old root pages, and the object's RAM
	// state — the centroids — belongs to its own build.
	// Persisted at the meta tail (back-compat: absent in older records => 0,
	// equal to any other 0).
	build uint64
}

func encodeMeta(mt *meta) []byte {
	buf := make([]byte, 0, 64)
	var b [8]byte
	put32 := func(v uint32) { binary.LittleEndian.PutUint32(b[:4], v); buf = append(buf, b[:4]...) }
	put64 := func(v uint64) { binary.LittleEndian.PutUint64(b[:8], v); buf = append(buf, b[:8]...) }
	put32(metaVersion)
	put32(uint32(mt.dim))
	put32(uint32(mt.nlist))
	put32(uint32(mt.pqM)) // reserved subquantizer count (see meta.pqM)
	put32(uint32(mt.assign))
	put32(uint32(mt.nprobe))
	if mt.normalize {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	put64(uint64(mt.count))
	put32(mt.nextLabel)
	putF := func(f float64) { put64(math.Float64bits(f)) }
	putF(mt.reconBase)
	putF(mt.driftSum)
	put64(uint64(mt.driftN))
	put64(uint64(mt.buildCount))
	put64(uint64(mt.churn))
	put32(0)             // reserved: precomputed-table budget
	buf = append(buf, 1) // reserved: int8 flag (always set for IVF-SQ)
	buf = append(buf, 1) // layout flag: IVF-SQ
	put64(mt.build)
	return buf
}

// ErrUnsupportedPQ is returned when a meta record names the removed IVF-PQ
// layout (:cell held PQ codes, not int8 vectors): this code cannot read such
// an index. The db layer quarantines it by its catalog mode before ever
// opening it here; this guards the raw store against a stray record.
var ErrUnsupportedPQ = errors.New("vivf: IVF-PQ index layout is no longer supported; drop the index and recreate it")

func decodeMeta(data []byte) (*meta, error) {
	if len(data) < 4 {
		return nil, errors.New("vivf: meta too short")
	}
	off := 0
	get32 := func() uint32 { v := binary.LittleEndian.Uint32(data[off:]); off += 4; return v }
	get64 := func() uint64 { v := binary.LittleEndian.Uint64(data[off:]); off += 8; return v }
	if get32() != metaVersion {
		return nil, errors.New("vivf: unsupported meta version")
	}
	mt := &meta{}
	mt.dim = int(get32())
	mt.nlist = int(get32())
	mt.pqM = int(get32()) // reserved subquantizer count, carried through
	mt.assign = int(get32())
	mt.nprobe = int(get32())
	mt.normalize = data[off] != 0
	off++
	mt.count = int64(get64())
	mt.nextLabel = get32()
	if off+40 <= len(data) { // drift fields (absent in pre-drift records → 0)
		mt.reconBase = math.Float64frombits(get64())
		mt.driftSum = math.Float64frombits(get64())
		mt.driftN = int64(get64())
		mt.buildCount = int64(get64())
		mt.churn = int64(get64())
	}
	if off+4 <= len(data) { // reserved: precomputed-table budget
		off += 4
	}
	if off < len(data) { // reserved: int8 flag
		off++
	}
	// The layout flag: set on every IVF-SQ record (the mode and the flag
	// arrived together); absent or clear means the removed IVF-PQ layout.
	if off >= len(data) || data[off] == 0 {
		return nil, ErrUnsupportedPQ
	}
	off++
	if off+8 <= len(data) { // build (absent in older records → 0)
		mt.build = binary.LittleEndian.Uint64(data[off:])
		off += 8
	}
	return mt, nil
}

// legacySubquantizers is the value earlier releases stored in the reserved
// subquantizer slot for an index of this dimension (their default when the
// caller set none): the largest of a small candidate set that divides dim
// with at least two components per subspace, else dim. Those releases divide
// by it at open, so a record this code writes carries the same non-zero value.
func legacySubquantizers(dim int) int {
	for _, m := range []int{96, 64, 48, 32, 16, 8, 4, 2} {
		if dim%m == 0 && dim/m >= 2 {
			return m
		}
	}
	return dim
}

// newBuild returns the identity of a build (meta.build).
func newBuild() uint64 {
	for {
		if b := rand.Uint64(); b != 0 {
			return b
		}
	}
}

// BuildOf returns the build the index's meta record in rtx's view names
// (meta.build); 0 for a record written before builds were identified.
func BuildOf(rtx *btree.ReadTx, vmeta *btree.Namespace) (uint64, error) {
	b, err := rtx.Get(vmeta, metaKey)
	if err != nil {
		return 0, err
	}
	mt, err := decodeMeta(b)
	if err != nil {
		return 0, err
	}
	return mt.build, nil
}

// encodeCentroids flattens [k][dim] float32 to a raw little-endian blob.
func encodeCentroids(cents [][]float32) []byte {
	if len(cents) == 0 {
		return nil
	}
	dim := len(cents[0])
	out := make([]byte, 0, len(cents)*dim*4)
	for _, c := range cents {
		out = append(out, f32bytes(c)...)
	}
	return out
}

// cellKey returns listID(be32)‖label(be32) so a cell is a contiguous key range.
func cellKey(buf []byte, listID, label uint32) []byte {
	buf = buf[:0]
	var b [8]byte
	binary.BigEndian.PutUint32(b[0:], listID)
	binary.BigEndian.PutUint32(b[4:], label)
	return append(buf, b[:]...)
}

// cellKeyLabel extracts the label from a :cell key.
func cellKeyLabel(key []byte) uint32 { return binary.BigEndian.Uint32(key[4:]) }

// f32bytes / bytesAsF32 are thin wrappers over the shared casts in
// internal/vecf, kept for call-site brevity (like normalizeInto over
// simd.NormalizeInto).
func f32bytes(v []float32) []byte { return vecf.F32Bytes(v) }

func bytesAsF32(b []byte, dim int) []float32 { return vecf.BytesAsF32(b, dim) }
