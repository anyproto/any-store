package vivf

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/anyproto/any-store/v2/internal/vecf"
)

// On-disk record formats for the IVF-PQ namespaces (little-endian). Mirrors the
// conventions of internal/vindex/codec.go. Namespaces (prefix + suffix):
//
//	:meta  fixed key "m"          -> index parameters + counters
//	:cb    "coarse" / "pq"        -> codebooks (raw float32 blobs)
//	:cell  listID(be32)‖label(be32) -> M-byte PQ code   (the inverted lists)
//	:vec   label(be32)            -> full/quantized vector (re-rank store)
//	:lbl   label(be32)            -> docID
//	:doc   docID                  -> label(le32)
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
	pqKey     = []byte("pq")
)

const metaVersion = 1

// meta is the single :meta record.
type meta struct {
	dim        int
	nlist      int
	m          int
	assign     int  // closure factor used at build (informational)
	nprobe     int  // default cells to scan at search
	precompMiB int  // precomputed-table RAM budget (StoreParams.PrecompMiB)
	normalize  bool // cosine: vectors stored/queried unit-normalized
	int8vec    bool // :vec re-rank store is int8-quantized
	sq         bool // IVF-SQ: :cell holds int8 full vectors (no PQ)
	count      int64
	nextLabel  uint32

	// Drift tracking (cheap, maintained incrementally): centroid quality decays as
	// the data distribution shifts away from the build-time codebooks. reconBase is
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
}

func encodeMeta(mt *meta) []byte {
	buf := make([]byte, 0, 64)
	var b [8]byte
	put32 := func(v uint32) { binary.LittleEndian.PutUint32(b[:4], v); buf = append(buf, b[:4]...) }
	put64 := func(v uint64) { binary.LittleEndian.PutUint64(b[:8], v); buf = append(buf, b[:8]...) }
	put32(metaVersion)
	put32(uint32(mt.dim))
	put32(uint32(mt.nlist))
	put32(uint32(mt.m))
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
	put32(uint32(int32(mt.precompMiB))) // signed; <0 disables the table
	if mt.int8vec {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	if mt.sq {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	return buf
}

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
	mt.m = int(get32())
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
	if off+4 <= len(data) { // precompMiB (absent in older records → 0 = default)
		mt.precompMiB = int(int32(get32()))
	}
	if off < len(data) { // int8vec (absent in older records → false)
		mt.int8vec = data[off] != 0
		off++
	}
	if off < len(data) { // sq (absent in older records → false)
		mt.sq = data[off] != 0
		off++
	}
	return mt, nil
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

// encodePQ flattens [m][pqK][dsub] to a raw blob (m·pqK·dsub floats).
func encodePQ(pqcb [][][]float32) []byte {
	if len(pqcb) == 0 {
		return nil
	}
	dsub := len(pqcb[0][0])
	out := make([]byte, 0, len(pqcb)*pqK*dsub*4)
	for _, cb := range pqcb {
		for _, c := range cb {
			out = append(out, f32bytes(c)...)
		}
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
