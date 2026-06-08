package vivf

import (
	"encoding/binary"
	"errors"
	"unsafe"
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
	dim       int
	nlist     int
	m         int
	assign    int  // closure factor used at build (informational)
	nprobe    int  // default cells to scan at search
	normalize bool // cosine: vectors stored/queried unit-normalized
	count     int64
	nextLabel uint32
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

// decodeCentroids reconstructs k rows of dim floats from a blob (zero-copy rows).
func decodeCentroids(data []byte, k, dim int) [][]float32 {
	out := make([][]float32, k)
	for i := 0; i < k; i++ {
		out[i] = bytesAsF32(data[i*dim*4:], dim)
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

// decodePQ reconstructs [m][pqK][dsub] from a blob.
func decodePQ(data []byte, m, dsub int) [][][]float32 {
	out := make([][][]float32, m)
	off := 0
	for mm := 0; mm < m; mm++ {
		cb := make([][]float32, pqK)
		for j := 0; j < pqK; j++ {
			cb[j] = bytesAsF32(data[off:], dsub)
			off += dsub * 4
		}
		out[mm] = cb
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

// cellKeyList extracts the listID from a :cell key.
func cellKeyList(key []byte) uint32 { return binary.BigEndian.Uint32(key[0:]) }

func labelKey(buf []byte, label uint32) []byte {
	buf = buf[:0]
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], label)
	return append(buf, b[:]...)
}

// f32bytes / bytesAsF32 reinterpret float32 slices as host bytes (little-endian
// amd64/arm64), as internal/vindex/codec.go does.
func f32bytes(v []float32) []byte {
	if len(v) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&v[0])), len(v)*4)
}

func bytesAsF32(b []byte, dim int) []float32 {
	if len(b) < dim*4 {
		return nil
	}
	return unsafe.Slice((*float32)(unsafe.Pointer(&b[0])), dim)
}
