package vindex

import (
	"encoding/binary"
	"errors"
	"math"
	"unsafe"
)

// On-disk record formats for the four namespaces. Integers are little-endian.
// Vectors are stored/read as the host float32 layout (amd64 and arm64 are both
// little-endian); a cross-endian build would need an explicit swap.

const metaVersion = 1

// meta is the single vmeta record (key metaKey).
type meta struct {
	dim          int
	metric       Metric
	m            int
	m0           int
	efC          int
	efS          int
	ml           float64
	entryLabel   uint32
	topLayer     int32
	count        int64
	nextLabel    uint32
	deletedCount int64
	hasEntry     bool
}

var metaKey = []byte("m")

func encodeMeta(buf []byte, mt *meta) []byte {
	var b [4]byte
	put32 := func(v uint32) { binary.LittleEndian.PutUint32(b[:], v); buf = append(buf, b[:]...) }
	put64 := func(v uint64) {
		var x [8]byte
		binary.LittleEndian.PutUint64(x[:], v)
		buf = append(buf, x[:]...)
	}
	put32(metaVersion)
	put32(uint32(mt.dim))
	buf = append(buf, byte(mt.metric))
	put32(uint32(mt.m))
	put32(uint32(mt.m0))
	put32(uint32(mt.efC))
	put32(uint32(mt.efS))
	put64(math.Float64bits(mt.ml))
	put32(mt.entryLabel)
	put32(uint32(mt.topLayer))
	put64(uint64(mt.count))
	put32(mt.nextLabel)
	put64(uint64(mt.deletedCount))
	if mt.hasEntry {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	return buf
}

func decodeMeta(data []byte) (*meta, error) {
	if len(data) < 4 {
		return nil, errors.New("vindex: meta too short")
	}
	off := 0
	get32 := func() uint32 { v := binary.LittleEndian.Uint32(data[off:]); off += 4; return v }
	get64 := func() uint64 { v := binary.LittleEndian.Uint64(data[off:]); off += 8; return v }
	if v := get32(); v != metaVersion {
		return nil, errors.New("vindex: unsupported meta version")
	}
	mt := &meta{}
	mt.dim = int(get32())
	mt.metric = Metric(data[off])
	off++
	mt.m = int(get32())
	mt.m0 = int(get32())
	mt.efC = int(get32())
	mt.efS = int(get32())
	mt.ml = math.Float64frombits(get64())
	mt.entryLabel = get32()
	mt.topLayer = int32(get32())
	mt.count = int64(get64())
	mt.nextLabel = get32()
	mt.deletedCount = int64(get64())
	mt.hasEntry = data[off] != 0
	return mt, nil
}

// adjacency record: level(2) | flags(1) | per layer [count(2) labels(4*count)].
const adjFlagDeleted = 1 << 0

func encodeAdj(buf []byte, level int32, deleted bool, neighbors [][]uint32) []byte {
	var b [4]byte
	binary.LittleEndian.PutUint16(b[:2], uint16(level))
	buf = append(buf, b[0], b[1])
	var flags byte
	if deleted {
		flags |= adjFlagDeleted
	}
	buf = append(buf, flags)
	for lc := int32(0); lc <= level; lc++ {
		ids := neighbors[lc]
		binary.LittleEndian.PutUint16(b[:2], uint16(len(ids)))
		buf = append(buf, b[0], b[1])
		for _, id := range ids {
			binary.LittleEndian.PutUint32(b[:], id)
			buf = append(buf, b[:]...)
		}
	}
	return buf
}

// adjHeader returns the node's level and deleted flag without decoding all
// neighbour lists (cheap fast path for tombstone checks).
func adjHeader(data []byte) (level int32, deleted bool, err error) {
	if len(data) < 3 {
		return 0, false, errors.New("vindex: adj too short")
	}
	level = int32(binary.LittleEndian.Uint16(data))
	deleted = data[2]&adjFlagDeleted != 0
	return level, deleted, nil
}

// adjNeighbors returns the neighbour labels of a node at the given layer,
// aliasing into data is avoided — it decodes into out (reused).
func adjNeighbors(data []byte, layer int32, out []uint32) ([]uint32, error) {
	if len(data) < 3 {
		return nil, errors.New("vindex: adj too short")
	}
	level := int32(binary.LittleEndian.Uint16(data))
	off := 3
	out = out[:0]
	for lc := int32(0); lc <= level; lc++ {
		if off+2 > len(data) {
			return nil, errors.New("vindex: truncated adj")
		}
		cnt := int(binary.LittleEndian.Uint16(data[off:]))
		off += 2
		if lc == layer {
			for i := 0; i < cnt; i++ {
				out = append(out, binary.LittleEndian.Uint32(data[off:]))
				off += 4
			}
			return out, nil
		}
		off += cnt * 4
	}
	return out, nil // layer above this node's level → no neighbours
}

// decodeAllAdj returns level, deleted, and all per-layer neighbour lists.
func decodeAllAdj(data []byte) (level int32, deleted bool, neighbors [][]uint32, err error) {
	level, deleted, err = adjHeader(data)
	if err != nil {
		return 0, false, nil, err
	}
	off := 3
	neighbors = make([][]uint32, level+1)
	for lc := int32(0); lc <= level; lc++ {
		cnt := int(binary.LittleEndian.Uint16(data[off:]))
		off += 2
		ids := make([]uint32, cnt)
		for i := 0; i < cnt; i++ {
			ids[i] = binary.LittleEndian.Uint32(data[off:])
			off += 4
		}
		neighbors[lc] = ids
	}
	return level, deleted, neighbors, nil
}

// f32bytes reinterprets a []float32 as its host byte view (write path).
func f32bytes(v []float32) []byte {
	if len(v) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&v[0])), len(v)*4)
}

// bytesAsF32 reinterprets a byte slice (length dim*4, 4-byte aligned) as
// []float32 without copying (read path).
func bytesAsF32(b []byte, dim int) []float32 {
	if len(b) < dim*4 {
		return nil
	}
	return unsafe.Slice((*float32)(unsafe.Pointer(&b[0])), dim)
}

func labelKey(buf []byte, label uint32) []byte {
	buf = buf[:0]
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], label) // big-endian → cursor in label order
	return append(buf, b[:]...)
}
