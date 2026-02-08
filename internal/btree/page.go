package btree

// SQLite-compatible page format implementation.
//
// Database header (first 100 bytes of page 1):
//
//	Offset  Size  Description
//	0       16    Magic string "BTree format 1\000"
//	16      2     Page size in bytes
//	18      1     File format write version (1=legacy, 2=WAL)
//	19      1     File format read version (1=legacy, 2=WAL)
//	20      1     Reserved space at end of each page
//	21      1     Max embedded payload fraction (must be 64)
//	22      1     Min embedded payload fraction (must be 32)
//	23      1     Leaf payload fraction (must be 32)
//	24      4     File change counter
//	28      4     Size of database in pages
//	32      4     First freelist trunk page
//	36      4     Total freelist pages
//	40      4     Schema cookie
//	44      4     Schema format number
//	48      4     Default page cache size
//	52      4     Largest root b-tree page (auto-vacuum/incremental-vacuum)
//	56      4     Database text encoding (1=UTF8, 2=UTF16le, 3=UTF16be)
//	60      4     User version
//	64      4     Incremental vacuum mode
//	68      4     Application ID
//	72      20    Reserved for expansion (must be zero)
//	92      4     Version-valid-for number
//	96      4     SQLite version number
//
// B-tree page header (immediately after DB header on page 1):
//
//	Offset  Size  Description
//	0       1     Page type flag (2=interior index, 5=interior table,
//	                              10=leaf index, 13=leaf table)
//	1       2     Offset to first free block
//	3       2     Number of cells on page
//	5       2     Offset to first byte of cell content area
//	7       1     Number of fragmented free bytes
//	8       4     Right-most child pointer (interior pages only)
//
// Cell format for table leaf pages:
//
//	varint   Total payload size
//	varint   Row ID (key)
//	payload  Actual data (may overflow to overflow pages)
//
// Cell format for table interior pages:
//
//	4 bytes  Left child page number
//	varint   Row ID (key)

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	// DefaultPageSize is the default database page size (4KB).
	DefaultPageSize = 4096

	// MinPageSize is the minimum allowed page size.
	MinPageSize = 512

	// MaxPageSize is the maximum allowed page size.
	MaxPageSize = 65536

	// dbHeaderSize is the size of the database file header on page 1.
	dbHeaderSize = 100

	// Magic string identifying our database format.
	dbMagic = "BTree format 1\x00"

	// Page type flags (matching SQLite).
	pageTypeIntIdx  = 2  // Interior index b-tree page
	pageTypeIntTbl  = 5  // Interior table b-tree page
	pageTypeLeafIdx = 10 // Leaf index b-tree page
	pageTypeLeafTbl = 13 // Leaf table b-tree page

	// pageTypeFreelistTrunk marks a freelist trunk page.
	pageTypeFreelistTrunk = 0

	// maxEmbeddedPayloadFrac is the max fraction of page for embedded payload.
	maxEmbeddedPayloadFrac = 64

	// minEmbeddedPayloadFrac is the min fraction of page for embedded payload.
	minEmbeddedPayloadFrac = 32

	// leafPayloadFrac is the fraction of page for leaf payload.
	leafPayloadFrac = 32
)

// dbHeader represents the 100-byte database file header.
type dbHeader struct {
	PageSize         uint32
	WriteVersion     uint8
	ReadVersion      uint8
	ReservedSpace    uint8
	FileChangeCount  uint32
	DatabaseSize     uint32 // in pages
	FirstFreelistPg  uint32
	TotalFreelistPgs uint32
	SchemaCookie     uint32
	SchemaFormat     uint32
	DefaultCacheSize uint32
	TextEncoding     uint32
	UserVersion      uint32
	AppID            uint32
	VersionValidFor  uint32
}

// serialize writes the database header to the first 100 bytes of buf.
func (h *dbHeader) serialize(buf []byte) {
	copy(buf[0:16], dbMagic)

	var ps uint16
	if h.PageSize >= 65536 {
		ps = 1 // SQLite convention: page size 65536 stored as 1
	} else {
		ps = uint16(h.PageSize)
	}
	binary.BigEndian.PutUint16(buf[16:18], ps)

	buf[18] = h.WriteVersion
	buf[19] = h.ReadVersion
	buf[20] = h.ReservedSpace
	buf[21] = maxEmbeddedPayloadFrac
	buf[22] = minEmbeddedPayloadFrac
	buf[23] = leafPayloadFrac

	binary.BigEndian.PutUint32(buf[24:28], h.FileChangeCount)
	binary.BigEndian.PutUint32(buf[28:32], h.DatabaseSize)
	binary.BigEndian.PutUint32(buf[32:36], h.FirstFreelistPg)
	binary.BigEndian.PutUint32(buf[36:40], h.TotalFreelistPgs)
	binary.BigEndian.PutUint32(buf[40:44], h.SchemaCookie)
	binary.BigEndian.PutUint32(buf[44:48], h.SchemaFormat)
	binary.BigEndian.PutUint32(buf[48:52], h.DefaultCacheSize)

	// Largest root b-tree page number (not used for now)
	binary.BigEndian.PutUint32(buf[52:56], 0)

	binary.BigEndian.PutUint32(buf[56:60], h.TextEncoding)
	binary.BigEndian.PutUint32(buf[60:64], h.UserVersion)

	// Incremental vacuum mode (0 = off)
	binary.BigEndian.PutUint32(buf[64:68], 0)

	binary.BigEndian.PutUint32(buf[68:72], h.AppID)

	// Reserved for expansion
	clear(buf[72:92])

	binary.BigEndian.PutUint32(buf[92:96], h.VersionValidFor)

	// Version number
	binary.BigEndian.PutUint32(buf[96:100], 1)
}

// deserialize reads the database header from the first 100 bytes of buf.
func (h *dbHeader) deserialize(buf []byte) error {
	if len(buf) < dbHeaderSize {
		return ErrCorrupt
	}

	if string(buf[0:15]) != dbMagic[:15] {
		return ErrCorrupt
	}

	ps := binary.BigEndian.Uint16(buf[16:18])
	if ps == 1 {
		h.PageSize = 65536
	} else {
		h.PageSize = uint32(ps)
	}

	h.WriteVersion = buf[18]
	h.ReadVersion = buf[19]
	h.ReservedSpace = buf[20]

	h.FileChangeCount = binary.BigEndian.Uint32(buf[24:28])
	h.DatabaseSize = binary.BigEndian.Uint32(buf[28:32])
	h.FirstFreelistPg = binary.BigEndian.Uint32(buf[32:36])
	h.TotalFreelistPgs = binary.BigEndian.Uint32(buf[36:40])
	h.SchemaCookie = binary.BigEndian.Uint32(buf[40:44])
	h.SchemaFormat = binary.BigEndian.Uint32(buf[44:48])
	h.DefaultCacheSize = binary.BigEndian.Uint32(buf[48:52])
	h.TextEncoding = binary.BigEndian.Uint32(buf[56:60])
	h.UserVersion = binary.BigEndian.Uint32(buf[60:64])
	h.AppID = binary.BigEndian.Uint32(buf[68:72])
	h.VersionValidFor = binary.BigEndian.Uint32(buf[92:96])

	return nil
}

// pageHeader represents the B-tree page header.
type pageHeader struct {
	pageType       uint8  // one of pageType* constants
	firstFreeBlk   uint16 // offset to first free block, 0 if none
	cellCount      uint16 // number of cells on this page
	cellContentOff uint16 // offset to first byte of cell content area
	fragBytes      uint8  // number of fragmented free bytes
	rightChild     uint32 // right-most child pointer (interior pages only)
}

// headerSize returns the size of this page header in bytes.
func (ph *pageHeader) headerSize() int {
	if ph.pageType == pageTypeIntIdx || ph.pageType == pageTypeIntTbl {
		return 12
	}
	return 8
}

// isLeaf returns true if this is a leaf page.
func (ph *pageHeader) isLeaf() bool {
	return ph.pageType == pageTypeLeafIdx || ph.pageType == pageTypeLeafTbl
}

// isInterior returns true if this is an interior page.
func (ph *pageHeader) isInterior() bool {
	return ph.pageType == pageTypeIntIdx || ph.pageType == pageTypeIntTbl
}

// serialize writes the page header to buf.
func (ph *pageHeader) serialize(buf []byte) {
	buf[0] = ph.pageType
	binary.BigEndian.PutUint16(buf[1:3], ph.firstFreeBlk)
	binary.BigEndian.PutUint16(buf[3:5], ph.cellCount)
	binary.BigEndian.PutUint16(buf[5:7], ph.cellContentOff)
	buf[7] = ph.fragBytes
	if ph.isInterior() {
		binary.BigEndian.PutUint32(buf[8:12], ph.rightChild)
	}
}

// deserialize reads the page header from buf.
func (ph *pageHeader) deserialize(buf []byte) {
	ph.pageType = buf[0]
	ph.firstFreeBlk = binary.BigEndian.Uint16(buf[1:3])
	ph.cellCount = binary.BigEndian.Uint16(buf[3:5])
	ph.cellContentOff = binary.BigEndian.Uint16(buf[5:7])
	ph.fragBytes = buf[7]
	if ph.isInterior() {
		ph.rightChild = binary.BigEndian.Uint32(buf[8:12])
	}
}

// page represents an in-memory database page.
type page struct {
	pgno     uint32 // page number (1-based)
	data     []byte // raw page data
	dirty    bool   // true if page has been modified
	pinCount int    // reference count
	header   pageHeader

	// For page cache linked list
	next *page
	prev *page
}

// usableSize returns the usable size of the page (total minus reserved).
func (p *page) usableSize(reservedSpace uint8) int {
	return len(p.data) - int(reservedSpace)
}

// cellPointerOffset returns the offset of the cell pointer array.
func (p *page) cellPointerOffset() int {
	off := p.header.headerSize()
	if p.pgno == 1 {
		off += dbHeaderSize
	}
	return off
}

// getCellOffset returns the offset of the i-th cell in the page data.
func (p *page) getCellOffset(i int) uint16 {
	base := p.cellPointerOffset() + i*2
	return binary.BigEndian.Uint16(p.data[base : base+2])
}

// setCellOffset sets the offset of the i-th cell in the cell pointer array.
func (p *page) setCellOffset(i int, off uint16) {
	base := p.cellPointerOffset() + i*2
	binary.BigEndian.PutUint16(p.data[base:base+2], off)
}

// Varint encoding/decoding (SQLite format).
// SQLite varints are big-endian, 1-9 bytes.

// putVarint writes a varint to buf and returns the number of bytes written.
func putVarint(buf []byte, v uint64) int {
	if v <= 0x7f {
		buf[0] = byte(v)
		return 1
	}
	if v <= 0x3fff {
		buf[0] = byte((v >> 7) | 0x80)
		buf[1] = byte(v & 0x7f)
		return 2
	}
	if v <= 0x1fffff {
		buf[0] = byte((v >> 14) | 0x80)
		buf[1] = byte((v >> 7) | 0x80)
		buf[2] = byte(v & 0x7f)
		return 3
	}
	if v <= 0x0fffffff {
		buf[0] = byte((v >> 21) | 0x80)
		buf[1] = byte((v >> 14) | 0x80)
		buf[2] = byte((v >> 7) | 0x80)
		buf[3] = byte(v & 0x7f)
		return 4
	}
	if v <= 0x07ffffffff {
		buf[0] = byte((v >> 28) | 0x80)
		buf[1] = byte((v >> 21) | 0x80)
		buf[2] = byte((v >> 14) | 0x80)
		buf[3] = byte((v >> 7) | 0x80)
		buf[4] = byte(v & 0x7f)
		return 5
	}
	if v <= 0x03ffffffffff {
		buf[0] = byte((v >> 35) | 0x80)
		buf[1] = byte((v >> 28) | 0x80)
		buf[2] = byte((v >> 21) | 0x80)
		buf[3] = byte((v >> 14) | 0x80)
		buf[4] = byte((v >> 7) | 0x80)
		buf[5] = byte(v & 0x7f)
		return 6
	}
	if v <= 0x01ffffffffffff {
		buf[0] = byte((v >> 42) | 0x80)
		buf[1] = byte((v >> 35) | 0x80)
		buf[2] = byte((v >> 28) | 0x80)
		buf[3] = byte((v >> 21) | 0x80)
		buf[4] = byte((v >> 14) | 0x80)
		buf[5] = byte((v >> 7) | 0x80)
		buf[6] = byte(v & 0x7f)
		return 7
	}
	if v <= 0x00ffffffffffffff {
		buf[0] = byte((v >> 49) | 0x80)
		buf[1] = byte((v >> 42) | 0x80)
		buf[2] = byte((v >> 35) | 0x80)
		buf[3] = byte((v >> 28) | 0x80)
		buf[4] = byte((v >> 21) | 0x80)
		buf[5] = byte((v >> 14) | 0x80)
		buf[6] = byte((v >> 7) | 0x80)
		buf[7] = byte(v & 0x7f)
		return 8
	}
	// 9-byte case: bytes 0-7 encode (v >> 8) as 8×7 bits, byte 8 encodes low 8 bits
	buf[0] = byte((v>>57)&0x7f | 0x80)
	buf[1] = byte((v>>50)&0x7f | 0x80)
	buf[2] = byte((v>>43)&0x7f | 0x80)
	buf[3] = byte((v>>36)&0x7f | 0x80)
	buf[4] = byte((v>>29)&0x7f | 0x80)
	buf[5] = byte((v>>22)&0x7f | 0x80)
	buf[6] = byte((v>>15)&0x7f | 0x80)
	buf[7] = byte((v>>8)&0x7f | 0x80)
	buf[8] = byte(v)
	return 9
}

// getVarint reads a varint from buf and returns the value and number of bytes consumed.
func getVarint(buf []byte) (uint64, int) {
	if buf[0] < 0x80 {
		return uint64(buf[0]), 1
	}

	var v uint64
	for i := range 8 {
		b := buf[i]
		if b < 0x80 {
			v = (v << 7) | uint64(b)
			return v, i + 1
		}
		v = (v << 7) | uint64(b&0x7f)
	}
	// 9th byte uses all 8 bits
	v = (v << 8) | uint64(buf[8])
	return v, 9
}

// varintSize returns the number of bytes needed to encode v as a varint.
func varintSize(v uint64) int {
	switch {
	case v <= 0x7f:
		return 1
	case v <= 0x3fff:
		return 2
	case v <= 0x1fffff:
		return 3
	case v <= 0x0fffffff:
		return 4
	case v <= 0x07ffffffff:
		return 5
	case v <= 0x03ffffffffff:
		return 6
	case v <= 0x01ffffffffffff:
		return 7
	case v <= 0x00ffffffffffffff:
		return 8
	default:
		return 9
	}
}

// checksum computes a CRC32 checksum for data (used in WAL frames).
func checksum(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
