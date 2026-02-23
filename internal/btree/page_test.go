package btree

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDBHeaderSerializeDeserialize(t *testing.T) {
	h := dbHeader{
		PageSize:         4096,
		WriteVersion:     2,
		ReadVersion:      2,
		ReservedSpace:    0,
		FileChangeCount:  42,
		DatabaseSize:     10,
		FirstFreelistPg:  3,
		TotalFreelistPgs: 1,
		SchemaCookie:     5,
		SchemaFormat:     5,
		DefaultCacheSize: 2000,
		TextEncoding:     1,
		UserVersion:      7,
		AppID:            12345,
		VersionValidFor:  42,
	}

	buf := make([]byte, dbHeaderSize)
	h.serialize(buf)

	// Verify magic string (dbMagic is 15 bytes; byte 16 is the null terminator from zeroed buf)
	assert.Equal(t, dbMagic, string(buf[0:len(dbMagic)]))

	var h2 dbHeader
	require.NoError(t, h2.deserialize(buf))
	assert.Equal(t, h.PageSize, h2.PageSize)
	assert.Equal(t, h.WriteVersion, h2.WriteVersion)
	assert.Equal(t, h.ReadVersion, h2.ReadVersion)
	assert.Equal(t, h.FileChangeCount, h2.FileChangeCount)
	assert.Equal(t, h.DatabaseSize, h2.DatabaseSize)
	assert.Equal(t, h.FirstFreelistPg, h2.FirstFreelistPg)
	assert.Equal(t, h.TotalFreelistPgs, h2.TotalFreelistPgs)
	assert.Equal(t, h.SchemaCookie, h2.SchemaCookie)
	assert.Equal(t, h.SchemaFormat, h2.SchemaFormat)
	assert.Equal(t, h.DefaultCacheSize, h2.DefaultCacheSize)
	assert.Equal(t, h.TextEncoding, h2.TextEncoding)
	assert.Equal(t, h.UserVersion, h2.UserVersion)
	assert.Equal(t, h.AppID, h2.AppID)
	assert.Equal(t, h.VersionValidFor, h2.VersionValidFor)
}

func TestDBHeaderPageSize65536(t *testing.T) {
	h := dbHeader{PageSize: 65536, WriteVersion: 2, ReadVersion: 2}
	buf := make([]byte, dbHeaderSize)
	h.serialize(buf)

	// Page size 65536 should be stored as 1
	assert.Equal(t, uint8(0), buf[16])
	assert.Equal(t, uint8(1), buf[17])

	var h2 dbHeader
	require.NoError(t, h2.deserialize(buf))
	assert.Equal(t, uint32(65536), h2.PageSize)
}

func TestDBHeaderDeserializeCorrupt(t *testing.T) {
	// Too short
	var h dbHeader
	assert.ErrorIs(t, h.deserialize(make([]byte, 50)), ErrCorrupt)

	// Wrong magic
	buf := make([]byte, dbHeaderSize)
	copy(buf[0:16], "Wrong magic str\x00")
	assert.ErrorIs(t, h.deserialize(buf), ErrCorrupt)
}

func TestDBHeaderNotSQLiteCompatible(t *testing.T) {
	assert.NotEqual(t, "SQLite format 3\000", dbMagic)
}

func TestPageHeaderSerializeDeserialize(t *testing.T) {
	tests := []pageHeader{
		{pageType: pageTypeLeafTbl, cellCount: 5, cellContentOff: 3000, fragBytes: 2},
		{pageType: pageTypeLeafIdx, cellCount: 0, cellContentOff: 4096},
		{pageType: pageTypeIntTbl, cellCount: 3, cellContentOff: 2000, rightChild: 42},
		{pageType: pageTypeIntIdx, cellCount: 10, cellContentOff: 1500, rightChild: 99, firstFreeBlk: 200},
	}

	for _, ph := range tests {
		buf := make([]byte, 12)
		ph.serialize(buf)

		var ph2 pageHeader
		ph2.deserialize(buf)
		assert.Equal(t, ph.pageType, ph2.pageType)
		assert.Equal(t, ph.cellCount, ph2.cellCount)
		assert.Equal(t, ph.cellContentOff, ph2.cellContentOff)
		assert.Equal(t, ph.fragBytes, ph2.fragBytes)
		assert.Equal(t, ph.firstFreeBlk, ph2.firstFreeBlk)
		if ph.isInterior() {
			assert.Equal(t, ph.rightChild, ph2.rightChild)
		}
	}
}

func TestPageHeaderIsLeafIsInterior(t *testing.T) {
	assert.True(t, (&pageHeader{pageType: pageTypeLeafTbl}).isLeaf())
	assert.True(t, (&pageHeader{pageType: pageTypeLeafIdx}).isLeaf())
	assert.False(t, (&pageHeader{pageType: pageTypeIntTbl}).isLeaf())
	assert.False(t, (&pageHeader{pageType: pageTypeIntIdx}).isLeaf())

	assert.True(t, (&pageHeader{pageType: pageTypeIntTbl}).isInterior())
	assert.True(t, (&pageHeader{pageType: pageTypeIntIdx}).isInterior())
	assert.False(t, (&pageHeader{pageType: pageTypeLeafTbl}).isInterior())
}

func TestPageHeaderSize(t *testing.T) {
	assert.Equal(t, 8, (&pageHeader{pageType: pageTypeLeafTbl}).headerSize())
	assert.Equal(t, 8, (&pageHeader{pageType: pageTypeLeafIdx}).headerSize())
	assert.Equal(t, 12, (&pageHeader{pageType: pageTypeIntTbl}).headerSize())
	assert.Equal(t, 12, (&pageHeader{pageType: pageTypeIntIdx}).headerSize())
}

func TestVarintRoundTrip(t *testing.T) {
	tests := []uint64{
		0, 1, 126, 127, 128, 129,
		0x3fff, 0x4000,
		0x1fffff, 0x200000,
		0x0fffffff, 0x10000000,
		0x07ffffffff, 0x0800000000,
		0x03ffffffffff, 0x040000000000,
		0x01ffffffffffff, 0x02000000000000,
		0x00ffffffffffffff, 0x0100000000000000,
		1<<63 - 1,
		0xffffffffffffffff,
	}
	buf := make([]byte, 9)
	for _, v := range tests {
		n := putVarint(buf, v)
		got, m := getVarint(buf)
		assert.Equal(t, n, m, "varint size mismatch for %d (0x%x)", v, v)
		assert.Equal(t, v, got, "varint value mismatch for 0x%x", v)
		assert.Equal(t, varintSize(v), n, "varintSize mismatch for 0x%x", v)
	}
}

func TestVarintSize(t *testing.T) {
	assert.Equal(t, 1, varintSize(0))
	assert.Equal(t, 1, varintSize(0x7f))
	assert.Equal(t, 2, varintSize(0x80))
	assert.Equal(t, 2, varintSize(0x3fff))
	assert.Equal(t, 3, varintSize(0x4000))
	assert.Equal(t, 9, varintSize(0xffffffffffffffff))
}

func TestCellPointerOffset(t *testing.T) {
	// Page 1 has db header offset
	pg := &page{pgno: 1, data: make([]byte, 4096), header: pageHeader{pageType: pageTypeLeafIdx}}
	assert.Equal(t, dbHeaderSize+8, pg.cellPointerOffset())

	// Non-page-1 leaf
	pg2 := &page{pgno: 2, data: make([]byte, 4096), header: pageHeader{pageType: pageTypeLeafIdx}}
	assert.Equal(t, 8, pg2.cellPointerOffset())

	// Interior page
	pg3 := &page{pgno: 2, data: make([]byte, 4096), header: pageHeader{pageType: pageTypeIntIdx}}
	assert.Equal(t, 12, pg3.cellPointerOffset())
}

func TestLeafCellParseWrite(t *testing.T) {
	key := []byte("testkey")
	value := []byte("testvalue")
	buf := make([]byte, 100)

	n := writeLeafCell(buf, key, value)
	assert.Equal(t, leafCellSize(key, value), n)

	cell, m, err := parseLeafCell(buf, 0)
	assert.NoError(t, err)
	assert.Equal(t, n, m)
	assert.Equal(t, key, cell.key)
	assert.Equal(t, value, cell.value)
	assert.Equal(t, uint32(0), cell.leftChild)
}

func TestInteriorCellParseWrite(t *testing.T) {
	key := []byte("separator")
	leftChild := uint32(42)
	buf := make([]byte, 100)

	n := writeInteriorCell(buf, leftChild, key)
	assert.Equal(t, interiorCellSize(key), n)

	cell, m, err := parseInteriorCell(buf, 0)
	assert.NoError(t, err)
	assert.Equal(t, n, m)
	assert.Equal(t, key, cell.key)
	assert.Equal(t, leftChild, cell.leftChild)
}

func TestLeafCellEmptyKeyValue(t *testing.T) {
	buf := make([]byte, 100)
	n := writeLeafCell(buf, []byte{}, []byte{})
	cell, m, err := parseLeafCell(buf, 0)
	assert.NoError(t, err)
	assert.Equal(t, n, m)
	assert.Empty(t, cell.key)
	assert.Empty(t, cell.value)
}

func TestChecksumDeterministic(t *testing.T) {
	data := []byte("hello world 1234")
	c1 := checksum(data)
	c2 := checksum(data)
	assert.Equal(t, c1, c2)

	// Different data gives different checksum
	data2 := []byte("hello world 1235")
	c3 := checksum(data2)
	assert.NotEqual(t, c1, c3)
}

func TestPageGetSetCellOffset(t *testing.T) {
	pg := &page{pgno: 2, data: make([]byte, 4096), header: pageHeader{pageType: pageTypeLeafIdx}}
	pg.setCellOffset(0, 3000)
	pg.setCellOffset(1, 2900)
	assert.Equal(t, uint16(3000), pg.getCellOffset(0))
	assert.Equal(t, uint16(2900), pg.getCellOffset(1))
}

func TestPageUsableSize(t *testing.T) {
	pg := &page{data: make([]byte, 4096)}
	assert.Equal(t, 4096, pg.usableSize(0))
	assert.Equal(t, 4088, pg.usableSize(8))
}
