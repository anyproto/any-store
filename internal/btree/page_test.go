package btree

import (
	"path/filepath"
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

// TestDeserializeBadPayloadFraction asserts that deserialize rejects a
// page-1 header whose embedded-payload-fraction bytes 21-23 are not exactly
// 64/32/32, mirroring C lockBtree (btree.c:3371-3373,
// memcmp(&page1[21],"\100\040\040",3) -> SQLITE_NOTADB). This guards against a
// non-any-store / non-SQLite-shaped file whose magic coincidentally matches but
// whose fraction bytes do not.
func TestDeserializeBadPayloadFraction(t *testing.T) {
	// Build a fully valid, round-trippable header via serialize() so only the
	// fraction bytes are under test.
	base := dbHeader{PageSize: 4096, WriteVersion: 2, ReadVersion: 2, DatabaseSize: 1}
	good := make([]byte, dbHeaderSize)
	base.serialize(good)

	// Sanity: the untouched, serialized header deserializes cleanly (and bytes
	// 21-23 are the required constants).
	var ok dbHeader
	require.NoError(t, ok.deserialize(good))
	require.Equal(t, []byte{maxEmbeddedPayloadFrac, minEmbeddedPayloadFrac, leafPayloadFrac}, good[21:24])

	// Each fraction byte, individually wrong, must be rejected.
	for _, off := range []int{21, 22, 23} {
		buf := make([]byte, dbHeaderSize)
		copy(buf, good)
		buf[off] = 0xFF // any non-conforming value
		var h dbHeader
		assert.ErrorIsf(t, h.deserialize(buf), ErrCorrupt, "byte %d not validated", off)
	}
}

// TestOpenRejectsUsableSizeBelow480 asserts the open path rejects an
// on-disk header whose ReservedSpace byte (offset 20) drives usableSize below
// 480, mirroring C lockBtree (btree.c:3422-3424, "the usable size is not
// allowed to be less than 480"). An xChaCha20-style 48-byte overhead on a
// claimed 512-byte page (usable 464) is exactly the corruption SQLite floors.
func TestOpenRejectsUsableSizeBelow480(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Create a valid 512-byte-page DB, then close it so we can corrupt page 1.
	db, err := testOpen(t, path, Options{PageSize: 512})
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// ReservedSpace (offset 20) is inside the always-plaintext dbHeader prefix.
	// 512 - 64 = 448 usable, which is < 480 and must be rejected. 64 is also a
	// large-enough reserved value that stock SQLite forbids at page size 512
	// (reserved cannot exceed 32 there).
	corruptByte(t, path, 20, 64)

	resetPoolForTest(t)
	db2, err := Open(path, Options{PageSize: 512})
	if db2 != nil {
		_ = db2.Close()
	}
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCorrupt)
}

func TestDBHeaderNotSQLiteCompatible(t *testing.T) {
	assert.NotEqual(t, "SQLite format 3\000", dbMagic)
}

// TestDBHeaderRejectsGenuineSQLiteMagic pins the assumed invariant that the
// "index-only B-trees" by-design drift relies on (docs/btree/NOTES.md
// #old-drift-not-implemented-by-design, "Integer-key (table) B-trees" line).
//
// Drift, by design and confirmed: any-store never *produces* table pages, but
// its readers isLeaf/isInterior (page.go:303-310) ACCEPT the table-page type
// bytes 5 (pageTypeIntTbl) and 13 (pageTypeLeafTbl) exactly like SQLite's
// decodeFlags. So if a genuine SQLite file — which legitimately stores table
// B-tree pages tagged 5/13 with rowid/table cell layout — were ever opened by
// any-store, those pages would be traversed with INDEX-cell layout, silently
// misparsing payloads. any-store has no decodeFlags-equivalent load-time
// page-type whitelist; the *only* thing that makes this case unreachable for
// real SQLite files is that dbHeader.deserialize (page.go:239) rejects them at
// header parse, before any page is ever loaded, because the 15-byte magic
// prefix differs ("BTree format 1\000" vs SQLite's "SQLite format 3\000",
// NOTES.md:253). That rejection is an *assumed* invariant, not an explicit
// whitelist — so we pin it here. A refactor that widens/weakens the magic
// comparison (e.g. accepts SQLite's magic, or compares too few bytes) would
// re-open the 5/13 table-page misparse path and must fail this test loudly.
func TestDBHeaderRejectsGenuineSQLiteMagic(t *testing.T) {
	const sqliteMagic = "SQLite format 3\x00" // 16 bytes, the genuine SQLite header magic

	// Guard: the two formats' 15-byte magic prefixes (the bytes deserialize
	// actually compares) must differ. If a refactor ever made them coincide,
	// the behavioral rejection below could not hold.
	require.NotEqual(t, sqliteMagic[:15], dbMagic[:15],
		"magic prefixes coincide: a genuine SQLite file (with 5/13 table pages) could be opened and traversed with index-cell layout")

	// Build a header that is byte-for-byte acceptable to deserialize EXCEPT for
	// its magic: start from a valid any-store header (valid page size, valid
	// 64/32/32 payload fractions, etc.), then overwrite only the 16 magic bytes
	// with SQLite's magic. This isolates the magic check as the sole cause of
	// rejection — proving the defense is the magic gate, not some incidental
	// downstream validation.
	base := dbHeader{PageSize: 4096, WriteVersion: 2, ReadVersion: 2, DatabaseSize: 1}
	buf := make([]byte, dbHeaderSize)
	base.serialize(buf)

	// Sanity: untouched, the header deserializes cleanly (so any rejection below
	// is attributable to the magic alone).
	var sane dbHeader
	require.NoError(t, sane.deserialize(buf))

	copy(buf[0:16], sqliteMagic)

	// THE PINNED INVARIANT: a genuine-SQLite-magic header is rejected at parse,
	// before any page (and thus any 5/13 table page) can be loaded.
	var h dbHeader
	assert.ErrorIs(t, h.deserialize(buf), ErrCorrupt,
		"deserialize must reject a genuine SQLite header so real SQLite table pages (type 5/13) never reach the index-only readers")

	// Positive control: restoring only the magic to any-store's value makes the
	// very same buffer deserialize cleanly. This proves the magic bytes are the
	// gate (the test fails for the right reason, not because of unrelated bytes).
	copy(buf[0:16], dbMagic)
	buf[15] = 0 // dbMagic is 15 bytes; ensure byte 15 (null terminator) is zero
	var h2 dbHeader
	assert.NoError(t, h2.deserialize(buf),
		"buffer differing from a valid header only in the magic must parse once the any-store magic is restored")
}

func TestDBHeaderSaltRoundTrip(t *testing.T) {
	h := dbHeader{
		PageSize:     4096,
		WriteVersion: 2,
		ReadVersion:  2,
		DatabaseSize: 1,
	}
	for i := range h.Salt {
		h.Salt[i] = byte(i ^ 0xAA)
	}

	buf := make([]byte, dbHeaderSize)
	h.serialize(buf)

	var got dbHeader
	require.NoError(t, got.deserialize(buf))
	assert.Equal(t, h.Salt, got.Salt)

	// The salt lives at bytes 72-87; verify it was serialized there.
	assert.Equal(t, h.Salt[:], buf[72:88])
	// Bytes 88-91 remain zero (reserved-for-expansion tail).
	assert.Equal(t, []byte{0, 0, 0, 0}, buf[88:92])
}

func TestDBHeaderDefaultSaltZero(t *testing.T) {
	// An unencrypted DB has a zeroed Salt field.
	h := dbHeader{PageSize: 4096, WriteVersion: 2, ReadVersion: 2}
	buf := make([]byte, dbHeaderSize)
	h.serialize(buf)
	var got dbHeader
	require.NoError(t, got.deserialize(buf))
	var zero [16]byte
	assert.Equal(t, zero, got.Salt)
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
