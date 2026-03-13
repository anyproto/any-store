package btree

import (
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anyproto/any-store/anyenc"
)

// TestDiagnoseCrashCorruption opens a corrupted DB and examines its structure.
func TestDiagnoseCrashCorruption(t *testing.T) {
	srcDB := "/tmp/crash-debug/iter-000009/store.db"
	if _, err := os.Stat(srcDB); err != nil {
		t.Skip("No corrupted DB at", srcDB)
	}

	// Copy the DB to a temp dir so we don't modify the original
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "store.db")
	data, err := os.ReadFile(srcDB)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dbPath, data, 0666); err != nil {
		t.Fatal(err)
	}
	// Also copy WAL if exists
	srcWAL := srcDB + "-wal"
	if walData, err := os.ReadFile(srcWAL); err == nil {
		os.WriteFile(dbPath+"-wal", walData, 0666)
	}

	pageSize := 4096
	t.Logf("DB size: %d bytes (%d pages at %d)", len(data), len(data)/pageSize, pageSize)

	// Read and log the DB header
	var hdr dbHeader
	if err := hdr.deserialize(data[:dbHeaderSize]); err != nil {
		t.Fatalf("Header deserialize: %v", err)
	}
	t.Logf("Header: PageSize=%d DatabaseSize=%d FirstFreelistPg=%d TotalFreelistPgs=%d FCC=%d SC=%d",
		hdr.PageSize, hdr.DatabaseSize, hdr.FirstFreelistPg, hdr.TotalFreelistPgs,
		hdr.FileChangeCount, hdr.SchemaCookie)

	// Open the DB
	db, err := testOpen(t, dbPath, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Run IntegrityCheck
	t.Log("Running IntegrityCheck...")
	if err := db.IntegrityCheckN(100); err != nil {
		t.Logf("IntegrityCheck FAILED:\n%v", err)
	} else {
		t.Log("IntegrityCheck PASSED")
	}

	// Get docs namespace
	docsNs, err := db.GetNamespace("docs")
	if err != nil {
		t.Fatalf("GetNamespace 'docs': %v", err)
	}
	t.Logf("docs namespace rootPage=%d", docsNs.RootPage())

	maxFrame, slot, err := db.pager.beginRead()
	if err != nil {
		t.Fatalf("beginRead: %v", err)
	}
	defer db.pager.endRead(slot)
	t.Logf("Read snapshot: maxFrame=%d", maxFrame)

	// Create a read-only btree for the docs namespace
	bt := &btree{
		pager:       db.pager,
		rootPage:    docsNs.RootPage(),
		walMaxFrame: maxFrame,
		writable:    false,
	}

	// Try to read the specific corrupted key
	t.Log("--- Reading corrupted key 'doc-004796' ---")
	val, err := bt.Get([]byte("doc-004796"))
	if err != nil {
		t.Logf("  Get error: %v", err)
	} else {
		t.Logf("  Value len=%d hex=%s", len(val), hex.EncodeToString(val))
		// Try to parse as anyenc
		p := &anyenc.Parser{}
		parsed, parseErr := p.Parse(val)
		if parseErr != nil {
			t.Logf("  Parse error: %v", parseErr)
			t.Logf("  Raw value: %s", hex.EncodeToString(val))
		} else {
			t.Logf("  Parsed OK: %s", parsed.String())
		}
	}

	// Scan ALL keys and find ones with parse errors
	t.Log("--- Scanning all keys for parse errors ---")
	cursor := bt.NewCursor()
	if cursor == nil {
		t.Fatal("NewCursor returned nil")
	}

	corruptCount := 0
	totalCount := 0
	for cursor.First(); cursor.Valid(); cursor.Next() {
		totalCount++
		key, kerr := cursor.Key()
		if kerr != nil {
			t.Logf("  cursor.Key error: %v", kerr)
			break
		}
		val, verr := cursor.Value()
		if verr != nil {
			t.Logf("  cursor.Value error at key=%s: %v", string(key), verr)
			corruptCount++
			continue
		}

		p := &anyenc.Parser{}
		_, parseErr := p.Parse(val)
		if parseErr != nil {
			corruptCount++
			keyStr := string(key)
			t.Logf("  CORRUPT key=%s valLen=%d err=%v", keyStr, len(val), parseErr)
			if len(val) > 200 {
				t.Logf("    first 100 bytes: %s", hex.EncodeToString(val[:100]))
				t.Logf("    last 50 bytes: %s", hex.EncodeToString(val[len(val)-50:]))
			} else {
				t.Logf("    full value: %s", hex.EncodeToString(val))
			}

			// Also try to determine what page this key is on
			if corruptCount <= 5 {
				findKeyPage(t, db.pager, bt.rootPage, maxFrame, key)
			}
		}
	}
	// cursor has no Err method — errors are returned inline
	t.Logf("Total keys: %d, corrupt: %d", totalCount, corruptCount)

	// Dump freelist
	t.Log("Freelist chain:")
	dumpFreelistDiag(t, data, pageSize, hdr.FirstFreelistPg, hdr.DatabaseSize)

	// Scan all pages in the DB file and report their types
	t.Log("Page type summary:")
	nPages := len(data) / pageSize
	typeCounts := make(map[byte]int)
	for i := 0; i < nPages; i++ {
		off := i * pageSize
		if i == 0 {
			off += dbHeaderSize
		}
		pt := data[off]
		typeCounts[pt]++
	}
	for pt, count := range typeCounts {
		t.Logf("  type %d: %d pages", pt, count)
	}
}

// findKeyPage traverses the btree to find which page a key is on
func findKeyPage(t *testing.T, p *pager, rootPgno, maxFrame uint32, key []byte) {
	pgno := rootPgno
	for depth := 0; depth < 10; depth++ {
		pg, err := p.getPageWriter(pgno, maxFrame)
		if err != nil {
			t.Logf("    page trace: depth=%d pgno=%d error=%v", depth, pgno, err)
			return
		}

		if pg.header.isLeaf() {
			t.Logf("    key '%s' is on LEAF page %d (cells=%d)", string(key), pgno, pg.header.cellCount)

			// Dump the cell that contains this key
			cpOff := pg.cellPointerOffset()
			usable := p.usableSize()
			for i := 0; i < int(pg.header.cellCount); i++ {
				cpBase := cpOff + i*2
				if cpBase+2 > len(pg.data) {
					break
				}
				off := int(binary.BigEndian.Uint16(pg.data[cpBase:]))
				cell, _, cerr := parseLeafCellWithSize(pg.data, off, usable)
				if cerr != nil {
					continue
				}
				if strings.HasPrefix(string(cell.key), string(key)) || string(cell.key) == string(key) {
					t.Logf("    cell %d at offset %d: keyLen=%d valLen=%d overflowPg=%d",
						i, off, len(cell.key), len(cell.value), cell.overflowPg)
					if len(cell.value) <= 200 {
						t.Logf("    cell value hex: %s", hex.EncodeToString(cell.value))
					}
				}
			}
			p.releasePage(pg)
			return
		}

		// Interior page - descend
		childPgno, _, _ := searchInteriorPage(pg, key)
		p.releasePage(pg)
		pgno = childPgno
	}
}

func dumpFreelistDiag(t *testing.T, data []byte, pageSize int, firstTrunk, dbSize uint32) {
	if firstTrunk == 0 {
		t.Log("  (empty freelist)")
		return
	}
	trunkPgno := firstTrunk
	seen := make(map[uint32]bool)
	for trunkPgno != 0 {
		if seen[trunkPgno] {
			t.Logf("  trunk page %d: CYCLE DETECTED!", trunkPgno)
			break
		}
		seen[trunkPgno] = true
		if trunkPgno > dbSize {
			t.Logf("  trunk pgno %d > dbSize %d", trunkPgno, dbSize)
			break
		}
		off := int(trunkPgno-1) * pageSize
		if off+8 > len(data) {
			t.Logf("  trunk pgno %d offset beyond file", trunkPgno)
			break
		}
		nextTrunk := binary.BigEndian.Uint32(data[off : off+4])
		leafCount := binary.BigEndian.Uint32(data[off+4 : off+8])
		t.Logf("  trunk page %d: next=%d leaves=%d", trunkPgno, nextTrunk, leafCount)

		maxLeaves := (pageSize - 8) / 4
		if int(leafCount) > maxLeaves {
			t.Logf("    CORRUPT: leafCount %d > max %d", leafCount, maxLeaves)
			break
		}

		for i := 0; i < int(leafCount); i++ {
			leafPgno := binary.BigEndian.Uint32(data[off+8+i*4:])
			if leafPgno == 0 || leafPgno > dbSize {
				t.Logf("    leaf[%d] = %d (INVALID)", i, leafPgno)
			}
		}

		trunkPgno = nextTrunk
	}
}
