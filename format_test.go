package anystore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/qplanner"
	"github.com/anyproto/any-store/v2/query"
)

// A backup of a database created before v2.5.0 is such a database too: the
// copy keeps the source's on-disk format, also when this release creates
// format 6 files by default.
func TestBackup_KeepsSourceFormat(t *testing.T) {
	legacySuite := os.Getenv("ANYSTORE_TEST_LEGACY_FORMAT") == "1"
	for _, legacy := range []bool{true, false} {
		if legacySuite && !legacy {
			continue
		}
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			// given
			btree.SetCreateLegacyFormatForTest(legacy)
			defer btree.SetCreateLegacyFormatForTest(legacySuite)
			dir := t.TempDir()
			db, err := Open(ctx, filepath.Join(dir, "src.db"), nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, db.Close()) }()
			coll, err := db.CreateCollection(ctx, "docs")
			require.NoError(t, err)
			const n = 200
			for i := 0; i < n; i++ {
				// incompressible enough to pass the 997-byte limit of format 5
				var sb strings.Builder
				for sb.Len() < 3000 {
					fmt.Fprintf(&sb, "%x", uint64(i+1)*2654435761+uint64(sb.Len())*40503)
				}
				require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":"%05d","t":"%s"}`, i, sb.String()))))
			}
			btree.SetCreateLegacyFormatForTest(legacySuite) // new files get the default format again

			// when
			backupPath := filepath.Join(dir, "backup.db")
			require.NoError(t, db.Backup(ctx, backupPath))

			// then
			data, err := os.ReadFile(backupPath)
			require.NoError(t, err)
			wantMagic, wantFormat := "any-store v2 fmt", uint32(6)
			if legacy {
				wantMagic, wantFormat = "any-store v2\x00\x00\x00\x00", uint32(5)
			}
			assert.Equal(t, wantMagic, string(data[:16]))
			assert.Equal(t, wantFormat, binary.BigEndian.Uint32(data[44:48]))

			copyDB, err := Open(ctx, backupPath, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, copyDB.Close()) }()
			copyColl, err := copyDB.Collection(ctx, "docs")
			require.NoError(t, err)
			count, err := copyColl.Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, n, count)
			require.NoError(t, copyDB.QuickCheck(ctx))
		})
	}
}

// createFormat makes the databases created from here on in this test files of
// schema format 5 (legacy) or of the current format, whatever the suite runs on.
func createFormat(t *testing.T, legacy bool) {
	t.Helper()
	btree.SetCreateLegacyFormatForTest(legacy)
	t.Cleanup(func() {
		btree.SetCreateLegacyFormatForTest(os.Getenv("ANYSTORE_TEST_LEGACY_FORMAT") == "1")
	})
}

// sketchRow returns the stored stat_data row of an index.
func sketchRow(t *testing.T, dbi *db, coll, index string) []byte {
	t.Helper()
	var row []byte
	require.NoError(t, dbi.doReadTx(ctx, func(tx *btree.ReadTx) (err error) {
		row, err = tx.AppendValue(dbi.systemNS, sketchKey(coll, index), nil)
		return err
	}))
	return row
}

// sameSketches fails with the first counter that differs between two sets of
// counters by index name: printed side by side they run to thousands of lines.
func sameSketches(t *testing.T, want, got map[string][]uint64, msgAndArgs ...any) {
	t.Helper()
	require.Len(t, got, len(want), msgAndArgs...)
	for name, w := range want {
		g := got[name]
		if !assert.Len(t, g, len(w), msgAndArgs...) {
			continue
		}
		for i := range w {
			if w[i] != g[i] {
				assert.Fail(t, fmt.Sprintf("index %s: counter %d is %d, want %d", name, i, g[i], w[i]), msgAndArgs...)
				break
			}
		}
	}
}

// sketchCounters returns the counters the planner reads for each index of a
// collection, by index name.
func sketchCounters(coll Collection) map[string][]uint64 {
	out := map[string][]uint64{}
	for _, idx := range coll.(*collection).loadIndexes() {
		out[idx.info.Name] = idx.loadPubSketch().Snapshot(nil)
	}
	return out
}

// The stat_data rows of a format 5 file stay in the V2 encoding, byte for
// byte: the releases that open such a file read no other and would take a
// compressed row for legacy bucket counts. A format 6 file stores them
// compressed. The file's format decides, not the format this process creates
// new files in, and both give the planner the same counters.
func TestSketchRows_EncodingFollowsFileFormat(t *testing.T) {
	indexes := []IndexInfo{{Name: "a", Fields: []string{"a"}}, {Name: "ab", Fields: []string{"a", "b"}}}
	inspected := map[bool]map[string][]uint64{}
	for _, legacy := range []bool{true, false} {
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			// given
			createFormat(t, legacy)
			path := filepath.Join(t.TempDir(), "s.db")
			db1, err := Open(ctx, path, nil)
			require.NoError(t, err)
			dbi := db1.(*db)
			wantFormat := uint32(6)
			if legacy {
				wantFormat = 5
			}
			require.Equal(t, wantFormat, dbi.btreeDB.SchemaFormat())
			coll, err := db1.CreateCollection(ctx, "docs")
			require.NoError(t, err)
			insert := func(c Collection, from, to int) {
				for i := from; i < to; i++ {
					require.NoError(t, c.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%7, i))))
				}
			}
			checkRows := func(d *db, c Collection, stage string) {
				t.Helper()
				for _, idx := range c.(*collection).loadIndexes() {
					row := sketchRow(t, d, "docs", idx.info.Name)
					if legacy {
						require.Equal(t, "SK02", string(row[:4]), "%s, index %s", stage, idx.info.Name)
						assert.True(t, bytes.Equal(idx.sketch.MarshalBinary(nil), row), "%s, index %s: not the V2 bytes", stage, idx.info.Name)
					} else {
						require.Equal(t, "SK03", string(row[:4]), "%s, index %s", stage, idx.info.Name)
						assert.True(t, bytes.Equal(idx.sketch.MarshalCompressed(nil), row), "%s, index %s: not the compressed bytes", stage, idx.info.Name)
						assert.Less(t, len(row), 600, "%s, index %s: fits a leaf many times over", stage, idx.info.Name)
					}
				}
			}

			// when: the index build saves the rows
			insert(coll, 0, 30)
			for _, info := range indexes {
				require.NoError(t, coll.EnsureIndex(ctx, info))
			}
			// then
			checkRows(dbi, coll, "after EnsureIndex")

			// when: commits save them
			insert(coll, 30, 60)
			_, err = coll.UpdateId(ctx, 3, query.MustParseModifier(`{"$set":{"a":100}}`))
			require.NoError(t, err)
			require.NoError(t, coll.DeleteId(ctx, 4))
			tx, err := db1.WriteTx(ctx)
			require.NoError(t, err)
			for i := 60; i < 70; i++ {
				require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%7, i))))
			}
			require.NoError(t, tx.Commit())
			// then
			checkRows(dbi, coll, "after commits")
			counters := sketchCounters(coll)
			require.NoError(t, db1.Close())

			// when: reopened by a process that creates the other format
			createFormat(t, !legacy)
			db2, err := Open(ctx, path, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, db2.Close()) }()
			dbi = db2.(*db)
			require.Equal(t, wantFormat, dbi.btreeDB.SchemaFormat())
			coll, err = db2.Collection(ctx, "docs")
			require.NoError(t, err)
			// then: the planner has the counters back, and the rows are untouched
			sameSketches(t, counters, sketchCounters(coll), "after reopen")
			checkRows(dbi, coll, "after reopen")
			count, err := coll.Find(`{"a":100}`).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, 1, count)

			// when: a commit saves them again
			insert(coll, 70, 80)
			// then
			checkRows(dbi, coll, "after a commit in the reopened file")
			inspected[legacy] = map[string][]uint64{}
			for _, info := range indexes {
				got, err := dbi.InspectIndexSketch(ctx, "docs", info.Name)
				require.NoError(t, err)
				assert.Equal(t, uint64(79), got.DocCount, info.Name)
				assert.Equal(t, len(info.Fields), got.Levels, info.Name)
				assert.Equal(t, qplanner.DefaultSketchSize, got.Size, info.Name)
				inspected[legacy][info.Name] = append(got.Buckets, got.DocCount)
			}
		})
	}
	sameSketches(t, inspected[true], inspected[false], "the same documents give the same sketch in either format")
}

// A row in the other encoding is read as it is and rewritten in the file's
// own by the next commit that saves it: V2 rows an earlier build left in a
// format 6 file, and compressed bytes that got into a format 5 file.
func TestSketchRows_OtherEncodingReadThenRewritten(t *testing.T) {
	for _, legacy := range []bool{true, false} {
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			// given
			createFormat(t, legacy)
			path := filepath.Join(t.TempDir(), "s.db")
			db1, err := Open(ctx, path, nil)
			require.NoError(t, err)
			coll, err := db1.CreateCollection(ctx, "docs")
			require.NoError(t, err)
			require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))
			other := qplanner.NewIndexSketch(qplanner.DefaultSketchSize, 1)
			for i := 0; i < 77; i++ {
				other.IncrementDocCount()
			}
			row, ownTag := other.MarshalBinary(nil), "SK03"
			if legacy {
				row, ownTag = other.MarshalCompressed(nil), "SK02"
			}
			require.NotEqual(t, ownTag, string(row[:4]))
			rawPut(t, db1.(*db), sketchKey("docs", "a"), row)
			require.NoError(t, db1.Close())

			// when
			db2, err := Open(ctx, path, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, db2.Close()) }()
			dbi := db2.(*db)
			coll, err = db2.Collection(ctx, "docs")
			require.NoError(t, err)

			// then: read as it is
			assert.Equal(t, uint64(77), coll.(*collection).loadIndexes()[0].loadPubSketch().GetDocCount())
			info, err := dbi.InspectIndexSketch(ctx, "docs", "a")
			require.NoError(t, err)
			assert.Equal(t, uint64(77), info.DocCount)
			assert.True(t, bytes.Equal(row, sketchRow(t, dbi, "docs", "a")), "a load rewrites nothing")

			// when
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":2}`)))

			// then: saved in the file's own encoding
			saved := sketchRow(t, dbi, "docs", "a")
			assert.Equal(t, ownTag, string(saved[:4]))
			info, err = dbi.InspectIndexSketch(ctx, "docs", "a")
			require.NoError(t, err)
			assert.Equal(t, uint64(78), info.DocCount)
		})
	}
}

// A compressed row that does not decode is no statistics, not wrong ones: the
// index opens with an empty sketch, queries answer from the data, and the next
// commit saves a sound row. The index has two fields: read as legacy buckets,
// its sketch would be flagged for a rebuild.
func TestSketchRows_CorruptCompressedRow(t *testing.T) {
	const block = 12 + 2 // header, then a planes byte per level
	damage := map[string]func(row []byte) []byte{
		"tag only":       func(row []byte) []byte { return row[:4] },
		"cut header":     func(row []byte) []byte { return row[:10] },
		"cut planes":     func(row []byte) []byte { return row[:block-1] },
		"truncated":      func(row []byte) []byte { return row[:len(row)-1] },
		"another length": func(row []byte) []byte { row[block]++; return row },
		"corrupt block": func(row []byte) []byte {
			_, n := binary.Uvarint(row[block:]) // the block's declared length stays
			for i := block + n; i < len(row); i++ {
				row[i] = 0xff
			}
			return row
		},
	}
	for name, damageRow := range damage {
		t.Run(name, func(t *testing.T) {
			// given
			createFormat(t, false)
			path := filepath.Join(t.TempDir(), "s.db")
			db1, err := Open(ctx, path, nil)
			require.NoError(t, err)
			coll, err := db1.CreateCollection(ctx, "docs")
			require.NoError(t, err)
			require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "ab", Fields: []string{"a", "b"}}))
			for i := 0; i < 20; i++ {
				require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%4, i))))
			}
			row := sketchRow(t, db1.(*db), "docs", "ab")
			require.Equal(t, "SK03", string(row[:4]))
			rawPut(t, db1.(*db), sketchKey("docs", "ab"), damageRow(row))
			require.NoError(t, db1.Close())

			// when
			db2, err := Open(ctx, path, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, db2.Close()) }()
			dbi := db2.(*db)
			coll, err = db2.Collection(ctx, "docs")
			require.NoError(t, err)

			// then
			sk := coll.(*collection).loadIndexes()[0].loadPubSketch()
			sameSketches(t, map[string][]uint64{"ab": make([]uint64, 2*qplanner.DefaultSketchSize+3)},
				map[string][]uint64{"ab": sk.Snapshot(nil)}, "an empty sketch")
			assert.False(t, sk.NeedsRebuild(), "not read as legacy buckets")
			info, err := dbi.InspectIndexSketch(ctx, "docs", "ab")
			require.NoError(t, err)
			assert.Equal(t, uint64(0), info.DocCount)
			count, err := coll.Find(`{"a":1}`).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, 5, count)

			// when
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":100,"a":1,"b":100}`)))

			// then
			saved := sketchRow(t, dbi, "docs", "ab")
			assert.Equal(t, "SK03", string(saved[:4]))
			info, err = dbi.InspectIndexSketch(ctx, "docs", "ab")
			require.NoError(t, err)
			assert.Equal(t, uint64(1), info.DocCount)
		})
	}
}

// A rename moves a sketch row as it is and a backup copies it with its page,
// in either format: the rows keep their bytes and the planner its counters.
func TestSketchRows_RenameAndBackup(t *testing.T) {
	indexes := []IndexInfo{{Name: "a", Fields: []string{"a"}}, {Name: "ab", Fields: []string{"a", "b"}}}
	for _, legacy := range []bool{true, false} {
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			// given
			createFormat(t, legacy)
			dir := t.TempDir()
			db1, err := Open(ctx, filepath.Join(dir, "s.db"), nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, db1.Close()) }()
			dbi := db1.(*db)
			coll, err := db1.CreateCollection(ctx, "docs")
			require.NoError(t, err)
			require.NoError(t, coll.EnsureIndex(ctx, indexes...))
			for i := 0; i < 40; i++ {
				require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%7, i))))
			}
			tag := "SK03"
			if legacy {
				tag = "SK02"
			}
			rows := map[string][]byte{}
			for _, info := range indexes {
				rows[info.Name] = sketchRow(t, dbi, "docs", info.Name)
				require.Equal(t, tag, string(rows[info.Name][:4]))
			}
			counters := sketchCounters(coll)
			sameRows := func(d *db, collName, stage string) {
				t.Helper()
				for _, info := range indexes {
					assert.True(t, bytes.Equal(rows[info.Name], sketchRow(t, d, collName, info.Name)), "%s, index %s", stage, info.Name)
				}
			}

			// when
			require.NoError(t, coll.Rename(ctx, "renamed"))

			// then
			sameRows(dbi, "renamed", "after the rename")
			for _, info := range indexes {
				_, err = dbi.InspectIndexSketch(ctx, "docs", info.Name)
				assert.ErrorIs(t, err, ErrIndexNotFound, "the row left the old name, index %s", info.Name)
			}
			sameSketches(t, counters, sketchCounters(coll), "after the rename")

			// when: the copy is made by a process that creates the other format
			createFormat(t, !legacy)
			backupPath := filepath.Join(dir, "backup.db")
			require.NoError(t, db1.Backup(ctx, backupPath))
			db2, err := Open(ctx, backupPath, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, db2.Close()) }()
			copyColl, err := db2.Collection(ctx, "renamed")
			require.NoError(t, err)

			// then
			sameRows(db2.(*db), "renamed", "in the backup")
			sameSketches(t, counters, sketchCounters(copyColl), "in the backup")
		})
	}
}

// An index of more fields than the compressed encoding's bound covers keeps
// its sketch in the V2 encoding, in a format 6 file too: what a save writes
// always loads.
func TestSketchRows_OversizedSketchStaysV2(t *testing.T) {
	// given
	createFormat(t, false)
	path := filepath.Join(t.TempDir(), "s.db")
	db1, err := Open(ctx, path, nil)
	require.NoError(t, err)
	coll, err := db1.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	fields := make([]string, 129) // 129 levels of 1,024 buckets, one level over the bound
	doc := anyenc.MustParseJson(`{"id":1}`)
	a := &anyenc.Arena{}
	for i := range fields {
		fields[i] = fmt.Sprintf("f%d", i)
		doc.Set(fields[i], a.NewNumberInt(i))
	}

	// when
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "wide", Fields: fields}))
	require.NoError(t, coll.Insert(ctx, doc))

	// then
	row := sketchRow(t, db1.(*db), "docs", "wide")
	require.Equal(t, "SK02", string(row[:4]))
	require.NoError(t, db1.Close())
	db2, err := Open(ctx, path, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, db2.Close()) }()
	info, err := db2.(*db).InspectIndexSketch(ctx, "docs", "wide")
	require.NoError(t, err)
	assert.Equal(t, 129, info.Levels)
	assert.Equal(t, uint64(1), info.DocCount)
}
