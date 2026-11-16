package anystore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
)

// SchemaFormat reports the format the file's header records: 5 for a file
// made the way releases before v2.5.0 make them, LatestSchemaFormat for one
// this release creates. A reopen keeps the file's format whatever new files
// get by then.
func TestSchemaFormat(t *testing.T) {
	skipIfInMemory(t, "the header is read from the file and the file reopened")
	for _, legacy := range []bool{true, false} {
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			want := LatestSchemaFormat
			if legacy {
				want = 5
			}
			createFormat(t, legacy)
			path := filepath.Join(t.TempDir(), "format.db")
			d, err := Open(ctx, path, nil)
			require.NoError(t, err)
			got, err := SchemaFormat(d)
			require.NoError(t, err)
			assert.Equal(t, want, got)
			require.NoError(t, d.Close())
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			assert.Equal(t, uint32(want), binary.BigEndian.Uint32(data[44:48]))

			createFormat(t, !legacy)
			d, err = Open(ctx, path, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, d.Close()) }()
			got, err = SchemaFormat(d)
			require.NoError(t, err)
			assert.Equal(t, want, got)
		})
	}
}

// foreignDB is a DB of the caller's own, which the package functions that
// take a DB refuse.
type foreignDB struct{ DB }

func TestSchemaFormat_UnsupportedDB(t *testing.T) {
	fx := newFixture(t)
	_, err := SchemaFormat(foreignDB{fx.DB})
	assert.ErrorIs(t, err, ErrUnsupportedDB)
	_, err = SchemaFormat(nil)
	assert.ErrorIs(t, err, ErrUnsupportedDB)
	var nilDB *db
	_, err = SchemaFormat(nilDB)
	assert.ErrorIs(t, err, ErrUnsupportedDB)
}

// --- CopyToLatestFormat ---

// openSource opens a source database for a copy test in a directory of its
// own, closed when the test ends.
func openSource(t *testing.T, conf *Config) (DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "source.db")
	d, err := Open(ctx, path, conf)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := d.Close(); err != nil && !errors.Is(err, ErrDBIsClosed) {
			t.Error(err)
		}
	})
	return d, path
}

// openCopy opens a copy, closed when the test ends.
func openCopy(t *testing.T, path string, conf *Config) DB {
	t.Helper()
	d, err := Open(ctx, path, conf)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	return d
}

// dirNames lists the names in dir.
func dirNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// copyEntry is a collection as the catalog stores it.
type copyEntry struct {
	cfg     collConfig
	indexes []IndexInfo
}

// catalogOf reads the catalog of d: the collection names in catalog order,
// and each one's stored options and index definitions.
func catalogOf(t *testing.T, d DB) ([]string, map[string]copyEntry) {
	t.Helper()
	dbi := d.(*db)
	var names []string
	entries := map[string]copyEntry{}
	require.NoError(t, dbi.doReadTx(ctx, func(tx *btree.ReadTx) (err error) {
		if names, err = dbi.collectionNames(tx); err != nil {
			return err
		}
		for _, name := range names {
			var e copyEntry
			if e.cfg, err = dbi.loadCollConfig(tx, name); err != nil {
				return err
			}
			if e.indexes, err = dbi.getIndexInfos(tx, name); err != nil {
				return err
			}
			entries[name] = e
		}
		return nil
	}))
	return names, entries
}

// storedDocs returns the documents of a collection, each as its encoder
// writes it, in primary-key order.
func storedDocs(t *testing.T, d DB, name string) [][]byte {
	t.Helper()
	coll, err := d.OpenCollection(ctx, name)
	require.NoError(t, err)
	it, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err)
	defer it.Close()
	var docs [][]byte
	for it.Next() {
		doc, err := it.Doc()
		require.NoError(t, err)
		docs = append(docs, doc.Value().MarshalTo(nil))
	}
	require.NoError(t, it.Err())
	return docs
}

// assertSameDatabase checks that dst holds what src holds: the same
// collections with the same stored options and index definitions, and the
// same documents, value by value.
func assertSameDatabase(t *testing.T, src, dst DB) {
	t.Helper()
	srcNames, srcEntries := catalogOf(t, src)
	dstNames, dstEntries := catalogOf(t, dst)
	require.Equal(t, srcNames, dstNames)
	for _, name := range srcNames {
		s, d := srcEntries[name], dstEntries[name]
		assert.Equal(t, s.cfg.Compression, d.cfg.Compression, name)
		assert.Equal(t, primaryKeyOf(s.cfg), primaryKeyOf(d.cfg), name)
		if assert.Len(t, d.indexes, len(s.indexes), name) {
			for i := range s.indexes {
				assert.True(t, sameIndexDefinition(s.indexes[i], d.indexes[i]), "%s: %+v != %+v", name, s.indexes[i], d.indexes[i])
			}
		}
		srcDocs, dstDocs := storedDocs(t, src, name), storedDocs(t, dst, name)
		if assert.Len(t, dstDocs, len(srcDocs), name) {
			for i := range srcDocs {
				if !bytes.Equal(srcDocs[i], dstDocs[i]) {
					assert.Fail(t, fmt.Sprintf("%s: document %d differs", name, i))
					break
				}
			}
		}
	}
}

// fileSum is the SHA-256 of a file's bytes.
func fileSum(t *testing.T, path string) [32]byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return sha256.Sum256(data)
}

// queryIDs runs a query and returns the primary keys of what it finds, in
// its order, each as JSON.
func queryIDs(t *testing.T, q Query) []string {
	t.Helper()
	it, err := q.Iter(ctx)
	require.NoError(t, err)
	defer it.Close()
	var ids []string
	for it.Next() {
		doc, err := it.Doc()
		require.NoError(t, err)
		ids = append(ids, doc.Value().Get("id").String())
	}
	require.NoError(t, it.Err())
	return ids
}

// textScores runs a $text search and returns each match's score by id.
func textScores(t *testing.T, coll Collection, search string) map[string]float64 {
	t.Helper()
	it, err := coll.Find(map[string]any{"$text": map[string]any{"$search": search}}).Iter(ctx)
	require.NoError(t, err)
	defer it.Close()
	scores := map[string]float64{}
	for it.Next() {
		doc, err := it.Doc()
		require.NoError(t, err)
		scores[doc.Value().Get("id").String()] = it.Score()
	}
	require.NoError(t, it.Err())
	return scores
}

const (
	roundTripPeople  = 240
	roundTripNotes   = 60
	roundTripVectors = 300
	roundTripDim     = 8
)

var roundTripWords = []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}

// fillRoundTripSource gives d a collection of each shape the copy has to
// carry: range indexes of every kind, a full-text index with its parameters,
// HNSW, IVF and brute-force vector indexes, the three compression settings,
// a custom primary key, indexes over no documents, and two collections whose
// names share a prefix up to ":". It returns the vectors of the "vectors"
// collection by document id.
func fillRoundTripSource(t *testing.T, d DB) [][]float32 {
	t.Helper()
	people, err := d.CreateCollection(ctx, "people")
	require.NoError(t, err)
	require.NoError(t, people.CreateIndex(ctx,
		IndexInfo{Name: "age", Fields: []string{"age"}},
		IndexInfo{Name: "email", Fields: []string{"email"}, Unique: true},
		IndexInfo{Name: "city_age", Fields: []string{"city", "-age"}, Sparse: true},
		IndexInfo{Name: "zip", Fields: []string{"addr.zip"}},
		IndexInfo{Name: "tags", Fields: []string{"tags"}},
	))
	for i := 0; i < roundTripPeople; i++ {
		city := ""
		if i%7 != 0 {
			city = fmt.Sprintf(`,"city":"c%d"`, i%5)
		}
		require.NoError(t, people.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"name":"p%03d","age":%d,"email":"p%d@example.test","addr":{"zip":%d},"tags":["t%d","t%d"]%s}`,
			i, i, i%50, i, i%17, i%3, i%4, city))))
	}

	notes, err := d.CreateCollection(ctx, "notes")
	require.NoError(t, err)
	require.NoError(t, notes.CreateIndex(ctx, IndexInfo{
		Name: "fts", Kind: IndexKindFulltext, Fields: []string{"title", "body"},
		Fulltext: &FulltextParams{B: 0.6, K1: 1.4, Weights: map[string]float64{"title": 2}},
	}))
	for i := 0; i < roundTripNotes; i++ {
		var body []string
		for j := 0; j <= i%9; j++ {
			body = append(body, roundTripWords[(i*3+j*5)%len(roundTripWords)])
		}
		require.NoError(t, notes.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(
			`{"id":"n%02d","title":"%s %s","body":"%s"}`,
			i, roundTripWords[i%len(roundTripWords)], roundTripWords[(i/3)%len(roundTripWords)], strings.Join(body, " ")))))
	}

	vectors, err := d.CreateCollection(ctx, "vectors")
	require.NoError(t, err)
	vecs := vrand(roundTripVectors, roundTripDim, 41)
	for i, v := range vecs {
		require.NoError(t, vectors.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"v":%s,"w":%s,"u":%s}`, i, vqJSON(v), vqJSON(v), vqJSON(v[:3])))))
	}
	require.NoError(t, vectors.CreateIndex(ctx,
		IndexInfo{Name: "hnsw", Kind: IndexKindVector, Vector: &VectorParams{
			Field: "v", Dim: roundTripDim, Metric: VectorL2, M: 12, EfConstruction: 64, EfSearch: 48}},
		IndexInfo{Name: "ivf", Kind: IndexKindVector, Vector: &VectorParams{
			Field: "w", Dim: roundTripDim, Metric: VectorL2, Mode: VectorModeIVFSQ,
			NList: 8, NProbe: 8, Closure: 2, CompactRatio: 0.5}},
		IndexInfo{Name: "brute", Kind: IndexKindVector, Vector: &VectorParams{
			Field: "u", Dim: 3, Metric: VectorCosine, Mode: VectorModeBruteForce}},
	))

	big := strings.Repeat("compressible text ", 40)
	for _, cc := range []struct {
		name string
		opts CollectionOptions
	}{
		{"s2", CollectionOptions{Compression: S2}},
		{"none", CollectionOptions{Compression: NoCompression}},
		{"inherit", CollectionOptions{}},
		{"keyed", CollectionOptions{PrimaryKey: "key"}},
	} {
		coll, err := d.CreateCollection(ctx, cc.name, cc.opts)
		require.NoError(t, err)
		pk := coll.PrimaryKey()
		for i := 0; i < 20; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{%q:"k%02d","n":%d,"blob":"%s"}`, pk, i, i, big))))
		}
	}

	empty, err := d.CreateCollection(ctx, "empty")
	require.NoError(t, err)
	require.NoError(t, empty.CreateIndex(ctx,
		IndexInfo{Name: "a", Fields: []string{"a"}, Unique: true},
		IndexInfo{Name: "text", Kind: IndexKindFulltext, Fields: []string{"t"}},
		IndexInfo{Name: "vec", Kind: IndexKindVector, Vector: &VectorParams{Field: "v", Dim: 4}},
	))

	a, err := d.CreateCollection(ctx, "A")
	require.NoError(t, err)
	ab, err := d.CreateCollection(ctx, "A:b")
	require.NoError(t, err)
	require.NoError(t, ab.CreateIndex(ctx, IndexInfo{Name: "c", Fields: []string{"x"}}))
	for i := 0; i < 5; i++ {
		require.NoError(t, a.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"x":%d}`, i, i))))
		require.NoError(t, ab.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"x":%d}`, i, i*10))))
	}
	return vecs
}

// vectorRecall is the share of the exact 10 nearest neighbours that k-NN
// searches of the field find, over 20 queries.
func vectorRecall(t *testing.T, coll Collection, field string, vecs [][]float32) float64 {
	t.Helper()
	const k = 10
	found := 0
	for qi, q := range vrand(20, roundTripDim, 97) {
		hits, err := vsearch(coll, field, q, k, 0)
		require.NoError(t, err, "query %d", qi)
		exact := bruteIDs(vecs, q, k)
		for _, h := range hits {
			if exact[string(h.DocId)] {
				found++
			}
		}
	}
	return float64(found) / float64(20*k)
}

// A format 5 database copies into a format 6 one that holds the same
// collections, options, index definitions and documents, value by value,
// and answers the same queries: range queries alike, full-text queries with
// the same documents and scores, vector queries within the usual recall.
// The source's file is not changed by the copy. The copy is format 6 also
// while new files are made in format 5.
func TestCopyToLatestFormat_RoundTrip(t *testing.T) {
	skipIfInMemory(t, "the source is a format 5 file")
	createFormat(t, true)
	src, srcPath := openSource(t, nil)
	vecs := fillRoundTripSource(t, src)
	require.NoError(t, src.Flush(ctx, 0, FlushModeCheckpointTruncate))
	before := fileSum(t, srcPath)

	dstDir := t.TempDir()
	dstPath := filepath.Join(dstDir, "copy.db")
	report, err := CopyToLatestFormat(ctx, src, dstPath, nil)
	require.NoError(t, err)

	assert.Equal(t, 10, report.Collections)
	assert.Equal(t, roundTripPeople+roundTripNotes+roundTripVectors+4*20+2*5, report.Documents)
	assert.Equal(t, 5+1+3+3+1, report.Indexes)
	assert.Positive(t, report.Duration)
	assert.Equal(t, before, fileSum(t, srcPath), "the source file is unchanged")
	fi, err := os.Stat(srcPath + "-wal")
	require.NoError(t, err)
	assert.Zero(t, fi.Size(), "nothing was written to the source")
	assert.Equal(t, []string{"copy.db"}, dirNames(t, dstDir), "only the copy is left")

	dst := openCopy(t, dstPath, nil)
	format, err := SchemaFormat(dst)
	require.NoError(t, err)
	assert.Equal(t, LatestSchemaFormat, format)
	format, err = SchemaFormat(src)
	require.NoError(t, err)
	assert.Equal(t, 5, format)
	assertSameDatabase(t, src, dst)

	open := func(d DB, name string) Collection {
		coll, err := d.OpenCollection(ctx, name)
		require.NoError(t, err)
		return coll
	}
	for _, q := range []struct {
		filter string
		sort   []any
	}{
		{`{"age":{"$gte":10,"$lt":20}}`, []any{"age", "id"}},
		{`{"email":"p42@example.test"}`, nil},
		{`{"city":"c1"}`, []any{"-age", "id"}},
		{`{"city":{"$exists":true},"age":{"$lt":5}}`, []any{"city", "id"}},
		{`{"addr.zip":3}`, []any{"id"}},
		{`{"tags":"t2"}`, []any{"-id"}},
	} {
		want := queryIDs(t, open(src, "people").Find(q.filter).Sort(q.sort...))
		require.NotEmpty(t, want, q.filter)
		assert.Equal(t, want, queryIDs(t, open(dst, "people").Find(q.filter).Sort(q.sort...)), q.filter)
	}
	for _, search := range []string{"alpha", "beta gamma", `"alpha zeta"`, "^zeta", "theta -alpha"} {
		want := textScores(t, open(src, "notes"), search)
		require.NotEmpty(t, want, search)
		assert.Equal(t, want, textScores(t, open(dst, "notes"), search), search)
	}
	for _, field := range []string{"v", "w"} {
		recall := vectorRecall(t, open(dst, "vectors"), field, vecs)
		assert.GreaterOrEqual(t, recall, 0.9, "recall of %s", field)
	}
	hits, err := vsearch(open(dst, "vectors"), "u", vecs[7][:3], 1, 0)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, idBytesOf(7), hits[0].DocId, "brute force is exact")
}

// Ties between equal full-text scores are broken by the internal insertion
// id, and the copy inserts in primary-key order: the order of tied results
// can change, their scores cannot.
func TestCopyToLatestFormat_FulltextTiesFollowPrimaryKey(t *testing.T) {
	src, _ := openSource(t, nil)
	coll, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{Kind: IndexKindFulltext, Fields: []string{"body"}}))
	insertJSON(t, coll, `{"id":"d3","body":"same words"}`, `{"id":"d1","body":"same words"}`, `{"id":"d2","body":"same words"}`)

	dstPath := filepath.Join(t.TempDir(), "copy.db")
	_, err = CopyToLatestFormat(ctx, src, dstPath, nil)
	require.NoError(t, err)
	dst := openCopy(t, dstPath, nil)
	dstColl, err := dst.OpenCollection(ctx, "docs")
	require.NoError(t, err)

	search := map[string]any{"$text": map[string]any{"$search": "same"}}
	srcIDs, srcScores := collectIter(t, coll.Find(search))
	dstIDs, dstScores := collectIter(t, dstColl.Find(search))
	assert.Equal(t, []string{"d3", "d1", "d2"}, srcIDs, "the source breaks the tie in insertion order")
	assert.Equal(t, []string{"d1", "d2", "d3"}, dstIDs, "the copy inserted in primary-key order")
	assert.Equal(t, srcScores, dstScores)
}

// A full-text index built under an older layout is rebuilt under today's:
// a names array whose first element is empty put the next element's first
// token at position 100, so ^mario did not match; in the copy it does.
func TestCopyToLatestFormat_FulltextLegacyLayout(t *testing.T) {
	fx, c := ftsTestColl(t, "names", "body")
	fxi := c.loadFtsIndexes()[0]
	fxi.legacyLeadingGap = true
	insertJSON(t, c, `{"id":"u2","names":["","mario kart"],"body":"x"}`)
	fxi.legacyLeadingGap = false
	find := func(coll Collection, search string) []string {
		ids, _ := collectIter(t, coll.Find(map[string]any{"$text": map[string]any{"$search": search}}))
		return ids
	}
	require.Empty(t, find(c, "^mario"))
	require.Equal(t, []string{"u2"}, find(c, `"mario kart"`))

	dstPath := filepath.Join(t.TempDir(), "copy.db")
	_, err := CopyToLatestFormat(ctx, fx.DB, dstPath, nil)
	require.NoError(t, err)
	dst := openCopy(t, dstPath, nil)
	dstColl, err := dst.OpenCollection(ctx, "docs")
	require.NoError(t, err)
	assert.Equal(t, []string{"u2"}, find(dstColl, "^mario"))
	assert.Equal(t, []string{"u2"}, find(dstColl, `"mario kart"`))
}

// putStored writes a document's stored bytes under its stored key straight
// into a collection's data tree, as an older release left them.
func putStored(t *testing.T, d DB, collName string, key, val []byte) {
	t.Helper()
	dbi := d.(*db)
	require.NoError(t, dbi.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		ns, err := tx.GetNamespace(collName)
		if err != nil {
			return err
		}
		return tx.Put(ns, key, val)
	}))
}

// assertNoCopy checks that a failed copy left nothing in its directory.
func assertNoCopy(t *testing.T, dstPath string) {
	t.Helper()
	assert.Empty(t, dirNames(t, filepath.Dir(dstPath)))
}

// What older releases left behind, written as stored bytes: each case does
// what the guide says it does.
func TestCopyToLatestFormat_OldReleaseData(t *testing.T) {
	t.Run("negative zero is copied as zero", func(t *testing.T) {
		src, _ := openSource(t, nil)
		_, err := src.CreateCollection(ctx, "nums")
		require.NoError(t, err)
		// The encoder before the normalization stored -0 with every bit
		// flipped, as other negative numbers.
		val := anyenc.MustParseJson(`{"id":"neg","v":12345.5}`).MarshalTo(nil)
		val = bytes.Replace(val, anyenc.AppendFloat64(nil, 12345.5), []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, 1)
		putStored(t, src, "nums", anyenc.MustParseJson(`"neg"`).MarshalTo(nil), val)
		coll, err := src.OpenCollection(ctx, "nums")
		require.NoError(t, err)
		doc, err := coll.FindId(ctx, "neg")
		require.NoError(t, err)
		require.True(t, math.Signbit(doc.Value().GetFloat64("v")), "the source holds -0")

		dstPath := filepath.Join(t.TempDir(), "copy.db")
		_, err = CopyToLatestFormat(ctx, src, dstPath, nil)
		require.NoError(t, err)
		dstColl, err := openCopy(t, dstPath, nil).OpenCollection(ctx, "nums")
		require.NoError(t, err)
		doc, err = dstColl.FindId(ctx, "neg")
		require.NoError(t, err)
		v := doc.Value().GetFloat64("v")
		assert.Zero(t, v)
		assert.False(t, math.Signbit(v), "the copy holds +0")
	})
	t.Run("an array primary key fails the copy", func(t *testing.T) {
		src, _ := openSource(t, nil)
		coll, err := src.CreateCollection(ctx, "arr")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"ok"}`)))
		putStored(t, src, "arr", anyenc.MustParseJson(`[1,2]`).MarshalTo(nil), anyenc.MustParseJson(`{"id":[1,2],"x":1}`).MarshalTo(nil))
		_, err = coll.FindId(ctx, anyenc.MustParseJson(`[1,2]`))
		require.NoError(t, err, "the source still reads it")

		dstPath := filepath.Join(t.TempDir(), "copy.db")
		_, err = CopyToLatestFormat(ctx, src, dstPath, nil)
		require.ErrorIs(t, err, ErrArrayPrimaryKey)
		assert.Contains(t, err.Error(), `collection "arr"`)
		assert.Contains(t, err.Error(), `document [1,2]`)
		assertNoCopy(t, dstPath)
	})
	t.Run("a quarantined unique index fails the copy", func(t *testing.T) {
		skipIfInMemory(t, "the quarantine is set by a reopen")
		// As TestIndexFormat_RebuildUniqueViolationQuarantines: a unique
		// sparse index of a release that skipped explicit nulls.
		dir := t.TempDir()
		fx := newFixturePath(t, dir)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "u", Fields: []string{"a"}, Sparse: true}))
		insertJSON(t, coll, `{"id":1,"a":1}`, `{"id":2,"a":null}`, `{"id":3,"a":null}`)
		legacyIndex(t, coll.(*collection), "u")
		setIndexRecord(t, coll.(*collection), "u", "unique", func(a *anyenc.Arena) *anyenc.Value { return a.NewTrue() })
		require.NoError(t, fx.Close())
		fx = newFixturePath(t, dir)
		coll, err = fx.OpenCollection(ctx, "test")
		require.NoError(t, err)
		require.True(t, formatIndex(t, coll, "u").outdated, "the index is quarantined")

		dstPath := filepath.Join(t.TempDir(), "copy.db")
		_, err = CopyToLatestFormat(ctx, fx.DB, dstPath, nil)
		require.ErrorIs(t, err, ErrUniqueConstraint)
		assert.Contains(t, err.Error(), `collection "test": index "u"`)
		assertNoCopy(t, dstPath)
	})
	t.Run("a trained IVF index over no vectors fails the copy", func(t *testing.T) {
		src, _ := openSource(t, nil)
		coll, err := src.CreateCollection(ctx, "emb")
		require.NoError(t, err)
		for i, v := range vrand(64, 8, 5) {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, v))))
		}
		require.NoError(t, coll.CreateIndex(ctx, IndexInfo{Name: "ivf", Kind: IndexKindVector,
			Vector: &VectorParams{Field: "v", Dim: 8, Metric: VectorL2, Mode: VectorModeIVFSQ}}))
		for i := 0; i < 64; i++ {
			require.NoError(t, coll.DeleteId(ctx, i))
		}

		dstPath := filepath.Join(t.TempDir(), "copy.db")
		_, err = CopyToLatestFormat(ctx, src, dstPath, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `collection "emb": index "ivf"`)
		assert.Contains(t, err.Error(), "IVF requires existing documents")
		assertNoCopy(t, dstPath)
	})
	t.Run("an index in a removed vector mode fails the copy", func(t *testing.T) {
		src, _ := openSource(t, nil)
		coll, err := src.CreateCollection(ctx, "emb")
		require.NoError(t, err)
		for i, v := range vrand(64, 8, 5) {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, v))))
		}
		require.NoError(t, coll.CreateIndex(ctx, IndexInfo{Name: "old", Kind: IndexKindVector,
			Vector: &VectorParams{Field: "v", Dim: 8, Metric: VectorL2, Mode: VectorModeIVFSQ}}))
		d := src.(*db)
		thisBuildTx(t, d, func(tx *btree.WriteTx) error { return forgeVectorMode(tx, d, "emb", "old", vectorModeIVFPQ) })

		dstPath := filepath.Join(t.TempDir(), "copy.db")
		_, err = CopyToLatestFormat(ctx, src, dstPath, nil)
		require.ErrorIs(t, err, ErrVectorIndexUnsupported)
		assert.Contains(t, err.Error(), `collection "emb": index "old"`)
		assertNoCopy(t, dstPath)
	})
}

// Sources of other kinds: encrypted, empty, each compression setting, a
// custom primary key, a source already in the latest format.
func TestCopyToLatestFormat_Sources(t *testing.T) {
	keyA := bytes.Repeat([]byte{'a'}, 32) // a raw key: no KDF
	keyB := bytes.Repeat([]byte{'b'}, 32)
	encrypted := func(key []byte) *Config { return &Config{Encryption: EncryptionConfig{Passphrase: key}} }

	t.Run("encrypted", func(t *testing.T) {
		skipIfInMemory(t, "the source is an encrypted file")
		src, _ := openSource(t, encrypted(keyA))
		coll, err := src.CreateCollection(ctx, "docs")
		require.NoError(t, err)
		require.NoError(t, coll.CreateIndex(ctx, IndexInfo{Fields: []string{"n"}}))
		for i := 0; i < 50; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"n":%d}`, i, i%7))))
		}
		for _, tc := range []struct {
			name      string
			key       []byte
			wrongKeys [][]byte
		}{
			{"unencrypted", nil, [][]byte{keyA}},
			{"same passphrase", keyA, [][]byte{nil, keyB}},
			{"other passphrase", keyB, [][]byte{nil, keyA}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var conf *Config
				if tc.key != nil {
					conf = encrypted(tc.key)
				}
				dstPath := filepath.Join(t.TempDir(), "copy.db")
				_, err := CopyToLatestFormat(ctx, src, dstPath, conf)
				require.NoError(t, err)
				for _, wrong := range tc.wrongKeys {
					var wrongConf *Config
					if wrong != nil {
						wrongConf = encrypted(wrong)
					}
					if d, err := Open(ctx, dstPath, wrongConf); err == nil {
						_, err = d.GetCollectionNames(ctx)
						require.NoError(t, d.Close())
						require.Error(t, err, "opened with the wrong key")
					}
				}
				dst := openCopy(t, dstPath, conf)
				assert.Equal(t, tc.key != nil, dst.IntegrityMode() == IntegrityAEAD)
				assertSameDatabase(t, src, dst)
			})
		}
	})
	t.Run("empty", func(t *testing.T) {
		src, _ := openSource(t, nil)
		dstPath := filepath.Join(t.TempDir(), "copy.db")
		report, err := CopyToLatestFormat(ctx, src, dstPath, nil)
		require.NoError(t, err)
		assert.Equal(t, CopyReport{Duration: report.Duration}, report)
		dst := openCopy(t, dstPath, nil)
		names, err := dst.GetCollectionNames(ctx)
		require.NoError(t, err)
		assert.Empty(t, names)
		format, err := SchemaFormat(dst)
		require.NoError(t, err)
		assert.Equal(t, LatestSchemaFormat, format)
	})
	t.Run("compression", func(t *testing.T) {
		src, _ := openSource(t, nil)
		big := strings.Repeat("compressible text ", 40)
		for _, cc := range []struct {
			name string
			c    Compression
		}{{"s2", S2}, {"none", NoCompression}, {"inherit", 0}} {
			coll, err := src.CreateCollection(ctx, cc.name, CollectionOptions{Compression: cc.c})
			require.NoError(t, err)
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":1,"blob":"%s"}`, big))))
		}
		for _, disable := range []bool{false, true} {
			t.Run(fmt.Sprintf("DisableCompression=%v", disable), func(t *testing.T) {
				conf := &Config{DisableCompression: disable}
				dstPath := filepath.Join(t.TempDir(), "copy.db")
				_, err := CopyToLatestFormat(ctx, src, dstPath, conf)
				require.NoError(t, err)
				dst := openCopy(t, dstPath, conf)
				assertSameDatabase(t, src, dst)
				_, entries := catalogOf(t, dst)
				assert.Equal(t, S2, entries["s2"].cfg.Compression)
				assert.Equal(t, NoCompression, entries["none"].cfg.Compression)
				assert.Equal(t, Compression(0), entries["inherit"].cfg.Compression, "inherits, not set")
				for name, want := range map[string]bool{"s2": true, "none": false, "inherit": !disable} {
					coll, err := dst.OpenCollection(ctx, name)
					require.NoError(t, err)
					st, err := coll.Stats(ctx)
					require.NoError(t, err)
					assert.Equal(t, want, st.CompressionEnabled, name)
				}
			})
		}
	})
	t.Run("custom primary key", func(t *testing.T) {
		src, _ := openSource(t, nil)
		coll, err := src.CreateCollection(ctx, "keyed", CollectionOptions{PrimaryKey: "key"})
		require.NoError(t, err)
		insertJSON(t, coll, `{"key":"b","n":2}`, `{"key":"a","n":1}`)
		dstPath := filepath.Join(t.TempDir(), "copy.db")
		_, err = CopyToLatestFormat(ctx, src, dstPath, nil)
		require.NoError(t, err)
		dst := openCopy(t, dstPath, nil)
		assertSameDatabase(t, src, dst)
		dstColl, err := dst.OpenCollection(ctx, "keyed")
		require.NoError(t, err)
		assert.Equal(t, "key", dstColl.PrimaryKey())
		doc, err := dstColl.FindId(ctx, "a")
		require.NoError(t, err)
		assert.Equal(t, 1.0, doc.Value().GetFloat64("n"))
	})
	t.Run("latest format", func(t *testing.T) {
		createFormat(t, false)
		src, _ := openSource(t, nil)
		fillRoundTripSource(t, src)
		dstPath := filepath.Join(t.TempDir(), "copy.db")
		_, err := CopyToLatestFormat(ctx, src, dstPath, nil)
		require.NoError(t, err)
		assertSameDatabase(t, src, openCopy(t, dstPath, nil))
	})
	t.Run("a collection named after another and a colon", func(t *testing.T) {
		src, _ := openSource(t, nil)
		a, err := src.CreateCollection(ctx, "A")
		require.NoError(t, err)
		ab, err := src.CreateCollection(ctx, "A:b")
		require.NoError(t, err)
		require.NoError(t, ab.CreateIndex(ctx, IndexInfo{Name: "c", Fields: []string{"x"}}))
		insertJSON(t, a, `{"id":1,"x":1}`)
		insertJSON(t, ab, `{"id":1,"x":2}`)
		dstPath := filepath.Join(t.TempDir(), "copy.db")
		report, err := CopyToLatestFormat(ctx, src, dstPath, nil)
		require.NoError(t, err)
		assert.Equal(t, 1, report.Indexes)
		dst := openCopy(t, dstPath, nil)
		assertSameDatabase(t, src, dst)
		_, entries := catalogOf(t, dst)
		assert.Empty(t, entries["A"].indexes)
		require.Len(t, entries["A:b"].indexes, 1)
		assert.Equal(t, "c", entries["A:b"].indexes[0].Name)
	})
}

// The call refuses what it cannot copy, or copy to, before it creates
// anything, and touches nothing it was not asked to create.
func TestCopyToLatestFormat_Refusals(t *testing.T) {
	src, _ := openSource(t, nil)
	coll, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	insertJSON(t, coll, `{"id":1}`)

	t.Run("a DB of another type", func(t *testing.T) {
		dstPath := filepath.Join(t.TempDir(), "copy.db")
		_, err := CopyToLatestFormat(ctx, foreignDB{src}, dstPath, nil)
		require.ErrorIs(t, err, ErrUnsupportedDB)
		assertNoCopy(t, dstPath)
	})
	t.Run("a transaction in ctx", func(t *testing.T) {
		dstPath := filepath.Join(t.TempDir(), "copy.db")
		rtx, err := src.ReadTx(ctx)
		require.NoError(t, err)
		_, err = CopyToLatestFormat(rtx.Context(), src, dstPath, nil)
		require.ErrorIs(t, err, ErrTxInContext)
		require.NoError(t, rtx.Commit())
		wtx, err := src.WriteTx(ctx)
		require.NoError(t, err)
		_, err = CopyToLatestFormat(wtx.Context(), src, dstPath, nil)
		require.ErrorIs(t, err, ErrTxInContext)
		require.NoError(t, wtx.Rollback())
		assertNoCopy(t, dstPath)
	})
	t.Run("in memory", func(t *testing.T) {
		dstPath := filepath.Join(t.TempDir(), "copy.db")
		_, err := CopyToLatestFormat(ctx, src, dstPath, &Config{InMemory: true})
		require.Error(t, err)
		assertNoCopy(t, dstPath)
	})
	for _, suffix := range []string{"", "-wal", "-wal-shm", ".lock"} {
		t.Run("the destination exists: "+suffix, func(t *testing.T) {
			dstPath := filepath.Join(t.TempDir(), "copy.db")
			require.NoError(t, os.WriteFile(dstPath+suffix, []byte("keep"), 0o644))
			_, err := CopyToLatestFormat(ctx, src, dstPath, nil)
			require.ErrorIs(t, err, ErrDestinationExists)
			assert.Equal(t, []string{filepath.Base(dstPath + suffix)}, dirNames(t, filepath.Dir(dstPath)))
			data, err := os.ReadFile(dstPath + suffix)
			require.NoError(t, err)
			assert.Equal(t, "keep", string(data))
		})
	}
	// The names a copy keeps for its lock and its staging files, in any
	// case: a database published under one would be locked, or removed as
	// a killed copy's leftover, by a copy to the name before the suffix.
	for _, suffix := range []string{".copy-lock", ".copy-staging", ".copy-staging-wal", ".copy-staging-wal-shm",
		".copy-staging.lock", ".COPY-LOCK", ".Copy-Staging", ".copy-\u017ftaging-WAL"} {
		t.Run("the destination is a reserved name: "+suffix, func(t *testing.T) {
			dstPath := filepath.Join(t.TempDir(), "copy.db")
			_, err := CopyToLatestFormat(ctx, src, dstPath+suffix, nil)
			require.ErrorContains(t, err, "a name the copy keeps for its own files")
			assertNoCopy(t, dstPath)
		})
	}
	t.Run("the destination is the source", func(t *testing.T) {
		_, err := CopyToLatestFormat(ctx, src, src.(*db).btreeDB.Path(), nil)
		require.ErrorIs(t, err, ErrDestinationExists)
	})
}

// A commit on the source while the copy runs is not in the copy, which is
// the source as of the copy's snapshot.
func TestCopyToLatestFormat_SnapshotIgnoresConcurrentCommits(t *testing.T) {
	src, _ := openSource(t, nil)
	for _, name := range []string{"a", "b"} {
		coll, err := src.CreateCollection(ctx, name)
		require.NoError(t, err)
		require.NoError(t, coll.CreateIndex(ctx, IndexInfo{Name: "n", Fields: []string{"n"}}))
		for i := 0; i < 10; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"n":%d}`, i, i))))
		}
	}
	snapshot := t.TempDir()
	require.NoError(t, src.Backup(ctx, filepath.Join(snapshot, "snapshot.db")))

	commits := 0
	testHookCopiedCollection = func(name string) {
		if name != "a" {
			return
		}
		// "a" is copied, "b" is not yet: change both, and the catalog.
		for _, coll := range []string{"a", "b"} {
			c, err := src.OpenCollection(ctx, coll)
			require.NoError(t, err)
			require.NoError(t, c.Insert(ctx, anyenc.MustParseJson(`{"id":100,"n":100}`)))
			require.NoError(t, c.DeleteId(ctx, 0))
			require.NoError(t, c.CreateIndex(ctx, IndexInfo{Name: "later", Fields: []string{"m"}}))
			commits++
		}
		_, err := src.CreateCollection(ctx, "c")
		require.NoError(t, err)
	}
	defer func() { testHookCopiedCollection = nil }()

	dstPath := filepath.Join(t.TempDir(), "copy.db")
	report, err := CopyToLatestFormat(ctx, src, dstPath, nil)
	require.NoError(t, err)
	require.Equal(t, 2, commits)
	assert.Equal(t, CopyReport{Collections: 2, Documents: 20, Indexes: 2, Duration: report.Duration}, report)
	assertSameDatabase(t, openCopy(t, filepath.Join(snapshot, "snapshot.db"), nil), openCopy(t, dstPath, nil))
}

// A ctx cancelled while the copy runs fails it, and the copy's files go.
func TestCopyToLatestFormat_Cancelled(t *testing.T) {
	src, _ := openSource(t, nil)
	for _, name := range []string{"a", "b"} {
		coll, err := src.CreateCollection(ctx, name)
		require.NoError(t, err)
		insertJSON(t, coll, `{"id":1}`, `{"id":2}`)
	}
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	testHookCopiedCollection = func(string) { cancel() }
	defer func() { testHookCopiedCollection = nil }()

	dstPath := filepath.Join(t.TempDir(), "copy.db")
	report, err := CopyToLatestFormat(cctx, src, dstPath, nil)
	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, report)
	assertNoCopy(t, dstPath)
}

// A cancellation that comes once the copy is verified and closed, before
// it is published, publishes nothing.
func TestCopyToLatestFormat_CancelledBeforePublication(t *testing.T) {
	src, _ := openSource(t, nil)
	coll, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	insertJSON(t, coll, `{"id":1}`)
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	testHookBeforePublish = func(string) { cancel() }
	defer func() { testHookBeforePublish = nil }()

	dstPath := filepath.Join(t.TempDir(), "copy.db")
	report, err := CopyToLatestFormat(cctx, src, dstPath, nil)
	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, report)
	assertNoCopy(t, dstPath)
}

// Two calls copying to one destination at once: the first holds the
// destination's lock for the whole call, so the second fails at once with
// ErrBusy, whenever it comes, and creates and removes nothing; the first
// publishes.
func TestCopyToLatestFormat_ConcurrentCopiesToOneDestination(t *testing.T) {
	src, _ := openSource(t, nil)
	coll, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{Fields: []string{"n"}}))
	for i := 0; i < 50; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"n":%d}`, i, i))))
	}
	dstDir := t.TempDir()
	dstPath := filepath.Join(dstDir, "copy.db")

	// The second call runs while the first is copying, and again when the
	// first is about to publish.
	second := func(string) {
		before := dirNames(t, dstDir)
		done := make(chan error, 1)
		go func() {
			_, err := CopyToLatestFormat(ctx, src, dstPath, nil)
			done <- err
		}()
		select {
		case err := <-done:
			assert.ErrorIs(t, err, ErrBusy)
			assert.NotErrorIs(t, err, ErrDestinationExists)
		case <-time.After(time.Minute):
			t.Error("the second copy waits")
		}
		assert.Equal(t, before, dirNames(t, dstDir), "the second copy touches nothing")
		assert.Contains(t, before, "copy.db.copy-staging")
		assert.Contains(t, before, "copy.db.copy-lock")
	}
	var attempts int
	testHookCopiedCollection = func(name string) { attempts++; second(name) }
	testHookBeforePublish = func(stage string) { attempts++; second(stage) }
	defer func() { testHookCopiedCollection, testHookBeforePublish = nil, nil }()

	_, err = CopyToLatestFormat(ctx, src, dstPath, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, attempts)
	assert.Equal(t, []string{"copy.db"}, dirNames(t, dstDir))
	assertSameDatabase(t, src, openCopy(t, dstPath, nil))

	// The lock went with the call: the destination is what refuses now.
	_, err = CopyToLatestFormat(ctx, src, dstPath, nil)
	require.ErrorIs(t, err, ErrDestinationExists)
}

// Unlimited free concurrency on one destination: of many calls started
// together exactly one publishes, each of the others fails with ErrBusy or,
// once the copy is there, ErrDestinationExists, and nothing but the copy is
// left.
func TestCopyToLatestFormat_ManyCopiesToOneDestination(t *testing.T) {
	src, _ := openSource(t, nil)
	coll, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	insertJSON(t, coll, `{"id":1}`, `{"id":2}`)
	dstDir := t.TempDir()
	dstPath := filepath.Join(dstDir, "copy.db")

	const callers = 8
	errs := make(chan error, callers)
	start := make(chan struct{})
	for range callers {
		go func() {
			<-start
			_, err := CopyToLatestFormat(ctx, src, dstPath, nil)
			errs <- err
		}()
	}
	close(start)
	var published int
	for range callers {
		select {
		case err := <-errs:
			switch {
			case err == nil:
				published++
			case errors.Is(err, ErrBusy), errors.Is(err, ErrDestinationExists):
			default:
				t.Errorf("unexpected error: %v", err)
			}
		case <-time.After(time.Minute):
			t.Fatal("the copies did not end")
		}
	}
	assert.Equal(t, 1, published)
	assert.Equal(t, []string{"copy.db"}, dirNames(t, dstDir))
	assertSameDatabase(t, src, openCopy(t, dstPath, nil))
}

// Spellings of one destination are one destination: while a copy to it
// runs, a copy to another spelling of it, absolute or relative, is refused
// with ErrBusy and touches nothing, so it removes none of the first copy's
// staging files; the first publishes. On Unix and Windows the spellings
// meet at the lock file, one file whatever names it; on the other
// platforms the copy lock is the process's (copylock_other.go).
func TestCopyToLatestFormat_DestinationSpellings(t *testing.T) {
	filesOnJS(t)
	src, _ := openSource(t, nil)
	coll, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	insertJSON(t, coll, `{"id":1}`, `{"id":2}`)
	dstDir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dstDir, "sub"), 0o755))
	dstPath := filepath.Join(dstDir, "copy.db")
	t.Chdir(dstDir)
	sep := string(filepath.Separator)
	spellings := []string{
		dstDir + sep + "." + sep + "copy.db",
		dstDir + sep + "sub" + sep + ".." + sep + "copy.db",
		"copy.db",
		"." + sep + "copy.db",
	}

	var tried int
	testHookCopiedCollection = func(string) {
		before := dirNames(t, dstDir)
		assert.Contains(t, before, "copy.db.copy-staging")
		for _, spelling := range spellings {
			tried++
			_, err := CopyToLatestFormat(ctx, src, spelling, nil)
			assert.ErrorIs(t, err, ErrBusy, spelling)
			assert.Equal(t, before, dirNames(t, dstDir), "a copy to %s touches nothing", spelling)
		}
	}
	defer func() { testHookCopiedCollection = nil }()

	_, err = CopyToLatestFormat(ctx, src, dstPath, nil)
	require.NoError(t, err)
	assert.Equal(t, len(spellings), tried)
	assert.Equal(t, []string{"copy.db", "sub"}, dirNames(t, dstDir))
	assertSameDatabase(t, src, openCopy(t, dstPath, nil))
}

// filesOnJS backs the databases of a test with the os package under
// js/wasm, where the btree has no file backend of its own unless SetVFS
// gives it one (internal/btree/osfuncs_vfs_js.go), and Node's file system
// serves os. Elsewhere it does nothing.
func filesOnJS(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "js" {
		return
	}
	SetVFS(VFS{
		OpenFile: func(name string, flag int, perm os.FileMode) (File, error) {
			f, err := os.OpenFile(name, flag, perm)
			if err != nil {
				return nil, err
			}
			return f, nil
		},
		Remove:    os.Remove,
		Fdatasync: func(f File) error { return f.Sync() },
	})
	t.Cleanup(ResetVFS)
}

// A lock another holder has on the destination fails the call at once with
// ErrBusy: nothing is created and nothing removed, not even the staging
// files a killed copy left, which could be the holder's.
func TestCopyToLatestFormat_LockHeld(t *testing.T) {
	src, _ := openSource(t, nil)
	coll, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	insertJSON(t, coll, `{"id":1}`)
	dstDir := t.TempDir()
	dstPath := filepath.Join(dstDir, "copy.db")
	release, err := acquireCopyLock(dstPath + ".copy-lock")
	require.NoError(t, err)
	staged := []string{"copy.db.copy-lock", "copy.db.copy-staging", "copy.db.copy-staging-wal"}
	for _, name := range staged[1:] {
		require.NoError(t, os.WriteFile(filepath.Join(dstDir, name), []byte("the holder's"), 0o644))
	}

	report, err := CopyToLatestFormat(ctx, src, dstPath, nil)
	require.ErrorIs(t, err, ErrBusy)
	assert.Contains(t, err.Error(), "another copy holds the copy lock of "+dstPath)
	assert.Zero(t, report)
	assert.Equal(t, staged, dirNames(t, dstDir))
	for _, name := range staged[1:] {
		data, err := os.ReadFile(filepath.Join(dstDir, name))
		require.NoError(t, err)
		assert.Equal(t, "the holder's", string(data))
	}

	// Once the holder is gone the call goes through, over what it left.
	release()
	_, err = CopyToLatestFormat(ctx, src, dstPath, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"copy.db"}, dirNames(t, dstDir))
	assertSameDatabase(t, src, openCopy(t, dstPath, nil))
}

// The lock of a copy, on every platform: one holder at a time, a second is
// refused at once with ErrBusy, and a released lock is free for the next.
// What the lock leaves on disk depends on the platform: copylock_test.go
// for Unix and Windows, copylock_other_test.go for the others.
func TestCopyLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "copy.db.copy-lock")
	release, err := acquireCopyLock(path)
	require.NoError(t, err)
	_, err = acquireCopyLock(path)
	require.ErrorIs(t, err, ErrBusy)
	release()
	release, err = acquireCopyLock(path)
	require.NoError(t, err)
	release()

	// Holders that come and go, each removing the lock file, where there is
	// one, as it leaves: never two at once.
	var holders, entered atomic.Int32
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 300 {
				release, err := acquireCopyLock(path)
				if err != nil {
					if !errors.Is(err, ErrBusy) {
						t.Errorf("unexpected error: %v", err)
					}
					continue
				}
				if holders.Add(1) != 1 {
					t.Error("two holders of one lock")
				}
				entered.Add(1)
				holders.Add(-1)
				release()
			}
		}()
	}
	wg.Wait()
	assert.NotZero(t, entered.Load())
}

// The staging files a killed copy left beside the destination, with no lock
// held, are removed by the next copy to it, which succeeds; a leftover lock
// file is taken over. Nothing else in the directory is touched.
func TestCopyToLatestFormat_StaleStagingRemoved(t *testing.T) {
	src, _ := openSource(t, nil)
	coll, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	insertJSON(t, coll, `{"id":1}`, `{"id":2}`)
	dstDir := t.TempDir()
	dstPath := filepath.Join(dstDir, "copy.db")
	for _, name := range []string{"copy.db.copy-lock", "copy.db.copy-staging", "copy.db.copy-staging-wal",
		"copy.db.copy-staging-wal-shm", "copy.db.copy-staging.lock"} {
		require.NoError(t, os.WriteFile(filepath.Join(dstDir, name), []byte("left by a killed copy"), 0o644))
	}
	// Not names of a copy to copy.db: they stay.
	foreign := []string{"copy.db.copy-staging2", "copy.db.tmp-0123456789abcdef", "other.db.copy-staging"}
	for _, name := range foreign {
		require.NoError(t, os.WriteFile(filepath.Join(dstDir, name), []byte("foreign"), 0o644))
	}

	report, err := CopyToLatestFormat(ctx, src, dstPath, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, report.Documents)
	assert.Equal(t, append([]string{"copy.db"}, foreign...), dirNames(t, dstDir))
	for _, name := range foreign {
		data, err := os.ReadFile(filepath.Join(dstDir, name))
		require.NoError(t, err)
		assert.Equal(t, "foreign", string(data))
	}
	assertSameDatabase(t, src, openCopy(t, dstPath, nil))
}

// A staging file of a killed copy that cannot be removed fails the call
// before anything is copied, naming the file.
func TestCopyToLatestFormat_StaleStagingNotRemovable(t *testing.T) {
	src, _ := openSource(t, nil)
	dstDir := t.TempDir()
	dstPath := filepath.Join(dstDir, "copy.db")
	require.NoError(t, os.WriteFile(dstPath+".copy-staging-wal", []byte("left"), 0o644))
	errRemove := errors.New("cannot remove")
	prevRemove := osRemove
	osRemove = func(name string) error {
		if name == dstPath+".copy-staging-wal" {
			return &os.PathError{Op: "remove", Path: name, Err: errRemove}
		}
		return prevRemove(name)
	}
	defer func() { osRemove = prevRemove }()

	_, err := CopyToLatestFormat(ctx, src, dstPath, nil)
	require.ErrorIs(t, err, errRemove)
	assert.Contains(t, err.Error(), dstPath+".copy-staging-wal")
	assert.Equal(t, []string{"copy.db.copy-staging-wal"}, dirNames(t, dstDir))
}

// A closed source, in memory or on disk, fails the call with ErrDBIsClosed
// before it creates, writes or removes anything: a staging file a killed
// copy left stays as it was, no lock file is made, and a destination that
// exists does not make it ErrDestinationExists.
func TestCopyToLatestFormat_ClosedSource(t *testing.T) {
	for _, inMemory := range []bool{false, true} {
		t.Run(fmt.Sprintf("InMemory=%v", inMemory), func(t *testing.T) {
			var src DB
			if inMemory {
				var err error
				src, err = Open(ctx, ":memory:", &Config{InMemory: true})
				require.NoError(t, err)
			} else {
				src, _ = openSource(t, nil)
			}
			coll, err := src.CreateCollection(ctx, "docs")
			require.NoError(t, err)
			insertJSON(t, coll, `{"id":1}`)
			require.NoError(t, src.Close())

			dir := t.TempDir()
			dstPath := filepath.Join(dir, "x")
			stage := dstPath + ".copy-staging"
			require.NoError(t, os.WriteFile(stage, []byte("left"), 0o644))
			left, err := os.Lstat(stage)
			require.NoError(t, err)
			for _, names := range [][]string{{"x.copy-staging"}, {"x", "x.copy-staging"}} {
				if len(names) == 2 {
					require.NoError(t, os.WriteFile(dstPath, []byte("keep"), 0o644))
				}
				report, err := CopyToLatestFormat(ctx, src, dstPath, nil)
				require.ErrorIs(t, err, ErrDBIsClosed)
				assert.Zero(t, report)
				assert.Equal(t, names, dirNames(t, dir), "the copy touches nothing")
				now, err := os.Lstat(stage)
				require.NoError(t, err)
				assert.True(t, os.SameFile(left, now), "the staging file is the one left there")
				data, err := os.ReadFile(stage)
				require.NoError(t, err)
				assert.Equal(t, "left", string(data))
			}
		})
	}
}

// A source whose database file is under a name a copy keeps for itself
// beside the destination is refused before anything is created or removed:
// the copy would remove the file as a killed copy's staging file, with its
// log and the commits not yet checkpointed out of it, or lock it as its
// lock file. A copy to x+"-wal", x+"-wal-shm" or x+".lock" holds x's copy
// lock too, so a source at x+".copy-lock" is refused for those as well. The
// files are compared by identity, so another spelling of the destination is
// refused too: a relative one, one through a symbolic link to the
// directory, and, on a case-insensitive filesystem, one in another case.
// The source keeps every file and every commit, and goes on committing.
func TestCopyToLatestFormat_SourceUnderReservedName(t *testing.T) {
	filesOnJS(t)
	for _, tc := range []struct {
		suffix string
		dsts   []string
	}{
		{".copy-staging", []string{"x"}},
		{".copy-staging-wal", []string{"x"}},
		{".copy-staging-wal-shm", []string{"x"}},
		{".copy-staging.lock", []string{"x"}},
		{".copy-lock", []string{"x", "x-wal", "x-wal-shm", "x.lock"}},
	} {
		suffix := tc.suffix
		t.Run(suffix, func(t *testing.T) {
			dir := t.TempDir()
			srcPath := filepath.Join(dir, "x"+suffix)
			// The commits stay in the source's log: removing the log would
			// take them with it.
			conf := &Config{DisableAutoCheckpoint: true}
			src, err := Open(ctx, srcPath, conf)
			require.NoError(t, err)
			closed := false
			defer func() {
				if !closed {
					_ = src.Close()
				}
			}()
			coll, err := src.CreateCollection(ctx, "docs")
			require.NoError(t, err)
			insertJSON(t, coll, `{"id":1}`, `{"id":2}`, `{"id":3}`)

			t.Chdir(dir)
			link := filepath.Join(t.TempDir(), "link")
			linked := os.Symlink(dir, link) == nil
			if !linked {
				t.Log("no symbolic link to the directory")
			}
			_, err = os.Stat(filepath.Join(dir, "X"+suffix))
			caseless := err == nil
			var spellings []string
			for _, dst := range tc.dsts {
				spellings = append(spellings, filepath.Join(dir, dst), dst)
				if linked {
					spellings = append(spellings, filepath.Join(link, dst))
				}
				if caseless {
					spellings = append(spellings, filepath.Join(dir, "X"+dst[1:]))
				}
			}
			before := dirNames(t, dir)
			for _, dstPath := range spellings {
				report, err := CopyToLatestFormat(ctx, src, dstPath, nil)
				require.ErrorContains(t, err, "a name the copy keeps for its own files, is the source's file", dstPath)
				assert.NotErrorIs(t, err, ErrDestinationExists)
				assert.NotErrorIs(t, err, ErrBusy)
				assert.Zero(t, report)
				assert.Equal(t, before, dirNames(t, dir), "a copy to %s touches nothing", dstPath)
			}

			insertJSON(t, coll, `{"id":4}`)
			closed = true
			require.NoError(t, src.Close())
			reopened := openCopy(t, srcPath, conf)
			c, err := reopened.OpenCollection(ctx, "docs")
			require.NoError(t, err)
			n, err := c.Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, 4, n)
		})
	}
}

// A source opened as d/link/../x.copy-staging, with d/link a symbolic link
// to r/sub, is the file r/x.copy-staging: the system resolves the link
// before the "..". Its path, which the btree cleans lexically, is
// d/x.copy-staging, which names nothing. A copy to r/x is refused by the
// files the source has open, not by that path: it fails, every file of the
// source stays as it was, and the source reopens with its commits. Where
// the system takes ".." lexically (Windows), the spelling names d/x.copy-staging,
// and the case does not arise.
func TestCopyToLatestFormat_SourceThroughLinkAndDotDot(t *testing.T) {
	filesOnJS(t)
	sep := string(filepath.Separator)
	for _, tc := range []struct {
		suffix string
		dsts   []string
	}{
		{".copy-staging", []string{"x"}},
		{".copy-staging-wal", []string{"x"}},
		{".copy-staging-wal-shm", []string{"x"}},
		{".copy-staging.lock", []string{"x"}},
		{".copy-lock", []string{"x", "x-wal"}},
	} {
		suffix := tc.suffix
		t.Run(suffix, func(t *testing.T) {
			top := t.TempDir()
			d, r := filepath.Join(top, "d"), filepath.Join(top, "r")
			require.NoError(t, os.MkdirAll(filepath.Join(r, "sub"), 0o755))
			require.NoError(t, os.Mkdir(d, 0o755))
			link := filepath.Join(d, "link")
			if err := os.Symlink(filepath.Join(r, "sub"), link); err != nil {
				t.Skipf("no symbolic link to a directory: %v", err)
			}
			require.NoError(t, os.WriteFile(filepath.Join(r, "probe"), nil, 0o644))
			if _, err := os.Stat(link + sep + ".." + sep + "probe"); err != nil {
				t.Skipf("the system does not resolve the link before \"..\": %v", err)
			}
			require.NoError(t, os.Remove(filepath.Join(r, "probe")))

			// The commits stay in the source's log: removing the log, or
			// the file beside it, would take them with it.
			conf := &Config{DisableAutoCheckpoint: true}
			spelled := link + sep + ".." + sep + "x" + suffix
			src, err := Open(ctx, spelled, conf)
			require.NoError(t, err)
			closed := false
			defer func() {
				if !closed {
					_ = src.Close()
				}
			}()
			require.Equal(t, filepath.Join(d, "x"+suffix), src.(*db).btreeDB.Path())
			coll, err := src.CreateCollection(ctx, "docs")
			require.NoError(t, err)
			insertJSON(t, coll, `{"id":1}`, `{"id":2}`, `{"id":3}`)

			names := dirNames(t, r)
			require.Contains(t, names, "x"+suffix)
			before := make(map[string]os.FileInfo, len(names))
			for _, name := range names {
				fi, err := os.Lstat(filepath.Join(r, name))
				require.NoError(t, err)
				before[name] = fi
			}
			for _, dst := range tc.dsts {
				dstPath := filepath.Join(r, dst)
				report, err := CopyToLatestFormat(ctx, src, dstPath, nil)
				require.ErrorContains(t, err, "a name the copy keeps for its own files, is the source's file", dstPath)
				assert.Zero(t, report)
				assert.Equal(t, names, dirNames(t, r), "a copy to %s touches nothing", dstPath)
				assert.Equal(t, []string{"link"}, dirNames(t, d))
				for name, fi := range before {
					now, err := os.Lstat(filepath.Join(r, name))
					if assert.NoError(t, err, name) {
						assert.True(t, os.SameFile(fi, now), "%s is the file it was", name)
					}
				}
			}

			insertJSON(t, coll, `{"id":4}`)
			closed = true
			require.NoError(t, src.Close())
			reopened := openCopy(t, filepath.Join(r, "x"+suffix), conf)
			c, err := reopened.OpenCollection(ctx, "docs")
			require.NoError(t, err)
			n, err := c.Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, 4, n)
		})
	}
}

// A lock that cannot be taken for another reason than its being held, and
// a rename the filesystem refuses, fail the call: the copy's files go and
// nothing is at the destination.
func TestCopyToLatestFormat_LockAndRenameFaults(t *testing.T) {
	src, _ := openSource(t, nil)
	coll, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	insertJSON(t, coll, `{"id":1}`)
	errFault := errors.New("refused")

	t.Run("lock", func(t *testing.T) {
		prevLock := lockCopy
		lockCopy = func(string) (func(), error) { return nil, errFault }
		defer func() { lockCopy = prevLock }()
		dstPath := filepath.Join(t.TempDir(), "copy.db")
		report, err := CopyToLatestFormat(ctx, src, dstPath, nil)
		require.ErrorIs(t, err, errFault)
		assert.NotErrorIs(t, err, ErrBusy)
		assert.Zero(t, report)
		assertNoCopy(t, dstPath)
	})
	t.Run("rename", func(t *testing.T) {
		prevRename := osRename
		var renames int
		osRename = func(oldpath, newpath string) error {
			renames++
			return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: errFault}
		}
		defer func() { osRename = prevRename }()
		dstPath := filepath.Join(t.TempDir(), "copy.db")
		report, err := CopyToLatestFormat(ctx, src, dstPath, nil)
		require.ErrorIs(t, err, errFault)
		assert.Contains(t, err.Error(), "publishing the copy")
		assert.Equal(t, 1, renames)
		assert.Zero(t, report)
		assertNoCopy(t, dstPath)
	})
}

// A file that appears at the destination or beside it while the copy runs,
// after the checks at the start, fails the publication with
// ErrDestinationExists at the check before the rename: the copy's staging
// files go and the file stays as it is, the one at the destination not
// replaced by the rename.
func TestCopyToLatestFormat_SidecarAppearsBeforePublication(t *testing.T) {
	src, _ := openSource(t, nil)
	coll, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	insertJSON(t, coll, `{"id":1}`)
	defer func() { testHookBeforePublish = nil }()
	prevRename := osRename
	var renames int
	osRename = func(oldpath, newpath string) error {
		renames++
		return prevRename(oldpath, newpath)
	}
	defer func() { osRename = prevRename }()
	for _, suffix := range []string{"", "-wal", "-wal-shm", ".lock"} {
		t.Run("appears: "+suffix, func(t *testing.T) {
			dstPath := filepath.Join(t.TempDir(), "copy.db")
			testHookBeforePublish = func(string) {
				require.NoError(t, os.WriteFile(dstPath+suffix, []byte("foreign"), 0o644))
			}
			_, err := CopyToLatestFormat(ctx, src, dstPath, nil)
			require.ErrorIs(t, err, ErrDestinationExists)
			assert.Equal(t, []string{"copy.db" + suffix}, dirNames(t, filepath.Dir(dstPath)))
			data, err := os.ReadFile(dstPath + suffix)
			require.NoError(t, err)
			assert.Equal(t, "foreign", string(data))
			assert.Zero(t, renames)
		})
	}
}

// Two copies whose destinations overlap, one at p and one at a name the
// database at p keeps beside it (p+"-wal", p+"-wal-shm", p+".lock", in any
// case): published both, the open of p would take the other copy for its
// log, its log's index or its sentinel, and truncate or replace it. Their
// copy locks are different files, so a copy to such a name holds p's copy
// lock too, and the two exclude each other however they interleave. A
// chain needs no more: p+"-wal-wal" is p+"-wal"'s log, and meets it at
// p+"-wal"'s lock.
//
// The interleaving forced here is the one that two separate locks let
// through: the second copy starts while the first is copying, under its
// locks, and the first goes on to its rename only once the second has
// returned or has come to its own rename, past every check of the
// destination. Exactly one publishes, in either start order: the second
// returns ErrBusy while the first waits for it, so at once, and touches
// nothing.
func TestCopyToLatestFormat_OverlappingDestinations(t *testing.T) {
	src, _ := openSource(t, nil)
	coll, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	insertJSON(t, coll, `{"id":1}`)
	prevRename := osRename
	defer func() {
		testHookCopiedCollection = nil
		osRename = prevRename
	}()

	// Each pair is a database's name and a name it keeps beside it, as
	// suffixes of p.
	for _, pair := range [][2]string{{"", "-wal"}, {"", "-wal-shm"}, {"", ".lock"}, {"", "-WAL"}, {"-wal", "-wal-wal"}} {
		for _, sidecarFirst := range []bool{false, true} {
			first, second := "p"+pair[0], "p"+pair[1]
			if sidecarFirst {
				first, second = second, first
			}
			t.Run(first+" then "+second, func(t *testing.T) {
				dir := t.TempDir()
				p := filepath.Join(dir, "copy.db")
				base := p + pair[0]
				first, second := base, p+pair[1]
				if sidecarFirst {
					first, second = second, first
				}
				other := map[string]string{first: second, second: first}
				// renaming[d] is closed when the copy to d comes to its
				// rename, returned[d] when the call for d returns.
				renaming := map[string]chan struct{}{first: make(chan struct{}), second: make(chan struct{})}
				returned := map[string]chan struct{}{first: make(chan struct{}), second: make(chan struct{})}
				osRename = func(oldpath, newpath string) error {
					close(renaming[newpath])
					select {
					case <-renaming[other[newpath]]:
					case <-returned[other[newpath]]:
					case <-time.After(time.Minute):
						t.Errorf("the copy to %s neither renames nor returns", other[newpath])
					}
					return prevRename(oldpath, newpath)
				}
				errs := map[string]chan error{first: make(chan error, 1), second: make(chan error, 1)}
				run := func(dst string) {
					_, err := CopyToLatestFormat(ctx, src, dst, nil)
					close(returned[dst])
					errs[dst] <- err
				}
				var started atomic.Bool
				testHookCopiedCollection = func(string) {
					if started.Swap(true) {
						return
					}
					// The first copy is copying: the second starts.
					before := dirNames(t, dir)
					go run(second)
					select {
					case <-returned[second]:
						assert.Equal(t, before, dirNames(t, dir), "the copy to %s touches nothing", second)
					case <-renaming[second]:
						t.Errorf("the copy to %s is past every check while the copy to %s runs", second, first)
					case <-time.After(time.Minute):
						t.Errorf("the copy to %s waits", second)
					}
				}
				go run(first)
				for _, dst := range []string{first, second} {
					select {
					case err := <-errs[dst]:
						if dst == first {
							assert.NoError(t, err, "the copy to %s publishes", dst)
						} else {
							assert.ErrorIs(t, err, ErrBusy, "the copy to %s is refused", dst)
							assert.ErrorContains(t, err, "another copy holds the copy lock of "+base)
						}
					case <-time.After(2 * time.Minute):
						t.Fatalf("the copy to %s does not end", dst)
					}
				}
				assert.Equal(t, []string{filepath.Base(first)}, dirNames(t, dir))
				assertSameDatabase(t, src, openCopy(t, first, nil))
			})
		}
	}
	t.Run("the database is there before", func(t *testing.T) {
		osRename = prevRename
		testHookCopiedCollection = nil
		dir := t.TempDir()
		p := filepath.Join(dir, "copy.db")
		_, err := CopyToLatestFormat(ctx, src, p, nil)
		require.NoError(t, err)
		for _, suffix := range []string{"-wal", "-wal-shm", ".lock"} {
			_, err = CopyToLatestFormat(ctx, src, p+suffix, nil)
			require.ErrorIs(t, err, ErrDestinationExists, suffix)
		}
		assert.Equal(t, []string{"copy.db"}, dirNames(t, dir))
	})
}

// The copy locks a copy holds: its destination's, after that of the
// database the destination would be a file of, matched in any case; one
// level, as a database reads only the files beside its own name. Where one
// lock serves the process, it is taken once.
func TestCopyLockNames(t *testing.T) {
	want := func(names ...string) []string {
		if !copyLockPerName {
			return names[len(names)-1:]
		}
		return names
	}
	for _, tc := range []struct {
		dst  string
		want []string
	}{
		{"p", want("p")},
		{"p-wal", want("p", "p-wal")},
		{"p-wal-shm", want("p", "p-wal-shm")},
		{"p.lock", want("p", "p.lock")},
		{"p-WAL", want("p", "p-WAL")},
		{"p.loc\u212a", want("p", "p.loc\u212a")},
		{"p-wal-wal", want("p-wal", "p-wal-wal")},
		{"p-wal.lock", want("p-wal", "p-wal.lock")},
		{"p-shm", want("p-shm")},
		{"-wal", want("-wal")},
	} {
		assert.Equal(t, tc.want, copyLockNames(tc.dst), tc.dst)
	}
}

// The names a database keeps beside it are refused as destinations whatever
// their case: on a case-insensitive filesystem app.db-WAL is app.db's log,
// and the open of app.db would take a copy published there for its log and
// truncate it. The refusal holds on every filesystem, at the start and at
// the check before the rename. While such a copy runs it holds app.db's
// copy lock, matched in any case too, so a copy to app.db is refused with
// ErrBusy; a file that something else puts there is found by the check.
func TestCopyToLatestFormat_SidecarNameInAnotherCase(t *testing.T) {
	src, _ := openSource(t, nil)
	coll, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	insertJSON(t, coll, `{"id":1}`)
	defer func() { testHookBeforePublish = nil }()
	// Unicode case equivalents too: U+017F LATIN SMALL LETTER LONG S folds
	// to s and U+212A KELVIN SIGN to k, and APFS takes them for those.
	suffixes := []string{"-WAL", "-waL", "-Wal-Shm", "-wAL-sHM", ".LOCK", ".Lock",
		"-wal-\u017fhm", "-WAL-\u017fHM", ".loc\u212a"}

	t.Run("the database is there before", func(t *testing.T) {
		testHookBeforePublish = nil
		dir := t.TempDir()
		p := filepath.Join(dir, "app.db")
		_, err := CopyToLatestFormat(ctx, src, p, nil)
		require.NoError(t, err)
		for _, suffix := range suffixes {
			_, err = CopyToLatestFormat(ctx, src, p+suffix, nil)
			require.ErrorIs(t, err, ErrDestinationExists, suffix)
		}
		assert.Equal(t, []string{"app.db"}, dirNames(t, dir))
		assertSameDatabase(t, src, openCopy(t, p, nil))
	})
	for _, suffix := range suffixes {
		t.Run("the database appears while the copy runs: "+suffix, func(t *testing.T) {
			dir := t.TempDir()
			p := filepath.Join(dir, "app.db")
			var ran atomic.Bool
			testHookBeforePublish = func(string) {
				if ran.Swap(true) {
					return
				}
				_, err := CopyToLatestFormat(ctx, src, p, nil)
				assert.ErrorIs(t, err, ErrBusy)
				require.NoError(t, os.WriteFile(p, []byte("foreign"), 0o644))
			}
			_, err := CopyToLatestFormat(ctx, src, p+suffix, nil)
			require.ErrorIs(t, err, ErrDestinationExists)
			assert.Equal(t, []string{"app.db"}, dirNames(t, dir))
			data, err := os.ReadFile(p)
			require.NoError(t, err)
			assert.Equal(t, "foreign", string(data))
		})
	}
}

// The copy is published by one rename of the staging file, in the
// destination's directory, and the directory is synced after it; before the
// rename only what an earlier copy may have left and the closed copy's
// sidecars are removed.
func TestCopyToLatestFormat_PublicationOrder(t *testing.T) {
	src, _ := openSource(t, nil)
	_, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)

	var calls []string
	var stage string
	testHookBeforePublish = func(s string) { stage = s }
	prevRename, prevRemove, prevSync := osRename, osRemove, dirSync
	osRename = func(oldpath, newpath string) error {
		calls = append(calls, "rename "+filepath.Base(oldpath)+" "+filepath.Base(newpath))
		return prevRename(oldpath, newpath)
	}
	osRemove = func(name string) error {
		calls = append(calls, "remove "+filepath.Base(name))
		return prevRemove(name)
	}
	dirSync = func(path string) error {
		calls = append(calls, "sync "+filepath.Base(filepath.Dir(path)))
		return prevSync(path)
	}
	defer func() {
		testHookBeforePublish = nil
		osRename, osRemove, dirSync = prevRename, prevRemove, prevSync
	}()

	dstDir := t.TempDir()
	dstPath := filepath.Join(dstDir, "copy.db")
	_, err = CopyToLatestFormat(ctx, src, dstPath, nil)
	require.NoError(t, err)
	assert.Equal(t, dstPath+".copy-staging", stage)
	assert.Equal(t, filepath.Dir(dstPath), filepath.Dir(stage), "the rename stays in one directory")
	dir := filepath.Base(dstDir)
	var published int
	for i, c := range calls {
		if c == "rename copy.db.copy-staging copy.db" {
			published = i
		}
	}
	require.NotZero(t, published)
	require.Greater(t, len(calls), published+1)
	assert.Equal(t, "sync "+dir, calls[published+1])
	for i, c := range calls {
		if i == published || i == published+1 {
			continue
		}
		assert.True(t, strings.HasPrefix(c, "remove copy.db.copy-staging"), "only staging files are removed: %s", c)
		if i > published {
			assert.NotEqual(t, "remove copy.db.copy-staging", c, "the published file is not removed under its old name")
		}
	}
	assert.Equal(t, []string{"copy.db"}, dirNames(t, dstDir))
}

// A failure after the rename comes back as ErrCopyPublished with what
// failed and with the filled report: the complete copy stays at the
// destination. A failure before the rename is never ErrCopyPublished.
func TestCopyToLatestFormat_FailureAfterPublication(t *testing.T) {
	src, _ := openSource(t, nil)
	coll, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{Fields: []string{"n"}}))
	insertJSON(t, coll, `{"id":1,"n":1}`, `{"id":2,"n":2}`)
	errFault := errors.New("injected fault")
	want := func(report CopyReport) CopyReport {
		return CopyReport{Collections: 1, Documents: 2, Indexes: 1, Duration: report.Duration}
	}

	t.Run("directory sync", func(t *testing.T) {
		prevSync := dirSync
		dirSync = func(string) error { return errFault }
		defer func() { dirSync = prevSync }()
		dstDir := t.TempDir()
		dstPath := filepath.Join(dstDir, "copy.db")
		report, err := CopyToLatestFormat(ctx, src, dstPath, nil)
		require.ErrorIs(t, err, ErrCopyPublished)
		require.ErrorIs(t, err, errFault)
		assert.Contains(t, err.Error(), "the copy is published at "+dstPath)
		assert.Equal(t, want(report), report)
		assert.NotZero(t, report.Duration)
		assert.Equal(t, []string{"copy.db"}, dirNames(t, dstDir))
		assertSameDatabase(t, src, openCopy(t, dstPath, nil))
	})
	t.Run("staging leftover", func(t *testing.T) {
		prevRename, prevRemove := osRename, osRemove
		var renamed bool
		osRename = func(oldpath, newpath string) error {
			renamed = true
			return prevRename(oldpath, newpath)
		}
		osRemove = func(name string) error {
			if renamed && strings.HasSuffix(name, ".copy-staging-wal") {
				return &os.PathError{Op: "remove", Path: name, Err: errFault}
			}
			return prevRemove(name)
		}
		defer func() { osRename, osRemove = prevRename, prevRemove }()
		dstDir := t.TempDir()
		dstPath := filepath.Join(dstDir, "copy.db")
		report, err := CopyToLatestFormat(ctx, src, dstPath, nil)
		require.ErrorIs(t, err, ErrCopyPublished)
		require.ErrorIs(t, err, errFault)
		assert.Contains(t, err.Error(), "a staging file is left on disk: "+dstPath+".copy-staging-wal")
		assert.Equal(t, want(report), report)
		assert.Equal(t, []string{"copy.db"}, dirNames(t, dstDir))
		assertSameDatabase(t, src, openCopy(t, dstPath, nil))
	})
	t.Run("before the rename", func(t *testing.T) {
		prevRename := osRename
		osRename = func(string, string) error { return errFault }
		defer func() { osRename = prevRename }()
		dstPath := filepath.Join(t.TempDir(), "copy.db")
		report, err := CopyToLatestFormat(ctx, src, dstPath, nil)
		require.ErrorIs(t, err, errFault)
		assert.NotErrorIs(t, err, ErrCopyPublished)
		assert.Zero(t, report)
		assertNoCopy(t, dstPath)
	})
}

// syncDir runs on Unix, where it fsyncs the directory, and does nothing
// elsewhere; a directory that does not open is no error, as in SQLite.
func TestSyncDir(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "f"), nil, 0o644))
	assert.NoError(t, syncDir(filepath.Join(dir, "f")))
	assert.NoError(t, syncDir(filepath.Join(dir, "missing", "f")))
}

// A failure after the copy began removes the copy's files; a removal that
// fails is reported with the failure, naming the file left.
func TestCopyToLatestFormat_CleanupFailureIsReported(t *testing.T) {
	src, _ := openSource(t, nil)
	coll, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	insertJSON(t, coll, `{"id":1}`)

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	testHookCopiedCollection = func(string) { cancel() }
	errRemove := errors.New("cannot remove")
	var left string
	prevRemove := osRemove
	osRemove = func(name string) error {
		// Only the copy's own log: nothing is there when the call starts.
		if _, err := os.Lstat(name); err == nil && strings.HasSuffix(name, ".copy-staging-wal") {
			left = name
			return errRemove
		}
		return prevRemove(name)
	}
	defer func() {
		testHookCopiedCollection = nil
		osRemove = prevRemove
	}()

	dstDir := t.TempDir()
	_, err = CopyToLatestFormat(cctx, src, filepath.Join(dstDir, "copy.db"), nil)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, errRemove)
	require.NotEmpty(t, left)
	assert.Contains(t, err.Error(), "left on disk: "+left)
	assert.Equal(t, []string{filepath.Base(left)}, dirNames(t, dstDir), "everything else is removed")
}

// A staging file whose creation cannot be finished is removed, and one that
// cannot be removed either is named in the error.
func TestCopyToLatestFormat_StagingFileNotClosed(t *testing.T) {
	src, _ := openSource(t, nil)
	prevOpen, prevRemove := osOpenFile, osRemove
	defer func() { osOpenFile, osRemove = prevOpen, prevRemove }()
	// The file comes back closed already, so closing it fails.
	osOpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		f, err := prevOpen(name, flag, perm)
		if err == nil {
			_ = f.Close()
		}
		return f, err
	}
	errRemove := errors.New("cannot remove")

	for _, removable := range []bool{true, false} {
		t.Run(fmt.Sprintf("removable=%v", removable), func(t *testing.T) {
			var stage string
			osRemove = func(name string) error {
				if _, err := os.Lstat(name); err == nil && strings.HasSuffix(name, ".copy-staging") && !removable {
					stage = name
					return errRemove
				}
				return prevRemove(name)
			}
			dstDir := t.TempDir()
			_, err := CopyToLatestFormat(ctx, src, filepath.Join(dstDir, "copy.db"), nil)
			require.ErrorIs(t, err, os.ErrClosed)
			if removable {
				assert.NotErrorIs(t, err, errRemove)
				assert.Empty(t, dirNames(t, dstDir))
				return
			}
			require.ErrorIs(t, err, errRemove)
			require.NotEmpty(t, stage)
			assert.Contains(t, err.Error(), "left on disk: "+stage)
			assert.Equal(t, []string{filepath.Base(stage)}, dirNames(t, dstDir))
		})
	}
}
