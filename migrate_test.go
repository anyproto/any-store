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

// Two calls copying to one destination at once: both build their copies,
// one publishes, and the other fails with ErrDestinationExists and leaves
// nothing behind.
func TestCopyToLatestFormat_ConcurrentCopiesToOneDestination(t *testing.T) {
	src, _ := openSource(t, nil)
	coll, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{Fields: []string{"n"}}))
	for i := 0; i < 50; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"n":%d}`, i, i))))
	}

	// Both calls reach publication before either links.
	var arrived sync.WaitGroup
	arrived.Add(2)
	testHookBeforePublish = func(string) {
		arrived.Done()
		arrived.Wait()
	}
	defer func() { testHookBeforePublish = nil }()

	dstDir := t.TempDir()
	dstPath := filepath.Join(dstDir, "copy.db")
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := CopyToLatestFormat(ctx, src, dstPath, nil)
			errs <- err
		}()
	}
	var published, refused int
	for range 2 {
		select {
		case err := <-errs:
			switch {
			case err == nil:
				published++
			case errors.Is(err, ErrDestinationExists):
				refused++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		case <-time.After(time.Minute):
			t.Fatal("the copies did not end")
		}
	}
	assert.Equal(t, 1, published)
	assert.Equal(t, 1, refused)
	assert.Equal(t, []string{"copy.db"}, dirNames(t, dstDir))
	assertSameDatabase(t, src, openCopy(t, dstPath, nil))
}

// A file that appears beside the destination while the copy runs fails the
// publication with ErrDestinationExists: the copy's staging files go, the
// file stays as it is, and nothing is at the destination.
func TestCopyToLatestFormat_SidecarAppearsBeforePublication(t *testing.T) {
	src, _ := openSource(t, nil)
	coll, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	insertJSON(t, coll, `{"id":1}`)
	defer func() { testHookBeforePublish = nil }()
	for _, suffix := range []string{"-wal", "-wal-shm", ".lock"} {
		t.Run(suffix, func(t *testing.T) {
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
		})
	}
}

// Two copies whose destinations overlap, one at p and one at p+"-wal",
// where the second would be read as the first one's log: in every order,
// the copy that comes second fails with ErrDestinationExists and leaves
// nothing behind.
func TestCopyToLatestFormat_OverlappingDestinations(t *testing.T) {
	src, _ := openSource(t, nil)
	coll, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	insertJSON(t, coll, `{"id":1}`)
	defer func() { testHookBeforePublish = nil }()
	// during runs second inside the first one's publication hook, once.
	nest := func(second string) {
		var ran atomic.Bool
		testHookBeforePublish = func(string) {
			if ran.Swap(true) {
				return
			}
			_, err := CopyToLatestFormat(ctx, src, second, nil)
			require.NoError(t, err)
		}
	}

	t.Run("the log's name is published while the database's copy runs", func(t *testing.T) {
		dir := t.TempDir()
		p := filepath.Join(dir, "copy.db")
		nest(p + "-wal")
		_, err := CopyToLatestFormat(ctx, src, p, nil)
		require.ErrorIs(t, err, ErrDestinationExists)
		assert.Equal(t, []string{"copy.db-wal"}, dirNames(t, dir))
	})
	t.Run("the database is published while the log's copy runs", func(t *testing.T) {
		dir := t.TempDir()
		p := filepath.Join(dir, "copy.db")
		nest(p)
		_, err := CopyToLatestFormat(ctx, src, p+"-wal", nil)
		require.ErrorIs(t, err, ErrDestinationExists)
		assert.Equal(t, []string{"copy.db"}, dirNames(t, dir))
		assertSameDatabase(t, src, openCopy(t, p, nil))
	})
	t.Run("the database is there before", func(t *testing.T) {
		testHookBeforePublish = nil
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

// The names a database keeps beside it are refused as destinations whatever
// their case: on a case-insensitive filesystem app.db-WAL is app.db's log,
// and the open of app.db would take a copy published there for its log and
// truncate it. The refusal holds on every filesystem, at the start and at
// the check before the link.
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
		t.Run("the database is published while the copy runs: "+suffix, func(t *testing.T) {
			dir := t.TempDir()
			p := filepath.Join(dir, "app.db")
			var ran atomic.Bool
			testHookBeforePublish = func(string) {
				if ran.Swap(true) {
					return
				}
				_, err := CopyToLatestFormat(ctx, src, p, nil)
				require.NoError(t, err)
			}
			_, err := CopyToLatestFormat(ctx, src, p+suffix, nil)
			require.ErrorIs(t, err, ErrDestinationExists)
			assert.Equal(t, []string{"app.db"}, dirNames(t, dir))
		})
	}
}

// The copy is published by a link, then the staging name goes, and the
// directory is synced after each.
func TestCopyToLatestFormat_PublicationOrder(t *testing.T) {
	src, _ := openSource(t, nil)
	_, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)

	var calls []string
	var stage string
	testHookBeforePublish = func(s string) { stage = s }
	prevLink, prevRemove, prevSync := osLink, osRemove, dirSync
	osLink = func(oldname, newname string) error {
		calls = append(calls, "link "+filepath.Base(oldname)+" "+filepath.Base(newname))
		return prevLink(oldname, newname)
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
		osLink, osRemove, dirSync = prevLink, prevRemove, prevSync
	}()

	dstDir := t.TempDir()
	dstPath := filepath.Join(dstDir, "copy.db")
	_, err = CopyToLatestFormat(ctx, src, dstPath, nil)
	require.NoError(t, err)
	require.NotEmpty(t, stage)
	base, dir := filepath.Base(stage), filepath.Base(dstDir)
	require.GreaterOrEqual(t, len(calls), 4)
	assert.Equal(t, []string{
		"link " + base + " copy.db",
		"sync " + dir,
		"remove " + base,
		"sync " + dir,
	}, calls[len(calls)-4:])
	for _, c := range calls[:len(calls)-4] {
		assert.True(t, strings.HasPrefix(c, "remove "+base+"-") || strings.HasPrefix(c, "remove "+base+"."), "before the link only the closed copy's sidecars go: %s", c)
	}
	assert.Equal(t, []string{"copy.db"}, dirNames(t, dstDir))
}

// A failure after the link comes back saying the copy is published: the
// complete copy stays at the destination, and the staging name goes.
func TestCopyToLatestFormat_FailureAfterPublication(t *testing.T) {
	src, _ := openSource(t, nil)
	coll, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	insertJSON(t, coll, `{"id":1}`)
	errSync := errors.New("cannot sync")
	prevSync := dirSync
	dirSync = func(string) error { return errSync }
	defer func() { dirSync = prevSync }()

	dstDir := t.TempDir()
	dstPath := filepath.Join(dstDir, "copy.db")
	_, err = CopyToLatestFormat(ctx, src, dstPath, nil)
	require.ErrorIs(t, err, errSync)
	assert.Contains(t, err.Error(), "the copy is published at "+dstPath)
	assert.Equal(t, []string{"copy.db"}, dirNames(t, dstDir))
	assertSameDatabase(t, src, openCopy(t, dstPath, nil))
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
		if strings.Contains(name, ".tmp-") && strings.HasSuffix(name, "-wal") {
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
				if strings.Contains(name, ".tmp-") && !removable {
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
