package anystore

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/btree"
	"github.com/anyproto/any-store/internal/objectid"
	"github.com/anyproto/any-store/query"
)

func TestCollection_Drop(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.Drop(ctx))
	_, err = fx.OpenCollection(ctx, "test")
	assert.ErrorIs(t, err, ErrCollectionNotFound)

	stats, err := fx.Stats(ctx)
	require.NoError(t, err)
	assert.Empty(t, stats.IndexesCount)
	assert.Empty(t, stats.CollectionsCount)
}

func TestCollection_Rename(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	const newName = "newName"
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.Rename(ctx, newName))
	assert.Equal(t, coll.Name(), newName)

	collections, err := fx.GetCollectionNames(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{newName}, collections)
}

func TestCollection_Insert(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "doc":"a"}`), anyenc.MustParseJson(`{"id":2, "doc":"b"}`)))
		assertCollCount(t, coll, 2)
	})
	t.Run("tx success", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)

		require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":1, "doc":"a"}`), anyenc.MustParseJson(`{"id":2, "doc":"b"}`)))

		// expect count=2 in tx
		count, err := coll.Count(tx.Context())
		require.NoError(t, err)
		assert.Equal(t, 2, count)

		// MVCC: a separate read outside the write tx cannot see uncommitted writes
		assertCollCount(t, coll, 0)

		require.NoError(t, tx.Commit())

		// expect count=2 after commit
		assertCollCount(t, coll, 2)
	})
	t.Run("err doc exists", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "doc":"a"}`), anyenc.MustParseJson(`{"id":2, "doc":"b"}`)))

		err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":3, "doc":"c"}`), anyenc.MustParseJson(`{"id":2, "doc":"b"}`))
		assert.ErrorIs(t, err, ErrDocExists)

		assertCollCount(t, coll, 2)
	})
}
func TestCollection_FindId(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		doc, err := coll.FindId(ctx, 1)
		assert.Nil(t, doc)
		assert.ErrorIs(t, err, ErrDocNotFound)
	})
	t.Run("found", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		const docJson = `{"id":1,"doc":2}`
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(docJson)))
		doc, err := coll.FindId(ctx, 1)
		require.NoError(t, err)
		assert.Equal(t, docJson, doc.Value().String())
	})
}

func TestCollection_UpdateOne(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		err = coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":"notFound", "d":2}`))
		assert.ErrorIs(t, err, ErrDocNotFound)
	})

	t.Run("success", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":"333","key":"value"}`))
		require.NoError(t, err)

		newDoc := `{"id":"333","key":"value2"}`

		err = coll.UpdateOne(ctx, anyenc.MustParseJson(newDoc))
		require.NoError(t, err)

		doc, err := coll.FindId(ctx, "333")
		require.NoError(t, err)
		assert.Equal(t, newDoc, doc.Value().String())
	})

	t.Run("doc without id", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		err = coll.UpdateOne(ctx, anyenc.MustParseJson(`{"a":"b"}`))
		assert.ErrorIs(t, err, ErrDocWithoutId)
	})
}

func TestCollection_UpdateId(t *testing.T) {
	mod := query.MustParseModifier(`{"$inc":{"a":1}}`)
	t.Run("not found", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		res, err := coll.UpdateId(ctx, "notFound", mod)
		assert.ErrorIs(t, err, ErrDocNotFound)
		assert.Empty(t, res)
	})

	t.Run("success", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":333,"key":"value"}`))
		require.NoError(t, err)
		id := 333

		res, err := coll.UpdateId(ctx, id, mod)
		require.NoError(t, err)
		assert.Equal(t, 1, res.Modified)

		doc, err := coll.FindId(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, float64(1), doc.Value().GetFloat64("a"))
	})
	t.Run("not modified", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "key":"value"}`))
		require.NoError(t, err)

		res, err := coll.UpdateId(ctx, 1, query.MustParseModifier(`{"$set":{"key":"value"}}`))
		require.NoError(t, err)
		assert.Equal(t, 0, res.Modified)
		assert.Equal(t, 1, res.Matched)
	})
}

func TestCollection_UpsertOne(t *testing.T) {
	t.Run("insert", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		t.Run("update", func(t *testing.T) {
			err = coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":"upd","val":1}`))
			require.NoError(t, err)
			newDoc := `{"id":"upd","val":2}`
			err = coll.UpsertOne(ctx, anyenc.MustParseJson(newDoc))
			require.NoError(t, err)
			doc, err := coll.FindId(ctx, "upd")
			require.NoError(t, err)
			assert.Equal(t, newDoc, doc.Value().String())
		})
	})
}

func TestCollection_UpsertId(t *testing.T) {
	mod := query.MustParseModifier(`{"$inc":{"a":1}}`)
	t.Run("insert", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		res, err := coll.UpsertId(ctx, 1, mod)
		require.NoError(t, err)
		assert.Equal(t, 0, res.Matched)
		assert.Equal(t, 1, res.Modified)

		doc, err := coll.FindId(ctx, 1)
		require.NoError(t, err)
		assert.Equal(t, float64(1), doc.Value().GetFloat64("a"))
	})
	t.Run("update", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "a":2}`)))
		res, err := coll.UpsertId(ctx, 1, mod)
		require.NoError(t, err)
		assert.Equal(t, 1, res.Matched)
		assert.Equal(t, 1, res.Modified)

		doc, err := coll.FindId(ctx, 1)
		require.NoError(t, err)
		assert.Equal(t, float64(3), doc.Value().GetFloat64("a"))
	})
	t.Run("not modified", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "key":"value"}`))
		require.NoError(t, err)

		res, err := coll.UpsertId(ctx, 1, query.MustParseModifier(`{"$set":{"key":"value"}}`))
		require.NoError(t, err)
		assert.Equal(t, 0, res.Modified)
		assert.Equal(t, 1, res.Matched)
	})
}

func TestCollection_DeleteOne(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	t.Run("not found", func(t *testing.T) {
		err = coll.DeleteId(ctx, "notFound")
		assert.ErrorIs(t, err, ErrDocNotFound)
	})
	t.Run("success", func(t *testing.T) {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"toDel", "a":2}`)))
		require.NoError(t, coll.DeleteId(ctx, "toDel"))
		assertCollCount(t, coll, 0)
	})
}

func TestCollection_CreateIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "doc":"a"}`), anyenc.MustParseJson(`{"id":2, "doc":"b"}`)))
	t.Run("err exists", func(t *testing.T) {
		err = coll.CreateIndex(ctx, IndexInfo{Fields: []string{"name"}}, IndexInfo{Fields: []string{"name"}})
		assert.ErrorIs(t, err, ErrIndexExists)
	})
	t.Run("success", func(t *testing.T) {
		require.NoError(t, coll.CreateIndex(ctx, IndexInfo{Fields: []string{"doc"}}))
		idxs := coll.GetIndexes()
		require.Len(t, idxs, 1)
		assert.Equal(t, "doc", idxs[0].Info().Name)
		count, err := idxs[0].Len(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, count)
	})
}

func TestCollection_EnsureIndex(t *testing.T) {
	t.Run("multiple", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "doc":"a"}`), anyenc.MustParseJson(`{"id":2, "doc":"b"}`)))
		err = coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"name"}}, IndexInfo{Fields: []string{"doc"}}, IndexInfo{Fields: []string{"name"}})
		assert.NoError(t, err, ErrIndexExists)
	})
	t.Run("single index", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		idx := IndexInfo{
			Fields: []string{"a"},
		}
		require.NoError(t, coll.EnsureIndex(ctx, idx))
		require.NoError(t, coll.EnsureIndex(ctx, idx))
	})
}

func TestCollection_DropIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.DropIndex(ctx, "a"))
	assert.Len(t, coll.GetIndexes(), 0)
	assert.ErrorIs(t, coll.DropIndex(ctx, "a"), ErrIndexNotFound)
}

func BenchmarkCollection_Insert(b *testing.B) {
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(b, err)
	a := &anyenc.Arena{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		a.Reset()
		doc := a.NewObject()
		doc.Set("id", a.NewString(objectid.NewObjectID().Hex()))
		require.NoError(b, coll.Insert(ctx, doc))
	}
}

func BenchmarkCollection_UpdateId(b *testing.B) {
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(b, err)

	require.NoError(b, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "v":0}`)))
	mod := query.MustParseModifier(`{"$inc":{"v":1}}`)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, err = coll.UpdateId(ctx, 1, mod)
		require.NoError(b, err)
	}
}

func BenchmarkCollection_FindId(b *testing.B) {
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(b, err)

	require.NoError(b, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "v":0}`)))

	b.Run("no parser", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, err = coll.FindId(ctx, 1)
			require.NoError(b, err)
		}
	})
	b.Run("with parser", func(b *testing.B) {
		p := &anyenc.Parser{}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, err = coll.FindIdWithParser(ctx, p, 1)
			require.NoError(b, err)
		}
	})
}

func BenchmarkCollection_Find(b *testing.B) {
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(b, err)
	tx, err := coll.WriteTx(ctx)
	require.NoError(b, err)
	for i := range 1000 {
		require.NoError(b, coll.Insert(tx.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id":%d, "a":%d, "b":%d}`, i, i, rand.Int()))))
	}
	require.NoError(b, tx.Commit())

	b.Run("count", func(b *testing.B) {
		for range b.N {
			b.ReportAllocs()
			_, _ = coll.Find(nil).Count(ctx)
		}
	})
	b.Run("count by filter", func(b *testing.B) {
		var f = query.MustParseCondition(`{"a":{"$gt":900}}`)
		for range b.N {
			b.ReportAllocs()
			_, _ = coll.Find(f).Count(ctx)
		}
	})

}

func BenchmarkCollection_InQuery(b *testing.B) {
	fx := newFixture(b)
	var builder strings.Builder
	builder.Grow(4000)
	builder.WriteString(`{"id":{"$in":[`)
	l := 1001
	for i := 1; i <= l; i++ {
		builder.WriteString(strconv.Itoa(i))
		if i < l {
			builder.WriteString(",")
		}
	}
	builder.WriteString(",400000")
	builder.WriteString("]}}")

	query := query.MustParseCondition(builder.String())
	coll, _ := fx.CreateCollection(ctx, "test_foo")
	coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}})
	vals := make([]*anyenc.Value, 1000000)
	for i := range 1000000 {
		// try to make random
		vals[i] = anyenc.MustParseJson(fmt.Sprintf(`{"id":%d, "a":%d}`, i+980, i+981))
	}
	b.Log(coll.Find(query).Explain(ctx))
	coll.Insert(ctx, vals...)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		coll.Find(query).Count(ctx)
	}
}

// setupLargeBlob inserts a single document with an ~sz-byte "payload"
// field containing a deterministic byte pattern. Returns the collection
// and the inserted doc id. The payload is written as a hex string so
// anyenc serialization stays stable; hex doubles the on-disk size, so
// pass sz/2 to get roughly sz bytes in the DB (close enough for a
// micro-bench — the goal is to span many overflow pages, not hit an
// exact size).
func setupLargeBlob(b *testing.B, sz int) (*collection, int) {
	b.Helper()
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "blob")
	require.NoError(b, err)

	// Build a hex payload: 2 hex chars per input byte.
	raw := make([]byte, sz/2)
	for i := range raw {
		raw[i] = byte(i * 17) // deterministic, non-repetitive
	}
	payload := fmt.Sprintf("%x", raw)

	doc := anyenc.MustParseJson(fmt.Sprintf(`{"id": 1, "payload": "%s"}`, payload))
	require.NoError(b, coll.Insert(ctx, doc))

	// Force a checkpoint so the blob actually lives in the DB file
	// (overflow chain anchored there), not only in the WAL. Eliminates
	// WAL-vs-DB read path variance in the measurement.
	require.NoError(b, fx.Flush(ctx, 0*time.Second, FlushModeCheckpointFull))

	return coll.(*collection), 1
}

// BenchmarkOverflow10MB_FindId measures the cost of reading a ~10MB
// blob value back via the public FindId API. Includes SHM hash lookup,
// page reads, overflow chain walk, decompression (if any), anyenc
// parse, and the Doc wrapper. A cpuprofile of this bench tells us
// which part dominates.
//
// Run: go test -run='^$' -bench=BenchmarkOverflow10MB_FindId -benchmem -cpuprofile=/tmp/ovfl.prof
func BenchmarkOverflow10MB_FindId(b *testing.B) {
	coll, id := setupLargeBlob(b, 10<<20) // ~10MB

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		d, err := coll.FindId(ctx, id)
		if err != nil {
			b.Fatalf("FindId: %v", err)
		}
		_ = d.Value() // force materialization
	}
}

// BenchmarkOverflow10MB_FindId_WithParser uses a reusable parser to
// isolate parser-allocation cost from overflow-read cost. Delta
// against the previous bench shows how much parser churn contributes.
func BenchmarkOverflow10MB_FindId_WithParser(b *testing.B) {
	coll, id := setupLargeBlob(b, 10<<20)

	p := &anyenc.Parser{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		d, err := coll.FindIdWithParser(ctx, p, id)
		if err != nil {
			b.Fatalf("FindId: %v", err)
		}
		_ = d.Value()
	}
}

// setupLargeBlobMmap is like setupLargeBlob but opens the DB with
// MmapSize = 64 MiB. Used to measure the mmap-vs-ReadAt delta.
func setupLargeBlobMmap(b *testing.B, sz int) (*collection, int) {
	b.Helper()
	fx := newFixture(b, &Config{MmapSize: 64 << 20})
	coll, err := fx.CreateCollection(ctx, "blob")
	require.NoError(b, err)

	raw := make([]byte, sz/2)
	for i := range raw {
		raw[i] = byte(i * 17)
	}
	payload := fmt.Sprintf("%x", raw)

	doc := anyenc.MustParseJson(fmt.Sprintf(`{"id": 1, "payload": "%s"}`, payload))
	require.NoError(b, coll.Insert(ctx, doc))
	require.NoError(b, fx.Flush(ctx, 0*time.Second, FlushModeCheckpointFull))

	return coll.(*collection), 1
}

// BenchmarkOverflow10MB_FindId_Mmap is the mmap-enabled counterpart
// to BenchmarkOverflow10MB_FindId. Delta against the baseline is the
// measured value of the mmap change.
func BenchmarkOverflow10MB_FindId_Mmap(b *testing.B) {
	coll, id := setupLargeBlobMmap(b, 10<<20)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		d, err := coll.FindId(ctx, id)
		if err != nil {
			b.Fatalf("FindId: %v", err)
		}
		_ = d.Value()
	}
}

// BenchmarkOverflow_SizeSweep_FindId scans blob sizes to expose how
// per-op cost scales with overflow-chain length. Slope tells us
// whether we're bound by the walk (linear in chain length) or by
// payload size (linear in bytes copied) or by syscalls (linear in
// pages).
func BenchmarkOverflow_SizeSweep_FindId(b *testing.B) {
	sizes := []int{
		64 << 10,  // 64 KB — ~16 overflow pages
		512 << 10, // 512 KB — ~128
		2 << 20,   // 2 MB — ~512
		10 << 20,  // 10 MB — ~2560
	}
	for _, sz := range sizes {
		b.Run(fmt.Sprintf("sz=%dKB", sz>>10), func(b *testing.B) {
			coll, id := setupLargeBlob(b, sz)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				d, err := coll.FindId(ctx, id)
				if err != nil {
					b.Fatalf("FindId: %v", err)
				}
				_ = d.Value()
			}
		})
	}
}

// BenchmarkOverflow10MB_FindIdCold measures FindId on a 10MB blob
// with per-iteration DB reopen. Reopening drops in-process caches
// (pcache, mmap region) — what remains is whatever the OS page
// cache serves. Exposes sequential-read layout wins that the
// warm-cache variant hides.
//
// NOTE: fully evicting the OS page cache would require root +
// platform-specific calls; this bench does not. Both before/after
// see the same OS-cache conditions, so the comparison is fair for
// measuring chain layout cost vs constant OS-cache overhead.
func BenchmarkOverflow10MB_FindIdCold(b *testing.B) {
	const sz = 10 << 20

	tmpDir, err := os.MkdirTemp("", "bench-cold-*")
	require.NoError(b, err)
	b.Cleanup(func() { _ = os.RemoveAll(tmpDir) })
	dbPath := filepath.Join(tmpDir, "bench.db")

	// One-time setup: open, insert blob, checkpoint, close.
	setupDB, err := Open(ctx, dbPath, nil)
	require.NoError(b, err)
	coll, err := setupDB.CreateCollection(ctx, "blob")
	require.NoError(b, err)
	raw := make([]byte, sz/2)
	for i := range raw {
		raw[i] = byte(i * 17)
	}
	payload := fmt.Sprintf("%x", raw)
	doc := anyenc.MustParseJson(fmt.Sprintf(`{"id": 1, "payload": "%s"}`, payload))
	require.NoError(b, coll.Insert(ctx, doc))
	require.NoError(b, setupDB.Flush(ctx, 0*time.Second, FlushModeCheckpointFull))
	require.NoError(b, setupDB.Close())

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		db2, err := Open(ctx, dbPath, nil)
		require.NoError(b, err)
		coll2, err := db2.OpenCollection(ctx, "blob")
		require.NoError(b, err)
		b.StartTimer()

		d, err := coll2.FindId(ctx, 1)
		if err != nil {
			b.Fatalf("FindId: %v", err)
		}
		_ = d.Value()

		b.StopTimer()
		require.NoError(b, db2.Close())
		b.StartTimer()
	}
}

// BenchmarkWAL_RepeatedDirtyWithinTx measures WAL file growth under
// a workload that repeatedly updates the same documents within a
// single transaction. Without frame-reuse, each update appends a
// new WAL frame. With frame-reuse, the existing frame is overwritten
// in place. Reports per-op time AND the resulting WAL file size at
// the end of the run via the `wal_bytes` custom metric.
func BenchmarkWAL_RepeatedDirtyWithinTx(b *testing.B) {
	const (
		nDocs    = 10
		nUpdates = 20
	)

	tmpDir, err := os.MkdirTemp("", "bench-reuse-*")
	require.NoError(b, err)
	b.Cleanup(func() { _ = os.RemoveAll(tmpDir) })
	dbPath := filepath.Join(tmpDir, "bench.db")

	db, err := Open(ctx, dbPath, &Config{DisableAutoCheckpoint: true})
	require.NoError(b, err)
	b.Cleanup(func() { _ = db.Close() })
	coll, err := db.CreateCollection(ctx, "docs")
	require.NoError(b, err)

	// Seed.
	for i := range nDocs {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"v":0,"payload":"%s"}`, i, strings.Repeat("x", 500)))
		require.NoError(b, coll.Insert(ctx, doc))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		tx, err := coll.WriteTx(ctx)
		require.NoError(b, err)
		for u := range nUpdates {
			for i := range nDocs {
				doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"v":%d,"payload":"%s"}`, i, u, strings.Repeat("x", 500)))
				require.NoError(b, coll.UpsertOne(tx.Context(), doc))
			}
		}
		require.NoError(b, tx.Commit())
	}
	b.StopTimer()

	if info, err := os.Stat(dbPath + "-wal"); err == nil {
		b.ReportMetric(float64(info.Size()), "wal_bytes")
	}
}

// BenchmarkWAL_SpillHeavyRepeatedDirty forces page-cache spills via a
// tiny CacheSize so the same dirty pages repeatedly hit writeFrames
// within one tx — exactly the scenario where SQLite's frame-reuse
// optimization (wal.c:4117-4156) eliminates WAL bloat. Without
// reuse, each spill of pgno X appends a new frame; with reuse, the
// existing in-tx frame is overwritten in place.
//
// Workload: enough distinct large docs to spread dirty pages well
// past the cache (working set ≫ cache), then re-update every doc
// inside a single tx so each spilled page must be re-spilled.
//
// Run BOTH variants to A/B-compare reuse savings:
//
//	go test -run=^$ -bench=BenchmarkWAL_SpillHeavyRepeatedDirty -benchtime=2x
func BenchmarkWAL_SpillHeavyRepeatedDirty_NoReuse(b *testing.B) {
	btree.DiagDisableReuse.Store(true)
	b.Cleanup(func() { btree.DiagDisableReuse.Store(false) })
	benchSpillHeavyRepeatedDirty(b)
}

func BenchmarkWAL_SpillHeavyRepeatedDirty(b *testing.B) {
	btree.DiagDisableReuse.Store(false)
	benchSpillHeavyRepeatedDirty(b)
}

func benchSpillHeavyRepeatedDirty(b *testing.B) {
	const (
		nDocs    = 1000
		nUpdates = 5
	)

	tmpDir, err := os.MkdirTemp("", "bench-spill-*")
	require.NoError(b, err)
	b.Cleanup(func() { _ = os.RemoveAll(tmpDir) })
	dbPath := filepath.Join(tmpDir, "bench.db")

	// CacheSize=4: tiny cache → constant spills as each upsert
	// dirties pages exceeding capacity.
	cfg := &Config{DisableAutoCheckpoint: true, CacheSize: 4}
	db, err := Open(ctx, dbPath, cfg)
	require.NoError(b, err)
	b.Cleanup(func() { _ = db.Close() })
	coll, err := db.CreateCollection(ctx, "docs")
	require.NoError(b, err)

	for i := range nDocs {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"v":0,"payload":"%s"}`, i, strings.Repeat("x", 2000)))
		require.NoError(b, coll.Insert(ctx, doc))
	}

	reuseStart := btree.DiagReuseFrames.Load()
	appendStart := btree.DiagAppendFrames.Load()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		tx, err := coll.WriteTx(ctx)
		require.NoError(b, err)
		for u := range nUpdates {
			for i := range nDocs {
				doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"v":%d,"payload":"%s"}`, i, u, strings.Repeat("x", 2000)))
				require.NoError(b, coll.UpsertOne(tx.Context(), doc))
			}
		}
		require.NoError(b, tx.Commit())
	}
	b.StopTimer()

	if info, err := os.Stat(dbPath + "-wal"); err == nil {
		b.ReportMetric(float64(info.Size()), "wal_bytes")
	}
	b.ReportMetric(float64(btree.DiagReuseFrames.Load()-reuseStart), "reuse")
	b.ReportMetric(float64(btree.DiagAppendFrames.Load()-appendStart), "append")
}
