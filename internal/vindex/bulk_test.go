package vindex

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/stretchr/testify/require"
)

// TestBulkBuildTiming compares BulkBuild vs the per-insert build at scale,
// file-backed (realistic). Skipped in -short.
func TestBulkBuildTiming(t *testing.T) {
	if testing.Short() {
		t.Skip("timing")
	}
	const (
		n   = 20000
		dim = 128
	)
	const seed = int64(1)
	vecs := randVecs(n, dim, 7)
	ids := make([][]byte, n)
	for i := range ids {
		ids[i] = docID(i)
	}
	params := Params{Dim: dim, Metric: Cosine, EfSearch: 64, Quantization: QuantInt8}

	// Per-insert build (batched 2000/tx), file-backed.
	d1 := filepath.Join(t.TempDir(), "ins.db")
	db1, err := btree.Open(d1, btree.Options{})
	require.NoError(t, err)
	wtx, err := db1.BeginWrite()
	require.NoError(t, err)
	ix1, err := Create(wtx, "vix", params, seed)
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())
	tIns := time.Now()
	wtx, err = db1.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		require.NoError(t, ix1.Insert(wtx, ids[i], vecs[i]))
		if (i+1)%2000 == 0 {
			require.NoError(t, wtx.Commit())
			wtx, err = db1.BeginWrite()
			require.NoError(t, err)
		}
	}
	require.NoError(t, wtx.Commit())
	insDur := time.Since(tIns)
	_ = db1.Close()

	// Bulk build, file-backed.
	d2 := filepath.Join(t.TempDir(), "bulk.db")
	db2, err := btree.Open(d2, btree.Options{})
	require.NoError(t, err)
	tBulk := time.Now()
	wtx, err = db2.BeginWrite()
	require.NoError(t, err)
	_, err = BulkBuild(wtx, "vix", params, seed, ids, vecs)
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())
	bulkDur := time.Since(tBulk)
	_ = db2.Close()

	t.Logf("per-insert: %s (%.0f/s)   bulk: %s (%.0f/s)   speedup %.1fx",
		insDur.Round(time.Millisecond), float64(n)/insDur.Seconds(),
		bulkDur.Round(time.Millisecond), float64(n)/bulkDur.Seconds(),
		insDur.Seconds()/bulkDur.Seconds())
}

// dumpNS reads every (key,value) of a namespace into a map for comparison.
func dumpNS(t *testing.T, db *btree.DB, ns *btree.Namespace) map[string][]byte {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	out := map[string][]byte{}
	cur := rtx.NewCursor(ns)
	defer cur.Close()
	require.NoError(t, cur.First())
	for cur.Valid() {
		k, err := cur.Key()
		require.NoError(t, err)
		v, err := cur.Value()
		require.NoError(t, err)
		out[string(append([]byte(nil), k...))] = append([]byte(nil), v...)
		require.NoError(t, cur.Next())
	}
	return out
}

// TestBulkBuildByteIdentical proves BulkBuild yields a graph byte-identical to the
// per-insert path (same seed, order, params) — so search, incremental insert,
// delete and compaction all behave identically, and the on-disk format matches.
func TestBulkBuildByteIdentical(t *testing.T) {
	const (
		n   = 1200
		dim = 24
	)
	const seed = int64(1)
	vecs := randVecs(n, dim, 7)
	ids := make([][]byte, n)
	for i := range ids {
		ids[i] = docID(i)
	}

	for _, metric := range []Metric{L2, Cosine} {
		for _, quant := range []Quantization{QuantNone, QuantInt8} {
			params := Params{Dim: dim, Metric: metric, EfSearch: 64, Quantization: quant}

			// Per-insert build.
			db1, err := btree.Open(":memory:", btree.Options{InMemory: true})
			require.NoError(t, err)
			wtx1, err := db1.BeginWrite()
			require.NoError(t, err)
			ix1, err := Create(wtx1, "vix", params, seed)
			require.NoError(t, err)
			require.NoError(t, wtx1.Commit())
			wtx1, err = db1.BeginWrite()
			require.NoError(t, err)
			for i := 0; i < n; i++ {
				require.NoError(t, ix1.Insert(wtx1, ids[i], vecs[i]))
			}
			require.NoError(t, wtx1.Commit())

			// Bulk build.
			db2, err := btree.Open(":memory:", btree.Options{InMemory: true})
			require.NoError(t, err)
			wtx2, err := db2.BeginWrite()
			require.NoError(t, err)
			ix2, err := BulkBuild(wtx2, "vix", params, seed, ids, vecs)
			require.NoError(t, err)
			require.NoError(t, wtx2.Commit())

			// Compare every namespace byte-for-byte.
			for _, p := range []struct {
				name     string
				a, b     *btree.Namespace
			}{
				{"meta", ix1.vmeta, ix2.vmeta},
				{"vec", ix1.vvec, ix2.vvec},
				{"adj", ix1.vadj, ix2.vadj},
				{"doc", ix1.vdoc, ix2.vdoc},
				{"lbl", ix1.vlbl, ix2.vlbl},
			} {
				m1 := dumpNS(t, db1, p.a)
				m2 := dumpNS(t, db2, p.b)
				require.Equalf(t, len(m1), len(m2), "metric=%v quant=%v ns=%s record count", metric, quant, p.name)
				for k, v1 := range m1 {
					require.Equalf(t, v1, m2[k], "metric=%v quant=%v ns=%s key=%x", metric, quant, p.name, k)
				}
			}
			_ = db1.Close()
			_ = db2.Close()
		}
	}
}

// TestBulkBuildSearchEquivalence confirms a bulk-built index is fully functional:
// search matches the per-insert index, and incremental insert/delete after a bulk
// build still work.
func TestBulkBuildSearchEquivalence(t *testing.T) {
	const (
		n   = 1500
		dim = 32
		k   = 10
	)
	const seed = int64(1)
	vecs := randVecs(n, dim, 11)
	queries := randVecs(50, dim, 99)
	ids := make([][]byte, n)
	for i := range ids {
		ids[i] = docID(i)
	}
	params := Params{Dim: dim, Metric: L2, EfSearch: 64}

	db1, ix1 := newTestIndex(t, dim, L2)
	insertAll(t, db1, ix1, vecs)

	db2, err := btree.Open(":memory:", btree.Options{InMemory: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })
	wtx, err := db2.BeginWrite()
	require.NoError(t, err)
	ix2, err := BulkBuild(wtx, "vix", params, seed, ids, vecs)
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())

	rtx1, err := db1.BeginRead()
	require.NoError(t, err)
	defer rtx1.Rollback()
	rtx2, err := db2.BeginRead()
	require.NoError(t, err)
	defer rtx2.Rollback()
	for qi, q := range queries {
		h1, err := ix1.Search(rtx1, q, k, 64)
		require.NoError(t, err)
		h2, err := ix2.Search(rtx2, q, k, 64)
		require.NoError(t, err)
		require.Equalf(t, len(h1), len(h2), "query %d count", qi)
		for i := range h1 {
			require.Equalf(t, h1[i].DocID, h2[i].DocID, "query %d hit %d", qi, i)
		}
	}

	// Incremental insert + delete after a bulk build must still work.
	more := randVecs(100, dim, 555)
	wtx, err = db2.BeginWrite()
	require.NoError(t, err)
	for i, v := range more {
		require.NoError(t, ix2.Insert(wtx, docID(n+i), v))
	}
	_, err = ix2.Delete(wtx, docID(0))
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())

	rtx3, err := db2.BeginRead()
	require.NoError(t, err)
	defer rtx3.Rollback()
	// the just-inserted vector retrieves itself
	hits, err := ix2.Search(rtx3, more[0], 1, 64)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, docID(n), hits[0].DocID)
}
