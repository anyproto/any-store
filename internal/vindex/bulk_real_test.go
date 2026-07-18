package vindex

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/stretchr/testify/require"
)

// reads the exported real dataset (from the consumer's ASV_COMPARE export).
func readF32(t *testing.T, path string) [][]float32 {
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	n := int(binary.LittleEndian.Uint32(b[0:]))
	d := int(binary.LittleEndian.Uint32(b[4:]))
	out := make([][]float32, n)
	off := 8
	for i := 0; i < n; i++ {
		row := make([]float32, d)
		for j := 0; j < d; j++ {
			row[j] = math.Float32frombits(binary.LittleEndian.Uint32(b[off:]))
			off += 4
		}
		out[i] = row
	}
	return out
}

func readI32(t *testing.T, path string) [][]int32 {
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	n := int(binary.LittleEndian.Uint32(b[0:]))
	w := int(binary.LittleEndian.Uint32(b[4:]))
	out := make([][]int32, n)
	off := 8
	for i := 0; i < n; i++ {
		row := make([]int32, w)
		for j := 0; j < w; j++ {
			row[j] = int32(binary.LittleEndian.Uint32(b[off:]))
			off += 4
		}
		out[i] = row
	}
	return out
}

// TestBulkRealRecallDiag compares recall of per-insert vs sequential-bulk vs
// parallel-bulk (1 and N threads) on REAL embeddings, isolating algorithm-quality
// loss from concurrency-induced loss. Needs /tmp/vbench from the consumer export.
func TestBulkRealRecallDiag(t *testing.T) {
	dir := os.Getenv("ASV_VBENCH")
	if dir == "" {
		dir = "/tmp/vbench"
	}
	if _, err := os.Stat(filepath.Join(dir, "base.f32")); err != nil {
		t.Skip("no exported dataset at " + dir)
	}
	base := readF32(t, filepath.Join(dir, "base.f32"))
	queries := readF32(t, filepath.Join(dir, "query.f32"))
	gt := readI32(t, filepath.Join(dir, "gt.i32"))
	qidx := readI32(t, filepath.Join(dir, "qidx.i32"))[0]
	n, dim := len(base), len(base[0])
	const k = 10
	params := Params{Dim: dim, Metric: Cosine, EfSearch: 64, Quantization: QuantInt8}
	ids := make([][]byte, n)
	for i := range ids {
		ids[i] = docID(i)
	}

	recall := func(ix *Index, rtx *btree.ReadTx) float64 {
		hit := 0
		for qi := range queries {
			self := uint64(qidx[qi]) + 1 // docID(i) encodes i+1
			gset := map[int32]bool{}
			for _, g := range gt[qi] {
				gset[g] = true
			}
			hits, err := ix.Search(rtx, queries[qi], k+1, 64)
			require.NoError(t, err)
			got := 0
			for _, h := range hits {
				id := binary.BigEndian.Uint64(h.DocID)
				if id == self {
					continue
				}
				if got >= k {
					break
				}
				got++
				if gset[int32(id-1)] {
					hit++
				}
			}
		}
		return float64(hit) / float64(len(queries)*k)
	}

	build := func(name string, fn func(wtx *btree.WriteTx) (*Index, error)) float64 {
		db, err := btree.Open(":memory:", btree.Options{InMemory: true})
		require.NoError(t, err)
		defer db.Close()
		wtx, err := db.BeginWrite()
		require.NoError(t, err)
		ix, err := fn(wtx)
		require.NoError(t, err)
		require.NoError(t, wtx.Commit())
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		defer rtx.Rollback()
		r := recall(ix, rtx)
		t.Logf("%-22s recall@%d=%.3f", name, k, r)
		return r
	}

	build("per-insert", func(wtx *btree.WriteTx) (*Index, error) {
		ix, err := Create(wtx, "vix", params, 1)
		if err != nil {
			return nil, err
		}
		for i := 0; i < n; i++ {
			if err := ix.Insert(wtx, ids[i], base[i]); err != nil {
				return nil, err
			}
		}
		return ix, nil
	})
	build("bulk-seq", func(wtx *btree.WriteTx) (*Index, error) {
		return BulkBuild(wtx, "vix", params, 1, ids, base)
	})
	build("bulk-par(1)", func(wtx *btree.WriteTx) (*Index, error) {
		return BulkBuildParallel(wtx, "vix", params, 1, ids, base, 1)
	})
	// Threads sweep (default repair): build time + recall vs worker count — the
	// fair comparison point vs engines with small default worker counts (pgvector
	// defaults to ~2). Run on a box with >= the max thread count to be meaningful.
	repairEfTune = 0
	for _, th := range []int{1, 2, 4, 8, 16} {
		db, err := btree.Open(":memory:", btree.Options{InMemory: true})
		require.NoError(t, err)
		wtx, err := db.BeginWrite()
		require.NoError(t, err)
		t0 := time.Now()
		ix, err := BulkBuildParallel(wtx, "vix", params, 1, ids, base, th)
		require.NoError(t, err)
		require.NoError(t, wtx.Commit())
		bt := time.Since(t0)
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		r := recall(ix, rtx)
		_ = rtx.Rollback()
		_ = db.Close()
		t.Logf("threads=%-2d build=%s (%.0f/s) recall@%d=%.3f", th, bt.Round(time.Millisecond), float64(n)/bt.Seconds(), k, r)
	}

	// Repair-ef sweep: recall vs build time. A narrower repair beam is cheaper
	// (better low-core scaling) — find the smallest ef that keeps recall parity.
	for _, ref := range []int{16, 32, 64, 128, 200} {
		repairEfTune = ref
		db, err := btree.Open(":memory:", btree.Options{InMemory: true})
		require.NoError(t, err)
		wtx, err := db.BeginWrite()
		require.NoError(t, err)
		t0 := time.Now()
		ix, err := BulkBuildParallel(wtx, "vix", params, 1, ids, base, 0)
		require.NoError(t, err)
		require.NoError(t, wtx.Commit())
		bt := time.Since(t0)
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		r := recall(ix, rtx)
		_ = rtx.Rollback()
		_ = db.Close()
		t.Logf("repairEf=%-3d build=%s recall@%d=%.3f", ref, bt.Round(time.Millisecond), k, r)
	}
	repairEfTune = 0
}
