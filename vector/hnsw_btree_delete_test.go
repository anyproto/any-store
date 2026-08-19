package vector

import (
	"testing"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBtreeHNSWDeletePersist(t *testing.T) {
	const (
		n   = 1200
		dim = 32
		k   = 10
	)
	vecs, keys := randVectors(n, dim, 14)
	queries, _ := randVectors(40, dim, 88)

	db := openTestDB(t, true)
	idx, err := OpenBtreeHNSW(db, "emb", dim, L2)
	require.NoError(t, err)
	idx.SetEf(64)
	for i := range vecs {
		idx.Add(keys[i], vecs[i])
	}
	require.NoError(t, idx.Flush())

	// delete 25%
	del := make(map[uint64]bool)
	for i := 0; i < n/4; i++ {
		require.True(t, idx.Delete(keys[i]))
		del[keys[i]] = true
	}
	require.NoError(t, idx.Flush())
	require.Equal(t, n-len(del), idx.Len())

	pre := make([][]SearchResult, len(queries))
	for i, q := range queries {
		pre[i] = idx.Search(q, k)
		for _, r := range pre[i] {
			require.False(t, del[r.Key], "deleted key in results before reload")
		}
	}

	// reopen: tombstones must survive
	idx2, err := OpenBtreeHNSW(db, "emb", dim, L2)
	require.NoError(t, err)
	idx2.SetEf(64)
	require.Equal(t, n-len(del), idx2.Len(), "live count after reload")
	for i, q := range queries {
		post := idx2.Search(q, k)
		require.Equal(t, len(pre[i]), len(post))
		for j := range post {
			assert.Equal(t, pre[i][j].Key, post[j].Key, "result drift after reload q%d", i)
			require.False(t, del[post[j].Key], "deleted key resurrected after reload")
		}
	}
}

// TestBtreeWriteAmplification measures bytes/records written per insert, delete
// and update, and the vector-vs-adjacency split that motivates separating the
// two on disk.
func TestBtreeWriteAmplification(t *testing.T) {
	const (
		n   = 4000
		dim = 128
	)
	vecs, keys := randVectors(n, dim, 21)
	moreVecs, _ := randVectors(n, dim, 99)

	db, err := btree.Open(":memory:", btree.Options{InMemory: true})
	require.NoError(t, err)
	defer db.Close()
	idx, err := OpenBtreeHNSW(db, "emb", dim, L2)
	require.NoError(t, err)

	// --- inserts: flush one at a time to attribute cost per insert ---
	var insRecords, insBytes, insVec, insAdj int
	before := idx.Stats()
	for i := 0; i < n; i++ {
		idx.Add(keys[i], vecs[i])
		require.NoError(t, idx.Flush())
	}
	after := idx.Stats()
	insRecords = after.RecordsWritten - before.RecordsWritten
	insBytes = after.BytesWritten - before.BytesWritten
	insVec = after.VectorBytes - before.VectorBytes
	insAdj = after.AdjacencyBytes - before.AdjacencyBytes
	t.Logf("INSERT  %d ops: %.1f records/op, %.0f bytes/op  (vector %.0f + adj %.0f + overhead)",
		n, f(insRecords)/n, f(insBytes)/n, f(insVec)/n, f(insAdj)/n)
	t.Logf("        => if vector stored separately, a re-link rewrites only ~%.0f adj bytes, not the full %.0f",
		f(insAdj)/f(insRecords), f(insBytes)/f(insRecords))

	// --- deletes (tombstone): per-op cost ---
	before = idx.Stats()
	const nDel = 500
	for i := 0; i < nDel; i++ {
		idx.Delete(keys[i])
		require.NoError(t, idx.Flush())
	}
	after = idx.Stats()
	t.Logf("DELETE  %d ops: %.1f records/op, %.1f bytes/op  (tombstone only)",
		nDel, f(after.RecordsWritten-before.RecordsWritten)/nDel, f(after.BytesWritten-before.BytesWritten)/nDel)

	// --- updates (delete+reinsert): per-op cost ---
	before = idx.Stats()
	const nUpd = 500
	for i := 0; i < nUpd; i++ {
		idx.Update(keys[nDel+i], moreVecs[i])
		require.NoError(t, idx.Flush())
	}
	after = idx.Stats()
	t.Logf("UPDATE  %d ops: %.1f records/op, %.0f bytes/op  (tombstone + new node + neighbours)",
		nUpd, f(after.RecordsWritten-before.RecordsWritten)/nUpd, f(after.BytesWritten-before.BytesWritten)/nUpd)

	raw := dim * 4
	t.Logf("note: one raw vector = %d bytes; an adjacency list ~ M0*4 = %d bytes", raw, idx.flat.M0*4)
}

func f(i int) float64 { return float64(i) }
