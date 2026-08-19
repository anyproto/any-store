package anystore

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// statsDoc builds a document whose marshaled size comfortably exceeds
// anyenc.CompressMinSize and whose payload is highly compressible.
func statsDoc(id int) *anyenc.Value {
	filler := strings.Repeat("compressible-", 64)
	return anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"k":%d,"filler":%q}`, id, id%10, filler))
}

func TestCollection_Stats(t *testing.T) {
	const docCount = 200

	t.Run("compression enabled", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "byK", Fields: []string{"k"}}))

		for i := 0; i < docCount; i++ {
			require.NoError(t, coll.Insert(ctx, statsDoc(i)))
		}

		st, err := coll.Stats(ctx)
		require.NoError(t, err)

		assert.Equal(t, "test", st.Name)
		assert.Equal(t, docCount, st.DocCount)
		assert.True(t, st.CompressionEnabled)
		// Highly compressible filler ⇒ stored form is much smaller.
		assert.Less(t, st.StoredDocsBytes, st.UncompressedDocsBytes)
		assert.Greater(t, st.CompressionRatio, 1.0)

		// Physical sizes are non-zero and the total is internally consistent.
		assert.Greater(t, st.DocsSizeBytes, 0)
		assert.Greater(t, st.IndexesSizeBytes, 0)
		assert.Equal(t, st.DocsSizeBytes+st.IndexesSizeBytes, st.TotalSizeBytes)

		require.Len(t, st.Indexes, 1)
		idx := st.Indexes[0]
		assert.Equal(t, "byK", idx.Name)
		assert.Equal(t, []string{"k"}, idx.Fields)
		assert.Equal(t, docCount, idx.EntryCount)
		assert.Greater(t, idx.SizeBytes, 0)
		assert.Greater(t, idx.PayloadBytes, 0)
		assert.Equal(t, uint64(docCount), idx.SketchDocCount)
		assert.Greater(t, idx.SketchSize, 0)
		// 10 distinct values for "k" ⇒ at most 10 non-empty buckets.
		assert.GreaterOrEqual(t, idx.SketchDistribution.NonEmptyBuckets, 1)
		assert.LessOrEqual(t, idx.SketchDistribution.NonEmptyBuckets, 10)
		assert.Greater(t, idx.SketchDistribution.MaxBucket, uint64(0))
	})

	t.Run("compression disabled", func(t *testing.T) {
		fx := newFixture(t, &Config{DisableCompression: true})
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		for i := 0; i < docCount; i++ {
			require.NoError(t, coll.Insert(ctx, statsDoc(i)))
		}

		st, err := coll.Stats(ctx)
		require.NoError(t, err)
		assert.False(t, st.CompressionEnabled)
		// Nothing compressed ⇒ stored equals uncompressed, ratio is exactly 1.
		assert.Equal(t, st.StoredDocsBytes, st.UncompressedDocsBytes)
		assert.Equal(t, 1.0, st.CompressionRatio)
	})

	// Stats reads each index's live in-memory sketch while writers may be
	// mutating it. Run Stats concurrently with inserts under -race to pin
	// that the sketch reads (Distribution/GetDocCount) stay race-free.
	t.Run("concurrent with writes", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "byK", Fields: []string{"k"}}))

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < docCount; i++ {
				require.NoError(t, coll.Insert(ctx, statsDoc(i)))
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_, sErr := coll.Stats(ctx)
				require.NoError(t, sErr)
			}
		}()
		wg.Wait()

		st, err := coll.Stats(ctx)
		require.NoError(t, err)
		assert.Equal(t, docCount, st.DocCount)
		require.Len(t, st.Indexes, 1)
		assert.Equal(t, uint64(docCount), st.Indexes[0].SketchDocCount)
	})

	t.Run("empty collection", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "empty")
		require.NoError(t, err)

		st, err := coll.Stats(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, st.DocCount)
		assert.Equal(t, 0, st.StoredDocsBytes)
		assert.Equal(t, 1.0, st.CompressionRatio)
		assert.Empty(t, st.Indexes)
		assert.Equal(t, st.DocsSizeBytes+st.IndexesSizeBytes, st.TotalSizeBytes)
	})
}
