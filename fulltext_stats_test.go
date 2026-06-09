package anystore

import (
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFtsStats_CorrectAndHelpful(t *testing.T) {
	fx, coll := ftsTestColl(t, "body")
	defer fx.finish()

	// Known corpus: distinct terms = {alpha, beta, gamma, delta, the} = 5.
	// tokens: doc1=3, doc2=3, doc3=2, empty doc indexed as 0 (excluded).
	insertJSON(t, coll,
		`{"id":"a","body":"alpha beta gamma"}`, // 3 tokens
		`{"id":"b","body":"alpha beta the"}`,   // 3 tokens
		`{"id":"c","body":"delta the"}`,        // 2 tokens
		`{"id":"d","body":""}`,                 // no text → not indexed
	)

	st, err := coll.Stats(ctx)
	require.NoError(t, err)

	require.Len(t, st.FtsIndexes, 1, "the full-text index must appear in Stats")
	fs := st.FtsIndexes[0]

	// Corpus stats are correct.
	assert.Equal(t, []string{"body"}, fs.Fields)
	assert.Equal(t, 3, fs.DocCount, "empty doc d is excluded")
	assert.Equal(t, 8, fs.TotalTokens, "3+3+2")
	assert.InDelta(t, 8.0/3.0, fs.AvgDocLen, 1e-9)
	// distinct terms: alpha, beta, gamma, the, delta
	assert.Equal(t, 5, fs.VocabSize)

	// Storage is reported and non-trivial, and the postings dominate.
	assert.Greater(t, fs.SizeBytes, 0)
	assert.Equal(t, fs.PostingsBytes+fs.VocabBytes+fs.DocmapBytes+fs.DocinfoBytes+fs.MetaBytes, fs.SizeBytes)
	assert.Greater(t, fs.PostingsBytes, 0)

	// The collection total includes the fts index.
	assert.Equal(t, fs.SizeBytes, st.FtsIndexesSizeBytes)
	assert.Equal(t, st.DocsSizeBytes+st.IndexesSizeBytes+st.VectorIndexesSizeBytes+st.FtsIndexesSizeBytes, st.TotalSizeBytes)
	assert.GreaterOrEqual(t, st.TotalSizeBytes, fs.SizeBytes)
}

func TestFtsStats_TracksMutations(t *testing.T) {
	fx, coll := ftsTestColl(t, "body")
	defer fx.finish()

	insertJSON(t, coll,
		`{"id":"a","body":"one two three"}`,
		`{"id":"b","body":"four five"}`,
	)
	st, _ := coll.Stats(ctx)
	assert.Equal(t, 2, st.FtsIndexes[0].DocCount)
	assert.Equal(t, 5, st.FtsIndexes[0].VocabSize)
	assert.Equal(t, 5, st.FtsIndexes[0].TotalTokens)

	// Delete one doc: counts shrink, vocab drops its now-orphaned terms.
	require.NoError(t, coll.DeleteId(ctx, "b"))
	st, _ = coll.Stats(ctx)
	assert.Equal(t, 1, st.FtsIndexes[0].DocCount)
	assert.Equal(t, 3, st.FtsIndexes[0].VocabSize, "four/five gone")
	assert.Equal(t, 3, st.FtsIndexes[0].TotalTokens)

	// Delta-update keeps DocCount stable but adjusts tokens/vocab.
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":"a","body":"one two three four"}`)))
	st, _ = coll.Stats(ctx)
	assert.Equal(t, 1, st.FtsIndexes[0].DocCount)
	assert.Equal(t, 4, st.FtsIndexes[0].VocabSize, "four re-added")
	assert.Equal(t, 4, st.FtsIndexes[0].TotalTokens)
}

func TestFtsStats_EmptyIndex(t *testing.T) {
	fx, coll := ftsTestColl(t, "body")
	defer fx.finish()
	st, err := coll.Stats(ctx)
	require.NoError(t, err)
	require.Len(t, st.FtsIndexes, 1)
	fs := st.FtsIndexes[0]
	assert.Equal(t, 0, fs.DocCount)
	assert.Equal(t, 0, fs.VocabSize)
	assert.Equal(t, 0.0, fs.AvgDocLen, "no divide-by-zero on an empty index")
}
