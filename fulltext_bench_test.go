package anystore

import (
	"fmt"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/require"
)

// Write-path benchmarks for the FTS index. The corpus deliberately reuses a
// modest shared vocabulary so the postings chunks and vocab keys for common
// terms are touched repeatedly within a batch — which is exactly where the
// per-tx write-back buffer should help (collapsing N chunk RMWs into one).

// ftsBenchDocs builds n documents whose bodies draw from a shared vocabulary
// (so terms recur across docs) plus a couple of per-doc unique tokens.
func ftsBenchDocs(n int) []*anyenc.Value {
	vocab := []string{
		"the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog",
		"aircraft", "crash", "flight", "engine", "failure", "landing", "runway",
		"pilot", "weather", "altitude", "mountain", "report", "investigation",
		"london", "paris", "tokyo", "boeing", "airbus", "passengers", "crew",
	}
	docs := make([]*anyenc.Value, n)
	for i := 0; i < n; i++ {
		// ~30-word body: rotating window over the shared vocab + a unique token.
		body := ""
		for w := 0; w < 30; w++ {
			body += vocab[(i+w*7)%len(vocab)] + " "
		}
		body += fmt.Sprintf("uniq%d", i)
		docs[i] = anyenc.MustParseJson(fmt.Sprintf(`{"id":"doc%d","body":%q,"n":%d}`, i, body, i))
	}
	return docs
}

func benchFtsColl(b *testing.B) (*fixture, Collection) {
	b.Helper()
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "bench")
	require.NoError(b, err)
	require.NoError(b, coll.EnsureIndex(ctx, IndexInfo{Kind: IndexKindFulltext, Fields: []string{"body"}}))
	return fx, coll
}

// BenchmarkFtsBatchInsert inserts a batch of docs in a single Insert call (one
// write tx) — the primary batch-write path.
func BenchmarkFtsBatchInsert(b *testing.B) {
	for _, batch := range []int{100, 1000} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				fx, coll := benchFtsColl(b)
				docs := ftsBenchDocs(batch)
				b.StartTimer()
				if err := coll.Insert(ctx, docs...); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				fx.finish()
				b.StartTimer()
			}
		})
	}
}

// BenchmarkFtsSingleInsert inserts docs one per tx (no cross-doc batching).
func BenchmarkFtsSingleInsert(b *testing.B) {
	fx, coll := benchFtsColl(b)
	defer fx.finish()
	docs := ftsBenchDocs(b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := coll.Insert(ctx, docs[i]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFtsBatchUpdate re-writes the body of many existing docs in one tx.
func BenchmarkFtsBatchUpdate(b *testing.B) {
	for _, batch := range []int{100, 1000} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				fx, coll := benchFtsColl(b)
				require.NoError(b, coll.Insert(ctx, ftsBenchDocs(batch)...))
				// New bodies: shift the window so most terms change.
				upd := make([]*anyenc.Value, batch)
				for j := 0; j < batch; j++ {
					upd[j] = anyenc.MustParseJson(fmt.Sprintf(
						`{"id":"doc%d","body":"updated content revision two %d","n":%d}`, j, j, j))
				}
				b.StartTimer()
				for j := 0; j < batch; j++ {
					if err := coll.UpsertOne(ctx, upd[j]); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				fx.finish()
				b.StartTimer()
			}
		})
	}
}

// BenchmarkFtsSearch measures the query/read path: a common term (large
// postings list — the scores-accumulator hot loop), a rarer term, and a 2-term
// query. The corpus reuses the shared vocab so "common" terms hit many docs.
func BenchmarkFtsSearch(b *testing.B) {
	fx, coll := benchFtsColl(b)
	defer fx.finish()
	require.NoError(b, coll.Insert(ctx, ftsBenchDocs(5000)...))

	queries := map[string]string{
		"common":   `{"$text":{"$search":"the"}}`,    // hits a large fraction
		"rare":     `{"$text":{"$search":"uniq1234"}}`, // hits exactly one
		"multi":    `{"$text":{"$search":"london paris tokyo"}}`,
	}
	for name, q := range queries {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				iter, err := coll.Find(q).Limit(10).Iter(ctx)
				if err != nil {
					b.Fatal(err)
				}
				for iter.Next() {
					if _, err := iter.Doc(); err != nil {
						b.Fatal(err)
					}
				}
				_ = iter.Close()
			}
		})
	}
}

// BenchmarkFtsSmallEdit simulates interactive note editing: a one-word change to
// an existing document. This is the case the delta-update should make cheap.
func BenchmarkFtsSmallEdit(b *testing.B) {
	fx, coll := benchFtsColl(b)
	defer fx.finish()
	// One reasonably long doc we edit repeatedly.
	base := "the quick brown fox jumps over the lazy dog near the old runway while the pilot reports clear weather and good visibility over the mountain range today"
	require.NoError(b, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":"edit","body":%q}`, base))))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// change a single trailing word each iteration
		body := fmt.Sprintf("%s word%d", base, i)
		if err := coll.UpsertOne(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":"edit","body":%q}`, body))); err != nil {
			b.Fatal(err)
		}
	}
}
