package anystore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/anyproto/any-store/anyenc"
	"github.com/stretchr/testify/require"
)

func TestProfile_IndexedSortPipeline(t *testing.T) {
	ctx := context.Background()
	EnablePipelinePerfCounters(true)
	t.Cleanup(func() { EnablePipelinePerfCounters(false) })
	db := newFixture(t)

	coll, err := db.CreateCollection(ctx, "golden")
	require.NoError(t, err)

	// Build a representative dataset and indexes used by sort benchmarks.
	a := &anyenc.Arena{}
	batch := make([]*anyenc.Value, 0, 1000)
	for i := range 100000 {
		doc := a.NewObject()
		doc.Set("id", a.NewNumberInt(i))
		doc.Set("a", a.NewNumberInt(i%100))
		doc.Set("b", a.NewNumberInt((i/100)%50))
		doc.Set("c", a.NewNumberInt((i/5000)%10))
		doc.Set("val", a.NewNumberInt(i*7%1000))
		doc.Set("email", a.NewString(fmt.Sprintf("user%d@test.com", i)))
		batch = append(batch, doc)
		if len(batch) == cap(batch) {
			require.NoError(t, coll.Insert(ctx, batch...))
			batch = batch[:0]
			a.Reset()
		}
	}
	if len(batch) > 0 {
		require.NoError(t, coll.Insert(ctx, batch...))
	}
	require.NoError(t, coll.CreateIndex(ctx,
		IndexInfo{Fields: []string{"a"}},
		IndexInfo{Fields: []string{"b"}},
		IndexInfo{Fields: []string{"c"}},
		IndexInfo{Fields: []string{"a", "b"}},
		IndexInfo{Fields: []string{"a", "-b"}},
		IndexInfo{Fields: []string{"email"}, Unique: true},
	))

	type tc struct {
		name string
		q    Query
	}
	cases := []tc{
		{"Sort/WithIdx", coll.Find(nil).Sort("a").Limit(100)},
		{"Sort/DescWithIdx", coll.Find(nil).Sort("-a").Limit(100)},
		{"FilterSort/SimpleIdx", coll.Find(`{"a":{"$gte":40,"$lte":60}}`).Sort("a").Limit(100)},
		{"FilterSort/CompoundIdx", coll.Find(`{"a":50}`).Sort("b").Limit(100)},
	}

	for _, c := range cases {
		ResetPipelinePerfCounters()
		start := time.Now()
		var docs int

		// Run enough iterations to smooth noise, but keep test runtime acceptable.
		for range 200 {
			it, ierr := c.q.Iter(ctx)
			require.NoError(t, ierr)
			for it.Next() {
				_, derr := it.Doc()
				require.NoError(t, derr)
				docs++
			}
			require.NoError(t, it.Close())
		}
		elapsed := time.Since(start)
		s := SnapshotPipelinePerfCounters()
		p := s.Planner

		t.Logf("[%s] elapsed=%s docs=%d", c.name, elapsed, docs)
		t.Logf("[%s] index: calls=%d yields=%d ns=%d", c.name, p.IndexNextCalls, p.IndexYields, p.IndexNextNs)
		t.Logf("[%s] fetch: calls=%d yields=%d total_ns=%d lookup_ns=%d parse_ns=%d",
			c.name, p.FetchNextCalls, p.FetchYields, p.FetchNextNs, p.FetchLookupNs, p.FetchParseNs)
		t.Logf("[%s] filter: calls=%d yields=%d total_ns=%d eval_ns=%d",
			c.name, p.FilterNextCalls, p.FilterYields, p.FilterNextNs, p.FilterEvalNs)
		t.Logf("[%s] doc: calls=%d parsed_hits=%d fallbacks=%d fallback_seek_ns=%d fallback_parse_ns=%d",
			c.name, s.DocCalls, s.DocParsedHits, s.DocFallbacks, s.DocFallbackSeekNs, s.DocFallbackParseNs)

		require.Greater(t, s.DocCalls, uint64(0))
	}
}
