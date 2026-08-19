package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/aggregate"
)

// collRows returns every document of coll ordered by id, as JSON strings.
func collRows(t *testing.T, coll Collection) []string {
	t.Helper()
	iter, err := coll.Find(nil).Sort("id").Iter(ctx)
	require.NoError(t, err)
	var res []string
	for iter.Next() {
		d, derr := iter.Doc()
		require.NoError(t, derr)
		res = append(res, d.Value().String())
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	return res
}

func TestCollection_AggregateOut(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "src")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"cat":"a","v":10}`),
		anyenc.MustParseJson(`{"id":2,"cat":"b","v":20}`),
		anyenc.MustParseJson(`{"id":3,"cat":"a","v":30}`),
	))
	groupOut := func(target string) string {
		return fmt.Sprintf(`[
			{"$group": {"_id": "$cat", "total": {"$sum": "$v"}}},
			{"$sort": {"id": 1}},
			{"$out": %q}
		]`, target)
	}

	t.Run("into new collection", func(t *testing.T) {
		iter, err := coll.Aggregate(groupOut("out_new")).Iter(ctx)
		require.NoError(t, err)
		// Empty cursor: the write already executed inside Iter.
		assert.False(t, iter.Next())
		require.NoError(t, iter.Err())
		require.NoError(t, iter.Close())

		target, err := fx.OpenCollection(ctx, "out_new")
		require.NoError(t, err)
		assert.Equal(t, expectJson(t,
			`{"id":"a","total":40}`,
			`{"id":"b","total":20}`,
		), collRows(t, target))
	})

	t.Run("count returns docs written", func(t *testing.T) {
		n, err := coll.Aggregate(groupOut("out_count")).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, n)
	})

	t.Run("close without next is a no-op after the eager write", func(t *testing.T) {
		iter, err := coll.Aggregate(groupOut("out_eager")).Iter(ctx)
		require.NoError(t, err)
		// Written before Close: Iter itself executed the pipeline.
		target, err := fx.OpenCollection(ctx, "out_eager")
		require.NoError(t, err)
		assertCollCount(t, target, 2)
		require.NoError(t, iter.Close())
		assertCollCount(t, target, 2)
		assert.ErrorIs(t, iter.Close(), ErrIterClosed)
	})

	t.Run("into existing replaces contents, indexes preserved and rebuilt", func(t *testing.T) {
		target, err := fx.CreateCollection(ctx, "out_existing")
		require.NoError(t, err)
		require.NoError(t, target.EnsureIndex(ctx, IndexInfo{Fields: []string{"total"}}))
		require.NoError(t, target.Insert(ctx,
			anyenc.MustParseJson(`{"id":"old1","total":999}`),
			anyenc.MustParseJson(`{"id":"old2","total":998}`),
		))

		n, err := coll.Aggregate(groupOut("out_existing")).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, n)
		assert.Equal(t, expectJson(t,
			`{"id":"a","total":40}`,
			`{"id":"b","total":20}`,
		), collRows(t, target))

		// The declared index survived and serves the new contents.
		require.Len(t, target.GetIndexes(), 1)
		q := target.Find(`{"total": {"$gt": 30}}`).Sort("-total")
		explain, err := q.Explain(ctx)
		require.NoError(t, err)
		var used bool
		for _, ie := range explain.Indexes {
			if ie.Used && ie.Name == "total" {
				used = true
			}
		}
		assert.True(t, used, "filter on the materialized field must use the index:\n%s", explain.Plan)
		count, err := q.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("swap is invisible to a reader on the old snapshot", func(t *testing.T) {
		target, err := fx.CreateCollection(ctx, "out_snapshot")
		require.NoError(t, err)
		require.NoError(t, target.Insert(ctx, anyenc.MustParseJson(`{"id":"old"}`)))

		rtx, err := fx.ReadTx(ctx)
		require.NoError(t, err)
		_, err = coll.Aggregate(groupOut("out_snapshot")).Count(ctx)
		require.NoError(t, err)
		// The reader's snapshot predates the swap: old contents, atomically.
		assertCollCountCtx(rtx.Context(), t, target, 1)
		require.NoError(t, rtx.Commit())
		assertCollCount(t, target, 2)
	})

	t.Run("empty result creates an empty collection", func(t *testing.T) {
		_, err := coll.Aggregate(`[{"$match": {"cat": "nope"}}, {"$out": "out_empty_new"}]`).Count(ctx)
		require.NoError(t, err)
		target, err := fx.OpenCollection(ctx, "out_empty_new")
		require.NoError(t, err)
		assertCollCount(t, target, 0)
	})

	t.Run("empty result empties an existing collection", func(t *testing.T) {
		target, err := fx.CreateCollection(ctx, "out_empty_existing")
		require.NoError(t, err)
		require.NoError(t, target.Insert(ctx, anyenc.MustParseJson(`{"id":"old"}`)))
		n, err := coll.Aggregate(`[{"$match": {"cat": "nope"}}, {"$out": "out_empty_existing"}]`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, n)
		assertCollCount(t, target, 0)
	})

	t.Run("duplicate result ids abort, nothing persisted", func(t *testing.T) {
		target, err := fx.CreateCollection(ctx, "out_dup")
		require.NoError(t, err)
		require.NoError(t, target.Insert(ctx, anyenc.MustParseJson(`{"id":"old"}`)))
		_, err = coll.Aggregate(`[
			{"$project": {"id": {"$literal": "same"}}},
			{"$out": "out_dup"}
		]`).Count(ctx)
		require.ErrorIs(t, err, ErrDocExists)
		// The whole write rolled back: the old contents survive.
		assert.Equal(t, expectJson(t, `{"id":"old"}`), collRows(t, target))
	})

	t.Run("result without id fails like Insert", func(t *testing.T) {
		_, err := coll.Aggregate(`[{"$project": {"v": 1}}, {"$out": "out_noid"}]`).Count(ctx)
		require.ErrorIs(t, err, ErrDocWithoutId)
		_, err = fx.OpenCollection(ctx, "out_noid")
		assert.ErrorIs(t, err, ErrCollectionNotFound)
	})

	t.Run("targeting the source is rejected", func(t *testing.T) {
		_, err := coll.Aggregate(`[{"$out": "src"}]`).Iter(ctx)
		require.ErrorIs(t, err, ErrAggregateIntoSource)
		assertCollCount(t, coll, 3)
	})

	t.Run("memory limit exceeded writes nothing", func(t *testing.T) {
		_, err := coll.Aggregate(`[{"$out": "out_limited"}]`).MemoryLimit(8).Count(ctx)
		require.ErrorIs(t, err, ErrAggMemoryLimitExceeded)
		_, err = fx.OpenCollection(ctx, "out_limited")
		assert.ErrorIs(t, err, ErrCollectionNotFound)
	})

	t.Run("whole-collection copy", func(t *testing.T) {
		n, err := coll.Aggregate(`[{"$out": "out_copy"}]`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 3, n)
		target, err := fx.OpenCollection(ctx, "out_copy")
		require.NoError(t, err)
		assert.Equal(t, collRows(t, coll), collRows(t, target))
	})
}

func TestCollection_AggregateMerge(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "src")
	require.NoError(t, err)
	// Results (sorted by id): id 1 matches the seeded target doc, id 2 does not.
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"b":9,"c":3}`),
		anyenc.MustParseJson(`{"id":2,"x":1}`),
	))
	seedTarget := func(t *testing.T, name string) Collection {
		t.Helper()
		target, err := fx.CreateCollection(ctx, name)
		require.NoError(t, err)
		require.NoError(t, target.Insert(ctx,
			anyenc.MustParseJson(`{"id":1,"a":1,"b":2}`),
			anyenc.MustParseJson(`{"id":9,"keep":true}`),
		))
		return target
	}
	mergeInto := func(name, whenMatched, whenNotMatched string) string {
		return fmt.Sprintf(`[
			{"$sort": {"id": 1}},
			{"$merge": {"into": %q, "whenMatched": %q, "whenNotMatched": %q}}
		]`, name, whenMatched, whenNotMatched)
	}
	seeded := []string{`{"id":1,"a":1,"b":2}`, `{"id":9,"keep":true}`}
	mergedDoc := `{"id":1,"a":1,"b":9,"c":3}` // shallow merge: b overwritten, a kept, c added
	replacedDoc := `{"id":1,"b":9,"c":3}`     // replace: a gone
	insertedDoc := `{"id":2,"x":1}`

	cases := []struct {
		whenMatched, whenNotMatched string
		wantErr                     error
		want                        []string // nil = target unchanged
		written                     int
	}{
		{"merge", "insert", nil, []string{mergedDoc, insertedDoc, `{"id":9,"keep":true}`}, 2},
		{"merge", "discard", nil, []string{mergedDoc, `{"id":9,"keep":true}`}, 1},
		{"merge", "fail", ErrMergeNotMatched, nil, 0},
		{"replace", "insert", nil, []string{replacedDoc, insertedDoc, `{"id":9,"keep":true}`}, 2},
		{"replace", "discard", nil, []string{replacedDoc, `{"id":9,"keep":true}`}, 1},
		{"replace", "fail", ErrMergeNotMatched, nil, 0},
		{"keepExisting", "insert", nil, []string{seeded[0], insertedDoc, seeded[1]}, 1},
		{"keepExisting", "discard", nil, []string{seeded[0], seeded[1]}, 0},
		{"keepExisting", "fail", ErrMergeNotMatched, nil, 0},
		{"fail", "insert", ErrMergeMatched, nil, 0},
		{"fail", "discard", ErrMergeMatched, nil, 0},
		{"fail", "fail", ErrMergeMatched, nil, 0},
	}
	for i, tc := range cases {
		t.Run(tc.whenMatched+"_"+tc.whenNotMatched, func(t *testing.T) {
			name := fmt.Sprintf("tgt_%d", i)
			target := seedTarget(t, name)
			n, err := coll.Aggregate(mergeInto(name, tc.whenMatched, tc.whenNotMatched)).Count(ctx)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				// whenMatched fail trips on the matching id 1; whenNotMatched
				// fail on the unmatched id 2.
				offending := "id 1"
				if tc.wantErr == ErrMergeNotMatched {
					offending = "id 2"
				}
				assert.ErrorContains(t, err, offending, "fail error must name the offending id")
				// Abort is all-or-nothing: even writes routed before the
				// failing doc are rolled back.
				assert.Equal(t, expectJson(t, seeded...), collRows(t, target))
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.written, n)
			assert.Equal(t, expectJson(t, tc.want...), collRows(t, target))
		})
	}

	t.Run("into nonexistent collection creates it", func(t *testing.T) {
		n, err := coll.Aggregate(`[{"$merge": "merge_new"}]`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, n)
		target, err := fx.OpenCollection(ctx, "merge_new")
		require.NoError(t, err)
		assert.Equal(t, expectJson(t, `{"id":1,"b":9,"c":3}`, insertedDoc), collRows(t, target))
	})

	t.Run("empty results are a no-op, target not created", func(t *testing.T) {
		n, err := coll.Aggregate(`[{"$match": {"id": "nope"}}, {"$merge": "merge_empty"}]`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, n)
		_, err = fx.OpenCollection(ctx, "merge_empty")
		assert.ErrorIs(t, err, ErrCollectionNotFound)
	})

	t.Run("result without id is rejected", func(t *testing.T) {
		_, err := coll.Aggregate(`[{"$project": {"x": 1}}, {"$merge": "merge_noid"}]`).Count(ctx)
		require.ErrorIs(t, err, ErrMergeNoId)
		_, err = fx.OpenCollection(ctx, "merge_noid")
		assert.ErrorIs(t, err, ErrCollectionNotFound)
	})

	t.Run("targeting the source is rejected", func(t *testing.T) {
		_, err := coll.Aggregate(`[{"$merge": "src"}]`).Count(ctx)
		require.ErrorIs(t, err, ErrAggregateIntoSource)
	})

	t.Run("target with a non-id primary key is rejected", func(t *testing.T) {
		_, err := fx.CreateCollection(ctx, "merge_pk", CollectionOptions{PrimaryKey: "key"})
		require.NoError(t, err)
		_, err = coll.Aggregate(`[{"$merge": "merge_pk"}]`).Count(ctx)
		require.ErrorIs(t, err, errAggMergePrimaryKey)
	})

	t.Run("memory limit exceeded writes nothing", func(t *testing.T) {
		target := seedTarget(t, "merge_limited")
		_, err := coll.Aggregate(`[{"$merge": "merge_limited"}]`).MemoryLimit(8).Count(ctx)
		require.ErrorIs(t, err, ErrAggMemoryLimitExceeded)
		assert.Equal(t, expectJson(t, seeded...), collRows(t, target))
	})

	t.Run("merge updates index entries", func(t *testing.T) {
		target := seedTarget(t, "merge_indexed")
		require.NoError(t, target.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))
		_, err := coll.Aggregate(`[{"$merge": "merge_indexed"}]`).Count(ctx)
		require.NoError(t, err)
		q := target.Find(`{"b": 9}`)
		explain, err := q.Explain(ctx)
		require.NoError(t, err)
		var used bool
		for _, ie := range explain.Indexes {
			if ie.Used && ie.Name == "b" {
				used = true
			}
		}
		assert.True(t, used, "merged field must be reachable via its index:\n%s", explain.Plan)
		n, err := q.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, n)
		// The old key (b=2) left the index with the update.
		n, err = target.Find(`{"b": 2}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, n)
	})
}

// TestCollection_AggregateMaterializeThenQuery is the motivating flow of
// SYN-129: derive values with $group, materialize them with $merge, then
// filter/sort on the derived field through a declared index instead of
// recomputing client-side.
func TestCollection_AggregateMaterializeThenQuery(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "events")
	require.NoError(t, err)
	var docs []*anyenc.Value
	for i := 0; i < 100; i++ {
		docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"space":"s%d","bytes":%d}`, i, i%10, i*10)))
	}
	require.NoError(t, coll.Insert(ctx, docs...))

	stats, err := fx.CreateCollection(ctx, "space_stats")
	require.NoError(t, err)
	require.NoError(t, stats.EnsureIndex(ctx, IndexInfo{Fields: []string{"total"}}))

	n, err := coll.Aggregate(`[
		{"$group": {"_id": "$space", "total": {"$sum": "$bytes"}, "n": {"$count": {}}}},
		{"$merge": {"into": "space_stats", "whenMatched": "replace"}}
	]`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, n)

	q := stats.Find(`{"total": {"$gte": 4900}}`).Sort("-total")
	explain, err := q.Explain(ctx)
	require.NoError(t, err)
	var used bool
	for _, ie := range explain.Indexes {
		if ie.Used && ie.Name == "total" {
			used = true
		}
	}
	assert.True(t, used, "derived-field query must run on the declared index:\n%s", explain.Plan)

	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	var totals []float64
	for iter.Next() {
		d, derr := iter.Doc()
		require.NoError(t, derr)
		totals = append(totals, d.Value().GetFloat64("total"))
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	assert.Equal(t, []float64{5400, 5300, 5200, 5100, 5000, 4900}, totals)

	// Re-running the aggregation refreshes the stats idempotently.
	n, err = coll.Aggregate(`[
		{"$group": {"_id": "$space", "total": {"$sum": "$bytes"}, "n": {"$count": {}}}},
		{"$merge": {"into": "space_stats", "whenMatched": "replace"}}
	]`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, n, "identical replace is a no-op write")
	assertCollCount(t, stats, 10)
}

func TestAggregateSinkStagesExported(t *testing.T) {
	assert.Contains(t, AggregateStages(), "$merge")
	assert.Contains(t, AggregateStages(), "$out")
	_ = aggregate.MergeSpec{} // sink specs are part of the pipeline vocabulary
}

func TestCollection_AggregateReadOnly(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "src")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"v":10}`),
		anyenc.MustParseJson(`{"id":2,"v":20}`),
	))

	t.Run("merge sink rejected, nothing created", func(t *testing.T) {
		q := coll.Aggregate(`[
			{"$group": {"_id": null, "s": {"$sum": "$v"}}},
			{"$merge": "ro_merge"}
		]`).ReadOnly()
		_, err := q.Iter(ctx)
		require.ErrorIs(t, err, ErrAggregateReadOnly)
		assert.ErrorContains(t, err, "$merge")
		_, err = q.Count(ctx)
		assert.ErrorIs(t, err, ErrAggregateReadOnly)
		// Fresh handle: the fail-fast must precede target creation.
		_, err = fx.OpenCollection(ctx, "ro_merge")
		assert.ErrorIs(t, err, ErrCollectionNotFound)
	})

	t.Run("out sink rejected", func(t *testing.T) {
		_, err := coll.Aggregate(`[{"$out": "ro_out"}]`).ReadOnly().Iter(ctx)
		require.ErrorIs(t, err, ErrAggregateReadOnly)
		assert.ErrorContains(t, err, "$out")
		_, err = fx.OpenCollection(ctx, "ro_out")
		assert.ErrorIs(t, err, ErrCollectionNotFound)
	})

	t.Run("plain pipeline unaffected", func(t *testing.T) {
		n, err := coll.Aggregate(`[{"$match": {"v": {"$gte": 10}}}]`).ReadOnly().Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, n)
	})

	t.Run("explain still allowed", func(t *testing.T) {
		ex, err := coll.Aggregate(`[{"$out": "ro_explain"}]`).ReadOnly().Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, ex.Plan, "$out")
		_, err = fx.OpenCollection(ctx, "ro_explain")
		assert.ErrorIs(t, err, ErrCollectionNotFound)
	})

	t.Run("without ReadOnly the sink still runs", func(t *testing.T) {
		n, err := coll.Aggregate(`[{"$out": "rw_out"}]`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, n)
	})
}

func benchSinkFixture(b *testing.B, targetDocs int) (*fixture, Collection) {
	b.Helper()
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "src")
	require.NoError(b, err)
	var docs []*anyenc.Value
	for i := 0; i < 10000; i++ {
		docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"cat":"c%d","v":%d,"name":"doc-%d"}`, i, i%100, i, i)))
	}
	require.NoError(b, coll.Insert(ctx, docs...))
	if targetDocs > 0 {
		target, err := fx.CreateCollection(ctx, "target")
		require.NoError(b, err)
		docs = docs[:0]
		for i := 0; i < targetDocs; i++ {
			docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(
				`{"id":%d,"cat":"old","v":0,"name":"old-%d"}`, i, i)))
		}
		require.NoError(b, target.Insert(ctx, docs...))
	}
	return fx, coll
}

func BenchmarkAggregateOut10k(b *testing.B) {
	_, coll := benchSinkFixture(b, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := coll.Aggregate(`[{"$out": "target"}]`).Count(ctx)
		if err != nil || n != 10000 {
			b.Fatal(n, err)
		}
	}
	b.ReportMetric(float64(10000*b.N)/b.Elapsed().Seconds(), "docs/s")
}

func BenchmarkAggregateMergeReplace10k(b *testing.B) {
	_, coll := benchSinkFixture(b, 10000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// gen changes per iteration so every doc is a real replace, never an
		// equality-skip no-op.
		n, err := coll.Aggregate(fmt.Sprintf(`[
			{"$addFields": {"gen": %d}},
			{"$merge": {"into": "target", "whenMatched": "replace"}}
		]`, i)).Count(ctx)
		if err != nil || n != 10000 {
			b.Fatal(n, err)
		}
	}
	b.ReportMetric(float64(10000*b.N)/b.Elapsed().Seconds(), "docs/s")
}
