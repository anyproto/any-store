package aggregate

import (
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

func TestSortStage(t *testing.T) {
	t.Run("single field asc desc", func(t *testing.T) {
		rows := []string{`{"id":1,"v":3}`, `{"id":2,"v":1}`, `{"id":3,"v":2}`}
		got := runPipeline(t, newSliceSource(rows...), `[{"$sort": {"v": 1}}]`, Limits{})
		assert.Equal(t, jsonRows(t, `{"id":2,"v":1}`, `{"id":3,"v":2}`, `{"id":1,"v":3}`), got)

		got = runPipeline(t, newSliceSource(rows...), `[{"$sort": {"v": -1}}]`, Limits{})
		assert.Equal(t, jsonRows(t, `{"id":1,"v":3}`, `{"id":3,"v":2}`, `{"id":2,"v":1}`), got)
	})

	t.Run("multi field", func(t *testing.T) {
		got := runPipeline(t, newSliceSource(
			`{"id":1,"a":"x","b":2}`,
			`{"id":2,"a":"y","b":1}`,
			`{"id":3,"a":"x","b":1}`,
		), `[{"$sort": {"a": 1, "b": -1}}]`, Limits{})
		assert.Equal(t, jsonRows(t,
			`{"id":1,"a":"x","b":2}`,
			`{"id":3,"a":"x","b":1}`,
			`{"id":2,"a":"y","b":1}`,
		), got)
	})

	t.Run("missing sorts as null first", func(t *testing.T) {
		got := runPipeline(t, newSliceSource(
			`{"id":1,"v":1}`,
			`{"id":2}`,
		), `[{"$sort": {"v": 1}}]`, Limits{})
		assert.Equal(t, jsonRows(t, `{"id":2}`, `{"id":1,"v":1}`), got)
	})

	t.Run("stable on equal keys", func(t *testing.T) {
		var rows []string
		for i := 1; i <= 30; i++ {
			rows = append(rows, fmt.Sprintf(`{"id":%d,"k":%d}`, i, i%3))
		}
		got := runPipeline(t, newSliceSource(rows...), `[{"$sort": {"k": 1}}]`, Limits{})
		require.Len(t, got, 30)
		// Within each key bucket, ids must be in source order.
		var lastK, lastId float64 = -1, -1
		for _, j := range got {
			var k, id float64
			_, err := fmt.Sscanf(j, `{"id":%v,"k":%v}`, &id, &k)
			require.NoError(t, err)
			if k == lastK {
				assert.Greater(t, id, lastId, "stable order violated in %s", j)
			}
			lastK, lastId = k, id
		}
	})

	t.Run("sort after group", func(t *testing.T) {
		got := runPipeline(t, newSliceSource(
			`{"id":1,"cat":"b","v":1}`,
			`{"id":2,"cat":"a","v":2}`,
			`{"id":3,"cat":"b","v":3}`,
		), `[
			{"$group": {"_id": "$cat", "s": {"$sum": "$v"}}},
			{"$sort": {"s": -1}}
		]`, Limits{})
		assert.Equal(t, jsonRows(t, `{"id":"b","s":4}`, `{"id":"a","s":2}`), got)
	})
}

func TestSortTopK(t *testing.T) {
	makeRows := func(n int) []string {
		rows := make([]string, 0, n)
		for i := 0; i < n; i++ {
			rows = append(rows, fmt.Sprintf(`{"id":%d,"v":%d,"pad":"%060d"}`, i, (i*7919)%n, i))
		}
		return rows
	}

	t.Run("matches full sort head", func(t *testing.T) {
		rows := makeRows(500)
		full := runPipeline(t, newSliceSource(rows...), `[{"$sort": {"v": 1}}]`, Limits{})
		topk := runPipeline(t, newSliceSource(rows...), `[{"$sort": {"v": 1}}, {"$skip": 3}, {"$limit": 10}]`, Limits{})
		assert.Equal(t, full[3:13], topk)
	})

	t.Run("topk stability matches stable full sort", func(t *testing.T) {
		var rows []string
		for i := 0; i < 200; i++ {
			rows = append(rows, fmt.Sprintf(`{"id":%d,"k":%d}`, i, i%5))
		}
		full := runPipeline(t, newSliceSource(rows...), `[{"$sort": {"k": 1}}]`, Limits{})
		topk := runPipeline(t, newSliceSource(rows...), `[{"$sort": {"k": 1}}, {"$limit": 12}]`, Limits{})
		assert.Equal(t, full[:12], topk)
	})

	t.Run("bounded memory", func(t *testing.T) {
		rows := makeRows(20000)
		specs := MustParsePipeline(`[{"$sort": {"v": 1}}, {"$limit": 5}]`)
		root, err := Build(newSliceSource(rows...), specs, Limits{}, Env{})
		require.NoError(t, err)
		defer root.Close()
		ctx := newTestCtx()
		n := 0
		for {
			v, err := root.Next(ctx)
			require.NoError(t, err)
			if v == nil {
				break
			}
			n++
		}
		assert.Equal(t, 5, n)
		// 20k rows of ~90 bytes ≈ 1.8MB if fully retained; top-5 must stay
		// tiny. MemAccount counts the per-row entry overhead too (12B/row),
		// so allow that plus slack.
		assert.Less(t, ctx.Mem.Used(), 20000*sortEntryOverhead+64<<10,
			"top-K sort must not retain the full input")
	})

	t.Run("memory limit enforced on full sort", func(t *testing.T) {
		rows := makeRows(5000)
		specs := MustParsePipeline(`[{"$sort": {"v": 1}}]`)
		root, err := Build(newSliceSource(rows...), specs, Limits{}, Env{})
		require.NoError(t, err)
		defer root.Close()
		ctx := newTestCtx()
		ctx.Mem = NewMemAccount(64 << 10)
		_, err = root.Next(ctx)
		assert.ErrorIs(t, err, ErrMemoryLimitExceeded)
	})

	t.Run("foldTopK rules", func(t *testing.T) {
		assert.Equal(t, 10, foldTopK(MustParsePipeline(`[{"$limit": 10}]`)))
		assert.Equal(t, 15, foldTopK(MustParsePipeline(`[{"$skip": 5}, {"$limit": 10}]`)))
		assert.Equal(t, 0, foldTopK(MustParsePipeline(`[{"$skip": 5}]`)), "skip without limit is unbounded")
		assert.Equal(t, 0, foldTopK(MustParsePipeline(`[{"$project": {"a": 1}}, {"$limit": 10}]`)),
			"a stage between sort and limit blocks folding")
		assert.Equal(t, 0, foldTopK(nil))
	})
}

// runSortStage drains a SortStage over rows and returns the emitted "id"
// fields, the first error, and the closed stage.
func runSortStage(t *testing.T, topK int, rows ...string) ([]string, error, *SortStage) {
	t.Helper()
	spec := MustParsePipeline(`[{"$sort": {"v": 1}}]`)[0].(SortSpec)
	st := &SortStage{Src: newSliceSource(rows...), Spec: spec, TopK: topK}
	defer st.Close()
	ctx := newTestCtx()
	var ids []string
	for {
		v, err := st.Next(ctx)
		if err != nil {
			return ids, err, st
		}
		if v == nil {
			return ids, nil, st
		}
		ids = append(ids, v.GetString("id"))
	}
}

// setSortArenaLimit lowers the arena limit for one test.
func setSortArenaLimit(t *testing.T, n int) {
	old := sortArenaLimit
	sortArenaLimit = uint64(n)
	t.Cleanup(func() { sortArenaLimit = old })
}

// The $sort stage applies its 32-bit arena limit as qplanner.SortIter does:
// to the rows it keeps, when it keeps them. A top-K loser is no error, a
// winner whose tail does not fit is kept after packing, and only rows that do
// not fit even packed are an error. Close releases the arena.
func TestSortStage_ArenaLimitAppliesOnAdmission(t *testing.T) {
	spec := MustParsePipeline(`[{"$sort": {"v": 1}}]`)[0].(SortSpec)
	rowLen := func(j string, topK bool) int {
		v := anyenc.MustParseJson(j)
		n := len(spec.Sort.AppendKey(nil, v)) + len(v.MarshalTo(nil))
		if topK {
			n += 4 // the sequence number
		}
		return n
	}
	kept := []string{`{"id":"1","v":"b1"}`, `{"id":"2","v":"b2"}`, `{"id":"3","v":"b3"}`, `{"id":"4","v":"b4"}`}
	loser := `{"id":"9","v":"c-loses-to-every-kept-row"}`
	winner := `{"id":"0x","v":"a0"}` // one byte longer than a kept row
	e := rowLen(kept[0], true)
	require.Equal(t, e+1, rowLen(winner, true))

	t.Run("a loser past the limit", func(t *testing.T) {
		setSortArenaLimit(t, 4*e)
		ids, err, _ := runSortStage(t, 4, append(slices.Clone(kept), loser)...)
		require.NoError(t, err)
		assert.Equal(t, []string{"1", "2", "3", "4"}, ids)
	})
	t.Run("a winner whose tail is past the limit", func(t *testing.T) {
		setSortArenaLimit(t, 4*e+1)
		ids, err, _ := runSortStage(t, 4, append(slices.Clone(kept), winner, loser)...)
		require.NoError(t, err)
		assert.Equal(t, []string{"0x", "1", "2", "3"}, ids)
	})
	t.Run("a winner that does not fit even packed", func(t *testing.T) {
		setSortArenaLimit(t, 4*e)
		_, err, st := runSortStage(t, 4, append(slices.Clone(kept), winner)...)
		require.ErrorIs(t, err, errSortTooLarge)
		assert.Nil(t, st.arena, "Close releases the arena")
	})
	t.Run("full sort at and past the limit", func(t *testing.T) {
		full := 0
		for _, r := range kept {
			full += rowLen(r, false)
		}
		setSortArenaLimit(t, full)
		ids, err, _ := runSortStage(t, 0, kept...)
		require.NoError(t, err)
		assert.Len(t, ids, 4)
		setSortArenaLimit(t, full-1)
		_, err, st := runSortStage(t, 0, kept...)
		require.ErrorIs(t, err, errSortTooLarge)
		assert.Nil(t, st.arena, "Close releases the arena")
	})
}

// The compaction guard does not double liveBytes, which overflows an int from
// half its range on.
func TestSortStage_NeedsCompactDoesNotOverflow(t *testing.T) {
	assert.False(t, sortNeedsCompact(math.MaxInt/2+1+1<<20, math.MaxInt/2+1))
	assert.True(t, sortNeedsCompact(3<<16, 1<<16))
	assert.False(t, sortNeedsCompact(2<<16, 1<<16))
}

// An error from the source leaves the rows collected so far in place, and the
// next Next resumes collecting: a $facet feed pauses the chain with
// errFacetPause after every row it feeds.
func TestSortStage_ResumesAfterSourceError(t *testing.T) {
	spec := MustParsePipeline(`[{"$sort": {"v": 1}}]`)[0].(SortSpec)
	for _, topK := range []int{0, 2} {
		feed := &facetFeed{}
		st := &SortStage{Src: feed, Spec: spec, TopK: topK}
		ctx := newTestCtx()
		for _, row := range []string{`{"id":"a","v":3}`, `{"id":"b","v":1}`, `{"id":"c","v":2}`} {
			feed.row = anyenc.MustParseJson(row)
			_, err := st.Next(ctx)
			require.ErrorIs(t, err, errFacetPause)
		}
		feed.done = true
		var ids []string
		for {
			v, err := st.Next(ctx)
			require.NoError(t, err)
			if v == nil {
				break
			}
			ids = append(ids, v.GetString("id"))
		}
		want := []string{"b", "c", "a"}
		if topK > 0 {
			want = want[:topK]
		}
		assert.Equal(t, want, ids, "TopK %d", topK)
		st.Close()
	}
}
