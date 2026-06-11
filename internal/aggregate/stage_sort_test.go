package aggregate

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		root, err := Build(newSliceSource(rows...), specs, Limits{})
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
		root, err := Build(newSliceSource(rows...), specs, Limits{})
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
