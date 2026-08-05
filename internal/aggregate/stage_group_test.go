package aggregate

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runGroup runs the pipeline and returns emitted rows sorted by their JSON
// encoding (group emit order is unspecified — first-seen — so tests sort).
func runGroup(t *testing.T, src Stage, pipeline string, limits Limits) []string {
	t.Helper()
	got := runPipeline(t, src, pipeline, limits)
	sort.Strings(got)
	return got
}

func sortedJsonRows(t *testing.T, jsons ...string) []string {
	t.Helper()
	rows := jsonRows(t, jsons...)
	sort.Strings(rows)
	return rows
}

func TestGroupStage(t *testing.T) {
	t.Run("sum avg count by key", func(t *testing.T) {
		src := newSliceSource(
			`{"id":1,"cat":"a","v":1}`,
			`{"id":2,"cat":"b","v":10}`,
			`{"id":3,"cat":"a","v":2}`,
			`{"id":4,"cat":"b","v":20}`,
			`{"id":5,"cat":"a","v":3}`,
		)
		got := runGroup(t, src, `[{"$group": {"_id": "$cat",
			"total": {"$sum": "$v"}, "avg": {"$avg": "$v"}, "n": {"$count": {}}}}]`, Limits{})
		assert.Equal(t, sortedJsonRows(t,
			`{"id":"a","total":6,"avg":2,"n":3}`,
			`{"id":"b","total":30,"avg":15,"n":2}`,
		), got)
	})

	t.Run("missing key groups as null", func(t *testing.T) {
		src := newSliceSource(
			`{"id":1,"v":1}`,
			`{"id":2,"cat":null,"v":2}`,
			`{"id":3,"cat":"x","v":4}`,
		)
		got := runGroup(t, src, `[{"$group": {"_id": "$cat", "n": {"$count": {}}}}]`, Limits{})
		assert.Equal(t, sortedJsonRows(t,
			`{"id":null,"n":2}`,
			`{"id":"x","n":1}`,
		), got)
	})

	t.Run("constant key single group", func(t *testing.T) {
		src := newSliceSource(`{"id":1,"v":1}`, `{"id":2,"v":2}`)
		got := runGroup(t, src, `[{"$group": {"_id": null, "s": {"$sum": "$v"}}}]`, Limits{})
		assert.Equal(t, sortedJsonRows(t, `{"id":null,"s":3}`), got)
	})

	t.Run("compound key", func(t *testing.T) {
		src := newSliceSource(
			`{"id":1,"a":1,"b":"x"}`,
			`{"id":2,"a":1,"b":"x"}`,
			`{"id":3,"a":1,"b":"y"}`,
		)
		got := runGroup(t, src, `[{"$group": {"_id": {"a": "$a", "b": "$b"}, "n": {"$count": {}}}}]`, Limits{})
		assert.Equal(t, sortedJsonRows(t,
			`{"id":{"a":1,"b":"x"},"n":2}`,
			`{"id":{"a":1,"b":"y"},"n":1}`,
		), got)
	})

	t.Run("non-numeric sum avg inputs skipped", func(t *testing.T) {
		src := newSliceSource(
			`{"id":1,"v":1}`,
			`{"id":2,"v":"str"}`,
			`{"id":3}`,
			`{"id":4,"v":3}`,
		)
		got := runGroup(t, src, `[{"$group": {"_id": null, "s": {"$sum": "$v"}, "a": {"$avg": "$v"}}}]`, Limits{})
		assert.Equal(t, sortedJsonRows(t, `{"id":null,"s":4,"a":2}`), got)
	})

	t.Run("empty numeric set: sum 0, avg null, min null", func(t *testing.T) {
		src := newSliceSource(`{"id":1,"v":"s"}`)
		got := runGroup(t, src, `[{"$group": {"_id": null,
			"s": {"$sum": "$v"}, "a": {"$avg": "$v"}, "m": {"$min": "$nope"}}}]`, Limits{})
		assert.Equal(t, sortedJsonRows(t, `{"id":null,"s":0,"a":null,"m":null}`), got)
	})

	t.Run("min max ignore null and missing", func(t *testing.T) {
		src := newSliceSource(
			`{"id":1,"v":5}`,
			`{"id":2,"v":null}`,
			`{"id":3}`,
			`{"id":4,"v":1}`,
			`{"id":5,"v":"str"}`,
		)
		// anyenc order: numbers < strings, so max is the string.
		got := runGroup(t, src, `[{"$group": {"_id": null, "lo": {"$min": "$v"}, "hi": {"$max": "$v"}}}]`, Limits{})
		assert.Equal(t, sortedJsonRows(t, `{"id":null,"lo":1,"hi":"str"}`), got)
	})

	t.Run("first and last", func(t *testing.T) {
		src := newSliceSource(
			`{"id":1,"v":"a"}`,
			`{"id":2,"v":"b"}`,
			`{"id":3,"v":"c"}`,
		)
		got := runGroup(t, src, `[{"$group": {"_id": null, "f": {"$first": "$v"}, "l": {"$last": "$v"}}}]`, Limits{})
		assert.Equal(t, sortedJsonRows(t, `{"id":null,"f":"a","l":"c"}`), got)
	})

	t.Run("first of missing omits field", func(t *testing.T) {
		src := newSliceSource(`{"id":1}`, `{"id":2,"v":"b"}`)
		got := runGroup(t, src, `[{"$group": {"_id": null, "f": {"$first": "$v"}, "l": {"$last": "$v"}}}]`, Limits{})
		assert.Equal(t, sortedJsonRows(t, `{"id":null,"l":"b"}`), got)
	})

	t.Run("push keeps null skips missing", func(t *testing.T) {
		src := newSliceSource(
			`{"id":1,"v":1}`,
			`{"id":2}`,
			`{"id":3,"v":null}`,
			`{"id":4,"v":{"o":1}}`,
		)
		got := runGroup(t, src, `[{"$group": {"_id": null, "all": {"$push": "$v"}}}]`, Limits{})
		assert.Equal(t, sortedJsonRows(t, `{"id":null,"all":[1,null,{"o":1}]}`), got)
	})

	t.Run("addToSet dedups across types", func(t *testing.T) {
		src := newSliceSource(
			`{"id":1,"v":1}`,
			`{"id":2,"v":"1"}`,
			`{"id":3,"v":1}`,
			`{"id":4,"v":{"a":1}}`,
			`{"id":5,"v":{"a":1}}`,
		)
		got := runGroup(t, src, `[{"$group": {"_id": null, "set": {"$addToSet": "$v"}}}]`, Limits{})
		assert.Equal(t, sortedJsonRows(t, `{"id":null,"set":[1,"1",{"a":1}]}`), got)
	})

	t.Run("group after unwind", func(t *testing.T) {
		src := newSliceSource(
			`{"id":1,"tags":["x","y"]}`,
			`{"id":2,"tags":["x"]}`,
		)
		got := runGroup(t, src, `[{"$unwind": "$tags"}, {"$group": {"_id": "$tags", "n": {"$count": {}}}}]`, Limits{})
		assert.Equal(t, sortedJsonRows(t,
			`{"id":"x","n":2}`,
			`{"id":"y","n":1}`,
		), got)
	})

	t.Run("project after group", func(t *testing.T) {
		src := newSliceSource(`{"id":1,"cat":"a","v":2}`, `{"id":2,"cat":"a","v":3}`)
		got := runGroup(t, src, `[
			{"$group": {"_id": "$cat", "s": {"$sum": "$v"}}},
			{"$project": {"key": "$id", "s": 1}}
		]`, Limits{})
		assert.Equal(t, sortedJsonRows(t, `{"key":"a","s":5}`), got)
	})

	t.Run("empty input emits nothing", func(t *testing.T) {
		src := newSliceSource()
		got := runGroup(t, src, `[{"$group": {"_id": "$cat", "n": {"$count": {}}}}]`, Limits{})
		assert.Empty(t, got)
	})
}

func TestGroupLimits(t *testing.T) {
	t.Run("group limit", func(t *testing.T) {
		src := newSliceSource(`{"id":1,"v":1}`, `{"id":2,"v":2}`, `{"id":3,"v":3}`)
		specs := MustParsePipeline(`[{"$group": {"_id": "$v"}}]`)
		root, err := Build(src, specs, Limits{MaxGroups: 2}, Env{})
		require.NoError(t, err)
		defer root.Close()
		_, err = root.Next(newTestCtx())
		assert.ErrorIs(t, err, ErrGroupLimitExceeded)
	})

	t.Run("accumulator array limit", func(t *testing.T) {
		src := newSliceSource(`{"id":1,"v":1}`, `{"id":2,"v":2}`, `{"id":3,"v":3}`)
		specs := MustParsePipeline(`[{"$group": {"_id": null, "all": {"$push": "$v"}}}]`)
		root, err := Build(src, specs, Limits{MaxAccumArrayLen: 2}, Env{})
		require.NoError(t, err)
		defer root.Close()
		_, err = root.Next(newTestCtx())
		assert.ErrorIs(t, err, ErrAccumArrayLimitExceeded)
	})

	t.Run("memory limit", func(t *testing.T) {
		src := newSliceSource(`{"id":1,"v":1}`, `{"id":2,"v":2}`, `{"id":3,"v":3}`)
		specs := MustParsePipeline(`[{"$group": {"_id": "$v"}}]`)
		root, err := Build(src, specs, Limits{}, Env{})
		require.NoError(t, err)
		defer root.Close()
		ctx := newTestCtx()
		ctx.Mem = NewMemAccount(100) // fits the first group only
		_, err = root.Next(ctx)
		assert.ErrorIs(t, err, ErrMemoryLimitExceeded)
	})

	t.Run("unlimited with negative limits", func(t *testing.T) {
		src := newSliceSource(`{"id":1,"v":1}`, `{"id":2,"v":2}`)
		specs := MustParsePipeline(`[{"$group": {"_id": "$v"}}]`)
		root, err := Build(src, specs, Limits{MaxGroups: -1}, Env{})
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
		assert.Equal(t, 2, n)
	})
}

func TestGroupSteadyStateAllocs(t *testing.T) {
	if testing.Short() {
		t.Skip("benchmark-backed test")
	}
	// With a fixed set of groups, the drain loop must not allocate per row.
	rows := make([][]byte, 0, 8)
	for _, j := range []string{
		`{"id":1,"cat":"a","v":1}`, `{"id":2,"cat":"b","v":2}`,
		`{"id":3,"cat":"c","v":3}`, `{"id":4,"cat":"d","v":4}`,
	} {
		rows = append(rows, mustMarshalJson(j))
	}
	src := &cyclingSource{rows: rows}
	stage := newGroupStage(src, MustParsePipeline(`[{"$group": {"_id": "$cat",
		"s": {"$sum": "$v"}, "m": {"$max": "$v"}, "n": {"$count": {}}}}]`)[0].(GroupSpec), Limits{}.WithDefaults())
	ctx := newTestCtx()

	// Warm up: create all groups, grow buffers.
	for i := 0; i < 1000; i++ {
		if _, err := stage.stepOne(ctx); err != nil {
			t.Fatal(err)
		}
	}
	res := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := stage.stepOne(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})
	assert.Zero(t, res.AllocsPerOp(), "$group drain must be alloc-free for existing groups")
}
