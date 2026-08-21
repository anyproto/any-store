package aggregate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/syncpool"
)

// sliceSource mimics the real source stage: it yields transient rows from a
// reusable parser (the previous row's memory is invalidated by the next
// Parse, like the production DocBuffer parser) and resets RowArena once per
// row, acting as the default row owner.
type sliceSource struct {
	rows [][]byte
	idx  int
	p    anyenc.Parser
}

func newSliceSource(jsons ...string) *sliceSource {
	s := &sliceSource{}
	for _, j := range jsons {
		s.rows = append(s.rows, anyenc.MustParseJson(j).MarshalTo(nil))
	}
	return s
}

func (s *sliceSource) Next(ctx *Ctx) (*anyenc.Value, error) {
	ctx.RowArena.Reset()
	if s.idx >= len(s.rows) {
		return nil, nil
	}
	v, err := s.p.Parse(s.rows[s.idx])
	s.idx++
	return v, err
}

func (s *sliceSource) Close() {}

func (s *sliceSource) String() string { return "source" }

func newTestCtx() *Ctx {
	return &Ctx{
		Context:  context.Background(),
		RowArena: &anyenc.Arena{},
		Buf:      syncpool.NewSyncPool(1 << 20).GetDocBuf(),
		Mem:      NewMemAccount(DefaultMemoryLimit),
	}
}

// runPipeline builds and drains a pipeline, returning each emitted row
// re-encoded as a JSON string (copied out before the next pull).
func runPipeline(t *testing.T, src Stage, pipeline string, limits Limits) []string {
	t.Helper()
	specs, err := ParsePipeline(pipeline)
	require.NoError(t, err)
	root, err := Build(src, specs, limits, Env{})
	require.NoError(t, err)
	defer root.Close()
	ctx := newTestCtx()
	var res []string
	for {
		v, err := root.Next(ctx)
		require.NoError(t, err)
		if v == nil {
			break
		}
		res = append(res, v.String())
	}
	return res
}

func mustMarshalJson(j string) []byte {
	return anyenc.MustParseJson(j).MarshalTo(nil)
}

func jsonRows(t *testing.T, jsons ...string) []string {
	t.Helper()
	res := make([]string, len(jsons))
	for i, j := range jsons {
		res[i] = anyenc.MustParseJson(j).String()
	}
	return res
}

func TestMatchStage(t *testing.T) {
	src := newSliceSource(`{"id":1,"a":1}`, `{"id":2,"a":5}`, `{"id":3,"a":10}`)
	got := runPipeline(t, src, `[{"$match": {"a": {"$gte": 5}}}]`, Limits{})
	assert.Equal(t, jsonRows(t, `{"id":2,"a":5}`, `{"id":3,"a":10}`), got)
}

func TestMatchExprStage(t *testing.T) {
	t.Run("field-to-field compare", func(t *testing.T) {
		src := newSliceSource(
			`{"id":1,"allocated":5,"capacity":10}`,
			`{"id":2,"allocated":15,"capacity":10}`,
			`{"id":3,"allocated":10,"capacity":10}`,
		)
		got := runPipeline(t, src, `[{"$match": {"$expr": {"$gt": ["$allocated", "$capacity"]}}}]`, Limits{})
		assert.Equal(t, jsonRows(t, `{"id":2,"allocated":15,"capacity":10}`), got)
	})
	t.Run("truthiness", func(t *testing.T) {
		// $cond truthiness: false, 0, null, missing → drop; everything else —
		// including "", [] and {} — keeps the row.
		src := newSliceSource(
			`{"id":1,"f":false}`,
			`{"id":2,"f":0}`,
			`{"id":3,"f":null}`,
			`{"id":4}`,
			`{"id":5,"f":""}`,
			`{"id":6,"f":[]}`,
			`{"id":7,"f":{}}`,
			`{"id":8,"f":1}`,
		)
		got := runPipeline(t, src, `[{"$match": {"$expr": "$f"}}]`, Limits{})
		assert.Equal(t, jsonRows(t,
			`{"id":5,"f":""}`,
			`{"id":6,"f":[]}`,
			`{"id":7,"f":{}}`,
			`{"id":8,"f":1}`,
		), got)
	})
	t.Run("mixed filter and expr", func(t *testing.T) {
		src := newSliceSource(
			`{"id":1,"cat":"a","x":2,"y":1}`,
			`{"id":2,"cat":"b","x":2,"y":1}`,
			`{"id":3,"cat":"a","x":1,"y":2}`,
		)
		got := runPipeline(t, src, `[{"$match": {"cat": "a", "$expr": {"$gt": ["$x", "$y"]}}}]`, Limits{})
		assert.Equal(t, jsonRows(t, `{"id":1,"cat":"a","x":2,"y":1}`), got)
	})
	t.Run("multiple exprs are conjoined", func(t *testing.T) {
		src := newSliceSource(
			`{"id":1,"x":5}`,
			`{"id":2,"x":15}`,
			`{"id":3,"x":25}`,
		)
		got := runPipeline(t, src, `[{"$match": {"$and": [
			{"$expr": {"$gt": ["$x", 10]}},
			{"$expr": {"$lt": ["$x", 20]}}
		]}}]`, Limits{})
		assert.Equal(t, jsonRows(t, `{"id":2,"x":15}`), got)
	})
	t.Run("collected array compares whole", func(t *testing.T) {
		// Implicit traversal yields the collected array; $eq compares it
		// elementwise as one value.
		src := newSliceSource(
			`{"id":1,"a":[{"b":1},{"b":2}]}`,
			`{"id":2,"a":[{"b":1},{"b":3}]}`,
			`{"id":3,"a":[]}`,
		)
		got := runPipeline(t, src, `[{"$match": {"$expr": {"$eq": ["$a.b", {"$literal": [1, 2]}]}}}]`, Limits{})
		assert.Equal(t, jsonRows(t, `{"id":1,"a":[{"b":1},{"b":2}]}`), got)
	})
	t.Run("after group referencing accumulator outputs", func(t *testing.T) {
		src := newSliceSource(
			`{"id":1,"cat":"a","v":10}`,
			`{"id":2,"cat":"b","v":40}`,
			`{"id":3,"cat":"a","v":30}`,
			`{"id":4,"cat":"b","v":40}`,
		)
		got := runPipeline(t, src, `[
			{"$group": {"_id": "$cat", "total": {"$sum": "$v"}, "n": {"$count": {}}}},
			{"$match": {"$expr": {"$gt": ["$total", {"$multiply": ["$n", 25]}]}}}
		]`, Limits{})
		assert.Equal(t, jsonRows(t, `{"id":"b","total":80,"n":2}`), got)
	})
	t.Run("after sort with computing expr", func(t *testing.T) {
		// The in-pipeline sort emits parser-owned rows and never resets the
		// arena; the residual match's resetArena keeps eval temporaries O(row).
		src := newSliceSource(
			`{"id":1,"x":3,"y":1}`,
			`{"id":2,"x":1,"y":1}`,
			`{"id":3,"x":9,"y":1}`,
		)
		got := runPipeline(t, src, `[
			{"$sort": {"x": -1}},
			{"$match": {"$expr": {"$gt": [{"$add": ["$x", "$y"]}, 3]}}}
		]`, Limits{})
		assert.Equal(t, jsonRows(t, `{"id":3,"x":9,"y":1}`, `{"id":1,"x":3,"y":1}`), got)
	})
}

func TestProjectStage(t *testing.T) {
	t.Run("include, rename, literal, missing omitted", func(t *testing.T) {
		src := newSliceSource(`{"id":1,"a":{"b":7},"c":"x"}`)
		got := runPipeline(t, src, `[{"$project": {"c": 1, "b": "$a.b", "lit": "v", "nope": "$zzz"}}]`, Limits{})
		assert.Equal(t, jsonRows(t, `{"c":"x","b":7,"lit":"v"}`), got)
	})
	t.Run("document expression", func(t *testing.T) {
		src := newSliceSource(`{"id":1,"x":1,"y":2}`)
		got := runPipeline(t, src, `[{"$project": {"pt": {"a": "$x", "b": "$y"}}}]`, Limits{})
		assert.Equal(t, jsonRows(t, `{"pt":{"a":1,"b":2}}`), got)
	})
	t.Run("compute operators", func(t *testing.T) {
		src := newSliceSource(`{"id":1,"a":2,"b":3,"s":"x"}`, `{"id":2,"b":3,"s":"y"}`)
		got := runPipeline(t, src, `[{"$project": {
			"n": {"$add": ["$a", {"$multiply": ["$b", 2]}, 1]},
			"s2": {"$concat": ["$s", "!"]}}}]`, Limits{})
		// Missing "a" nulls the whole $add, Mongo null-propagation.
		assert.Equal(t, jsonRows(t, `{"n":9,"s2":"x!"}`, `{"n":null,"s2":"y!"}`), got)
	})
	t.Run("implicit array traversal", func(t *testing.T) {
		src := newSliceSource(
			`{"id":1,"a":[{"b":1},{"b":2},{"c":3}]}`,
			`{"id":2,"a":[]}`,
			`{"id":3,"a":5}`,
		)
		got := runPipeline(t, src, `[{"$project": {"p": "$a.b"}}]`, Limits{})
		// Collected array for docs, [] for empty/none, missing for a scalar.
		assert.Equal(t, jsonRows(t, `{"p":[1,2]}`, `{"p":[]}`, `{}`), got)
	})
}

func TestAddFieldsStage(t *testing.T) {
	t.Run("overlay and overwrite", func(t *testing.T) {
		src := newSliceSource(`{"id":1,"a":1}`)
		got := runPipeline(t, src, `[{"$addFields": {"b": "$a", "a": 100}}]`, Limits{})
		assert.Equal(t, jsonRows(t, `{"id":1,"a":100,"b":1}`), got)
	})
	t.Run("missing result removes field", func(t *testing.T) {
		src := newSliceSource(`{"id":1,"a":1}`)
		got := runPipeline(t, src, `[{"$addFields": {"a": "$zzz"}}]`, Limits{})
		assert.Equal(t, jsonRows(t, `{"id":1}`), got)
	})
}

func TestUnwindStage(t *testing.T) {
	t.Run("default drops missing, null, empty", func(t *testing.T) {
		src := newSliceSource(
			`{"id":1,"t":["a","b"]}`,
			`{"id":2}`,
			`{"id":3,"t":null}`,
			`{"id":4,"t":[]}`,
			`{"id":5,"t":"scalar"}`,
		)
		got := runPipeline(t, src, `[{"$unwind": "$t"}]`, Limits{})
		assert.Equal(t, jsonRows(t,
			`{"id":1,"t":"a"}`,
			`{"id":1,"t":"b"}`,
			`{"id":5,"t":"scalar"}`,
		), got)
	})
	t.Run("preserveNullAndEmptyArrays", func(t *testing.T) {
		src := newSliceSource(
			`{"id":1,"t":["a"]}`,
			`{"id":2}`,
			`{"id":3,"t":null}`,
			`{"id":4,"t":[]}`,
		)
		got := runPipeline(t, src, `[{"$unwind": {"path": "$t", "preserveNullAndEmptyArrays": true}}]`, Limits{})
		assert.Equal(t, jsonRows(t,
			`{"id":1,"t":"a"}`,
			`{"id":2}`,
			`{"id":3,"t":null}`,
			`{"id":4}`, // empty array: field removed, Mongo semantics
		), got)
	})
	t.Run("nested path", func(t *testing.T) {
		src := newSliceSource(`{"id":1,"a":{"b":[1,2]},"k":"x"}`)
		got := runPipeline(t, src, `[{"$unwind": "$a.b"}]`, Limits{})
		assert.Equal(t, jsonRows(t,
			`{"id":1,"a":{"b":1},"k":"x"}`,
			`{"id":1,"a":{"b":2},"k":"x"}`,
		), got)
	})
	t.Run("unwind then project", func(t *testing.T) {
		src := newSliceSource(`{"id":1,"t":[1,2,3]}`)
		got := runPipeline(t, src, `[{"$unwind": "$t"}, {"$project": {"t": 1}}]`, Limits{})
		assert.Equal(t, jsonRows(t, `{"t":1}`, `{"t":2}`, `{"t":3}`), got)
	})
}

func TestSkipLimitCountStages(t *testing.T) {
	t.Run("skip and limit", func(t *testing.T) {
		src := newSliceSource(`{"id":1}`, `{"id":2}`, `{"id":3}`, `{"id":4}`)
		got := runPipeline(t, src, `[{"$skip": 1}, {"$limit": 2}]`, Limits{})
		assert.Equal(t, jsonRows(t, `{"id":2}`, `{"id":3}`), got)
	})
	t.Run("count", func(t *testing.T) {
		src := newSliceSource(`{"id":1}`, `{"id":2}`, `{"id":3}`)
		got := runPipeline(t, src, `[{"$match": {"id": {"$gte": 2}}}, {"$count": "n"}]`, Limits{})
		assert.Equal(t, jsonRows(t, `{"n":2}`), got)
	})
	t.Run("count of empty stream", func(t *testing.T) {
		src := newSliceSource()
		got := runPipeline(t, src, `[{"$count": "n"}]`, Limits{})
		assert.Equal(t, jsonRows(t, `{"n":0}`), got)
	})
}

func TestCountStageCancellation(t *testing.T) {
	// An endless upstream must be interrupted by context cancellation.
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	ctx := newTestCtx()
	ctx.Context = cctx

	root, err := Build(&endlessSource{}, MustParsePipeline(`[{"$count": "n"}]`), Limits{}, Env{})
	require.NoError(t, err)
	defer root.Close()
	_, err = root.Next(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

type endlessSource struct {
	p   anyenc.Parser
	raw []byte
}

func (s *endlessSource) Next(ctx *Ctx) (*anyenc.Value, error) {
	ctx.RowArena.Reset()
	if s.raw == nil {
		s.raw = anyenc.MustParseJson(`{"id":1}`).MarshalTo(nil)
	}
	return s.p.Parse(s.raw)
}

func (s *endlessSource) Close() {}

func (s *endlessSource) String() string { return "endless" }

// cyclingSource yields rows forever, cycling over its fixture set; used for
// steady-state allocation measurement.
type cyclingSource struct {
	rows [][]byte
	idx  int
	p    anyenc.Parser
}

func (s *cyclingSource) Next(ctx *Ctx) (*anyenc.Value, error) {
	ctx.RowArena.Reset()
	v, err := s.p.Parse(s.rows[s.idx%len(s.rows)])
	s.idx++
	return v, err
}

func (s *cyclingSource) Close() {}

func (s *cyclingSource) String() string { return "cycling" }

func TestStreamingStagesAllocFree(t *testing.T) {
	if testing.Short() {
		t.Skip("benchmark-backed test")
	}
	src := &cyclingSource{rows: [][]byte{
		anyenc.MustParseJson(`{"id":1,"a":5,"t":["x","y","z"],"o":{"n":1}}`).MarshalTo(nil),
		anyenc.MustParseJson(`{"id":2,"a":50,"t":["q"],"o":{"n":2}}`).MarshalTo(nil),
	}}
	root, err := Build(src, MustParsePipeline(`[
		{"$match": {"a": {"$gte": 5}}},
		{"$addFields": {"n2": "$o.n"}},
		{"$unwind": "$t"},
		{"$project": {"t": 1, "n2": 1, "pt": {"v": "$a"}}}
	]`), Limits{}, Env{})
	require.NoError(t, err)
	defer root.Close()
	ctx := newTestCtx()

	// Warm up arena/parser caches.
	for i := 0; i < 1000; i++ {
		if _, err := root.Next(ctx); err != nil {
			t.Fatal(err)
		}
	}
	res := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			v, err := root.Next(ctx)
			if err != nil || v == nil {
				b.Fatal(v, err)
			}
		}
	})
	assert.Zero(t, res.AllocsPerOp(), "streaming stages must be alloc-free in steady state")
}

// exprMatchFixture is a realistic doc stream for $expr $match measurement:
// one row matches the field-to-field predicate, one is discarded.
func exprMatchFixture() *cyclingSource {
	return &cyclingSource{rows: [][]byte{
		anyenc.MustParseJson(`{"id":1,"name":"pool-a","allocated":150,"capacity":100,"tags":["x","y"],"meta":{"region":"eu","tier":2}}`).MarshalTo(nil),
		anyenc.MustParseJson(`{"id":2,"name":"pool-b","allocated":40,"capacity":100,"tags":["z"],"meta":{"region":"us","tier":1}}`).MarshalTo(nil),
	}}
}

func TestMatchExprAllocFree(t *testing.T) {
	if testing.Short() {
		t.Skip("benchmark-backed test")
	}
	root, err := Build(exprMatchFixture(), MustParsePipeline(`[
		{"$match": {"$expr": {"$gt": ["$allocated", "$capacity"]}}}
	]`), Limits{}, Env{})
	require.NoError(t, err)
	defer root.Close()
	ctx := newTestCtx()

	for i := 0; i < 1000; i++ {
		if _, err := root.Next(ctx); err != nil {
			t.Fatal(err)
		}
	}
	res := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			v, err := root.Next(ctx)
			if err != nil || v == nil {
				b.Fatal(v, err)
			}
		}
	})
	assert.Zero(t, res.AllocsPerOp(), "$expr $match must be alloc-free in steady state")
}

func BenchmarkMatchExprStage(b *testing.B) {
	root, err := Build(exprMatchFixture(), MustParsePipeline(`[
		{"$match": {"$expr": {"$gt": ["$allocated", "$capacity"]}}}
	]`), Limits{}, Env{})
	if err != nil {
		b.Fatal(err)
	}
	defer root.Close()
	ctx := newTestCtx()
	b.ReportAllocs()
	for b.Loop() {
		v, err := root.Next(ctx)
		if err != nil || v == nil {
			b.Fatal(v, err)
		}
	}
}
