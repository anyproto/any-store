package aggregate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

func TestFacetStage(t *testing.T) {
	rows := []string{
		`{"id":1,"cat":"a","v":10,"tags":["x","y"]}`,
		`{"id":2,"cat":"b","v":20,"tags":["x"]}`,
		`{"id":3,"cat":"a","v":30,"tags":[]}`,
		`{"id":4,"cat":"b","v":40}`,
	}
	t.Run("dashboard fan-out", func(t *testing.T) {
		got := runPipeline(t, newSliceSource(rows...), `[{"$facet": {
			"count": [{"$count": "n"}],
			"sums": [{"$group": {"_id": "$cat", "total": {"$sum": "$v"}}}],
			"top2": [{"$sort": {"v": -1}}, {"$limit": 2}, {"$project": {"v": 1}}],
			"big": [{"$match": {"v": {"$gte": 30}}}, {"$count": "n"}]
		}}]`, Limits{})
		assert.Equal(t, jsonRows(t, `{
			"count": [{"n":4}],
			"sums": [{"id":"a","total":40},{"id":"b","total":60}],
			"top2": [{"v":40},{"v":30}],
			"big": [{"n":2}]
		}`), got)
	})
	t.Run("empty input", func(t *testing.T) {
		got := runPipeline(t, newSliceSource(), `[{"$facet": {
			"count": [{"$count": "n"}],
			"docs": [{"$match": {}}]
		}}]`, Limits{})
		assert.Equal(t, jsonRows(t, `{"count": [{"n":0}], "docs": []}`), got)
	})
	t.Run("stages after facet", func(t *testing.T) {
		got := runPipeline(t, newSliceSource(rows...), `[
			{"$facet": {"count": [{"$count": "n"}], "top": [{"$sort": {"v": -1}}, {"$limit": 1}]}},
			{"$unwind": "$count"},
			{"$unwind": "$top"},
			{"$project": {"best": "$top.v", "n": "$count.n"}}
		]`, Limits{})
		assert.Equal(t, jsonRows(t, `{"best":40,"n":4}`), got)
	})
	t.Run("mutating facet does not leak into siblings", func(t *testing.T) {
		// "mut" overlays/unwinds its (private) rows; "raw", driven after it
		// each round, must see the pristine shared documents.
		got := runPipeline(t, newSliceSource(rows...), `[{"$facet": {
			"mut": [{"$addFields": {"z": 1}}, {"$unwind": "$tags"}, {"$count": "n"}],
			"raw": [{"$project": {"id": 1, "tags": 1}}]
		}}]`, Limits{})
		assert.Equal(t, jsonRows(t, `{
			"mut": [{"n":3}],
			"raw": [{"id":1,"tags":["x","y"]},{"id":2,"tags":["x"]},{"id":3,"tags":[]},{"id":4}]
		}`), got)
	})
	t.Run("abandoned undo stays facet-local", func(t *testing.T) {
		// $limit finishes "mut" before $addFields' overlay of row 1 is undone;
		// the pollution must stay in mut's private row copy.
		got := runPipeline(t, newSliceSource(rows...), `[{"$facet": {
			"mut": [{"$addFields": {"z": 1}}, {"$limit": 1}],
			"raw": [{"$project": {"id": 1, "z": {"$ifNull": ["$z", "none"]}}}]
		}}]`, Limits{})
		assert.Equal(t, jsonRows(t, `{
			"mut": [{"id":1,"cat":"a","v":10,"tags":["x","y"],"z":1}],
			"raw": [{"id":1,"z":"none"},{"id":2,"z":"none"},{"id":3,"z":"none"},{"id":4,"z":"none"}]
		}`), got)
	})
	t.Run("all facets limited stops the scan", func(t *testing.T) {
		src := newSliceSource(rows...)
		got := runPipeline(t, src, `[{"$facet": {
			"a": [{"$limit": 1}],
			"b": [{"$limit": 2}, {"$project": {"id": 1}}]
		}}]`, Limits{})
		assert.Equal(t, jsonRows(t, `{
			"a": [{"id":1,"cat":"a","v":10,"tags":["x","y"]}],
			"b": [{"id":1},{"id":2}]
		}`), got)
		assert.Less(t, src.idx, len(rows), "source scan must stop once every facet finished")
	})
	t.Run("facet after blocking stage", func(t *testing.T) {
		got := runPipeline(t, newSliceSource(rows...), `[
			{"$group": {"_id": "$cat", "total": {"$sum": "$v"}}},
			{"$facet": {
				"n": [{"$count": "n"}],
				"grand": [{"$group": {"_id": null, "sum": {"$sum": "$total"}}}]
			}}
		]`, Limits{})
		assert.Equal(t, jsonRows(t, `{"n": [{"n":2}], "grand": [{"id":null,"sum":100}]}`), got)
	})
}

func TestFacetStageMemoryLimit(t *testing.T) {
	// Facet result buffers are inherently retained: they must count against
	// the shared memory budget and fail with the same structured error the
	// other blocking stages return.
	specs := MustParsePipeline(`[{"$facet": {"all": [{"$match": {}}]}}]`)
	root, err := Build(newSliceSource(
		`{"id":1,"pad":"0123456789012345678901234567890123456789"}`,
		`{"id":2,"pad":"0123456789012345678901234567890123456789"}`,
		`{"id":3,"pad":"0123456789012345678901234567890123456789"}`,
	), specs, Limits{}, Env{})
	require.NoError(t, err)
	defer root.Close()
	ctx := newTestCtx()
	ctx.Mem = NewMemAccount(64)
	_, err = root.Next(ctx)
	assert.ErrorIs(t, err, ErrMemoryLimitExceeded)

	// The failure is latched: a caller ignoring the error and pulling again
	// must get the same error back, never a resumed half-fed scan.
	_, err2 := root.Next(ctx)
	assert.Equal(t, err, err2)
}

// TestFacetStreamingAllocBound pins the fan-out's per-row cost: for
// streaming-only facets the whole run allocates only setup state and
// amortized result-buffer growth — no per-row allocations.
func TestFacetStreamingAllocBound(t *testing.T) {
	if testing.Short() {
		t.Skip("benchmark-backed test")
	}
	const nRows = 10_000
	row := anyenc.MustParseJson(`{"id":1,"cat":"a","v":10,"tags":["x","y"]}`).MarshalTo(nil)
	rows := make([][]byte, nRows)
	for i := range rows {
		rows[i] = row
	}
	specs := MustParsePipeline(`[{"$facet": {
		"big": [{"$match": {"v": {"$gte": 5}}}, {"$count": "n"}],
		"proj": [{"$project": {"v": 1}}, {"$limit": 100}],
		"mut": [{"$addFields": {"z": 1}}, {"$count": "n"}]
	}}]`)
	ctx := newTestCtx()
	allocs := testing.AllocsPerRun(5, func() {
		root, err := Build(&sliceSource{rows: rows}, specs, Limits{}, Env{})
		if err != nil {
			t.Fatal(err)
		}
		defer root.Close()
		for {
			v, nerr := root.Next(ctx)
			if nerr != nil {
				t.Fatal(nerr)
			}
			if v == nil {
				break
			}
		}
	})
	// Setup (stages, per-facet buffers) plus logarithmic buffer growth; a
	// per-row allocation would cost >= nRows.
	t.Logf("allocs per %d-row run: %.0f", nRows, allocs)
	assert.Less(t, allocs, float64(500), "facet fan-out must not allocate per row")
}
