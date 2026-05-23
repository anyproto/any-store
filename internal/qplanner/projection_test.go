package qplanner

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
)

// --- field-root extraction unit tests -------------------------------------

func TestFilterFieldRoots(t *testing.T) {
	cond := func(s string) query.Filter { return query.MustParseCondition(s) }

	cases := []struct {
		name   string
		filter query.Filter
		want   []string
		ok     bool
	}{
		{"nil", nil, nil, true},
		{"all", query.All{}, nil, true},
		{"eq", cond(`{"a":50}`), []string{"a"}, true},
		{"range", cond(`{"a":{"$gte":40,"$lte":60}}`), []string{"a"}, true},
		{"ne", cond(`{"a":{"$ne":50}}`), []string{"a"}, true},
		{"two_fields_comma", cond(`{"a":50,"b":25}`), []string{"a", "b"}, true},
		{"and", cond(`{"$and":[{"a":{"$gt":50}},{"b":10}]}`), []string{"a", "b"}, true},
		{"complex_and_or", cond(`{"$and":[{"a":{"$gt":50}},{"$or":[{"b":10},{"c":1}]}]}`),
			[]string{"a", "b", "c"}, true},
		{"or", cond(`{"$or":[{"a":1},{"b":2}]}`), []string{"a", "b"}, true},
		{"nor", cond(`{"$nor":[{"a":1},{"b":2}]}`), []string{"a", "b"}, true},
		{"not_via_nin", cond(`{"a":{"$nin":[1,2,3]}}`), []string{"a"}, true},
		{"exists", cond(`{"a":{"$exists":true}}`), []string{"a"}, true},
		{"exists_false", cond(`{"a":{"$exists":false}}`), []string{"a"}, true},
		{"in", cond(`{"a":{"$in":[1,2,3]}}`), []string{"a"}, true},
		{"nested_path_root", cond(`{"meta.x":5}`), []string{"meta"}, true},
		{"dup_field", cond(`{"$and":[{"a":{"$gt":1}},{"a":{"$lt":9}}]}`), []string{"a"}, true},
		{"not_with_op", cond(`{"a":{"$not":{"$gt":5}}}`), []string{"a"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := filterFieldRoots(nil, tc.filter)
			assert.Equal(t, tc.ok, ok)
			if tc.ok {
				assert.ElementsMatch(t, tc.want, got)
			}
		})
	}
}

func TestSortFieldRoots(t *testing.T) {
	got, ok := sortFieldRoots(nil, query.MustParseSort("val"))
	require.True(t, ok)
	assert.Equal(t, []string{"val"}, got)

	got, ok = sortFieldRoots(nil, query.MustParseSort("a", "-b"))
	require.True(t, ok)
	assert.Equal(t, []string{"a", "b"}, got)

	got, ok = sortFieldRoots(nil, query.MustParseSort("meta.x"))
	require.True(t, ok)
	assert.Equal(t, []string{"meta"}, got)

	got, ok = sortFieldRoots(nil, nil)
	require.True(t, ok)
	assert.Nil(t, got)
}

func TestScanProjection_Union(t *testing.T) {
	filter := query.MustParseCondition(`{"a":{"$gte":40,"$lte":60}}`)
	valFields := query.MustParseSort("val").Fields()
	aFields := query.MustParseSort("a").Fields()

	// Without sort fields: only filter roots.
	pf, ok := scanProjection(nil, filter, nil)
	require.True(t, ok)
	assert.ElementsMatch(t, []string{"a"}, pf)

	// With sort fields folded in: union.
	pf, ok = scanProjection(nil, filter, valFields)
	require.True(t, ok)
	assert.ElementsMatch(t, []string{"a", "val"}, pf)

	// Overlapping fields dedup.
	pf, ok = scanProjection(nil, query.MustParseCondition(`{"a":50}`), aFields)
	require.True(t, ok)
	assert.Equal(t, []string{"a"}, pf)

	// All-filter => nothing to project => not ok.
	_, ok = scanProjection(nil, query.All{}, nil)
	assert.False(t, ok)

	// Allocation-free when the result fits a caller-provided inline buffer.
	// This mirrors the benchmark Count path (filter only, no sorter), which is
	// the per-op-allocation-sensitive scenario.
	var buf [4]string
	pf, ok = scanProjection(buf[:0], filter, nil)
	require.True(t, ok)
	assert.ElementsMatch(t, []string{"a"}, pf)
	allocs := testing.AllocsPerRun(100, func() {
		_, _ = scanProjection(buf[:0], filter, nil)
	})
	assert.Zero(t, allocs, "filter projection into a fitting buffer must not allocate")

	// The complex AND/OR filter (multiple roots) must also stay allocation-free.
	complexF := query.MustParseCondition(`{"$and":[{"a":{"$gt":50}},{"$or":[{"b":10},{"c":1}]}]}`)
	allocs = testing.AllocsPerRun(100, func() {
		_, _ = scanProjection(buf[:0], complexF, nil)
	})
	assert.Zero(t, allocs, "complex filter projection into a fitting buffer must not allocate")

	// Folding pre-fetched sort fields into a fitting buffer is also alloc-free
	// (the caller supplies Fields(), so scanProjection itself allocates nothing).
	allocs = testing.AllocsPerRun(100, func() {
		_, _ = scanProjection(buf[:0], filter, valFields)
	})
	assert.Zero(t, allocs, "filter∪sort projection into a fitting buffer must not allocate")
}

// --- end-to-end iterator equivalence: projection ON vs OFF ----------------

// projTestDoc mirrors the benchmark buildDoc shape (multi-field, string + int
// arrays) so the equivalence check exercises the exact decode the optimization
// skips (the 80-element nums array).
func projTestDoc(a *anyenc.Arena, id int) *anyenc.Value {
	doc := a.NewObject()
	doc.Set("id", a.NewNumberInt(id))
	doc.Set("a", a.NewNumberInt(id%100))
	doc.Set("b", a.NewNumberInt((id/100)%50))
	doc.Set("c", a.NewNumberInt((id/5000)%10))
	doc.Set("val", a.NewNumberInt(id*7%1000))
	doc.Set("email", a.NewString(fmt.Sprintf("user%d@test.com", id)))
	tags := a.NewArray()
	tags.SetArrayItem(0, a.NewString(fmt.Sprintf("tag-%d", id%20)))
	tags.SetArrayItem(1, a.NewString(fmt.Sprintf("cat-%d", id%10)))
	doc.Set("tags", tags)
	nums := a.NewArray()
	for k := 0; k < 80; k++ {
		nums.SetArrayItem(k, a.NewNumberInt((id+k*7)%100))
	}
	doc.Set("nums", nums)
	return doc
}

// seedProjBtree writes n projTestDoc rows keyed by encoded integer id.
func seedProjBtree(t *testing.T, n int) (*btree.DB, *btree.Namespace) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "proj.db")
	db, err := btree.Open(path, btree.Options{PageSize: 4096, CacheSize: 1024, InMemory: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("data")
	require.NoError(t, err)
	a := &anyenc.Arena{}
	for i := 0; i < n; i++ {
		a.Reset()
		k := anyenc.AppendAnyValue(nil, i)
		require.NoError(t, wtx.Put(ns, k, projTestDoc(a, i).MarshalTo(nil)))
	}
	require.NoError(t, wtx.Commit())
	return db, ns
}

type collected struct {
	docId string
	doc   string // marshaled full doc bytes (as string for comparison)
}

// runFullScanCollect drives a FullScanIter (optionally projected) to completion
// and returns the emitted (docId, full-materialized-doc) pairs. It materializes
// the doc exactly like planIterator.Doc(): from Plan.DocParsed when present,
// else by a fresh fetch+parse. This is the contract the public iterator relies
// on, so equivalence here proves emit-path correctness.
func runFullScanCollect(t *testing.T, db *btree.DB, ns *btree.Namespace, filter query.Filter, project []string, emitFull bool) []collected {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	sp := syncpool.NewSyncPool(1 << 20)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	plan := &Plan{}
	data := &CursorSource{Tx: rtx, Ns: ns}
	it := &FullScanIter{
		Source:        data,
		Filter:        filter,
		Buf:           buf,
		Plan:          plan,
		ProjectFields: project,
		EmitFull:      emitFull,
	}
	defer it.Close()

	// Independent parser + cursor for emit-time materialization, so we don't
	// disturb the iterator's own buffer mid-iteration.
	var emitParser anyenc.Parser
	emitCur := data.NewCursor()
	defer emitCur.Close()

	var out []collected
	for {
		plan.DocParsed = nil
		_, docId, _, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		idCopy := string(docId)

		var doc *anyenc.Value
		if plan.DocParsed != nil {
			doc = plan.DocParsed
		} else {
			require.NoError(t, emitCur.SeekExact([]byte(idCopy)))
			val, verr := emitCur.Value()
			require.NoError(t, verr)
			doc, err = emitParser.ParseOwned(append([]byte(nil), val...))
			require.NoError(t, err)
		}
		out = append(out, collected{docId: idCopy, doc: string(doc.MarshalTo(nil))})
	}
	return out
}

// TestFullScanIter_ProjectionEquivalence is the core end-to-end guarantee:
// a filtered scan must return the SAME doc set AND the SAME full doc contents
// whether projection is on or off, across the benchmark filter shapes and both
// emit modes (EmitFull = direct-emit, and projected-cache = count/sort-above).
func TestFullScanIter_ProjectionEquivalence(t *testing.T) {
	// 12000 docs so the multi-field filters actually match rows:
	// b=(id/100)%50 reaches 25 at id>=2500; c=(id/5000)%10 reaches 1 at id>=5000.
	db, ns := seedProjBtree(t, 12000)

	filters := []struct {
		name string
		f    query.Filter
	}{
		{"eq", query.MustParseCondition(`{"a":50}`)},
		{"range", query.MustParseCondition(`{"a":{"$gte":40,"$lte":60}}`)},
		{"ne", query.MustParseCondition(`{"a":{"$ne":50}}`)},
		{"complex_and_or", query.MustParseCondition(`{"$and":[{"a":{"$gt":50}},{"$or":[{"b":10},{"c":1}]}]}`)},
		{"in", query.MustParseCondition(`{"a":{"$in":[10,30,50,70,90]}}`)},
		{"two_field", query.MustParseCondition(`{"a":50,"b":25}`)},
	}

	for _, fc := range filters {
		t.Run(fc.name, func(t *testing.T) {
			// Ground truth: projection OFF (full parse), direct emit.
			want := runFullScanCollect(t, db, ns, fc.f, nil, true)
			require.NotEmpty(t, want, "filter %s should match some docs", fc.name)

			project, ok := filterFieldRoots(nil, fc.f)
			require.True(t, ok, "filter %s must be statically projectable", fc.name)

			// Projection ON, EmitFull=true (direct emit re-parses full on match).
			gotEmitFull := runFullScanCollect(t, db, ns, fc.f, project, true)
			assert.Equal(t, want, gotEmitFull, "projection ON / EmitFull must match full-parse results")

			// Projection ON, EmitFull=false (count / sort-above: projected doc
			// cached). The set of matched docIds must be identical; the cached
			// doc is projected, so we only compare docIds here — emit-path
			// correctness for this mode is covered by the SortIter re-parse and
			// the public-API tests.
			gotProjected := runFullScanCollect(t, db, ns, fc.f, project, false)
			require.Equal(t, len(want), len(gotProjected),
				"projected-cache mode must match the same number of docs")
			for i := range want {
				assert.Equal(t, want[i].docId, gotProjected[i].docId,
					"projected-cache mode must match the same docId set/order")
			}
		})
	}
}

// TestSortIter_ProjectionEquivalence pins that an unindexed sort (the
// Sort/*NoIdx path) yields the SAME ordered docId sequence whether the sort
// key is extracted via projected or full parse.
func TestSortIter_ProjectionEquivalence(t *testing.T) {
	db, ns := seedProjBtree(t, 1000)

	for _, sortSpec := range []string{"val", "-val", "a", "-b"} {
		t.Run(sortSpec, func(t *testing.T) {
			sorter := query.MustParseSort(sortSpec)
			sf, ok := sortFieldRoots(nil, sorter)
			require.True(t, ok)

			collect := func(project []string) []string {
				rtx, err := db.BeginRead()
				require.NoError(t, err)
				defer func() { _ = rtx.Rollback() }()
				sp := syncpool.NewSyncPool(1 << 20)
				buf := sp.GetDocBuf()
				defer sp.ReleaseDocBuf(buf)

				plan := &Plan{}
				data := &CursorSource{Tx: rtx, Ns: ns}
				fs := &FullScanIter{Source: data, Buf: buf, Plan: plan}
				si := &SortIter{
					Source:        fs,
					Data:          data,
					Sorter:        sorter,
					Buf:           buf,
					Plan:          plan,
					TopK:          100,
					ProjectFields: project,
				}
				defer si.Close()
				var ids []string
				for {
					_, docId, _, err := si.Next()
					require.NoError(t, err)
					if docId == nil {
						break
					}
					ids = append(ids, string(docId))
				}
				return ids
			}

			want := collect(nil)    // full parse
			got := collect(sf)      // projected sort key
			require.NotEmpty(t, want)
			assert.Equal(t, want, got, "projected sort-key extraction must yield identical order")
		})
	}
}

// TestFullScanSortScanSideProjection covers the production no-filter sort path
// (Sort/*NoIdx): the FullScanIter itself projects the sort key from the cursor
// value and caches it (EmitFull=false), so the SortIter reuses it instead of
// re-fetching+re-parsing. The sorted docId order AND the emitted full-doc
// contents (after SortIter clears DocParsed and emit re-fetches in full) must
// match a full-parse, no-scan-side-projection baseline.
func TestFullScanSortScanSideProjection(t *testing.T) {
	db, ns := seedProjBtree(t, 1500)

	for _, sortSpec := range []string{"val", "-val", "a"} {
		t.Run(sortSpec, func(t *testing.T) {
			sorter := query.MustParseSort(sortSpec)
			sf, ok := sortFieldRoots(nil, sorter)
			require.True(t, ok)

			// collect drives FullScan(no filter) -> Sort -> emit, mirroring
			// planIterator: read DocParsed if present (sort clears it, so emit
			// re-fetches by docId in full). scanProject toggles whether the
			// FullScanIter projects+caches the sort key from the scan.
			collect := func(scanProject []string) []collected {
				rtx, err := db.BeginRead()
				require.NoError(t, err)
				defer func() { _ = rtx.Rollback() }()
				sp := syncpool.NewSyncPool(1 << 20)
				buf := sp.GetDocBuf()
				defer sp.ReleaseDocBuf(buf)

				plan := &Plan{}
				data := &CursorSource{Tx: rtx, Ns: ns}
				fs := &FullScanIter{
					Source:        data,
					Buf:           buf,
					Plan:          plan,
					ProjectFields: scanProject, // nil => no scan-side parse
					// EmitFull stays false: a sort consumes the cached doc and
					// re-parses survivors in full on emit.
				}
				si := &SortIter{
					Source: fs, Data: data, Sorter: sorter, Buf: buf, Plan: plan,
					TopK: 100,
				}
				defer si.Close()

				var emitParser anyenc.Parser
				emitCur := data.NewCursor()
				defer emitCur.Close()

				var out []collected
				for {
					_, docId, _, err := si.Next()
					require.NoError(t, err)
					if docId == nil {
						break
					}
					idCopy := string(docId)
					// SortIter.Next clears DocParsed, so emit re-fetches in full.
					require.Nil(t, plan.DocParsed, "SortIter must clear DocParsed before emit")
					require.NoError(t, emitCur.SeekExact([]byte(idCopy)))
					val, verr := emitCur.Value()
					require.NoError(t, verr)
					doc, perr := emitParser.ParseOwned(append([]byte(nil), val...))
					require.NoError(t, perr)
					out = append(out, collected{docId: idCopy, doc: string(doc.MarshalTo(nil))})
				}
				return out
			}

			want := collect(nil) // SortIter own-parses full (no scan-side projection)
			got := collect(sf)   // FullScanIter projects sort key + caches; SortIter reuses
			require.NotEmpty(t, want)
			// Same ordered docIds AND same full emitted doc contents.
			assert.Equal(t, want, got,
				"scan-side projected sort must match full-parse order and emitted contents")
		})
	}
}
