package anystore

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

func mustParseItem(t testing.TB, s string) item {
	it, err := newItem(anyenc.MustParseJson(s))
	require.NoError(t, err)
	return it
}

func assertIdxKeyBuf(t *testing.T, idx *index, keyCase fillKeysCase) {
	var keysStrings = make([]string, len(idx.keysBuf))
	for i, k := range idx.keysBuf {
		keysStrings[i] = k.String()
	}
	require.Equal(t, len(keyCase.expected), len(idx.keysBuf), keyCase.doc, strings.Join(keysStrings, ","))
	for i, k := range keysStrings {
		assert.Equal(t, keyCase.expected[i], k, keyCase.doc)
	}
}

func assertIndexLen(t testing.TB, idx Index, expected int) bool {
	count, err := idx.Len(ctx)
	require.NoError(t, err)
	return assert.Equal(t, expected, count)
}

type fillKeysCaseIndex struct {
	name  string
	info  IndexInfo
	cases []fillKeysCase
}

type fillKeysCase struct {
	doc      string
	expected []string
}

var fillKeysCases = []fillKeysCaseIndex{
	{
		name: "one field",
		info: IndexInfo{Fields: []string{"a"}},
		cases: []fillKeysCase{
			{`{"id":1,"a":"b"}`, []string{`"b"`}},
			{`{"id":1,"a":["b","c"]}`, []string{`"b"`, `"c"`, `["b","c"]`}},
			{`{"id":1,"a":["a", "a", "b", "c", "b"]}`, []string{`"a"`, `"b"`, `"c"`, `["a","a","b","c","b"]`}},
			{`{"id":1}`, []string{"null"}},
			{`{"id":1,"a":null}`, []string{"null"}},
		},
	},
	{
		name: "one field sparse",
		info: IndexInfo{Fields: []string{"a"}, Sparse: true},
		cases: []fillKeysCase{
			{`{"id":1,"a":"b"}`, []string{`"b"`}},
			{`{"id":1,"a":["b","c"]}`, []string{`"b"`, `"c"`, `["b","c"]`}},
			{`{"id":1,"a":["a", "a", "b", "c", "b"]}`, []string{`"a"`, `"b"`, `"c"`, `["a","a","b","c","b"]`}},
			{`{"id":1}`, []string{}},
			{`{"id":1,"a":null}`, []string{}},
		},
	},
	{
		name: "reverse",
		info: IndexInfo{Fields: []string{"-a"}},
		cases: []fillKeysCase{
			{`{"id":1,"a":"b"}`, []string{`"b"`}},
			{`{"id":1,"a":["b","c"]}`, []string{`"b"`, `"c"`, `["b","c"]`}},
			{`{"id":1,"a":["a", "a", "b", "c", "b"]}`, []string{`"a"`, `"b"`, `"c"`, `["a","a","b","c","b"]`}},
		},
	},
	{
		name: "two fields",
		info: IndexInfo{Fields: []string{"a", "b"}},
		cases: []fillKeysCase{
			{`{"id":1,"a":1}`, []string{"1/null"}},
			{`{"id":1,"a":1,"b":2}`, []string{"1/2"}},
			{`{"id":1,"a":[1,2],"b":2}`, []string{"1/2", "2/2", "[1,2]/2"}},
			{`{"id":1,"a":[1,2,1],"b":[2,1,2]}`, []string{
				"1/2", "1/1", "1/[2,1,2]",
				"2/2", "2/1", "2/[2,1,2]",
				"[1,2,1]/2", "[1,2,1]/1", "[1,2,1]/[2,1,2]"}},
		},
	},
	{
		name: "two fields sparse",
		info: IndexInfo{Fields: []string{"a", "b"}, Sparse: true},
		cases: []fillKeysCase{
			{`{"id":1,"a":"1"}`, []string{}},
			{`{"id":1,"b":"2"}`, []string{}},
			{`{"id":1,"a":[1,2]}`, []string{}},
		},
	},
}

func TestIndex_fillKeysBuf(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	newIdx := func(i IndexInfo) *index {
		i.Name = i.createName()
		idx := &index{info: i, c: coll.(*collection)}
		require.NoError(t, idx.init(ctx))
		return idx
	}
	for _, idxCase := range fillKeysCases[:] {
		t.Run(idxCase.name, func(t *testing.T) {
			idx := newIdx(idxCase.info)
			for _, keyCase := range idxCase.cases[:] {
				idx.fillKeysBuf(mustParseItem(t, keyCase.doc))
				assertIdxKeyBuf(t, idx, keyCase)
			}
		})
	}
}

func TestIndex_Insert(t *testing.T) {
	fx := newFixture(t)
	t.Run("uniq", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_uniq")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, coll.Close())
		}()
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1,"a":1}`),
			anyenc.MustParseJson(`{"id":2,"a":2}`),
			anyenc.MustParseJson(`{"id":3,"a":3}`),
		))
		assert.ErrorIs(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":2}`)), ErrUniqueConstraint)
		assertCollCount(t, coll, 3)
		assertIndexLen(t, coll.GetIndexes()[0], 3)
	})
	t.Run("sparse", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_sparse")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, coll.Close())
		}()
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true}))

		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1,"a":1}`),
			anyenc.MustParseJson(`{"id":2,"a":2}`),
			anyenc.MustParseJson(`{"id":3,"b":3}`),
		))
		assertCollCount(t, coll, 3)
		assertIndexLen(t, coll.GetIndexes()[0], 2)
	})
	t.Run("simple", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_simple")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, coll.Close())
		}()
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`), anyenc.MustParseJson(`{"id":2,"a":1}`), anyenc.MustParseJson(`{"id":3,"b":3}`)))
		assertCollCount(t, coll, 3)
		assertIndexLen(t, coll.GetIndexes()[0], 3)
	})
}

func TestIndex_Update(t *testing.T) {
	fx := newFixture(t)
	t.Run("uniq", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_uniq")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, coll.Close())
		}()
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "a":1}`), anyenc.MustParseJson(`{"id":2, "a":2}`), anyenc.MustParseJson(`{"id":3,"a":3}`)))
		require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":2,"a":4}`)))
		assert.ErrorIs(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":2, "a":1}`)), ErrUniqueConstraint)
		res, err := coll.FindId(ctx, 2)
		require.NoError(t, err)
		assert.Equal(t, `{"id":2,"a":4}`, res.Value().String())
	})
	t.Run("sparse", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_sparse")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, coll.Close())
		}()
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true}))

		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "a":1}`), anyenc.MustParseJson(`{"id":2, "a":2}`), anyenc.MustParseJson(`{"id":3, "b":3}`)))
		assertIndexLen(t, coll.GetIndexes()[0], 2)
		require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1, "b":1}`)))
		assertIndexLen(t, coll.GetIndexes()[0], 1)
		require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":3, "a":1}`)))
		assertIndexLen(t, coll.GetIndexes()[0], 2)
	})
}

func TestIndex_Delete(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_simple")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, coll.Close())
	}()
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "a":1}`), anyenc.MustParseJson(`{"id":2, "a":1}`), anyenc.MustParseJson(`{"id":3, "b":3}`)))
	assertIndexLen(t, coll.GetIndexes()[0], 3)

	require.NoError(t, coll.DeleteId(ctx, 1))
	assertIndexLen(t, coll.GetIndexes()[0], 2)

	require.NoError(t, coll.DeleteId(ctx, 2))
	assertIndexLen(t, coll.GetIndexes()[0], 1)

	require.NoError(t, coll.DeleteId(ctx, 3))
	assertIndexLen(t, coll.GetIndexes()[0], 0)
}

func Benchmark_fillKeysBuf(b *testing.B) {
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(b, err)
	newIdx := func(i IndexInfo) *index {
		i.Name = i.createName()
		idx := &index{info: i, c: coll.(*collection)}
		require.NoError(b, idx.init(ctx))
		return idx
	}

	b.Run("simple", func(b *testing.B) {
		idx := newIdx(IndexInfo{Fields: []string{"a"}})
		doc := mustParseItem(b, `{"id":1, "a":"2"}`)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx.fillKeysBuf(doc)
		}
	})
	b.Run("array", func(b *testing.B) {
		idx := newIdx(IndexInfo{Fields: []string{"a"}})
		doc := mustParseItem(b, `{"id":1, "a":["1", "2", "3", "2"]}`)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx.fillKeysBuf(doc)
		}
	})

}

// TestCollection_GetIndexes_FlagsRoundTrip guards the persisted index metadata
// against the reader: Sparse and Unique are stored in separate columns and must
// come back on their own fields after a reopen. Reading both from one column
// silently reports every sparse index as unique and every non-sparse unique
// index as non-unique, which breaks anything that reproduces index definitions.
func TestCollection_GetIndexes_FlagsRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "any-store-test.db")

	want := []IndexInfo{
		{Name: "plain", Fields: []string{"a"}},
		{Name: "sparse", Fields: []string{"b"}, Sparse: true},
		{Name: "unique", Fields: []string{"c"}, Unique: true},
		{Name: "both", Fields: []string{"d"}, Sparse: true, Unique: true},
	}

	db, err := Open(ctx, path, nil)
	require.NoError(t, err)
	coll, err := db.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, want...))
	require.NoError(t, coll.Close())
	require.NoError(t, db.Close())

	db, err = Open(ctx, path, nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()
	coll, err = db.OpenCollection(ctx, "test")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, coll.Close())
	}()

	got := make(map[string]IndexInfo)
	for _, idx := range coll.GetIndexes() {
		got[idx.Info().Name] = idx.Info()
	}
	require.Len(t, got, len(want))
	for _, w := range want {
		g, ok := got[w.Name]
		require.True(t, ok, "index %s missing after reopen", w.Name)
		assert.Equal(t, w.Fields, g.Fields, "index %s: fields", w.Name)
		assert.Equal(t, w.Sparse, g.Sparse, "index %s: sparse", w.Name)
		assert.Equal(t, w.Unique, g.Unique, "index %s: unique", w.Name)
	}
}

// GO-7510: the planner never reads IndexInfo.Sparse, so a negative predicate is
// answered by joining the sparse index table -- which holds only the documents
// that DO carry the field, the complement of what `!= true` matches.
func sparseNeSeed(t *testing.T, coll Collection) {
	t.Helper()
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "isUninstalled", Fields: []string{"isUninstalled"}, Sparse: true}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "resolvedLayout", Fields: []string{"resolvedLayout"}}))
	for i := range 24 {
		layout := "page"
		if i >= 12 {
			layout = "note"
		}
		j := fmt.Sprintf(`{"id":%d,"resolvedLayout":%q}`, i, layout)
		switch i {
		case 20, 21:
			j = fmt.Sprintf(`{"id":%d,"resolvedLayout":%q,"isUninstalled":true}`, i, layout)
		case 22, 23:
			j = fmt.Sprintf(`{"id":%d,"resolvedLayout":%q,"isUninstalled":false}`, i, layout)
		}
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(j)))
	}
}

func sparseNeCount(t *testing.T, coll Collection, cond string) int {
	t.Helper()
	c, err := coll.Find(cond).Count(ctx)
	require.NoError(t, err)
	iter, err := coll.Find(cond).Iter(ctx)
	require.NoError(t, err)
	n := 0
	for iter.Next() {
		n++
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	assert.Equalf(t, c, n, "Count and Iter disagree for %s", cond)
	return c
}

func TestIndex_SparseNotEqualDoesNotDropRows(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "objects")
	require.NoError(t, err)
	sparseNeSeed(t, coll)

	assert.Equal(t, 22, sparseNeCount(t, coll, `{"isUninstalled":{"$ne":true}}`))
	assert.Equal(t, 10, sparseNeCount(t, coll, `{"resolvedLayout":"note","isUninstalled":{"$ne":true}}`))
	assert.Equal(t, 12, sparseNeCount(t, coll, `{"resolvedLayout":"page","isUninstalled":{"$ne":true}}`))
}

func TestIndex_SparseNotEqualNoDocumentCarriesField(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "objects")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "isUninstalled", Fields: []string{"isUninstalled"}, Sparse: true}))
	for i := range 24 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"resolvedLayout":"note"}`, i))))
	}
	assert.Equal(t, 24, sparseNeCount(t, coll, `{"isUninstalled":{"$ne":true}}`))
}

// Sort over a sparse field must not drop the documents the index never stored.
func TestIndex_SparseSortDoesNotDropRows(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "objects")
	require.NoError(t, err)
	sparseNeSeed(t, coll)

	iter, err := coll.Find(`{}`).Sort("isUninstalled").Iter(ctx)
	require.NoError(t, err)
	n := 0
	for iter.Next() {
		n++
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	assert.Equal(t, 24, n, "Sort over a sparse index dropped documents")
}

// The gate must not cost us the optimization it is guarding: a predicate that
// DOES guarantee presence must still be answered from the sparse index.
func TestIndex_SparseStillUsedWhenPresenceGuaranteed(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "objects")
	require.NoError(t, err)
	sparseNeSeed(t, coll)

	for _, tc := range []struct {
		cond      string
		want      int
		useSparse bool
	}{
		{`{"isUninstalled":true}`, 2, true},                 // eq: present
		{`{"isUninstalled":false}`, 2, true},                // eq: present
		{`{"isUninstalled":{"$in":[true,false]}}`, 4, true}, // both present
		// $exists:true matches a field explicitly set to null, which a sparse
		// index also skips (index.go: sparse drops nil AND TypeNull), so it
		// cannot serve this either. Conservative, and still correct.
		{`{"isUninstalled":{"$exists":true}}`, 4, false},
		{`{"isUninstalled":{"$ne":true}}`, 22, false},      // matches absent
		{`{"isUninstalled":{"$exists":false}}`, 20, false}, // matches absent
	} {
		assert.Equalf(t, tc.want, sparseNeCount(t, coll, tc.cond), "count for %s", tc.cond)

		explain, err := coll.Find(tc.cond).Explain(ctx)
		require.NoError(t, err)
		usedSparse := false
		for _, idx := range explain.Indexes {
			if idx.Name == "isUninstalled" && idx.Used {
				usedSparse = true
			}
		}
		assert.Equalf(t, tc.useSparse, usedSparse,
			"sparse index usage for %s (sql: %s)", tc.cond, explain.Sql)
	}
}
