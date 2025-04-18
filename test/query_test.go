package test

import (
	"encoding/json"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/query"
)

func init() {
	go http.ListenAndServe(":6060", nil)
}

func TestQueries(t *testing.T) {
	t.Run("no-index", func(t *testing.T) {
		testFile(t, "data/no-index.json")
	})
	t.Run("simple indexes", func(t *testing.T) {
		testFile(t, "data/simple-indexes.json")
	})
}

func TestCollection_ReadUncommitted(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	err = coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":1, "doc":"a"}`))
	require.NoError(t, err)
	iter, err := coll.Find(query.Key{
		Path:   []string{"doc"},
		Filter: query.NewComp(query.CompOpEq, "a"),
	}).Iter(tx.Context())
	require.NoError(t, err)
	var got []string
	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err)
		got = append(got, doc.Value().GetString("doc"))
	}
	assert.Equal(t, []string{"a"}, got)
	err = iter.Close()
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)
}

type TestCases struct {
	Tests         []TestCase        `json:"tests"`
	Data          []json.RawMessage `json:"data"`
	Indexes       [][]string        `json:"indexes"`
	SparseIndexes [][]string        `json:"sparseIndexes"`
}

type TestCase struct {
	Cond   json.RawMessage `json:"cond"`
	Limit  uint            `json:"limit"`
	Offset uint            `json:"offset"`
	Sort   []string        `json:"sort"`

	ExpectedExplain string            `json:"expectedExplain"`
	ExpectedQuery   string            `json:"expectedQuery"`
	ExpectedIds     []json.RawMessage `json:"expectedIds"`
}

func testFile(t *testing.T, filename string) {
	fx := newFixture(t)

	fileData, err := os.ReadFile(filename)
	require.NoError(t, err)

	var testCases TestCases

	require.NoError(t, json.Unmarshal(fileData, &testCases))

	var docs = make([]*anyenc.Value, len(testCases.Data))
	for i, doc := range testCases.Data {
		docs[i] = anyenc.MustParseJson(string(doc))
	}

	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	for _, indexFields := range testCases.Indexes {
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
			Fields: indexFields,
		}))
	}

	for _, indexFields := range testCases.SparseIndexes {
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
			Fields: indexFields,
			Sparse: true,
		}))
	}

	st := time.Now()
	require.NoError(t, coll.Insert(ctx, docs...))
	t.Logf("inserted %d docs; %v", len(docs), time.Since(st))

	for j, tc := range testCases.Tests[:] {
		var cond any
		if tc.Cond != nil {
			cond = tc.Cond
		}
		q := coll.Find(cond).Limit(tc.Limit).Offset(tc.Offset)
		if tc.Sort != nil {
			var sorts = make([]any, len(tc.Sort))
			for i, s := range tc.Sort {
				sorts[i] = s
			}
			q.Sort(sorts...)
		}

		st := time.Now()

		iter, err := q.Iter(ctx)
		require.NoError(t, err)

		var result = make([]string, 0)
		for iter.Next() {
			doc, err := iter.Doc()
			require.NoError(t, err)
			result = append(result, doc.Value().Get("id").String())
		}
		dur := time.Since(st)
		var expected = make([]string, len(tc.ExpectedIds))
		for i, eId := range tc.ExpectedIds {
			expected[i] = fastjson.MustParseBytes(eId).String()
		}
		assert.Equal(t, expected, result, j)
		require.NoError(t, iter.Close())

		explain, err := q.Explain(ctx)
		require.NoError(t, err)
		if tc.ExpectedExplain != "" {
			assert.Equal(t, strings.TrimSpace(tc.ExpectedExplain), strings.TrimSpace(strings.Join(explain.SqliteExplain, "\n")), j)
		}
		if tc.ExpectedQuery != "" {
			assert.Equal(t, strings.TrimSpace(tc.ExpectedQuery), strings.TrimSpace(explain.Sql), j)
		}

		t.Logf("%d\t%s\t%v", j, explain.Sql, dur)
	}

}
