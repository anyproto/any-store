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

	var docs = make([]any, len(testCases.Data))
	for i, doc := range testCases.Data {
		docs[i] = []byte(doc)
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

	for j, tc := range testCases.Tests {
		q := coll.Query().Limit(tc.Limit).Offset(tc.Offset)
		if tc.Cond != nil {
			q.Cond(tc.Cond)
		}
		if tc.Sort != nil {
			var sorts = make([]any, len(tc.Sort))
			for i, s := range tc.Sort {
				sorts[i] = s
			}
			q.Sort(sorts...)
		}

		st := time.Now()

		iter := q.Iter(ctx)

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
		query, explain, err := q.Explain(ctx)
		require.NoError(t, err)
		if tc.ExpectedExplain != "" {
			assert.Equal(t, strings.TrimSpace(tc.ExpectedExplain), strings.TrimSpace(explain), j)
		}
		if tc.ExpectedQuery != "" {
			assert.Equal(t, strings.TrimSpace(tc.ExpectedQuery), strings.TrimSpace(query), j)
		}

		require.NoError(t, iter.Close())
		t.Logf("%d\t%s\t%v", j, query, dur)
	}

}
