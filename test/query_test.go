package test

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
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
	dataDir := "data"
	err := filepath.WalkDir(dataDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".json") {
			return nil
		}
		// Compute relative path from dataDir and strip .json extension
		rel, err := filepath.Rel(dataDir, path)
		if err != nil {
			return err
		}
		name := strings.TrimSuffix(rel, ".json")
		t.Run(name, func(t *testing.T) {
			testFile(t, path)
		})
		return nil
	})
	require.NoError(t, err)
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
	UniqueIndexes [][]string        `json:"uniqueIndexes"`
}

type TestCase struct {
	Name   string          `json:"name"`
	Cond   json.RawMessage `json:"cond"`
	Limit  uint            `json:"limit"`
	Offset uint            `json:"offset"`
	Sort   []string        `json:"sort"`

	ExpectedQuery           string            `json:"expectedQuery"`
	ExpectedIds             []json.RawMessage `json:"expectedIds"`
	ExpectedCount           *int              `json:"expectedCount"`
	ExpectedPlanContains    []string          `json:"expectedPlanContains"`
	ExpectedPlanNotContains []string          `json:"expectedPlanNotContains"`
	IndexHints              []IndexHintJSON   `json:"indexHints"`
}

type IndexHintJSON struct {
	IndexName string `json:"indexName"`
	Boost     int    `json:"boost"`
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

	for _, indexFields := range testCases.UniqueIndexes {
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
			Fields: indexFields,
			Unique: true,
		}))
	}

	if len(docs) > 0 {
		st := time.Now()
		require.NoError(t, coll.Insert(ctx, docs...))
		t.Logf("inserted %d docs; %v", len(docs), time.Since(st))
	}

	for j, tc := range testCases.Tests[:] {
		testName := fmt.Sprintf("%d", j)
		if tc.Name != "" {
			testName = tc.Name
		}
		t.Run(testName, func(t *testing.T) {
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

			for _, h := range tc.IndexHints {
				q = q.IndexHint(anystore.IndexHint{IndexName: h.IndexName, Boost: h.Boost})
			}

			st := time.Now()

			if tc.ExpectedCount != nil {
				count, err := q.Count(ctx)
				require.NoError(t, err)
				assert.Equal(t, *tc.ExpectedCount, count)
			}

			if tc.ExpectedIds != nil {
				iter, err := q.Iter(ctx)
				require.NoError(t, err)

				var result = make([]string, 0)
				for iter.Next() {
					doc, err := iter.Doc()
					require.NoError(t, err)
					result = append(result, doc.Value().Get("id").String())
				}
				var expected = make([]string, len(tc.ExpectedIds))
				for i, eId := range tc.ExpectedIds {
					expected[i] = fastjson.MustParseBytes(eId).String()
				}
				assert.Equal(t, expected, result)
				require.NoError(t, iter.Close())
			}

			dur := time.Since(st)

			needExplain := tc.ExpectedQuery != "" ||
				len(tc.ExpectedPlanContains) > 0 || len(tc.ExpectedPlanNotContains) > 0

			if needExplain {
				explain, err := q.Explain(ctx)
				require.NoError(t, err)
				if tc.ExpectedQuery != "" {
					assert.Equal(t, strings.TrimSpace(tc.ExpectedQuery), strings.TrimSpace(explain.Sql))
				}
				for _, s := range tc.ExpectedPlanContains {
					assert.Contains(t, explain.Sql, s)
				}
				for _, s := range tc.ExpectedPlanNotContains {
					assert.NotContains(t, explain.Sql, s)
				}
				t.Logf("%s\t%v", explain.Sql, dur)
			} else {
				t.Logf("dur: %v", dur)
			}
		})
	}

}
