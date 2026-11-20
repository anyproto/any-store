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

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
)

// ANYSTORE_TEST_PPROF=<addr> serves net/http/pprof for the run. Off by
// default: an unconditional listener collides between concurrent runs and
// hides the bind error.
func init() {
	addr := os.Getenv("ANYSTORE_TEST_PPROF")
	if addr == "" {
		return
	}
	go func() {
		if err := http.ListenAndServe(addr, nil); err != nil {
			fmt.Fprintf(os.Stderr, "pprof listener on %s: %v\n", addr, err)
		}
	}()
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

// emptyLogicalColl holds five documents {id:i, a:i}, i = 1..5, and a
// full-text index, so a $text filter is a valid source next to the logical
// operators under test.
func emptyLogicalColl(t *testing.T) anystore.Collection {
	_, coll := ftsTestColl(t, "body")
	for i := 1; i <= 5; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"body":"hello world"}`, i, i))))
	}
	return coll
}

// An empty object matches every document wherever a filter is expected: as
// the whole filter and as an element of $and, $or or $nor. Every verb gives
// the same answer, and none panics.
func TestQuery_EmptyObjectInLogicalOperator(t *testing.T) {
	for _, tc := range []struct {
		filter string
		want   int
	}{
		{`{}`, 5},
		{`{"$or":[{},{"a":1}]}`, 5},
		{`{"$and":[{},{"a":1}]}`, 1},
		{`{"$nor":[{}]}`, 0},
		{`{"$nor":[{},{"a":1}]}`, 0},
		{`{"$or":[{}]}`, 5},
		{`{"$and":[{}]}`, 5},
		{`{"$or":[{"$and":[{}]},{"a":5}]}`, 5},
		{`{"$text":{"$search":"hello"},"$and":[{}]}`, 5},
	} {
		t.Run(tc.filter, func(t *testing.T) {
			coll := emptyLogicalColl(t)

			n, err := coll.Find(tc.filter).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, tc.want, n, "Count")

			assert.Len(t, writeOrderIterIds(t, coll.Find(tc.filter)), tc.want, "Iter")

			n, err = coll.Aggregate(fmt.Sprintf(`[{"$match":%s}]`, tc.filter)).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, tc.want, n, "Aggregate $match")

			res, err := coll.Find(tc.filter).Update(ctx, `{"$set":{"touched":true}}`)
			require.NoError(t, err)
			assert.Equal(t, tc.want, res.Modified, "Update")
			assertQueryCount(t, coll.Find(`{"touched":true}`), tc.want)

			res, err = coll.Find(tc.filter).Delete(ctx)
			require.NoError(t, err)
			assert.Equal(t, tc.want, res.Modified, "Delete")
			assertQueryCount(t, coll.Find(nil), 5-tc.want)
		})
	}
}

// A Go-built query.And with no children is a filter, not JSON: it never meets
// the parser's empty-array rule and matches every document, alone or nested.
// Callers build one when no field of their own filter is set.
func TestQuery_GoEmptyAndMatchesAll(t *testing.T) {
	for _, tc := range []struct {
		name   string
		filter query.Filter
		want   int
	}{
		{"And{}", query.And{}, 5},
		{"And{And{}}", query.And{query.And{}}, 5},
		{"And{And{}, a=1}", query.And{query.And{}, query.MustParseCondition(`{"a":1}`)}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coll := emptyLogicalColl(t)

			n, err := coll.Find(tc.filter).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, tc.want, n, "Count")

			assert.Len(t, writeOrderIterIds(t, coll.Find(tc.filter)), tc.want, "Iter")

			res, err := coll.Find(tc.filter).Update(ctx, `{"$set":{"touched":true}}`)
			require.NoError(t, err)
			assert.Equal(t, tc.want, res.Modified, "Update")
			assertQueryCount(t, coll.Find(`{"touched":true}`), tc.want)

			res, err = coll.Find(tc.filter).Delete(ctx)
			require.NoError(t, err)
			assert.Equal(t, tc.want, res.Modified, "Delete")
			assertQueryCount(t, coll.Find(nil), 5-tc.want)
		})
	}
}

// An empty array under $and, $or or $nor is a parse error from every verb,
// at every level and next to any other key. No verb treats it as "no filter":
// a Delete or Update with it changes nothing.
func TestQuery_EmptyLogicalArrayIsParseError(t *testing.T) {
	for _, filter := range []string{
		`{"$or":[]}`,
		`{"$and":[]}`,
		`{"$nor":[]}`,
		`{"$and":[],"a":1}`,
		`{"$or":[],"a":1}`,
		`{"$nor":[],"a":1}`,
		`{"$or":[{"$and":[]},{"a":5}]}`,
		`{"$text":{"$search":"hello"},"$and":[]}`,
	} {
		t.Run(filter, func(t *testing.T) {
			coll := emptyLogicalColl(t)
			requireParseError := func(verb string, err error) {
				t.Helper()
				var pe *query.ParseError
				require.ErrorAs(t, err, &pe, verb)
				assert.Contains(t, pe.Reason, "must be a non-empty array", verb)
			}

			_, err := coll.Find(filter).Count(ctx)
			requireParseError("Count", err)

			_, err = coll.Find(filter).Iter(ctx)
			requireParseError("Iter", err)

			_, err = coll.Find(filter).Explain(ctx)
			requireParseError("Explain", err)

			_, err = coll.Aggregate(fmt.Sprintf(`[{"$match":%s}]`, filter)).Count(ctx)
			requireParseError("Aggregate $match", err)

			_, err = coll.Find(filter).Update(ctx, `{"$set":{"touched":true}}`)
			requireParseError("Update", err)
			assertQueryCount(t, coll.Find(`{"touched":true}`), 0)

			_, err = coll.Find(filter).Delete(ctx)
			requireParseError("Delete", err)
			assertQueryCount(t, coll.Find(nil), 5)
		})
	}
}

// {"$all": []} matches no document (MongoDB's rule), alone and in a compound
// filter, with and without an index on the field: next to the match-all {} a
// conjunction of no equalities would have let a Delete empty the collection.
func TestQuery_AllWithEmptyArrayMatchesNothing(t *testing.T) {
	for _, index := range []string{"", "tags", "sparse tags"} {
		for _, filter := range []string{
			`{"tags":{"$all":[]}}`,
			`{"$and":[{},{"tags":{"$all":[]}}]}`,
			`{"$and":[{"a":{"$gte":1}},{"tags":{"$all":[]}}]}`,
			`{"$or":[{"tags":{"$all":[]}},{"a":1}]}`,
		} {
			t.Run(index+"/"+filter, func(t *testing.T) {
				fx := newFixture(t)
				coll, err := fx.CreateCollection(ctx, "all_empty")
				require.NoError(t, err)
				switch index {
				case "tags":
					require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"tags"}}))
				case "sparse tags":
					require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"tags"}, Sparse: true}))
				}
				insertJSON(t, coll,
					`{"id":1,"a":1,"tags":["x","y"]}`,
					`{"id":2,"a":2,"tags":[]}`,
					`{"id":3,"a":3,"tags":"x"}`,
					`{"id":4,"a":4}`,
				)
				want := 0
				if strings.Contains(filter, `"$or"`) {
					want = 1 // the {"a":1} branch
				}

				n, err := coll.Find(filter).Count(ctx)
				require.NoError(t, err)
				assert.Equal(t, want, n, "Count")
				assert.Len(t, writeOrderIterIds(t, coll.Find(filter)), want, "Iter")
				n, err = coll.Aggregate(fmt.Sprintf(`[{"$match":%s}]`, filter)).Count(ctx)
				require.NoError(t, err)
				assert.Equal(t, want, n, "Aggregate $match")

				res, err := coll.Find(filter).Update(ctx, `{"$set":{"touched":true}}`)
				require.NoError(t, err)
				assert.Equal(t, want, res.Modified, "Update")
				assertQueryCount(t, coll.Find(`{"touched":true}`), want)

				res, err = coll.Find(filter).Delete(ctx)
				require.NoError(t, err)
				assert.Equal(t, want, res.Modified, "Delete")
				assertQueryCount(t, coll.Find(nil), 4-want)
			})
		}
	}
}
