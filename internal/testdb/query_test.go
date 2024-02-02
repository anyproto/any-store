package testdb

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"

	anystore "github.com/anyproto/any-store"
)

func TestQueries(t *testing.T) {
	testFile(t, "data/set1.json")
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
	Hint   string          `json:"hint"`

	ExpectedExplain string            `json:"expectedExplain"`
	ExpectedIds     []json.RawMessage `json:"expectedIds"`
}

func testFile(t *testing.T, filename string) {
	fx := NewFixture(t)
	defer fx.Finish(t)

	fileData, err := os.ReadFile(filename)
	require.NoError(t, err)

	var testCases TestCases

	require.NoError(t, json.Unmarshal(fileData, &testCases))

	var docs = make([]any, len(testCases.Data))
	for i, doc := range testCases.Data {
		docs[i] = []byte(doc)
	}

	db, err := anystore.OpenWithBadger(fx.DB)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	coll, err := db.Collection("test")
	require.NoError(t, err)

	for _, indexFields := range testCases.Indexes {
		require.NoError(t, coll.EnsureIndex(anystore.Index{
			Fields: indexFields,
		}))
	}

	for _, indexFields := range testCases.SparseIndexes {
		require.NoError(t, coll.EnsureIndex(anystore.Index{
			Fields: indexFields,
			Sparse: true,
		}))
	}

	_, err = coll.InsertMany(docs...)
	require.NoError(t, err)
	t.Logf("inserted %d docs", len(docs))

	for j, tc := range testCases.Tests {
		q := coll.Find().Limit(tc.Limit).Offset(tc.Offset)
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
		if tc.Hint != "" {
			q.IndexHint(tc.Hint)
		}

		st := time.Now()

		iter, err := q.Iter()
		require.NoError(t, err)

		var result []string
		for iter.Next() {
			result = append(result, iter.Item().Value().Get("id").String())
		}
		dur := time.Since(st)
		var expected = make([]string, len(tc.ExpectedIds))
		for i, eId := range tc.ExpectedIds {
			expected[i] = fastjson.MustParseBytes(eId).String()
		}

		assert.Equal(t, expected, result)
		assert.Equal(t, tc.ExpectedExplain, iter.Explain())

		require.NoError(t, iter.Close())
		t.Logf("%d\t%s\t%v", j, tc.ExpectedExplain, dur)
	}

}
