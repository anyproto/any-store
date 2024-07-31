package test

import (
	"encoding/json"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"
	"zombiezen.com/go/sqlite"

	anystore "github.com/anyproto/any-store"
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

func BenchmarkAny(t *testing.B) {

	db, err := anystore.Open(ctx, "azaza.db", nil)
	require.NoError(t, err)
	defer db.Close()

	coll, err := db.Collection(ctx, "test")
	require.NoError(t, err)
	tx, err := db.WriteTx(ctx)
	require.NoError(t, err)
	//for range 10000 {
	//	randomValue := rand.Intn(100)
	//	jsonData := fmt.Sprintf(`{"a": %d}`, randomValue)
	//	err = coll.Insert(tx.Context(), jsonData)
	//	require.NoError(t, err)
	//}
	require.NoError(t, tx.Commit())
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		iter, err := coll.Find(`{"a": {"$lt": 10}}`).Iter(ctx)
		require.NoError(t, err)
		var count int
		for iter.Next() {
			_, err = iter.Doc()
			require.NoError(t, err)
			count++
		}
		t.Log(count)
		require.NoError(t, iter.Close())
	}
}
func BenchmarkZombie(t *testing.B) {
	// Open an in-memory database.
	conn, err := sqlite.OpenConn("azaza.db", sqlite.OpenReadWrite) //todo file
	filter := query.MustParseCondition(`{"a": {"$lt": 10}}`)
	parser := &fastjson.Parser{}
	conn.CreateFunction("any_filter", &sqlite.FunctionImpl{
		NArgs: 1,
		Scalar: func(ctx sqlite.Context, args []sqlite.Value) (sqlite.Value, error) {
			data := args[0]
			doc, _ := parser.ParseBytes(data.Blob())
			ok := filter.Ok(doc)
			if ok {
				return sqlite.IntegerValue(1), nil
			}
			return sqlite.IntegerValue(0), nil
		},
		MakeAggregate: nil,
		Deterministic: true,
		AllowIndirect: false,
	})
	require.NoError(t, err)
	defer conn.Close()

	//// Создаем таблицу
	//err = sqlitex.ExecScript(conn, `CREATE TABLE example (
	//	id INTEGER PRIMARY KEY,
	//	data TEXT
	//);`)
	//if err != nil {
	//	log.Fatal(err)
	//}

	//// Вставляем JSON данные в таблицу
	//jsonData := `{"a": 2}`
	//stmt := conn.Prep("INSERT INTO example (data) VALUES ($data);")
	//stmt.SetText("$data", jsonData)
	//if _, err := stmt.Step(); err != nil {
	//	log.Fatal(err)
	//}

	// Начинаем транзакцию
	//err = sqlitex.ExecuteTransient(conn, "BEGIN TRANSACTION;", nil)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//stmt := conn.Prep("INSERT INTO example (data) VALUES ($data);")
	//for i := 0; i < 100000; i++ {
	//	// Генерируем случайное число
	//	randomValue := rand.Intn(100) // значение от 0 до 99
	//
	//	// Формируем JSON-строку
	//	jsonData := fmt.Sprintf(`{"a": %d}`, randomValue)
	//
	//	// Выполняем вставку
	//	stmt.SetText("$data", jsonData)
	//	if _, err := stmt.Step(); err != nil {
	//		log.Fatal(err)
	//	}
	//	stmt.Reset()
	//}
	//
	//// Завершаем транзакцию
	//err = sqlitex.ExecuteTransient(conn, "COMMIT;", nil)
	//if err != nil {
	//	log.Fatal(err)
	//}

	// Выполняем запрос, чтобы получить записи, где a > 1

	query := `SELECT data 
	FROM _test_docs
	WHERE any_filter(data) = 1;`
	stmt := conn.Prep(query)
	t.ResetTimer()
	var buf []byte
	for i := 0; i < t.N; i++ {
		var count int
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				log.Fatal(err)
			}
			if !hasRow {
				break
			}
			count++
			size := stmt.ColumnLen(0)
			buf = slices.Grow(buf[:0], size)
			_ = stmt.ColumnBytes(0, buf)
		}
		stmt.Reset()
		t.Log(count)
	}
}
