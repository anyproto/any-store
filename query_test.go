package anystore

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

func TestCollQuery_Count(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1, "a":1}`),
		anyenc.MustParseJson(`{"id":2, "a":2}`),
		anyenc.MustParseJson(`{"id":3, "a":3}`),
		anyenc.MustParseJson(`{"id":4, "a":4}`),
		anyenc.MustParseJson(`{"id":5, "a":5}`),
	))

	t.Run("no filter", func(t *testing.T) {
		assertQueryCount(t, coll.Find(nil), 5)
	})

	t.Run("filter", func(t *testing.T) {
		assertQueryCount(t, coll.Find(`{"a":{"$in":[2,3,4]}}`), 3)
	})

}

func TestCollQuery_Explain(t *testing.T) {
	fx := newFixture(t)

	assertExplain := func(t testing.TB, q Query, expQuery, expExplain string) {
		explain, err := q.Explain(ctx)
		require.NoError(t, err, explain.Sql)
		sqliteExplain := strings.Join(explain.SqliteExplain, "\n")
		if expQuery != "" {
			assert.Equal(t, expQuery, strings.TrimSpace(explain.Sql))
		} else {
			t.Log(explain.Sql)
		}
		if expExplain != "" {
			assert.Equal(t, strings.TrimSpace(expExplain), sqliteExplain)
		} else {
			t.Log(explain)
		}
	}
	assertIndexes := func(t *testing.T, q Query, expIndexes []IndexExplain) {
		explain, err := q.Explain(ctx)
		require.NoError(t, err, explain.Sql)
		assert.Equal(t, expIndexes, explain.Indexes, explain.Sql)
	}

	t.Run("no index", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1, "a":"a1"}`),
			anyenc.MustParseJson(`{"id":2, "a":"a2"}`),
			anyenc.MustParseJson(`{"id":3, "a":"a3"}`),
			anyenc.MustParseJson(`{"id":4, "a":"a4"}`),
			anyenc.MustParseJson(`{"id":5, "a":"a5"}`),
		))

		assertExplain(t, coll.Find(nil),
			"SELECT data FROM '_test_docs'",
			"SCAN _test_docs",
		)
		assertExplain(t, coll.Find(`{"id":4}`),
			"SELECT data FROM '_test_docs' WHERE  ((id = :val_0_0_0)) AND any_filter(1, data)",
			"SEARCH _test_docs USING INDEX sqlite_autoindex__test_docs_1 (id=?)",
		)
		assertExplain(t, coll.Find(`{"id":{"$gt":2}}`),
			"SELECT data FROM '_test_docs' WHERE  ((id > :val_0_0_0)) AND any_filter(1, data)",
			"SEARCH _test_docs USING INDEX sqlite_autoindex__test_docs_1 (id>?)",
		)
		assertExplain(t, coll.Find(`{"id":{"$gte":2}}`),
			"SELECT data FROM '_test_docs' WHERE  ((id >= :val_0_0_0)) AND any_filter(1, data)",
			"SEARCH _test_docs USING INDEX sqlite_autoindex__test_docs_1 (id>?)",
		)
		assertExplain(t, coll.Find(`{"id":{"$lt":2}}`),
			"SELECT data FROM '_test_docs' WHERE  ((id < :val_0_0_0_end)) AND any_filter(1, data)",
			"SEARCH _test_docs USING INDEX sqlite_autoindex__test_docs_1 (id<?)",
		)
		assertExplain(t, coll.Find(`{"id":{"$lte":2}}`),
			"SELECT data FROM '_test_docs' WHERE  ((id <= :val_0_0_0_end)) AND any_filter(1, data)",
			"SEARCH _test_docs USING INDEX sqlite_autoindex__test_docs_1 (id<?)",
		)
		assertExplain(t, coll.Find(`{"id":{"$in":[1,2]}}`),
			"SELECT data FROM '_test_docs' WHERE  ((id = :val_0_0_0) OR (id = :val_0_0_1)) AND any_filter(1, data)",
			"SEARCH _test_docs USING INDEX sqlite_autoindex__test_docs_1 (id=?)",
		)
		assertExplain(t, coll.Find(`{"id":{"$nin":[1,2]}}`),
			"SELECT data FROM '_test_docs' WHERE any_filter(1, data)",
			"SCAN _test_docs",
		)
		assertExplain(t, coll.Find(`{"$or":[{"id":{"$gt":3}}, {"id":{"$lt":2}}]}`),
			"SELECT data FROM '_test_docs' WHERE  ((id < :val_0_0_0_end) OR (id > :val_0_0_1)) AND any_filter(1, data)",
			"SCAN _test_docs",
		)
		assertExplain(t, coll.Find(nil).Limit(5).Offset(3),
			"SELECT data FROM '_test_docs'  LIMIT 5 OFFSET 3",
			"SCAN _test_docs",
		)
		assertExplain(t, coll.Find(nil).Sort("-a"),
			"SELECT data FROM '_test_docs'  ORDER BY any_sort(1, data)",
			"SCAN _test_docs\nUSE TEMP B-TREE FOR ORDER BY",
		)
		assertExplain(t, coll.Find(nil).Sort("-id"),
			"SELECT data FROM '_test_docs'  ORDER BY id DESC",
			"SCAN _test_docs USING INDEX sqlite_autoindex__test_docs_1",
		)
	})
	t.Run("more than 1000", func(t *testing.T) {
		var builder strings.Builder
		builder.Grow(4000)
		builder.WriteString(`{"id":{"$in":[`)
		l := 999
		for i := 1; i <= l; i++ {
			builder.WriteString(strconv.Itoa(i))
			if i < l {
				builder.WriteString(",")
			}
		}
		builder.WriteString("]}}")
		result := builder.String()

		coll, _ := fx.CreateCollection(ctx, "test_foo")

		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1, "a":"a1"}`),
			anyenc.MustParseJson(`{"id":2, "a":"a2"}`),
			anyenc.MustParseJson(`{"id":3, "a":"a3"}`),
			anyenc.MustParseJson(`{"id":4, "a":"a4"}`),
			anyenc.MustParseJson(`{"id":5, "a":"a5"}`),
		))
		assertExplain(t, coll.Find(result),
			"SELECT data FROM '_test_foo_docs' WHERE any_filter(1, data)",
			"SCAN _test_foo_docs",
		)
	})
	t.Run("simple index", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_s")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1, "a":"a1", "b":"b1"}`),
			anyenc.MustParseJson(`{"id":2, "a":"a2"}`),
			anyenc.MustParseJson(`{"id":3, "a":"a3"}`),
			anyenc.MustParseJson(`{"id":4, "a":"a4"}`),
			anyenc.MustParseJson(`{"id":5, "a":"a5"}`),
		))
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

		assertExplain(t, coll.Find(nil),
			"SELECT data FROM '_test_s_docs'",
			"SCAN _test_s_docs",
		)
		assertExplain(t, coll.Find(`{"a":"a4"}`),
			"SELECT data FROM '_test_s_docs' JOIN '_test_s_a_idx' ON '_test_s_a_idx'.docId = id WHERE  (('_test_s_a_idx'.val0 = :val_1_0_0)) AND any_filter(1, data)",
			"SEARCH _test_s_a_idx USING COVERING INDEX sqlite_autoindex__test_s_a_idx_1 (val0=?)\nSEARCH _test_s_docs USING INDEX sqlite_autoindex__test_s_docs_1 (id=?)",
		)
		assertExplain(t, coll.Find(`{"a":{"$gt":1}}`),
			"SELECT data FROM '_test_s_docs' JOIN '_test_s_a_idx' ON '_test_s_a_idx'.docId = id WHERE  (('_test_s_a_idx'.val0 > :val_1_0_0)) AND any_filter(1, data)",
			"SEARCH _test_s_a_idx USING COVERING INDEX sqlite_autoindex__test_s_a_idx_1 (val0>?)\nSEARCH _test_s_docs USING INDEX sqlite_autoindex__test_s_docs_1 (id=?)",
		)
		assertExplain(t, coll.Find(`{"a":{"$gte":1}}`),
			"SELECT data FROM '_test_s_docs' JOIN '_test_s_a_idx' ON '_test_s_a_idx'.docId = id WHERE  (('_test_s_a_idx'.val0 >= :val_1_0_0)) AND any_filter(1, data)",
			"SEARCH _test_s_a_idx USING COVERING INDEX sqlite_autoindex__test_s_a_idx_1 (val0>?)\nSEARCH _test_s_docs USING INDEX sqlite_autoindex__test_s_docs_1 (id=?)",
		)
		assertExplain(t, coll.Find(`{"a":{"$lt":1}}`),
			"SELECT data FROM '_test_s_docs' JOIN '_test_s_a_idx' ON '_test_s_a_idx'.docId = id WHERE  (('_test_s_a_idx'.val0 < :val_1_0_0_end)) AND any_filter(1, data)",
			"SEARCH _test_s_a_idx USING COVERING INDEX sqlite_autoindex__test_s_a_idx_1 (val0<?)\nSEARCH _test_s_docs USING INDEX sqlite_autoindex__test_s_docs_1 (id=?)",
		)
		assertExplain(t, coll.Find(`{"a":{"$lte":1}}`),
			"SELECT data FROM '_test_s_docs' JOIN '_test_s_a_idx' ON '_test_s_a_idx'.docId = id WHERE  (('_test_s_a_idx'.val0 <= :val_1_0_0_end)) AND any_filter(1, data)",
			"SEARCH _test_s_a_idx USING COVERING INDEX sqlite_autoindex__test_s_a_idx_1 (val0<?)\nSEARCH _test_s_docs USING INDEX sqlite_autoindex__test_s_docs_1 (id=?)",
		)
		assertExplain(t, coll.Find(nil).Sort("a"),
			"SELECT data FROM '_test_s_docs' JOIN '_test_s_a_idx' ON '_test_s_a_idx'.docId = id  ORDER BY '_test_s_a_idx'.val0",
			"SCAN _test_s_a_idx USING COVERING INDEX sqlite_autoindex__test_s_a_idx_1\nSEARCH _test_s_docs USING INDEX sqlite_autoindex__test_s_docs_1 (id=?)",
		)
		assertExplain(t, coll.Find(nil).Sort("-a"),
			"SELECT data FROM '_test_s_docs' JOIN '_test_s_a_idx' ON '_test_s_a_idx'.docId = id  ORDER BY '_test_s_a_idx'.val0 DESC",
			"SCAN _test_s_a_idx USING COVERING INDEX sqlite_autoindex__test_s_a_idx_1\nSEARCH _test_s_docs USING INDEX sqlite_autoindex__test_s_docs_1 (id=?)",
		)
		assertExplain(t, coll.Find(nil).Sort("a", "id"),
			"SELECT data FROM '_test_s_docs' JOIN '_test_s_a_idx' ON '_test_s_a_idx'.docId = id  ORDER BY '_test_s_a_idx'.val0, id",
			"SCAN _test_s_a_idx USING COVERING INDEX sqlite_autoindex__test_s_a_idx_1\nSEARCH _test_s_docs USING INDEX sqlite_autoindex__test_s_docs_1 (id=?)\nUSE TEMP B-TREE FOR LAST TERM OF ORDER BY",
		)
		assertExplain(t, coll.Find(nil).Sort("a", "id", "b"),
			"SELECT data FROM '_test_s_docs' JOIN '_test_s_a_idx' ON '_test_s_a_idx'.docId = id  ORDER BY '_test_s_a_idx'.val0, id, any_sort(1, data)",
			"SCAN _test_s_a_idx USING COVERING INDEX sqlite_autoindex__test_s_a_idx_1\nSEARCH _test_s_docs USING INDEX sqlite_autoindex__test_s_docs_1 (id=?)\nUSE TEMP B-TREE FOR LAST 2 TERMS OF ORDER BY",
		)
	})
	t.Run("many indexes", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_m")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1, "a":"a1", "b":"b1", "c":"c1"}`),
			anyenc.MustParseJson(`{"id":2, "a":"a2", "c":"c2"}`),
			anyenc.MustParseJson(`{"id":3, "a":"a3", "c":"c3"}`),
			anyenc.MustParseJson(`{"id":4, "a":"a4", "c":"c4"}`),
			anyenc.MustParseJson(`{"id":5, "a":"a5", "c": "c5"}`),
		))
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"d"}}))
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b", "a"}}))
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b", "a", "-c"}}))
		assertIndexes(t, coll.Find(`{"a":1}`),
			[]IndexExplain{
				{"a", 10, true},
				{"b,a", 1, false},
				{"b,a,-c", 1, false},
				{"d", -1, false},
			},
		)
		assertIndexes(t, coll.Find(`{"a":1, "d":1}`),
			[]IndexExplain{
				{"a", 10, true},
				{"d", 10, false},
				{"b,a", 1, false},
				{"b,a,-c", 1, false},
			},
		)
		assertIndexes(t, coll.Find(`{"a":1, "b":2}`),
			[]IndexExplain{
				{"b,a", 20, true},
				{"b,a,-c", 19, false},
				{"a", 10, false},
				{"d", -1, false},
			},
		)
		assertIndexes(t, coll.Find(`{"a":1, "b":2, "c":3}`),
			[]IndexExplain{
				{"b,a,-c", 40, true},
				{"b,a", 20, false},
				{"a", 10, false},
				{"d", -1, false},
			},
		)
		assertIndexes(t, coll.Find(`{"a":1, "b":2, "c":3}`),
			[]IndexExplain{
				{"b,a,-c", 40, true},
				{"b,a", 20, false},
				{"a", 10, false},
				{"d", -1, false},
			},
		)
		assertIndexes(t, coll.Find(`{"a":1}`).Sort("b", "a"),
			[]IndexExplain{
				{"b,a", 25, true},
				{"b,a,-c", 25, false},
				{"a", 10, false},
				{"d", -1, false},
			},
		)
		assertIndexes(t, coll.Find(`{"a":1}`).Sort("a"),
			[]IndexExplain{
				{"a", 21, true},
				{"b,a", 6, false},
				{"b,a,-c", 6, false},
				{"d", -1, false},
			},
		)
		assertIndexes(t, coll.Find(`{"a":1}`).Sort("d"),
			[]IndexExplain{
				{"a", 10, true},
				{"d", 10, true},
				{"b,a", 1, false},
				{"b,a,-c", 1, false},
			},
		)
		assertIndexes(t, coll.Find(`{"a":1}`).Sort("a", "b"),
			[]IndexExplain{
				{"a", 21, true},
				{"b,a", 11, true},
				{"b,a,-c", 11, false},
				{"d", -1, false},
			},
		)
		assertIndexes(t, coll.Find(`{"a":1}`).Sort("b", "a"),
			[]IndexExplain{
				{"b,a", 25, true},
				{"b,a,-c", 25, false},
				{"a", 10, false},
				{"d", -1, false},
			},
		)
		t.Run("index hint", func(t *testing.T) {
			assertIndexes(t, coll.Find(`{"a":1}`).Sort("b", "a").IndexHint(IndexHint{IndexName: "b,a,-c", Boost: 10}),
				[]IndexExplain{
					{"b,a,-c", 35, true},
					{"b,a", 25, false},
					{"a", 10, false},
					{"d", -1, false},
				},
			)
		})
	})
	t.Run("composite index", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_comp")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1, "a":1, "b":1}`),
			anyenc.MustParseJson(`{"id":2, "a":1, "b":2}`),
			anyenc.MustParseJson(`{"id":3, "a":1, "b":3}`),
			anyenc.MustParseJson(`{"id":4, "a":2, "b":1}`),
			anyenc.MustParseJson(`{"id":5, "a":2, "b":2}`),
			anyenc.MustParseJson(`{"id":6, "a":2, "b":3}`),
		))
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

		assertExplain(t, coll.Find(`{"a":1}`),
			"SELECT data FROM '_test_comp_docs' JOIN '_test_comp_a,b_idx' ON '_test_comp_a,b_idx'.docId = id WHERE  (('_test_comp_a,b_idx'.val0 = :val_1_0_0)) AND any_filter(1, data)",
			"SEARCH _test_comp_a,b_idx USING COVERING INDEX sqlite_autoindex__test_comp_a,b_idx_1 (val0=?)\nSEARCH _test_comp_docs USING INDEX sqlite_autoindex__test_comp_docs_1 (id=?)",
		)

		assertExplain(t, coll.Find(`{"a":1,"b":2}`),
			"SELECT data FROM '_test_comp_docs' JOIN '_test_comp_a,b_idx' ON '_test_comp_a,b_idx'.docId = id WHERE  (('_test_comp_a,b_idx'.val0 = :val_1_0_0)) AND (('_test_comp_a,b_idx'.val1 = :val_1_1_0)) AND any_filter(1, data)",
			"SEARCH _test_comp_a,b_idx USING COVERING INDEX sqlite_autoindex__test_comp_a,b_idx_1 (val0=? AND val1=?)\nSEARCH _test_comp_docs USING INDEX sqlite_autoindex__test_comp_docs_1 (id=?)",
		)

		assertExplain(t, coll.Find(`{"b":2}`),
			"SELECT data FROM '_test_comp_docs' JOIN '_test_comp_a,b_idx' ON '_test_comp_a,b_idx'.docId = id WHERE  (('_test_comp_a,b_idx'.val1 = :val_1_0_0)) AND any_filter(1, data)",
			"SCAN _test_comp_a,b_idx\nSEARCH _test_comp_docs USING INDEX sqlite_autoindex__test_comp_docs_1 (id=?)",
		)

		assertExplain(t, coll.Find(`{"b":{"$gt":1}}`),
			"SELECT data FROM '_test_comp_docs' JOIN '_test_comp_a,b_idx' ON '_test_comp_a,b_idx'.docId = id WHERE  (('_test_comp_a,b_idx'.val1 > :val_1_0_0)) AND any_filter(1, data)",
			"SCAN _test_comp_a,b_idx\nSEARCH _test_comp_docs USING INDEX sqlite_autoindex__test_comp_docs_1 (id=?)",
		)

		assertExplain(t, coll.Find(`{"b":{"$lte":1}}`),
			"SELECT data FROM '_test_comp_docs' JOIN '_test_comp_a,b_idx' ON '_test_comp_a,b_idx'.docId = id WHERE  (('_test_comp_a,b_idx'.val1 <= :val_1_0_0_end)) AND any_filter(1, data)",
			"SCAN _test_comp_a,b_idx\nSEARCH _test_comp_docs USING INDEX sqlite_autoindex__test_comp_docs_1 (id=?)",
		)
	})
}

func assertQueryCount(t testing.TB, q Query, exp int) {
	count, err := q.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, exp, count)
}

func TestCollQuery_Update(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":1}`),
		anyenc.MustParseJson(`{"id":2,"a":1}`),
		anyenc.MustParseJson(`{"id":3,"a":1}`),
		anyenc.MustParseJson(`{"id":4,"a":1}`),
	))

	assertQueryCount(t, coll.Find(`{"a":1}`), 4)

	mRes, err := coll.Find(`{"id":{"$in":[1,3]}}`).Update(ctx, `{"$inc":{"a":1}}`)
	require.NoError(t, err)
	assert.Equal(t, ModifyResult{Matched: 2, Modified: 2}, mRes)

	assertQueryCount(t, coll.Find(`{"a":1}`), 2)
}

func TestCollQuery_Delete(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":1}`),
		anyenc.MustParseJson(`{"id":2,"a":1}`),
		anyenc.MustParseJson(`{"id":3,"a":1}`),
		anyenc.MustParseJson(`{"id":4,"a":1}`),
	))

	assertQueryCount(t, coll.Find(`{"a":1}`), 4)

	mRes, err := coll.Find(`{"id":{"$in":[1,3]}}`).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, ModifyResult{Matched: 2, Modified: 2}, mRes)

	assertCollCount(t, coll, 2)
}

func TestFilterIn(t *testing.T) {
	fx := newFixture(t)
	t.Run("in array with arrays", func(t *testing.T) {
		coll, _ := fx.CreateCollection(ctx, "test_in_arr")
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1, "a":"a1"}`),
			anyenc.MustParseJson(`{"id":2, "a":"a2"}`),
			anyenc.MustParseJson(`{"id":3, "a":"a3"}`),
			anyenc.MustParseJson(`{"id":4, "a":["a3", "a5", "a6"]}`),
			anyenc.MustParseJson(`{"id":5, "a":"a5"}`),
		))
		result := coll.Find(`{"a": {"$in": ["a3", "a5"]}}`)
		length, err := result.Count(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 3, length)
	})

}

func TestCollQuery_IterReleasesTxOnQueryError(t *testing.T) {
	// With a single read connection, leaking the read tx on a failed Iter makes
	// every subsequent read block forever in GetRead.
	fx := newFixture(t, &Config{ReadConnections: 1})
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "a":1}`)))

	// dropping the collection removes the underlying table, so preparing the
	// query below fails inside Iter after the read tx has been acquired
	require.NoError(t, coll.Drop(ctx))

	_, err = coll.Find(nil).Iter(ctx)
	require.Error(t, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = fx.GetCollectionNames(ctx)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("read blocked: Iter leaked the read connection")
	}
}

func TestCollQuery_UpdateRollsBackOnError(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"u"}, Unique: true}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1, "u":1}`),
		anyenc.MustParseJson(`{"id":2, "u":2}`),
	))

	// $set u:9 succeeds for the first doc, then collides with it on the second,
	// so the update fails after having already written
	_, err = coll.Find(nil).Update(ctx, `{"$set": {"u": 9}}`)
	require.Error(t, err)

	// the write of the first doc must not survive the failed update
	doc, err := coll.FindId(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, float64(1), doc.Value().GetFloat64("u"))
}

func TestCollQuery_UpdateDeleteMarkTxModified(t *testing.T) {
	// Update/Delete run their own write tx. Without SetModified the tx is
	// released as EventReleaseWriteWithoutChanges, so the durability controller
	// never marks the db dirty for writes made through a query.
	insert := func(t *testing.T, fx *fixture, name string) Collection {
		coll, err := fx.CreateCollection(ctx, name)
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1, "a":1}`),
			anyenc.MustParseJson(`{"id":2, "a":2}`),
		))
		return coll
	}

	// white-box: run inside a caller-supplied tx so the flag is observable
	t.Run("update marks the tx", func(t *testing.T) {
		fx := newFixture(t)
		coll := insert(t, fx, "test")
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		res, err := coll.Find(nil).Update(tx.Context(), `{"$set": {"a": 42}}`)
		require.NoError(t, err)
		require.Equal(t, 2, res.Modified)
		assert.True(t, tx.(writeTx).modified, "tx not marked modified")
		require.NoError(t, tx.Commit())
	})

	t.Run("delete marks the tx", func(t *testing.T) {
		fx := newFixture(t)
		coll := insert(t, fx, "test")
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		res, err := coll.Find(nil).Delete(tx.Context())
		require.NoError(t, err)
		require.Equal(t, 2, res.Modified)
		assert.True(t, tx.(writeTx).modified, "tx not marked modified")
		require.NoError(t, tx.Commit())
	})

	t.Run("no match leaves the tx unmarked", func(t *testing.T) {
		fx := newFixture(t)
		coll := insert(t, fx, "test")
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		res, err := coll.Find(`{"a": 999}`).Update(tx.Context(), `{"$set": {"a": 42}}`)
		require.NoError(t, err)
		require.Equal(t, 0, res.Modified)
		assert.False(t, tx.(writeTx).modified, "tx marked modified without writes")
		require.NoError(t, tx.Commit())
	})

	// end-to-end: the sentinel file is the observable consequence of the
	// EventReleaseWriteWithChanges the standalone tx must emit
	t.Run("standalone update marks the db dirty", func(t *testing.T) {
		conf := &Config{Durability: DurabilityConfig{Sentinel: true}}
		tmpDir, err := os.MkdirTemp("", "any-store-*")
		require.NoError(t, err)
		dbPath := filepath.Join(tmpDir, "any-store-test.db")
		sentinelPath := dbPath + ".lock"

		fx := newFixturePath(t, tmpDir, conf)
		insert(t, fx, "test")
		// the inserts above mark the db dirty; closing clears the sentinel so
		// the query update below is the only thing that can set it again
		require.NoError(t, fx.DB.Close())
		require.NoFileExists(t, sentinelPath)

		reopened, err := Open(ctx, dbPath, conf)
		require.NoError(t, err)
		defer func() { _ = reopened.Close() }()
		coll, err := reopened.OpenCollection(ctx, "test")
		require.NoError(t, err)

		res, err := coll.Find(nil).Update(ctx, `{"$set": {"a": 42}}`)
		require.NoError(t, err)
		require.Equal(t, 2, res.Modified)
		assert.FileExists(t, sentinelPath, "query update did not mark the db dirty")
	})
}
