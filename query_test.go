package anystore

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollQuery_Count(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, `{"a":1}`, `{"a":2}`, `{"a":3}`, `{"a":4}`, `{"a":5}`))

	t.Run("no filter", func(t *testing.T) {
		assertQueryCount(t, coll.Query(), 5)
	})

	t.Run("filter", func(t *testing.T) {
		assertQueryCount(t, coll.Query().Cond(`{"a":{"$in":[2,3,4]}}`), 3)
	})

}

func TestCollQuery_Explain(t *testing.T) {
	fx := newFixture(t)

	assertExplain := func(t testing.TB, q Query, expQuery, expExplain string) {
		query, explain, err := q.Explain(ctx)
		require.NoError(t, err, query)
		if expQuery != "" {
			assert.Equal(t, strings.TrimSpace(expQuery), strings.TrimSpace(query))
		} else {
			t.Log(query)
		}
		if expExplain != "" {
			assert.Equal(t, strings.TrimSpace(expExplain), strings.TrimSpace(explain))
		} else {
			t.Log(explain)
		}
	}

	t.Run("no index", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx, `{"id":1,"a":"a1"}`, `{"id":2, "a":"a2"}`, `{"id":3, "a":"a3"}`, `{"id":4, "a":"a4"}`, `{"id":5, "a":"a5"}`))

		assertExplain(t, coll.Query(),
			"SELECT data FROM '_test_docs'",
			"SCAN _test_docs",
		)
		assertExplain(t, coll.Query().Cond(`{"id":4}`),
			"SELECT data FROM '_test_docs' WHERE  ((id = :val_0_0_0)) AND any_filter(1, data)",
			"SEARCH _test_docs USING INDEX sqlite_autoindex__test_docs_1 (id=?)",
		)
		assertExplain(t, coll.Query().Cond(`{"id":{"$gt":2}}`),
			"SELECT data FROM '_test_docs' WHERE  ((id > :val_0_0_0)) AND any_filter(1, data)",
			"SEARCH _test_docs USING INDEX sqlite_autoindex__test_docs_1 (id>?)",
		)
		assertExplain(t, coll.Query().Cond(`{"id":{"$gte":2}}`),
			"SELECT data FROM '_test_docs' WHERE  ((id >= :val_0_0_0)) AND any_filter(1, data)",
			"SEARCH _test_docs USING INDEX sqlite_autoindex__test_docs_1 (id>?)",
		)
		assertExplain(t, coll.Query().Cond(`{"id":{"$lt":2}}`),
			"SELECT data FROM '_test_docs' WHERE  ((id < :val_0_0_0_end)) AND any_filter(1, data)",
			"SEARCH _test_docs USING INDEX sqlite_autoindex__test_docs_1 (id<?)",
		)
		assertExplain(t, coll.Query().Cond(`{"id":{"$lte":2}}`),
			"SELECT data FROM '_test_docs' WHERE  ((id <= :val_0_0_0_end)) AND any_filter(1, data)",
			"SEARCH _test_docs USING INDEX sqlite_autoindex__test_docs_1 (id<?)",
		)
		assertExplain(t, coll.Query().Cond(`{"id":{"$in":[1,2]}}`),
			"SELECT data FROM '_test_docs' WHERE  ((id = :val_0_0_0) OR (id = :val_0_0_1)) AND any_filter(1, data)",
			"SEARCH _test_docs USING INDEX sqlite_autoindex__test_docs_1 (id=?)",
		)
		assertExplain(t, coll.Query().Cond(`{"id":{"$nin":[1,2]}}`),
			"SELECT data FROM '_test_docs' WHERE any_filter(1, data)",
			"SCAN _test_docs",
		)
		assertExplain(t, coll.Query().Cond(`{"$or":[{"id":{"$gt":3}}, {"id":{"$lt":2}}]}`),
			"SELECT data FROM '_test_docs' WHERE  ((id < :val_0_0_0_end) OR (id > :val_0_0_1)) AND any_filter(1, data)",
			"SCAN _test_docs",
		)
		assertExplain(t, coll.Query().Limit(5).Offset(3),
			"SELECT data FROM '_test_docs'  LIMIT 5 OFFSET 3",
			"SCAN _test_docs",
		)
		assertExplain(t, coll.Query().Sort("-a"),
			"SELECT data FROM '_test_docs'  ORDER BY any_sort(1, data)",
			"SCAN _test_docs\nUSE TEMP B-TREE FOR ORDER BY",
		)
		assertExplain(t, coll.Query().Sort("-id"),
			"SELECT data FROM '_test_docs'  ORDER BY id DESC",
			"SCAN _test_docs USING INDEX sqlite_autoindex__test_docs_1",
		)
	})
	t.Run("simple index", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_s")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx, `{"id":1, "a":"a1", "b":"b1"}`, `{"id":2, "a":"a2"}`, `{"id":3, "a":"a3"}`, `{"id":4, "a":"a4"}`, `{"id":5, "a":"a5"}`))
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

		assertExplain(t, coll.Query(),
			"SELECT data FROM '_test_s_docs'",
			"SCAN _test_s_docs",
		)
		assertExplain(t, coll.Query().Cond(`{"a":"a4"}`),
			"SELECT data FROM '_test_s_docs' JOIN '_test_s_a_idx' ON '_test_s_a_idx'.docId = id WHERE  (('_test_s_a_idx'.val0 = :val_1_0_0)) AND any_filter(1, data)",
			"SEARCH _test_s_a_idx USING COVERING INDEX sqlite_autoindex__test_s_a_idx_1 (val0=?)\nSEARCH _test_s_docs USING INDEX sqlite_autoindex__test_s_docs_1 (id=?)",
		)
		assertExplain(t, coll.Query().Cond(`{"a":{"$gt":1}}`),
			"SELECT data FROM '_test_s_docs' JOIN '_test_s_a_idx' ON '_test_s_a_idx'.docId = id WHERE  (('_test_s_a_idx'.val0 > :val_1_0_0)) AND any_filter(1, data)",
			"SEARCH _test_s_a_idx USING COVERING INDEX sqlite_autoindex__test_s_a_idx_1 (val0>?)\nSEARCH _test_s_docs USING INDEX sqlite_autoindex__test_s_docs_1 (id=?)",
		)
		assertExplain(t, coll.Query().Cond(`{"a":{"$gte":1}}`),
			"SELECT data FROM '_test_s_docs' JOIN '_test_s_a_idx' ON '_test_s_a_idx'.docId = id WHERE  (('_test_s_a_idx'.val0 >= :val_1_0_0)) AND any_filter(1, data)",
			"SEARCH _test_s_a_idx USING COVERING INDEX sqlite_autoindex__test_s_a_idx_1 (val0>?)\nSEARCH _test_s_docs USING INDEX sqlite_autoindex__test_s_docs_1 (id=?)",
		)
		assertExplain(t, coll.Query().Cond(`{"a":{"$lt":1}}`),
			"SELECT data FROM '_test_s_docs' JOIN '_test_s_a_idx' ON '_test_s_a_idx'.docId = id WHERE  (('_test_s_a_idx'.val0 < :val_1_0_0_end)) AND any_filter(1, data)",
			"SEARCH _test_s_a_idx USING COVERING INDEX sqlite_autoindex__test_s_a_idx_1 (val0<?)\nSEARCH _test_s_docs USING INDEX sqlite_autoindex__test_s_docs_1 (id=?)",
		)
		assertExplain(t, coll.Query().Cond(`{"a":{"$lte":1}}`),
			"SELECT data FROM '_test_s_docs' JOIN '_test_s_a_idx' ON '_test_s_a_idx'.docId = id WHERE  (('_test_s_a_idx'.val0 <= :val_1_0_0_end)) AND any_filter(1, data)",
			"SEARCH _test_s_a_idx USING COVERING INDEX sqlite_autoindex__test_s_a_idx_1 (val0<?)\nSEARCH _test_s_docs USING INDEX sqlite_autoindex__test_s_docs_1 (id=?)",
		)
		assertExplain(t, coll.Query().Sort("a"),
			"SELECT data FROM '_test_s_docs' JOIN '_test_s_a_idx' ON '_test_s_a_idx'.docId = id  ORDER BY '_test_s_a_idx'.val0",
			"SCAN _test_s_a_idx USING COVERING INDEX sqlite_autoindex__test_s_a_idx_1\nSEARCH _test_s_docs USING INDEX sqlite_autoindex__test_s_docs_1 (id=?)",
		)
		assertExplain(t, coll.Query().Sort("-a"),
			"SELECT data FROM '_test_s_docs' JOIN '_test_s_a_idx' ON '_test_s_a_idx'.docId = id  ORDER BY '_test_s_a_idx'.val0 DESC",
			"SCAN _test_s_a_idx USING COVERING INDEX sqlite_autoindex__test_s_a_idx_1\nSEARCH _test_s_docs USING INDEX sqlite_autoindex__test_s_docs_1 (id=?)",
		)
		assertExplain(t, coll.Query().Sort("a", "id"),
			"SELECT data FROM '_test_s_docs' JOIN '_test_s_a_idx' ON '_test_s_a_idx'.docId = id  ORDER BY '_test_s_a_idx'.val0, id",
			"SCAN _test_s_a_idx USING COVERING INDEX sqlite_autoindex__test_s_a_idx_1\nSEARCH _test_s_docs USING INDEX sqlite_autoindex__test_s_docs_1 (id=?)\nUSE TEMP B-TREE FOR RIGHT PART OF ORDER BY",
		)
		assertExplain(t, coll.Query().Sort("a", "id", "b"),
			"SELECT data FROM '_test_s_docs' JOIN '_test_s_a_idx' ON '_test_s_a_idx'.docId = id  ORDER BY '_test_s_a_idx'.val0, id, any_sort(1, data)",
			"SCAN _test_s_a_idx USING COVERING INDEX sqlite_autoindex__test_s_a_idx_1\nSEARCH _test_s_docs USING INDEX sqlite_autoindex__test_s_docs_1 (id=?)\nUSE TEMP B-TREE FOR RIGHT PART OF ORDER BY",
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
	require.NoError(t, coll.Insert(ctx, `{"id":1,"a":1}`, `{"id":2,"a":1}`, `{"id":3,"a":1}`, `{"id":4,"a":1}`))

	assertQueryCount(t, coll.Query().Cond(`{"a":1}`), 4)

	require.NoError(t, coll.Query().Cond(`{"id":{"$in":[1,3]}}`).Update(ctx, `{"$inc":{"a":1}}`))

	assertQueryCount(t, coll.Query().Cond(`{"a":1}`), 2)
}
