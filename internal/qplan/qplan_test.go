package qplan

import (
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/encoding"
	"github.com/anyproto/any-store/internal/index"
	"github.com/anyproto/any-store/internal/key"
	"github.com/anyproto/any-store/internal/qcontext"
	"github.com/anyproto/any-store/internal/sort"
	"github.com/anyproto/any-store/internal/testdb"
	"github.com/anyproto/any-store/query"
)

func TestQPlan_Make(t *testing.T) {
	t.Run("empty query empty sort", func(t *testing.T) {
		qp := QPlan{}
		it := qp.Make(testQCtx(), true)
		assert.Equal(t, "SCAN(id)", it.String())
	})
	t.Run("empty query but sort", func(t *testing.T) {
		qp := QPlan{
			Sort: sort.MustParseSort("a"),
		}
		it := qp.Make(testQCtx(), true)
		assert.Equal(t, "SORT(SCAN(id))", it.String())
	})
	t.Run("id bound", func(t *testing.T) {
		qp := QPlan{
			Condition: query.MustParseCondition(`{"id":5}`),
		}
		it := qp.Make(testQCtx(), true)
		assert.Equal(t, "SCAN(id, Bounds{['5','5']})", it.String())
	})
	t.Run("id bound, id sort rev", func(t *testing.T) {
		qp := QPlan{
			Condition: query.MustParseCondition(`{"id":5}`),
			Sort:      sort.MustParseSort("-id"),
		}
		it := qp.Make(testQCtx(), true)
		assert.Equal(t, "SCAN(id, Bounds{['5','5']}, rev)", it.String())
	})
	t.Run("id bound, complex sort rev", func(t *testing.T) {
		qp := QPlan{
			Condition: query.MustParseCondition(`{"id":5}`),
			Sort:      sort.MustParseSort("id", "a"),
		}
		it := qp.Make(testQCtx(), true)
		assert.Equal(t, "SORT(SCAN(id, Bounds{['5','5']}))", it.String())
	})
	t.Run("one index filter", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.Finish(t)

		idx := fx.createIndex(t, "a", 10, 5)

		qp := QPlan{
			Indexes:   []*index.Index{idx},
			Condition: query.MustParseCondition(`{"a": {"$gt":5}}`),
		}
		it := qp.Make(testQCtx(), true)
		assert.Equal(t, "FETCH(INDEX(a, Bounds{('5',inf]}))", it.String())
	})
}

func testQCtx() *qcontext.QueryContext {
	return &qcontext.QueryContext{
		Txn:    &badger.Txn{},
		DataNS: key.NewNS("test"),
		Parser: &fastjson.Parser{},
	}
}

func newFixture(t *testing.T) *fixture {
	return &fixture{
		Fixture: testdb.NewFixture(t),
	}
}

type fixture struct {
	*testdb.Fixture
}

func (fx *fixture) createIndex(t *testing.T, field string, count, bitmap int) (idx *index.Index) {
	require.NoError(t, fx.Update(func(txn *badger.Txn) error {
		var err error
		idx, err = index.OpenIndex(txn, index.Info{
			IndexNS: key.NewNS("/testindex/" + field),
			Fields:  []string{field},
		})
		require.NoError(t, err)
		for i := 0; i < count; i++ {
			doc := fastjson.MustParse(fmt.Sprintf(`{"id": %d, "%s": %d}`, i, field, i%bitmap))
			require.NoError(t, idx.Insert(txn, encoding.AppendJSONValue(nil, doc.Get("id")), doc))
		}
		return nil
	}))
	return
}
