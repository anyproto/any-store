package index

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/encoding"
	"github.com/anyproto/any-store/internal/key"
	"github.com/anyproto/any-store/internal/parser"
	"github.com/anyproto/any-store/internal/testdb"
)

func TestIndex_Insert(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		fx := newFixture(t, Info{IndexNS: key.NewNS("/test/collection"), Fields: []string{"a"}})
		defer fx.finish(t)
		require.NoError(t, fx.db.Update(func(txn *badger.Txn) error {
			for i := 0; i < 10; i++ {
				id, v := newTestDoc(t, map[string]int{"id": i, "a": i % 5})
				if err := fx.Insert(txn, id, v); err != nil {
					return err
				}
			}
			return nil
		}))
		assert.Equal(t, uint64(5), fx.stats.bitmap.GetCardinality())
		assert.Equal(t, uint64(10), fx.stats.count)
	})
	t.Run("array", func(t *testing.T) {
		fx := newFixture(t, Info{IndexNS: key.NewNS("/test/collection"), Fields: []string{"a"}})
		defer fx.finish(t)
		require.NoError(t, fx.db.Update(func(txn *badger.Txn) error {
			for i := 0; i < 10; i++ {
				id, v := newTestDoc(t, map[string]any{"id": i, "a": []int{1, 2, 3, 4, 5}})
				if err := fx.Insert(txn, id, v); err != nil {
					return err
				}
			}
			return nil
		}))
		assert.Equal(t, uint64(5), fx.stats.bitmap.GetCardinality())
		assert.Equal(t, uint64(50), fx.stats.count)
	})
}

func TestInfo_fillKeysBuf(t *testing.T) {
	t.Run("not sparse", func(t *testing.T) {
		idx, err := OpenIndex(nil, Info{
			IndexNS: key.NewNS("/test/namespace"),
			Fields:  []string{"a"},
			Sparse:  false,
		})
		require.NoError(t, err)
		idx.fillKeysBuf(fastjson.MustParse(`{"a":1}`))
		assert.Len(t, idx.keysBuf, 1)
	})
	t.Run("sparse", func(t *testing.T) {
		idx, err := OpenIndex(nil, Info{
			IndexNS: key.NewNS("/test/namespace"),
			Fields:  []string{"a"},
			Sparse:  true,
		})
		require.NoError(t, err)
		idx.fillKeysBuf(fastjson.MustParse(`{"b":1}`))
		assert.Len(t, idx.keysBuf, 0)
	})
}
func BenchmarkIndex_fillKeysBuf(b *testing.B) {
	b.Run("simple", func(b *testing.B) {
		idx, err := OpenIndex(nil, Info{
			IndexNS: key.NewNS("/test/namespace"),
			Fields:  []string{"a"},
			Sparse:  false,
		})
		require.NoError(b, err)
		v := fastjson.MustParse(`{"a":"1", "b":"2"}`)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx.fillKeysBuf(v)
		}
	})
	b.Run("arrays", func(b *testing.B) {
		idx, err := OpenIndex(nil, Info{
			IndexNS: key.NewNS("/test/namespace"),
			Fields:  []string{"a", "b"},
			Sparse:  false,
		})
		require.NoError(b, err)
		v := fastjson.MustParse(`{"a":["1", "2", "3"], "b":["4", "5"]}`)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx.fillKeysBuf(v)
		}
	})
}

func TestIndex_Update(t *testing.T) {
	fx := newFixture(t, Info{IndexNS: key.NewNS("/test/update"), Fields: []string{"a"}})
	defer fx.finish(t)

	require.NoError(t, fx.db.Update(func(txn *badger.Txn) error {
		id, prev := newTestDoc(t, map[string]any{"id": 1, "a": []int{1, 2, 3}})
		if err := fx.Insert(txn, id, prev); err != nil {
			return err
		}
		assertKeys(t, txn, fx, []key.Key{
			fx.dataNS.GetKey().AppendAny(1).AppendAny(1),
			fx.dataNS.GetKey().AppendAny(2).AppendAny(1),
			fx.dataNS.GetKey().AppendAny(3).AppendAny(1),
		})
		_, d := newTestDoc(t, map[string]any{"id": 1, "a": []int{1, 3, 4}})

		if err := fx.Update(txn, id, prev, d); err != nil {
			return err
		}
		assertKeys(t, txn, fx, []key.Key{
			fx.dataNS.GetKey().AppendAny(1).AppendAny(1),
			fx.dataNS.GetKey().AppendAny(3).AppendAny(1),
			fx.dataNS.GetKey().AppendAny(4).AppendAny(1),
		})
		return nil
	}))
}

func TestIndex_FlushStats(t *testing.T) {
	info := Info{IndexNS: key.NewNS("/test/collection"), Fields: []string{"a"}}
	fx := newFixture(t, info)
	defer fx.finish(t)
	require.NoError(t, fx.db.Update(func(txn *badger.Txn) error {
		id, v := newTestDoc(t, map[string]any{"id": "id", "a": []int{1, 2, 3}})
		if err := fx.Insert(txn, id, v); err != nil {
			return err
		}
		assert.Equal(t, uint64(3), fx.stats.bitmap.GetCardinality())
		assert.Equal(t, uint64(3), fx.stats.count)

		require.NoError(t, fx.FlushStats(txn))

		idx, err := OpenIndex(txn, info)
		require.NoError(t, err)
		assert.Equal(t, uint64(3), idx.stats.bitmap.GetCardinality())
		assert.Equal(t, uint64(3), idx.stats.count)
		return nil
	}))
}

func TestIndex_Drop(t *testing.T) {
	info := Info{IndexNS: key.NewNS("/test/collection"), Fields: []string{"a"}}
	fx := newFixture(t, info)
	defer fx.finish(t)

	require.NoError(t, fx.db.Update(func(txn *badger.Txn) error {
		id, prev := newTestDoc(t, map[string]any{"id": 1, "a": []int{1, 2, 3}})
		if err := fx.Insert(txn, id, prev); err != nil {
			return err
		}
		assertKeys(t, txn, fx, []key.Key{
			fx.dataNS.GetKey().AppendAny(1).AppendAny(1),
			fx.dataNS.GetKey().AppendAny(2).AppendAny(1),
			fx.dataNS.GetKey().AppendAny(3).AppendAny(1),
		})

		if err := fx.Drop(txn); err != nil {
			return err
		}
		assertKeys(t, txn, fx, nil)
		return nil
	}))
}

func newFixture(t *testing.T, i Info) *fixture {
	fx := &fixture{
		db: testdb.NewFixture(t),
	}
	require.NoError(t, fx.db.View(func(txn *badger.Txn) error {
		var err error
		fx.Index, err = OpenIndex(txn, i)
		return err
	}))
	return fx
}

type fixture struct {
	db *testdb.Fixture
	*Index
}

func (fx *fixture) finish(t *testing.T) {
	fx.db.Finish(t)
}

func newTestDoc(t *testing.T, doc any) ([]byte, *fastjson.Value) {
	p := &fastjson.Parser{}
	val, err := parser.AnyToJSON(p, doc)
	require.NoError(t, err)
	id := val.Get("id")
	require.NotNil(t, id)
	return encoding.AppendJSONValue(nil, id), val
}

func assertKeys(t *testing.T, txn *badger.Txn, fx *fixture, keys []key.Key) {
	indexKeys, err := fx.keys(txn)
	require.NoError(t, err)
	if !assert.Equal(t, indexKeys, keys) {
		for i := range keys {
			t.Log("exp", keys[i].String())
			t.Log("act", indexKeys[i].String())
		}
	}
}
