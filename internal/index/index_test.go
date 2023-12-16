package index

import (
	"os"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/encoding"
	"github.com/anyproto/any-store/internal/key"
	"github.com/anyproto/any-store/internal/parser"
)

func TestIndex_Insert(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		fx := newFixture(t, Info{CollectionNS: key.NewNS("/test/collection"), Fields: []string{"a"}})
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
		fx := newFixture(t, Info{CollectionNS: key.NewNS("/test/collection"), Fields: []string{"a"}})
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

func BenchmarkIndex_fillKeysBuf(b *testing.B) {
	b.Run("simple", func(b *testing.B) {
		idx, err := OpenIndex(nil, Info{
			CollectionNS: key.NewNS("/test/namespace"),
			Fields:       []string{"a"},
			Sparse:       false,
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
			CollectionNS: key.NewNS("/test/namespace"),
			Fields:       []string{"a", "b"},
			Sparse:       false,
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

func newFixture(t *testing.T, i Info) *fixture {
	tmpDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	db, err := badger.Open(badger.DefaultOptions(tmpDir).WithLoggingLevel(badger.WARNING))
	require.NoError(t, err)
	fx := &fixture{
		tmpDir: tmpDir,
		db:     db,
	}
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		fx.Index, err = OpenIndex(txn, i)
		return err
	}))
	return fx
}

type fixture struct {
	tmpDir string
	db     *badger.DB
	*Index
}

func (fx *fixture) finish(t *testing.T) {
	require.NoError(t, fx.db.Close())
	_ = os.RemoveAll(fx.tmpDir)
}

func newTestDoc(t *testing.T, doc any) ([]byte, *fastjson.Value) {
	p := &fastjson.Parser{}
	val, err := parser.AnyToJSON(p, doc)
	require.NoError(t, err)
	id := val.Get("id")
	require.NotNil(t, id)
	return encoding.AppendJSONValue(nil, id), val
}
