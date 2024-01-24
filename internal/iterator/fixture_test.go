package iterator

import (
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"
	"golang.org/x/exp/rand"

	"github.com/anyproto/any-store/internal/encoding"
	"github.com/anyproto/any-store/internal/index"
	"github.com/anyproto/any-store/internal/key"
	"github.com/anyproto/any-store/internal/qcontext"
	"github.com/anyproto/any-store/internal/testdb"
)

type fixture struct {
	QCtx    *qcontext.QueryContext
	indexNS *key.NS
	*testdb.Fixture
}

func newFixture(t *testing.T) *fixture {
	dataNs := key.NewNS("test")
	indexNs := key.NewNS("testIndex")
	return &fixture{
		QCtx: &qcontext.QueryContext{
			DataNS: dataNs,
			Parser: &fastjson.Parser{},
		},
		indexNS: indexNs,
		Fixture: testdb.NewFixture(t),
	}
}

func fillTestIndex(t *testing.T, fx *fixture, n int) {
	require.NoError(t, fx.Update(func(txn *badger.Txn) error {
		idx, err := index.OpenIndex(txn, index.Info{
			IndexNS: fx.indexNS,
			Fields:  []string{"a"},
		})
		if err != nil {
			return err
		}
		if err = txn.Set(key.NewNS("indexTesa").GetKey().AppendAny(-1).AppendAny(-1), nil); err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			doc := fastjson.MustParse(fmt.Sprintf(`{"id": %d, "a": %v}`, i, i))
			id := encoding.AppendAnyValue(nil, i)
			if err = idx.Insert(txn, id, doc); err != nil {
				return err
			}
		}
		if err = txn.Set(key.NewNS("indexTesz").GetKey().AppendAny(99999).AppendAny(999999), nil); err != nil {
			return err
		}
		return nil
	}))
}

func fillTestData(t *testing.T, fx *fixture, n int) {
	require.NoError(t, fx.Update(func(txn *badger.Txn) error {
		idx, err := index.OpenIndex(txn, index.Info{
			IndexNS: fx.indexNS,
			Fields:  []string{"a"},
		})
		if err != nil {
			return err
		}
		if err = txn.Set(key.NewNS("indexTesa").GetKey().AppendAny(-1).AppendAny(-1), nil); err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			doc := fastjson.MustParse(fmt.Sprintf(`{"id": %d, "a": %v, "b": %v}`, i, i, rand.Float64()))
			id := encoding.AppendAnyValue(nil, i)
			if err = idx.Insert(txn, id, doc); err != nil {
				return err
			}
			if err = txn.Set(fx.QCtx.DataNS.GetKey().AppendAny(i), []byte(doc.String())); err != nil {
				return err
			}
		}
		if err = txn.Set(key.NewNS("indexTesz").GetKey().AppendAny(99999).AppendAny(999999), nil); err != nil {
			return err
		}
		return nil
	}))
}

func toAny(t *testing.T, b []byte) any {
	v, _, err := encoding.DecodeToAny(b)
	require.NoError(t, err)
	return v
}
