package qcontext

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/key"
)

type QueryContext struct {
	Txn    *badger.Txn
	DataNS *key.NS
	Parser *fastjson.Parser
}

func (qc *QueryContext) Fetch(id []byte, onValue func(b []byte) error) (err error) {
	var it *badger.Item
	qc.DataNS.ReuseKey(func(k key.Key) key.Key {
		k = append(k, id...)
		it, err = qc.Txn.Get(k)
		return k
	})
	if err != nil {
		return
	}
	if onValue != nil {
		return it.Value(onValue)
	}
	return
}
