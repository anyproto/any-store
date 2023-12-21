package iterator

import (
	"errors"

	"github.com/dgraph-io/badger/v4"

	"github.com/anyproto/any-store/internal/key"
)

type InIterator struct {
	Txn  *badger.Txn
	Keys []key.Key
	IdIterator
	err error
}

func (ii *InIterator) Next() bool {
	for ii.IdIterator.Next() {
		for i, k := range ii.Keys {
			var kl = len(k)
			k = append(k, ii.Values()[len(ii.Values())-1]...)
			_, err := ii.Txn.Get(k)
			ii.Keys[i] = k[:kl]
			switch err {
			case badger.ErrKeyNotFound:
				continue
			case nil:
				return true
			default:
				ii.err = err
				return false
			}
		}
	}
	return false
}

func (ii *InIterator) Close() (err error) {
	if e := ii.IdIterator.Close(); e != nil {
		err = errors.Join(err, e)
	}
	return errors.Join(err, ii.err)
}
