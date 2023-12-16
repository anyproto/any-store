package anystore

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/encoding"
	"github.com/anyproto/any-store/internal/index"
	"github.com/anyproto/any-store/internal/key"
	"github.com/anyproto/any-store/internal/parser"
	"github.com/anyproto/any-store/query"
)

var (
	ErrDuplicatedId     = errors.New("duplicated id")
	ErrDocumentNotFound = errors.New("document not found")
)

type Collection struct {
	db     *DB
	dataNS *key.NS
	name   string
	mu     sync.RWMutex
}

type Result struct {
	AffectedRows int
}

type setter interface {
	Set(key, value []byte) error
}

func newCollection(db *DB, name string) (c *Collection, err error) {
	c = &Collection{db: db, name: name}
	if err = c.init(); err != nil {
		return nil, err
	}
	return
}

func (c *Collection) init() (err error) {
	c.dataNS = key.NewNS(nsPrefix.String() + "/" + c.name)
	return
}

func (c *Collection) InsertOne(doc any) (docId any, err error) {
	p := parserPool.Get()
	defer parserPool.Put(p)
	a := arenaPool.Get()
	defer arenaPool.Put(a)

	it, err := parseItem(p, a, doc, true)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	err = c.db.db.Update(func(txn *badger.Txn) error {
		var k key.Key
		c.dataNS.ReuseKey(func(rk key.Key) key.Key {
			k = it.appendId(rk)
			return k
		})
		_, getErr := txn.Get(k)
		if getErr == nil {
			idAny, _, _ := encoding.DecodeToAny(it.appendId(nil))
			return fmt.Errorf("%w: %v", ErrDuplicatedId, idAny)
		} else if getErr != badger.ErrKeyNotFound {
			return getErr
		}
		if setErr := txn.Set(k, it.val.MarshalTo(nil)); setErr != nil {
			return setErr
		}
		if idxErr := c.handleInsertIndexes(txn, it); idxErr != nil {
			return idxErr
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	docId, _, err = encoding.DecodeToAny(it.appendId(nil))
	return
}

func (c *Collection) InsertMany(docs ...any) (result Result, err error) {
	p := parserPool.Get()
	defer parserPool.Put(p)
	a := arenaPool.Get()
	defer arenaPool.Put(a)

	c.mu.Lock()
	defer c.mu.Unlock()

	for len(docs) != 0 {

		var handled int
		err = c.db.db.Update(func(txn *badger.Txn) (err error) {
			var (
				i   int
				it  item
				doc any
			)
			defer func() {
				if err == badger.ErrTxnTooBig {
					docs = docs[i:]
					handled = i
					err = nil
				}
			}()
			for i, doc = range docs {
				if it, err = parseItem(p, a, doc, true); err != nil {
					return err
				}
				k := key.Key(it.appendId(c.dataNS.GetKey()))
				_, getErr := txn.Get(k)
				if getErr == nil {
					idAny, _, _ := encoding.DecodeToAny(it.appendId(nil))
					return fmt.Errorf("%w: %v", ErrDuplicatedId, idAny)
				} else if getErr != badger.ErrKeyNotFound {
					return getErr
				}
				if err = txn.Set(k, it.val.MarshalTo(nil)); err != nil {
					return
				}
				if err = c.handleInsertIndexes(txn, it); err != nil {
					return err
				}
			}
			handled = len(docs)
			docs = nil
			return nil
		})
		if err != nil {
			return
		} else {
			result.AffectedRows += handled
		}
	}
	return
}

func (c *Collection) UpsertOne(doc any) (docId any, err error) {
	p := parserPool.Get()
	defer parserPool.Put(p)
	a := arenaPool.Get()
	defer arenaPool.Put(a)

	it, err := parseItem(p, a, doc, true)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	err = c.db.db.Update(func(txn *badger.Txn) error {
		var k key.Key
		c.dataNS.ReuseKey(func(rk key.Key) key.Key {
			k = it.appendId(rk)
			return k
		})
		res, getErr := txn.Get(k)
		var prevValue item
		if getErr == nil {
			if err = res.Value(func(val []byte) error {
				prevValue.val, err = p.ParseBytes(val)
				return err
			}); err != nil {
				return err
			}
		} else if getErr != badger.ErrKeyNotFound {
			return getErr
		}
		if setErr := txn.Set(k, it.val.MarshalTo(nil)); setErr != nil {
			return setErr
		}
		if prevValue.val == nil {
			if idxErr := c.handleInsertIndexes(txn, it); idxErr != nil {
				return idxErr
			}
		} else {
			if idxErr := c.handleUpdateIndexes(txn, prevValue, it); idxErr != nil {
				return idxErr
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	docId, _, err = encoding.DecodeToAny(it.appendId(nil))
	return
}

func (c *Collection) UpsertMany(docs ...any) (result Result, err error) {
	p := parserPool.Get()
	defer parserPool.Put(p)
	a := arenaPool.Get()
	defer arenaPool.Put(a)

	c.mu.Lock()
	defer c.mu.Unlock()

	for len(docs) != 0 {
		var handled int
		err = c.db.db.Update(func(txn *badger.Txn) (err error) {
			var (
				i   int
				it  item
				doc any
			)
			defer func() {
				if err == badger.ErrTxnTooBig {
					docs = docs[i:]
					handled = i
					err = nil
				}
			}()
			for i, doc = range docs {
				if it, err = parseItem(p, a, doc, true); err != nil {
					return
				}
				k := key.Key(it.appendId(c.dataNS.GetKey()))
				res, getErr := txn.Get(k)
				var prevValue item
				if getErr == nil {
					if err = res.Value(func(val []byte) error {
						prevValue.val, err = p.ParseBytes(val)
						return err
					}); err != nil {
						return err
					}
				} else if getErr != badger.ErrKeyNotFound {
					return getErr
				}
				if err = txn.Set(k, it.val.MarshalTo(nil)); err != nil {
					return
				}

				if prevValue.val == nil {
					if err = c.handleInsertIndexes(txn, it); err != nil {
						return
					}
				} else {
					if err = c.handleUpdateIndexes(txn, prevValue, it); err != nil {
						return
					}
				}
			}
			handled = len(docs)
			docs = nil
			return nil
		})
		if err != nil {
			return
		} else {
			result.AffectedRows += handled
		}
	}
	return
}

func (c *Collection) UpdateId(ctx context.Context, docId string, update any) (err error) {
	return
}

func (c *Collection) UpdateMany(ctx context.Context, query, update any) (result Result, err error) {
	return
}

func (c *Collection) DeleteId(docId any) (err error) {
	p := parserPool.Get()
	defer parserPool.Put(p)

	c.mu.Lock()
	defer c.mu.Unlock()

	return c.db.db.Update(func(txn *badger.Txn) error {
		var k key.Key
		c.dataNS.ReuseKey(func(rk key.Key) key.Key {
			k = rk.AppendAny(docId)
			return k
		})
		res, err := txn.Get(k)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return ErrDocumentNotFound
			} else {
				return err
			}
		}
		var it = item{}
		if err = res.Value(func(val []byte) error {
			it.val, err = fastjson.ParseBytes(val)
			return err
		}); err != nil {
			return err
		}
		if err = txn.Delete(k); err != nil {
			return err
		}
		if err = c.handleDeleteIndexes(txn, it); err != nil {
			return err
		}
		return nil
	})
}

func (c *Collection) DeleteMany(ctx context.Context, query any) (err error) {
	return
}

func (c *Collection) Indexes(ctx context.Context) (indexes []index.Index, err error) {
	return
}

func (c *Collection) AddIndex(ctx context.Context, index index.Index) (err error) {
	return
}

func (c *Collection) DropIndex(ctx context.Context, indexName string) (err error) {
	return
}

func (c *Collection) FindId(docId any) (res Item, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	err = c.db.db.View(func(txn *badger.Txn) error {
		k := c.dataNS.GetKey().AppendAny(docId)
		it, err := txn.Get(k)
		if err != nil {
			return err
		}

		p := parserPool.Get()
		defer parserPool.Put(p)

		return it.Value(func(val []byte) error {
			jval, err := p.ParseBytes(val)
			if err != nil {
				return err
			}
			res = item{
				val: jval,
			}
			return nil
		})
	})
	if err == badger.ErrKeyNotFound {
		err = ErrDocumentNotFound
	}
	return
}

func (c *Collection) FindMany(q any) (iterator Iterator, err error) {
	filter, err := query.ParseCondition(q)
	if err != nil {
		return nil, err
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	iter := newDirectIterator()
	iter.txn = c.db.db.NewTransaction(false)
	iter.it = iter.txn.NewIterator(badger.IteratorOptions{
		PrefetchSize:   100,
		PrefetchValues: true,
		Prefix:         c.dataNS.Bytes(),
	})
	iter.it.Rewind()
	iter.filter = filter
	return iter, nil
}

func (c *Collection) Count(query any) (count int, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	err = c.db.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchSize:   1000,
			PrefetchValues: false,
			Prefix:         c.dataNS.Bytes(),
		})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	return
}

func (c *Collection) handleInsertIndexes(txn *badger.Txn, it item) (err error) {
	return
}

func (c *Collection) handleDeleteIndexes(txn *badger.Txn, it item) (err error) {
	return
}

func (c *Collection) handleUpdateIndexes(txn *badger.Txn, prev, new item) (err error) {
	return
}

func parseItem(p *fastjson.Parser, a *fastjson.Arena, doc any, autoId bool) (it item, err error) {
	docJ, err := parser.AnyToJSON(p, doc)
	if err != nil {
		return item{}, err
	}
	return newItem(docJ, a, autoId)
}
