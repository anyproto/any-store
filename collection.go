package anystore

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/encoding"
	"github.com/anyproto/any-store/internal/key"
	"github.com/anyproto/any-store/internal/parser"
)

var (
	ErrDuplicatedId = errors.New("duplicated id")
)

type Collection struct {
	db              *DB
	dataNS, indexNS key.NS
	name            string
	mu              sync.RWMutex
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
	c.dataNS = key.NewNS(nsPrefix.Copy().AppendString(c.name).AppendString("data"))
	c.indexNS = key.NewNS(nsPrefix.Copy().AppendString(c.name).AppendString("index"))
	return
}

func (c *Collection) InsertOne(doc any) (docId any, err error) {
	p := parserPool.Get()
	defer parserPool.Put(p)

	it, err := parseItem(p, doc, true)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	err = c.db.db.Update(func(txn *badger.Txn) error {
		k := c.dataNS.Peek().AppendPart(it.id)
		_, getErr := txn.Get(k)
		if getErr == nil {
			idAny, _ := encoding.DecodeToAny(it.id)
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
	return encoding.DecodeToAny(it.id)
}

func (c *Collection) InsertMany(docs ...any) (result Result, err error) {
	p := parserPool.Get()
	defer parserPool.Put(p)

	var items = make([]item, 0, len(docs))

	for _, doc := range docs {
		it, err := parseItem(p, doc, true)
		if err != nil {
			return Result{}, err
		}
		items = append(items, it)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for len(items) != 0 {
		var handled int
		err = c.db.db.Update(func(txn *badger.Txn) error {
			for i, it := range items {
				k := c.dataNS.Peek().AppendPart(it.id).Copy()
				_, getErr := txn.Get(k)
				if getErr == nil {
					idAny, _ := encoding.DecodeToAny(it.id)
					return fmt.Errorf("%w: %v", ErrDuplicatedId, idAny)
				} else if getErr != badger.ErrKeyNotFound {
					return getErr
				}
				if err = txn.Set(k, it.val.MarshalTo(nil)); err != nil {
					if err == badger.ErrTxnTooBig {
						items = items[i:]
						handled = i
						return nil
					}
					return err
				}
				if err = c.handleInsertIndexes(txn, it); err != nil {
					if err == badger.ErrTxnTooBig {
						items = items[i:]
						handled = i
						return nil
					}
					return err
				}
			}
			handled = len(items)
			items = nil
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

func (c *Collection) UpsertOne(ctx context.Context, doc any) (docId string, err error) {
	return
}

func (c *Collection) UpsertMany(ctx context.Context, docs ...any) (result Result, err error) {
	return
}

func (c *Collection) UpdateId(ctx context.Context, docId string, update any) (err error) {
	return
}

func (c *Collection) UpdateMany(ctx context.Context, query, update any) (result Result, err error) {
	return
}

func (c *Collection) DeleteId(ctx context.Context, docId string) (err error) {
	return
}

func (c *Collection) DeleteMany(ctx context.Context, query any) (err error) {
	return
}

func (c *Collection) Indexes(ctx context.Context) (indexes []Index, err error) {
	return
}

func (c *Collection) AddIndex(ctx context.Context, index Index) (err error) {
	return
}

func (c *Collection) DropIndex(ctx context.Context, indexName string) (err error) {
	return
}

func (c *Collection) FindId(ctx context.Context, docId string) (item Item, err error) {
	return
}

func (c *Collection) FindMany(ctx context.Context, query any) (iterator Iterator, err error) {
	return
}

func (c *Collection) Len(ctx context.Context) (int, error) {
	return 0, nil
}

func (c *Collection) Count(query any) (count int, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	err = c.db.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchSize:   1000,
			PrefetchValues: false,
			Prefix:         c.dataNS.Peek(),
		})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	return
}

func (c *Collection) handleInsertIndexes(tx setter, it item) (err error) {
	return
}

func parseItem(p *fastjson.Parser, doc any, autoId bool) (it item, err error) {
	docJ, err := parser.AnyToJSON(p, doc)
	if err != nil {
		return item{}, err
	}
	return newItem(docJ, !autoId)
}
