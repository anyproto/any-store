package anystore

import (
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/key"
)

var (
	nsPrefix = key.NewNS("/any")

	arenaPool  = &fastjson.ArenaPool{}
	parserPool = &fastjson.ParserPool{}
)

func Open(path string) (*DB, error) {
	options := badger.DefaultOptions(path).WithLoggingLevel(badger.WARNING)

	options = options.WithIndexCacheSize(100 << 20)
	options.NumMemtables = 1
	options.MemTableSize = 32 << 20
	options = options.WithBaseTableSize(1 << 20)
	options = options.WithBlockCacheSize(1 << 20)
	return OpenBadgerOptions(options)
}

func OpenBadgerOptions(options badger.Options) (*DB, error) {
	bdb, err := badger.Open(options)
	if err != nil {
		return nil, err
	}
	db := &DB{
		db:                bdb,
		openedCollections: make(map[string]*Collection),
	}
	db.system, err = newSystemCollection(db)
	if err != nil {
		return nil, err
	}
	return db, nil
}

type DB struct {
	db                *badger.DB
	system            *systemCollection
	openedCollections map[string]*Collection
	mu                sync.Mutex
}

func (db *DB) Collection(name string) (*Collection, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	coll, ok := db.openedCollections[name]
	if ok {
		return coll, nil
	}
	coll, err := newCollection(db, name)
	if err != nil {
		return nil, err
	}
	db.openedCollections[name] = coll
	return coll, nil
}

func (db *DB) Close() (err error) {
	return db.db.Close()
}
