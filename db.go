package anystore

import (
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
	db, err := badger.Open(options)
	if err != nil {
		return nil, err
	}
	return &DB{
		db: db,
	}, nil
}

type DB struct {
	db *badger.DB
}

func (db *DB) Collection(name string) (*Collection, error) {
	return newCollection(db, name)
}

func (db *DB) CollectionNames() (names []string, err error) {
	return
}

func (db *DB) Close() (err error) {
	return db.db.Close()
}
