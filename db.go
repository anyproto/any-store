package anystore

import "context"

type DB interface {
	WriteTx(ctx context.Context) WriteTx
	CreateCollection(ctx context.Context, collectionName string) (Collection, error)
	OpenCollection(ctx context.Context, collectionName string) (Collection, error)
	Close() error
}

func Open(ctx context.Context, path string, config *Config) (db DB, err error) {
	return
}
