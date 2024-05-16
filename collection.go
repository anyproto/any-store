package anystore

import "context"

type Collection interface {
	Name() string

	FindId(ctx context.Context, id any) (Doc, error)
	Query() Query

	Insert(ctx context.Context, doc ...any) (err error)

	UpdateId(ctx context.Context, id, doc any) (err error)
	UpsertId(ctx context.Context, id, doc any) (err error)

	EnsureIndex(ctx context.Context, info ...IndexInfo) (err error)
	DropIndex(ctx context.Context, indexName string) (err error)

	Rename(ctx context.Context, newName string) (err error)
	Drop(ctx context.Context) (err error)

	Close() error
}
