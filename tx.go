package anystore

import "context"

type WriteTx interface {
	ReadTx
	Rollback(ctx context.Context) error
}

type ReadTx interface {
	Context() context.Context
	Commit(ctx context.Context) error
}
