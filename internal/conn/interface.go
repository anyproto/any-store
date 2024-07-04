package conn

import (
	"context"

	"zombiezen.com/go/sqlite"
)

type Stmt interface {
	Exec(ctx context.Context, bind func(stmt *sqlite.Stmt)) (err error)
	Query(ctx context.Context, result func(stmt *sqlite.Stmt) (err error)) (err error)
	Close() (err error)
}

type Conn interface {
	Exec(ctx context.Context, query string) (err error)
	Query(ctx context.Context, query string, result func(stmt *sqlite.Stmt) error) error
	Prepare(query string) (*sqlite.Stmt, error)
	BeginTx() (Tx, error)
	Close() error
}

type Tx interface {
	Commit() error
	Rollback() error
}
