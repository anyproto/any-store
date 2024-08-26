package driver

import (
	"context"
	"errors"

	"zombiezen.com/go/sqlite"
)

type Stmt struct {
	conn *sqlite.Conn
	stmt *sqlite.Stmt
}

func (s Stmt) Exec(ctx context.Context, bind func(stmt *sqlite.Stmt), result func(stmt *sqlite.Stmt) error) (err error) {
	defer func() {
		err = errors.Join(err, s.stmt.Reset())
		err = errors.Join(err, s.stmt.ClearBindings())
	}()
	if ctx.Done() != nil {
		s.conn.SetInterrupt(ctx.Done())
		defer s.conn.SetInterrupt(nil)
	}
	if bind != nil {
		bind(s.stmt)
	}
	if err = result(s.stmt); err != nil {
		return
	}
	return
}

func (s Stmt) Close() error {
	return s.stmt.Finalize()
}

func StmtExecNoResults(stmt *sqlite.Stmt) (err error) {
	_, err = stmt.Step()
	return
}
