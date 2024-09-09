package driver

import (
	"context"
	"errors"

	"zombiezen.com/go/sqlite"
)

type Stmt struct {
	conn *Conn
	stmt *sqlite.Stmt
}

func (s Stmt) Exec(ctx context.Context, bind func(stmt *sqlite.Stmt), result func(stmt *sqlite.Stmt) error) (err error) {
	defer func() {
		if s.conn.IsClosed() {
			err = errors.Join(err, ErrDBIsClosed)
			return
		}
		if rErr := s.stmt.ClearBindings(); rErr != nil {
			if err == nil {
				err = rErr
			}
			return
		}
		if rErr := s.stmt.Reset(); rErr != nil {
			if err == nil {
				err = rErr
			}
			return
		}
	}()
	if ctx.Done() != nil {
		s.conn.conn.SetInterrupt(ctx.Done())
		defer s.conn.conn.SetInterrupt(nil)
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
