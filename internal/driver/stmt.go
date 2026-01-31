package driver

import (
	"context"
	"sync"

	"github.com/anyproto/go-sqlite"
)

type Stmt struct {
	conn *Conn
	stmt *sqlite.Stmt
	mu   sync.Mutex
}

func (s *Stmt) Exec(ctx context.Context, bind func(stmt *sqlite.Stmt), result func(stmt *sqlite.Stmt) error) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stmt == nil {
		return ErrStmtIsClosed
	}
	return s.exec(ctx, bind, result)
}

func (s *Stmt) exec(ctx context.Context, bind func(stmt *sqlite.Stmt), result func(stmt *sqlite.Stmt) error) (err error) {
	defer func() {
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

func (s *Stmt) Step() (rowReturned bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stmt == nil {
		return false, ErrStmtIsClosed
	}
	return s.stmt.Step()
}

func (s *Stmt) BindBytes(param int, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stmt == nil {
		return
	}
	s.stmt.BindBytes(param, value)
}

func (s *Stmt) ColumnBytes(col int, buf []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stmt == nil {
		return 0, ErrStmtIsClosed
	}
	return s.stmt.ColumnBytes(col, buf), nil
}

func (s *Stmt) ColumnLen(col int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stmt == nil {
		return 0, ErrStmtIsClosed
	}
	return s.stmt.ColumnLen(col), nil
}

func (s *Stmt) Close() error {
	if s == nil {
		return nil
	}
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	if s.conn.isClosed {
		return ErrDBIsClosed
	}
	return s.close()
}

func (s *Stmt) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stmt == nil {
		return nil
	}
	delete(s.conn.activeStmts, s.stmt)
	err := s.stmt.Finalize()
	s.stmt = nil
	return err
}

func StmtExecNoResults(stmt *sqlite.Stmt) (err error) {
	_, err = stmt.Step()
	return
}
