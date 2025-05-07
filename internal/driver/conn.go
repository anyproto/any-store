package driver

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

type Conn struct {
	conn *sqlite.Conn
	begin,
	beginImmediate,
	commit,
	rollback *Stmt
	activeStmts map[*Stmt]struct{}
	isClosed    bool
	lastUsage   atomic.Int64
	isActive    atomic.Bool
	mu          sync.Mutex
}

func (c *Conn) ExecNoResult(ctx context.Context, query string) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isClosed {
		return ErrDBIsClosed
	}
	if ctx.Done() != nil {
		c.conn.SetInterrupt(ctx.Done())
	}
	defer c.conn.SetInterrupt(nil)
	return sqlitex.ExecScript(c.conn, query)
}

func (c *Conn) Exec(ctx context.Context, query string, bind func(stmt *sqlite.Stmt), result func(stmt *sqlite.Stmt) error) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isClosed {
		return ErrDBIsClosed
	}
	sqliteStmt, _, err := c.conn.PrepareTransient(query)
	if err != nil {
		return
	}
	defer func() {
		_ = sqliteStmt.Finalize()
	}()
	stmt := Stmt{stmt: sqliteStmt, conn: c}
	return stmt.exec(ctx, bind, result)
}

func (c *Conn) ExecCached(ctx context.Context, query string, bind func(stmt *sqlite.Stmt), result func(stmt *sqlite.Stmt) error) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isClosed {
		return ErrDBIsClosed
	}
	sqliteStmt, err := c.conn.Prepare(query)
	if err != nil {
		return
	}
	stmt := Stmt{stmt: sqliteStmt, conn: c}
	return stmt.exec(ctx, bind, result)
}

func (c *Conn) Query(ctx context.Context, query string) (*Stmt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isClosed {
		return nil, ErrDBIsClosed
	}
	if ctx.Done() != nil {
		c.conn.SetInterrupt(ctx.Done())
	}
	defer c.conn.SetInterrupt(nil)
	stmt, _, err := c.conn.PrepareTransient(query)
	return c.newStmt(stmt), err
}

func (c *Conn) Prepare(query string) (*Stmt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isClosed {
		return nil, ErrDBIsClosed
	}
	return c.prepare(query)
}

func (c *Conn) prepare(query string) (*Stmt, error) {
	stmt, err := c.conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	return c.newStmt(stmt), nil
}

func (c *Conn) Begin(ctx context.Context) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isClosed {
		return ErrDBIsClosed
	}
	if c.begin == nil {
		if c.begin, err = c.prepare("BEGIN"); err != nil {
			return
		}
	}
	return c.begin.Exec(ctx, nil, StmtExecNoResults)
}

func (c *Conn) BeginImmediate(ctx context.Context) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isClosed {
		return ErrDBIsClosed
	}
	if c.beginImmediate == nil {
		if c.beginImmediate, err = c.prepare("BEGIN IMMEDIATE"); err != nil {
			return
		}
	}
	return c.beginImmediate.Exec(ctx, nil, StmtExecNoResults)
}

func (c *Conn) Commit(ctx context.Context) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isClosed {
		return ErrDBIsClosed
	}
	if c.commit == nil {
		if c.commit, err = c.prepare("COMMIT"); err != nil {
			return
		}
	}
	return c.commit.Exec(ctx, nil, StmtExecNoResults)
}

func (c *Conn) Rollback(ctx context.Context) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isClosed {
		return ErrDBIsClosed
	}
	if c.rollback == nil {
		if c.rollback, err = c.prepare("ROLLBACK"); err != nil {
			return
		}
	}
	return c.rollback.Exec(ctx, nil, StmtExecNoResults)
}

func (c *Conn) Backup(ctx context.Context, path string) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isClosed {
		return ErrDBIsClosed
	}
	descConn, err := sqlite.OpenConn(path)
	if err != nil {
		return
	}
	backup, err := sqlite.NewBackup(descConn, "", c.conn, "")
	if err != nil {
		return
	}
	defer func() {
		err = errors.Join(err, backup.Close())
	}()
	_, err = backup.Step(-1)
	return
}

func (c *Conn) newStmt(stmt *sqlite.Stmt) *Stmt {
	dStmt := &Stmt{conn: c, stmt: stmt}
	c.activeStmts[dStmt] = struct{}{}
	return dStmt
}

func (c *Conn) Close() (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isClosed {
		return ErrDBIsClosed
	} else {
		for stmt := range c.activeStmts {
			err = errors.Join(err, stmt.close())
		}
		return errors.Join(err, c.conn.Close())
	}
}
