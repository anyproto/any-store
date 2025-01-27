package driver

import (
	"context"
	"errors"
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
	isClosed atomic.Bool
}

func (c *Conn) ExecNoResult(ctx context.Context, query string) (err error) {
	if ctx.Done() != nil {
		c.conn.SetInterrupt(ctx.Done())
	}
	defer c.conn.SetInterrupt(nil)
	return sqlitex.ExecScript(c.conn, query)
}

func (c *Conn) Exec(ctx context.Context, query string, bind func(stmt *sqlite.Stmt), result func(stmt *sqlite.Stmt) error) (err error) {
	sqliteStmt, _, err := c.conn.PrepareTransient(query)
	if err != nil {
		return
	}
	defer func() {
		_ = sqliteStmt.Finalize()
	}()
	stmt := Stmt{stmt: sqliteStmt, conn: c}
	return stmt.Exec(ctx, bind, result)
}

func (c *Conn) ExecCached(ctx context.Context, query string, bind func(stmt *sqlite.Stmt), result func(stmt *sqlite.Stmt) error) (err error) {
	sqliteStmt, err := c.conn.Prepare(query)
	if err != nil {
		return
	}
	stmt := Stmt{stmt: sqliteStmt, conn: c}
	return stmt.Exec(ctx, bind, result)
}

func (c *Conn) Query(ctx context.Context, query string) (*sqlite.Stmt, error) {
	if ctx.Done() != nil {
		c.conn.SetInterrupt(ctx.Done())
	}
	defer c.conn.SetInterrupt(nil)
	stmt, _, err := c.conn.PrepareTransient(query)
	return stmt, err
}

func (c *Conn) Prepare(query string) (*Stmt, error) {
	stmt, err := c.conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &Stmt{conn: c, stmt: stmt}, nil
}

func (c *Conn) Begin(ctx context.Context) (err error) {
	if c.begin == nil {
		if c.begin, err = c.Prepare("BEGIN"); err != nil {
			return
		}
	}
	return c.begin.Exec(ctx, nil, StmtExecNoResults)
}

func (c *Conn) BeginImmediate(ctx context.Context) (err error) {
	if c.beginImmediate == nil {
		if c.beginImmediate, err = c.Prepare("BEGIN IMMEDIATE"); err != nil {
			return
		}
	}
	return c.beginImmediate.Exec(ctx, nil, StmtExecNoResults)
}

func (c *Conn) Commit(ctx context.Context) (err error) {
	if c.commit == nil {
		if c.commit, err = c.Prepare("COMMIT"); err != nil {
			return
		}
	}
	return c.commit.Exec(ctx, nil, StmtExecNoResults)
}

func (c *Conn) Rollback(ctx context.Context) (err error) {
	if c.rollback == nil {
		if c.rollback, err = c.Prepare("ROLLBACK"); err != nil {
			return
		}
	}
	return c.rollback.Exec(ctx, nil, StmtExecNoResults)
}

func (c *Conn) Backup(ctx context.Context, path string) (err error) {
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

func (c *Conn) IsClosed() bool {
	return c.isClosed.Load()
}

func (c *Conn) Close() (err error) {
	if !c.isClosed.Swap(true) {
		return c.conn.Close()
	}
	return nil
}
