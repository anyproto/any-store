package driver

import (
	"context"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

type Conn struct {
	conn *sqlite.Conn
	begin,
	commit,
	rollback Stmt
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
	stmt := Stmt{stmt: sqliteStmt, conn: c.conn}
	return stmt.Exec(ctx, bind, result)
}

func (c *Conn) ExecCached(ctx context.Context, query string, bind func(stmt *sqlite.Stmt), result func(stmt *sqlite.Stmt) error) (err error) {
	sqliteStmt, err := c.conn.Prepare(query)
	if err != nil {
		return
	}
	stmt := Stmt{stmt: sqliteStmt, conn: c.conn}
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

func (c *Conn) Prepare(query string) (Stmt, error) {
	stmt, err := c.conn.Prepare(query)
	if err != nil {
		return Stmt{}, err
	}
	return Stmt{conn: c.conn, stmt: stmt}, nil
}

func (c *Conn) Begin(ctx context.Context) (err error) {
	if c.begin.stmt == nil {
		if c.begin, err = c.Prepare("BEGIN"); err != nil {
			return
		}
	}
	return c.begin.Exec(ctx, nil, StmtExecNoResults)
}

func (c *Conn) Commit(ctx context.Context) (err error) {
	if c.commit.stmt == nil {
		if c.commit, err = c.Prepare("COMMIT"); err != nil {
			return
		}
	}
	return c.commit.Exec(ctx, nil, StmtExecNoResults)
}

func (c *Conn) Rollback(ctx context.Context) (err error) {
	if c.rollback.stmt == nil {
		if c.rollback, err = c.Prepare("ROLLBACK"); err != nil {
			return
		}
	}
	return c.rollback.Exec(ctx, nil, StmtExecNoResults)
}

func (c *Conn) Close() (err error) {
	return c.conn.Close()
}
