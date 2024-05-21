package conn

import (
	"database/sql/driver"

	"github.com/mattn/go-sqlite3"
)

var (
	_ Stmt = (*sqlite3.SQLiteStmt)(nil)
	_ Conn = (*sqlite3.SQLiteConn)(nil)
)

type Stmt interface {
	driver.Stmt
	driver.StmtExecContext
	driver.StmtQueryContext
}

type Conn interface {
	driver.Conn
	driver.ConnBeginTx
	driver.ConnPrepareContext
	driver.ExecerContext
	driver.QueryerContext
}
