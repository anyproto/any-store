package connwrap

import (
	"context"
	"errors"
	"fmt"

	"zombiezen.com/go/sqlite"

	"github.com/anyproto/any-store/internal/conn"
)

type Conn struct {
	*sqlite.Conn
}

func (c Conn) ExecOnce(ctx context.Context, query string, bind func(stmt *sqlite.Stmt) error) (err error) {
	c.SetInterrupt(ctx.Done())
	stmt, tb, err := c.PrepareTransient(query)
	if err != nil {
		return
	}
	defer func() {
		err = errors.Join(err, stmt.Finalize())
	}()
	if tb != 0 {
		return fmt.Errorf("prepare: trailyng bytes")
	}
	if bind != nil {
		if err = bind(stmt); err != nil {
			return
		}
	}
	_, err = stmt.Step()
	return
}

func (c Conn) Query(ctx context.Context, query string, result func(stmt *sqlite.Stmt) error) error {
	//TODO implement me
	panic("implement me")
}

func (c Conn) BeginTx() (conn.Tx, error) {
	//TODO implement me
	panic("implement me")
}
