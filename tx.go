package anystore

import (
	"context"
	"database/sql/driver"
	"sync/atomic"

	"github.com/anyproto/any-store/internal/conn"
)

type WriteTx interface {
	ReadTx
	Rollback() error
}

type ReadTx interface {
	Context() context.Context
	Commit() error
	conn() conn.Conn
	instanceId() string
}

type commonTx struct {
	db         *db
	initialCtx context.Context
	con        conn.Conn
	tx         driver.Tx
	done       atomic.Bool
}

func (tx *commonTx) conn() conn.Conn {
	return tx.con
}

func (tx *commonTx) instanceId() string {
	return tx.db.instanceId
}

type readTx struct {
	commonTx
}

func (r *readTx) Context() context.Context {
	return context.WithValue(r.commonTx.initialCtx, ctxKeyTx, r)
}

func (r *readTx) Commit() error {
	if !r.done.Swap(true) {
		defer r.db.cm.ReleaseRead(r.con)
		return r.commonTx.tx.Commit()
	}
	return nil
}

type writeTx struct {
	readTx
}

func (w *writeTx) Context() context.Context {
	return context.WithValue(w.commonTx.initialCtx, ctxKeyTx, w)
}

func (w *writeTx) Rollback() error {
	if !w.done.Swap(true) {
		defer w.db.cm.ReleaseWrite(w.con)
		return w.commonTx.tx.Rollback()
	}
	return nil
}

func (w *writeTx) Commit() error {
	if !w.done.Swap(true) {
		defer w.db.cm.ReleaseWrite(w.con)
		return w.commonTx.tx.Commit()
	}
	return nil
}

type noOpTx struct {
	ReadTx
}

func (noOpTx) Commit() error {
	return nil
}

func (noOpTx) Rollback() error {
	return nil
}
