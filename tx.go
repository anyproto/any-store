package anystore

import (
	"context"
	"database/sql/driver"
	"sync/atomic"

	"github.com/anyproto/any-store/internal/conn"
	"github.com/anyproto/any-store/internal/objectid"
)

// WriteTx represents a read-write transaction.
type WriteTx interface {
	// ReadTx is embedded to provide read-only transaction methods.
	ReadTx

	// Rollback rolls back the transaction.
	// Returns an error if the rollback fails.
	Rollback() error
}

// ReadTx represents a read-only transaction.
type ReadTx interface {
	// Context returns the context associated with the transaction.
	Context() context.Context

	// Commit commits the transaction.
	// Returns an error if the commit fails.
	Commit() error

	// Done returns true if the transaction is completed (committed or rolled back).
	Done() bool

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
	if r.done.CompareAndSwap(false, true) {
		defer r.db.cm.ReleaseRead(r.con)
		return r.commonTx.tx.Commit()
	}
	return nil
}

func (r *readTx) Done() bool {
	return r.done.Load()
}

type writeTx struct {
	readTx
}

func (w *writeTx) Context() context.Context {
	return context.WithValue(w.commonTx.initialCtx, ctxKeyTx, w)
}

func (w *writeTx) Rollback() error {
	if w.done.CompareAndSwap(false, true) {
		defer w.db.cm.ReleaseWrite(w.con)
		return w.commonTx.tx.Rollback()
	}
	return nil
}

func (w *writeTx) Commit() error {
	if w.done.CompareAndSwap(false, true) {
		defer w.db.cm.ReleaseWrite(w.con)
		return w.commonTx.tx.Commit()
	}
	return nil
}

func newSavepointTx(ctx context.Context, wrTx WriteTx) (WriteTx, error) {
	tx := &savepointTx{
		id:      objectid.NewObjectID().Hex(),
		WriteTx: wrTx,
	}
	if _, err := tx.conn().ExecContext(ctx, "SAVEPOINT  '"+tx.id+"'", nil); err != nil {
		return nil, err
	}
	return tx, nil
}

type savepointTx struct {
	id string
	WriteTx
	done atomic.Bool
}

func (tx *savepointTx) Commit() error {
	if tx.done.CompareAndSwap(false, true) {
		if _, err := tx.conn().ExecContext(context.TODO(), "RELEASE SAVEPOINT '"+tx.id+"'", nil); err != nil {
			return err
		}
	}
	return nil
}

func (tx *savepointTx) Rollback() error {
	if tx.done.CompareAndSwap(false, true) {
		if _, err := tx.conn().ExecContext(context.TODO(), "ROLLBACK TO SAVEPOINT  '"+tx.id+"'", nil); err != nil {
			return err
		}
	}
	return nil
}

func (tx *savepointTx) Done() bool {
	return tx.done.Load()
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
