package anystore

import (
	"context"
	"database/sql/driver"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/anyproto/any-store/internal/conn"
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

var savepointIds atomic.Uint64

var savepointPool = &sync.Pool{
	New: func() any {
		return &savepointTx{}
	},
}

func newSavepointTx(ctx context.Context, wrTx WriteTx) (WriteTx, error) {
	tx := savepointPool.Get().(*savepointTx)
	tx.reset(wrTx)
	if _, err := tx.conn().ExecContext(ctx, unsafe.String(unsafe.SliceData(tx.createQuery), len(tx.createQuery)), nil); err != nil {
		return nil, err
	}
	return tx, nil
}

const (
	savepointCreateQuery   = "SAVEPOINT sp"
	savepointReleaseQuery  = "RELEASE SAVEPOINT sp"
	savepointRollbackQuery = "ROLLBACK TO SAVEPOINT sp"
)

type savepointTx struct {
	WriteTx
	id            uint64
	createQuery   []byte
	releaseQuery  []byte
	rollbackQuery []byte
	done          atomic.Bool
}

func (tx *savepointTx) reset(wtx WriteTx) {
	tx.id = savepointIds.Add(1)
	tx.WriteTx = wtx
	tx.done.Store(false)
	if len(tx.createQuery) == 0 {
		tx.createQuery = make([]byte, 0, len(savepointCreateQuery)+10)
		tx.createQuery = append(tx.createQuery, []byte(savepointCreateQuery)...)
		tx.createQuery = strconv.AppendUint(tx.createQuery, tx.id, 10)
	} else {
		tx.createQuery = strconv.AppendUint(tx.createQuery[:len(savepointCreateQuery)], tx.id, 10)
	}
	if len(tx.releaseQuery) == 0 {
		tx.releaseQuery = make([]byte, 0, len(savepointReleaseQuery)+10)
		tx.releaseQuery = append(tx.releaseQuery, []byte(savepointReleaseQuery)...)
		tx.releaseQuery = strconv.AppendUint(tx.releaseQuery, tx.id, 10)
	} else {
		tx.releaseQuery = strconv.AppendUint(tx.releaseQuery[:len(savepointReleaseQuery)], tx.id, 10)
	}
	if len(tx.rollbackQuery) == 0 {
		tx.rollbackQuery = make([]byte, 0, len(savepointRollbackQuery)+10)
		tx.rollbackQuery = append(tx.rollbackQuery, []byte(savepointRollbackQuery)...)
		tx.rollbackQuery = strconv.AppendUint(tx.rollbackQuery, tx.id, 10)
	} else {
		tx.rollbackQuery = strconv.AppendUint(tx.rollbackQuery[:len(savepointRollbackQuery)], tx.id, 10)
	}
}

func (tx *savepointTx) Commit() error {
	if tx.done.CompareAndSwap(false, true) {
		if _, err := tx.conn().ExecContext(context.TODO(), unsafe.String(unsafe.SliceData(tx.releaseQuery), len(tx.releaseQuery)), nil); err != nil {
			return err
		}
		savepointPool.Put(tx)
	}
	return nil
}

func (tx *savepointTx) Rollback() error {
	if tx.done.CompareAndSwap(false, true) {
		if _, err := tx.conn().ExecContext(context.TODO(), unsafe.String(unsafe.SliceData(tx.rollbackQuery), len(tx.rollbackQuery)), nil); err != nil {
			return err
		}
		savepointPool.Put(tx)
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
