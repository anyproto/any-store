package anystore

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/anyproto/any-store/internal/driver"
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

	conn() *driver.Conn
	instanceId() string
}

type commonTx struct {
	db         *db
	ctx        context.Context
	initialCtx context.Context
	con        *driver.Conn
	done       atomic.Bool
}

func (tx *commonTx) conn() *driver.Conn {
	return tx.con
}

func (tx *commonTx) reset() {
	tx.done.Store(false)
}

func (tx *commonTx) instanceId() string {
	return tx.db.instanceId
}

var readTxPool = &sync.Pool{
	New: func() any {
		return &readTx{}
	},
}

type readTx struct {
	commonTx
}

func (r *readTx) Context() context.Context {
	return r.ctx
}

func (r *readTx) Commit() error {
	if r.done.CompareAndSwap(false, true) {
		defer r.db.cm.ReleaseRead(r.con)
		defer readTxPool.Put(r)
		return r.con.Commit(context.Background())
	}
	return nil
}

func (r *readTx) Done() bool {
	return r.done.Load()
}

var writeTxPool = &sync.Pool{
	New: func() any {
		return &writeTx{}
	},
}

type writeTx struct {
	readTx
}

func (w *writeTx) Context() context.Context {
	return w.ctx
}

func (w *writeTx) Rollback() error {
	if w.done.CompareAndSwap(false, true) {
		defer w.db.cm.ReleaseWrite(w.con)
		defer writeTxPool.Put(w)
		return w.con.Rollback(context.Background())
	}
	return nil
}

func (w *writeTx) Commit() error {
	if w.done.CompareAndSwap(false, true) {
		defer w.db.cm.ReleaseWrite(w.con)
		defer writeTxPool.Put(w)
		return w.con.Commit(context.Background())
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
	if err := tx.conn().Exec(ctx, unsafe.String(unsafe.SliceData(tx.createQuery), len(tx.createQuery)), nil, driver.StmtExecNoResults); err != nil {
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
		if err := tx.conn().Exec(context.TODO(), unsafe.String(unsafe.SliceData(tx.releaseQuery), len(tx.releaseQuery)), nil, driver.StmtExecNoResults); err != nil {
			return err
		}
		savepointPool.Put(tx)
	}
	return nil
}

func (tx *savepointTx) Rollback() error {
	if tx.done.CompareAndSwap(false, true) {
		if err := tx.conn().Exec(context.TODO(), unsafe.String(unsafe.SliceData(tx.rollbackQuery), len(tx.rollbackQuery)), nil, driver.StmtExecNoResults); err != nil {
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
