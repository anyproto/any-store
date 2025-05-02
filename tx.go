package anystore

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/anyproto/any-store/internal/driver"
)

var txVersion atomic.Uint32

func newTxVersion() uint32 {
	if ver := txVersion.Add(1); ver != 0 {
		return ver
	} else {
		return txVersion.Add(1)
	}
}

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
	db      *db
	ctx     context.Context
	con     *driver.Conn
	version atomic.Uint32
}

func (tx *commonTx) conn() *driver.Conn {
	return tx.con
}

func (tx *commonTx) instanceId() string {
	return tx.db.instanceId
}

var txPool = &sync.Pool{
	New: func() any {
		return &commonTx{}
	},
}

type readTx struct {
	*commonTx
	version uint32
}

func (r readTx) Context() context.Context {
	return r.ctx
}

func (r readTx) Commit() error {
	if r.commonTx.version.CompareAndSwap(r.version, 0) {
		defer r.db.cm.ReleaseRead(r.con)
		defer txPool.Put(r.commonTx)
		return r.con.Commit(context.Background())
	}
	return nil
}

func (r readTx) Done() bool {
	return r.commonTx.version.Load() != r.version
}

type writeTx struct {
	*commonTx
	version uint32
}

func (w writeTx) Context() context.Context {
	return w.ctx
}

func (w writeTx) Rollback() error {
	if w.commonTx.version.CompareAndSwap(w.version, 0) {
		defer w.db.cm.ReleaseWrite(w.con)
		defer txPool.Put(w.commonTx)
		return w.con.Rollback(context.Background())
	}
	return nil
}

func (w writeTx) Commit() error {
	if w.commonTx.version.CompareAndSwap(w.version, 0) {
		defer w.db.cm.ReleaseWrite(w.con)
		defer txPool.Put(w.commonTx)
		return w.con.Commit(context.Background())
	}
	return nil
}

func (w writeTx) Done() bool {
	return w.commonTx.version.Load() != w.version
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
	return savepointWrapper{savepointTx: tx, version: tx.version.Load()}, nil
}

const (
	savepointCreateQuery   = "SAVEPOINT sp"
	savepointReleaseQuery  = "RELEASE SAVEPOINT sp"
	savepointRollbackQuery = "ROLLBACK TO SAVEPOINT sp"
)

type savepointWrapper struct {
	*savepointTx
	version uint32
}

type savepointTx struct {
	WriteTx
	id            uint64
	createQuery   []byte
	releaseQuery  []byte
	rollbackQuery []byte
	version       atomic.Uint32
}

func (tx *savepointTx) reset(wtx WriteTx) {
	tx.id = savepointIds.Add(1)
	tx.WriteTx = wtx
	tx.version.Store(newTxVersion())
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

func (w savepointWrapper) Commit() error {
	if w.savepointTx.version.CompareAndSwap(w.version, 0) {
		if err := w.conn().Exec(context.TODO(), unsafe.String(unsafe.SliceData(w.releaseQuery), len(w.releaseQuery)), nil, driver.StmtExecNoResults); err != nil {
			return err
		}
		savepointPool.Put(w.savepointTx)
	}
	return nil
}

func (w savepointWrapper) Rollback() error {
	if w.savepointTx.version.CompareAndSwap(w.version, 0) {
		if err := w.conn().Exec(context.TODO(), unsafe.String(unsafe.SliceData(w.rollbackQuery), len(w.rollbackQuery)), nil, driver.StmtExecNoResults); err != nil {
			return err
		}
		savepointPool.Put(w.savepointTx)
	}
	return nil
}

func (w savepointWrapper) Done() bool {
	return w.savepointTx.version.Load() != w.version
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
