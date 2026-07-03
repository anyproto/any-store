package anystore

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/anyproto/any-store/v2/internal/btree"
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

	// SetModified marks the transaction as having made modifications
	// used internally for sentinel mechanism
	SetModified()
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

	btreeReadTx() *btree.ReadTx
	btreeWriteTx() *btree.WriteTx
	instanceId() string
	dbRef() *db
}

type commonTx struct {
	db       *db
	ctx      context.Context
	readTx   *btree.ReadTx
	writeTx  *btree.WriteTx
	version  atomic.Uint32
	modified bool
}

func (tx *commonTx) btreeReadTx() *btree.ReadTx {
	return tx.readTx
}

func (tx *commonTx) btreeWriteTx() *btree.WriteTx {
	return tx.writeTx
}

func (tx *commonTx) SetModified() {
	tx.modified = true
}

func (tx *commonTx) instanceId() string {
	return tx.db.instanceId
}

func (tx *commonTx) dbRef() *db {
	return tx.db
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
		defer txPool.Put(r.commonTx)
		return r.readTx.Rollback()
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
		defer txPool.Put(w.commonTx)
		return w.writeTx.Rollback()
	}
	return nil
}

func (w writeTx) Commit() error {
	if w.commonTx.version.CompareAndSwap(w.version, 0) {
		defer txPool.Put(w.commonTx)
		if w.modified {
			w.writeTx.MarkDataChanged()
			// Flush the full-text write-back buffer into this same tx BEFORE the
			// btree commit, so postings commit atomically with the documents.
			if err := w.db.flushAllFtsPending(w.writeTx); err != nil {
				_ = w.writeTx.Rollback()
				return err
			}
			if err := w.db.persistAllDirtySketches(w.writeTx); err != nil {
				_ = w.writeTx.Rollback()
				return err
			}
		}
		err := w.writeTx.Commit()
		if err == nil && w.modified {
			w.db.recoveryController.OnWriteEvent()
		}
		return err
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
	btWtx := wrTx.btreeWriteTx()
	// Flush buffered full-text writes BEFORE creating the savepoint, so their
	// btree writes land outside its scope. This keeps the fts pending buffer
	// savepoint-consistent: at every savepoint creation the buffer is empty, so
	// after RollbackToSavepoint (a) the buffer holds only ops made inside the
	// rolled-back scope (discarded by resetAllFtsPending), and (b) everything
	// flushed after this point sits inside the scope and is reverted by the
	// btree itself. Without this, buffered postings from before the savepoint
	// survive its rollback and flush at outer commit — while the seq/docmap
	// writes they depend on were reverted, attributing ghost postings to
	// whichever document later reuses the IntDocID.
	if err := wrTx.dbRef().flushAllFtsPending(btWtx); err != nil {
		return nil, err
	}
	spId, err := btWtx.Savepoint()
	if err != nil {
		return nil, err
	}
	tx := savepointPool.Get().(*savepointTx)
	tx.reset(wrTx, spId)
	return savepointWrapper{savepointTx: tx, version: tx.version.Load()}, nil
}

type savepointWrapper struct {
	*savepointTx
	version uint32
}

type savepointTx struct {
	WriteTx
	savepointId int
	version     atomic.Uint32
}

func (tx *savepointTx) reset(wtx WriteTx, spId int) {
	tx.WriteTx = wtx
	tx.savepointId = spId
	tx.version.Store(newTxVersion())
}

func (w savepointWrapper) Commit() error {
	if w.savepointTx.version.CompareAndSwap(w.version, 0) {
		btWtx := w.WriteTx.btreeWriteTx()
		if err := btWtx.ReleaseSavepoint(w.savepointId); err != nil {
			return err
		}
		savepointPool.Put(w.savepointTx)
	}
	return nil
}

func (w savepointWrapper) Rollback() error {
	if w.savepointTx.version.CompareAndSwap(w.version, 0) {
		btWtx := w.WriteTx.btreeWriteTx()
		db := w.WriteTx.dbRef()
		if err := btWtx.RollbackToSavepoint(w.savepointId); err != nil {
			return err
		}
		// The fts pending buffers hold only ops made inside this savepoint's
		// scope (they were flushed empty at its creation), and the btree state
		// those ops were derived from has just been reverted — discard them.
		db.resetAllFtsPending()
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
