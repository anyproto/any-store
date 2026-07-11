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

	// onRollbackUndo registers a reversal of an in-memory schema publication;
	// see commonTx.undo. Unexported: DDL-internal.
	onRollbackUndo(f func())
	undoLen() int
	runUndo(from int)
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

	// undo holds reversals of in-memory schema publications (collection
	// registered in db.openedCollections, index set swaps, renames) made by DDL
	// inside this tx. DDL must publish at execution time — not at top-level
	// commit — because writes later in the SAME tx must see and maintain an
	// index/collection created earlier in it. But a rollback then reverts the
	// on-disk catalog without those maps, leaving handles whose cached root
	// pages are freed — a later write through such a handle lands on whatever
	// namespace reuses the page (silent cross-namespace corruption; see
	// docs/known-issues.md I-11). Undos run in REVERSE order on rollback —
	// full, failed-commit, or to a savepoint (savepointTx records a mark) —
	// and are discarded on successful commit. Single-writer state: mutated
	// only under the btree write lock, like the tx itself; runUndo must also
	// execute while that lock is still held (see writeTx.Rollback).
	undo []func()
}

func (tx *commonTx) onRollbackUndo(f func()) {
	tx.undo = append(tx.undo, f)
}

func (tx *commonTx) undoLen() int {
	return len(tx.undo)
}

// runUndo executes and discards undo entries [from:] in reverse registration
// order, so nested publications (e.g. an index created on a collection created
// in the same tx) unwind consistently. Executed slots are cleared so the
// pooled commonTx does not pin the captured handles and index-set snapshots.
func (tx *commonTx) runUndo(from int) {
	for i := len(tx.undo) - 1; i >= from; i-- {
		tx.undo[i]()
	}
	clear(tx.undo[from:])
	tx.undo = tx.undo[:from]
}

// discardUndo drops all registered undos without running them: the commit
// succeeded, so the publications they would revert are now durable.
func (tx *commonTx) discardUndo() {
	clear(tx.undo)
	tx.undo = tx.undo[:0]
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
		// Unwind in-memory schema publications BEFORE the btree rollback: the
		// btree releases the global write lock inside Rollback, and a writer
		// acquiring it in the gap would still see the phantom state — or
		// commit its own index-set swaps that a late undo would then clobber.
		// Running the undos first is safe: this tx's writes were never
		// reader-visible, so the restored snapshots match the committed
		// on-disk state throughout.
		w.commonTx.runUndo(0)
		return w.writeTx.Rollback()
	}
	return nil
}

func (w writeTx) Commit() error {
	if w.commonTx.version.CompareAndSwap(w.version, 0) {
		defer txPool.Put(w.commonTx)
		// Every non-committed exit below must unwind the in-memory schema
		// publications, BEFORE its btree rollback releases the global write
		// lock (see Rollback).
		if w.modified {
			w.writeTx.MarkDataChanged()
			// Flush the full-text write-back buffer into this same tx BEFORE the
			// btree commit, so postings commit atomically with the documents.
			if err := w.db.flushAllFtsPending(w.writeTx); err != nil {
				w.commonTx.runUndo(0)
				_ = w.writeTx.Rollback()
				return err
			}
			if err := w.db.persistAllDirtySketches(w.writeTx); err != nil {
				w.commonTx.runUndo(0)
				_ = w.writeTx.Rollback()
				return err
			}
		}
		// The btree commit releases the global write lock before returning an
		// error, so a failed-commit unwind would run outside the critical
		// section — hold the DDL-unwind gate (paired with newWriteTx) across
		// commit + unwind so no new writer can observe the phantoms in the
		// gap. Only needed when this tx actually registered undos.
		if len(w.commonTx.undo) > 0 {
			w.db.ddlUnwindGate.Lock()
			defer w.db.ddlUnwindGate.Unlock()
		}
		err := w.writeTx.Commit()
		if err == nil {
			if w.modified {
				w.db.recoveryController.OnWriteEvent()
			}
			w.commonTx.discardUndo()
		} else {
			// A failed btree commit leaves the catalog without this tx's DDL —
			// unwind (under the gate).
			w.commonTx.runUndo(0)
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
	// undoMark is the parent tx's undo length at savepoint creation: a
	// rollback to this savepoint unwinds exactly the schema publications made
	// inside its scope (entries [undoMark:]); a release keeps them registered
	// on the parent so an outer rollback still unwinds them.
	undoMark int
	version  atomic.Uint32
}

func (tx *savepointTx) reset(wtx WriteTx, spId int) {
	tx.WriteTx = wtx
	tx.savepointId = spId
	tx.undoMark = wtx.undoLen()
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
		err := btWtx.RollbackToSavepoint(w.savepointId)
		// The fts pending buffers hold only ops made inside this savepoint's
		// scope (they were flushed empty at its creation), and the btree state
		// those ops were derived from has just been reverted — discard them.
		// Both this and the undo unwind run even when RollbackToSavepoint
		// fails: its error returns happen before any mutation, the outer tx is
		// doomed either way, and matching the in-memory schema state to the
		// last committed disk state is the conservative choice.
		// RollbackToSavepoint keeps the write lock in all cases, so the unwind
		// runs inside the critical section.
		db.resetAllFtsPending()
		w.WriteTx.runUndo(w.undoMark)
		if err != nil {
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
