package anystore

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// txVersion numbers transactions and savepoints: a handle tells its own
// from the one its pooled state serves next by it. 64 bits: at 2^32 the
// numbers would come round within a day of writes, and a handle of an
// ended transaction would pass for one of the transaction reusing its
// state.
var txVersion atomic.Uint64

func newTxVersion() uint64 {
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
	// markModified is SetModified for a caller that holds the turn.
	markModified()

	// schemaLog is the transaction's uncommitted schema changes; see
	// txSchema. Unexported: DDL-internal.
	schemaLog() *txSchema

	// The stack of open savepoints; see commonTx.savepoints.
	// Unexported: savepoint-internal.
	savepointOpened(sp *savepointTx)
	savepointEnded(sp *savepointTx)
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

	// The transaction's lock, one call at a time; see txHandle.
	lock()
	unlock()

	// The open iterators that hold cursors on the transaction; see
	// commonTx.iters. Unexported: iterator-internal.
	iterOpened(pi *planIterator)
	iterClosed(pi *planIterator)
}

// commonTx is a transaction's pooled state (db.txPool): what the handles of
// one transaction share, and what a later transaction of the db takes over
// once this one ended. A handle tells the two apart by version, and it
// reads nothing else of a transaction that may have ended — its lock and
// its context are the handle's own (txHandle).
//
// One call at a time: every call made on a transaction — an operation run
// with its context, a savepoint's, its commit or rollback, a method of an
// iterator — takes the transaction's turn (txHandle.mu) and gives it back
// before control returns to the caller. What the transaction holds — the
// page cache of its btree tx, the full-text buffers, the schema log, the
// savepoint stack — is built for one caller at a time, so a transaction
// several goroutines share runs their calls one after another, as SQLite's
// serialized threading mode runs the calls made on one connection
// (sqlite3_mutex_enter on db->mutex at every entry point). SQLite's mutex is
// recursive and Go's is not: a call made from inside another call — a
// callback's — runs under the enclosing call's turn only through the
// context an internal savepoint hands out (heldTx); user code the library
// calls holding the turn must not call back into the transaction. The
// turn is taken at the entry points (lockCtxTx, enterWriteTx, the methods
// of the transaction, of a savepoint, of an iterator); an iterator or a
// savepoint the caller keeps relocks per call. A call that waited
// re-checks Done under the lock: the call it waited for may have ended
// the transaction.
type commonTx struct {
	db      *db
	readTx  *btree.ReadTx
	writeTx *btree.WriteTx
	version atomic.Uint64
	// modified: SetModified is public, so a caller may set it outside the lock.
	modified atomic.Bool

	// schema is the transaction's own schema state (txSchema), in the
	// btree transaction's Aux slot for the resolve of every operation. For a
	// write transaction its log holds the uncommitted DDL: the writes later
	// in the same tx see and maintain an index or collection created earlier
	// in it through the log, nothing else does until the commit installs
	// it, and a rollback — full, failed-commit, or to a savepoint
	// (savepointTx records a mark) — discards the scope's entries. Writer
	// state: mutated under the btree write lock.
	schema txSchema

	// savepoints is the stack of open savepoints, outermost first. A
	// savepoint that ends takes the savepoints opened inside it with it —
	// SQLite destroys every savepoint nested inside the one a RELEASE or
	// ROLLBACK TO names (vdbe.c, OP_Savepoint). One that is no longer on
	// the stack is gone (savepointTx.ended): its btree savepoint id may
	// name another savepoint by now, and its log mark cuts the log at a
	// position that pairs nothing. Single-writer state, like the log.
	savepoints []*savepointTx

	// sketches is the indexes whose live sketch the commit persisted
	// (persistSketches): settled as the commit becomes visible
	// (settleSketches), left flagged by a commit that does not.
	sketches []*index

	// iters is the open iterators that hold cursors on the transaction —
	// pinned pages of its btree tx. The transaction's end trips them
	// (tripIters) before the btree tx ends: the pages are released with the
	// transaction, and an iterator closed later touches none of them. SQLite
	// trips the cursors of a transaction it rolls back
	// (sqlite3BtreeTripAllCursors); a commit with cursors open it refuses,
	// or turns into a read transaction the cursors go on in.
	iters []*planIterator
}

func (tx *commonTx) savepointOpened(sp *savepointTx) {
	tx.savepoints = append(tx.savepoints, sp)
}

// savepointEnded pops the savepoint and every savepoint opened inside it,
// marking each as ended.
func (tx *commonTx) savepointEnded(sp *savepointTx) {
	if i := slices.Index(tx.savepoints, sp); i >= 0 {
		for _, inner := range tx.savepoints[i:] {
			inner.ended.Store(true)
		}
		clear(tx.savepoints[i:])
		tx.savepoints = tx.savepoints[:i]
	}
}

func (tx *commonTx) iterOpened(pi *planIterator) {
	tx.iters = append(tx.iters, pi)
}

func (tx *commonTx) iterClosed(pi *planIterator) {
	if i := slices.Index(tx.iters, pi); i >= 0 {
		tx.iters = slices.Delete(tx.iters, i, i+1)
	}
}

// tripIters releases the pages the open iterators hold, as the transaction
// ends: under its lock, before its btree tx ends.
func (tx *commonTx) tripIters() {
	for _, pi := range tx.iters {
		pi.trip()
	}
	clear(tx.iters)
	tx.iters = tx.iters[:0]
}

func (tx *commonTx) schemaLog() *txSchema {
	return &tx.schema
}

// schemaCommitted runs inside the btree commit as it becomes visible
// (btree.WriteTx.OnCommitted): the logged versions become the heads, the
// registry follows them, then the epoch's known passes the commit's cookie
// — so a transaction the epoch admits finds the handles and the registry
// as the commit left them.
func (tx *commonTx) schemaCommitted(fileChangeCounter, schemaCookie uint32) {
	tx.settleSketches()
	if testHookBeforeInstall != nil {
		testHookBeforeInstall()
	}
	// settling from before the first handle changes — the install closes
	// the dropped ones, and a slot held by a closed handle is one a
	// registration may take — until the epoch has moved (sinceNow).
	tx.db.settling.Store(1<<32 | uint64(schemaCookie))
	defer tx.db.settling.Store(0)
	tx.schema.install(tx.db.epoch.Load().gen)
	if testHookAfterInstall != nil {
		testHookAfterInstall()
	}
	tx.db.settleLog(&tx.schema)
	if testHookAfterSettle != nil {
		testHookAfterSettle()
	}
	tx.db.schemaCommitted(fileChangeCounter, schemaCookie)
}

// sketchesCommitted is the commit's callback when only sketches were
// persisted (schemaCommitted settles them too).
func (tx *commonTx) sketchesCommitted(_, _ uint32) {
	tx.settleSketches()
}

// settleSketches publishes the live sketches the commit persisted
// (commonTx.sketches) as the reader snapshots and clears their flags: the
// committed row and the live object agree from here on. Inside the btree
// commit as it becomes visible (btree.WriteTx.OnCommitted), under the write
// lock: a commit that fails settles nothing, its indexes stay flagged and
// listed, and the next write transaction rebases them to the committed
// bytes (resetUncommittedSketches). Settled at the persist instead, the
// deltas of a failed commit would stay in the live sketch unflagged, for
// the next commit to persist.
func (tx *commonTx) settleSketches() {
	for _, idx := range tx.sketches {
		idx.storePubSketch(idx.sketch)
		idx.sketchModified = false
	}
	clear(tx.sketches)
	tx.sketches = tx.sketches[:0]
}

// testHookBeforeInstall, when set, runs inside a schema-changing commit as
// it becomes visible, before the heads are installed; testHookAfterInstall
// once they are, before the registry is settled; testHookAfterSettle once
// it is, before the epoch moves; testHookBeforeBtreeCommit right before the
// btree commit, everything of the commit's persisted into the btree tx;
// testHookAfterBtreeCommit right after the btree commit returned, the
// write lock released. Tests only.
var (
	testHookBeforeInstall     func()
	testHookAfterInstall      func()
	testHookAfterSettle       func()
	testHookBeforeBtreeCommit func(tx *btree.WriteTx)
	testHookAfterBtreeCommit  func()
)

// release returns the pooled state, with nothing pinned. A log still there
// belongs to a commit that did not come back (a panic in the btree): its
// handles are released as after a rollback.
func (tx *commonTx) release() {
	if len(tx.schema.log) > 0 {
		tx.db.discardLog(&tx.schema, 0)
	}
	tx.schema.release()
	clear(tx.sketches)
	tx.sketches = tx.sketches[:0]
	tx.db.txPool.Put(tx)
}

func (tx *commonTx) btreeReadTx() *btree.ReadTx {
	return tx.readTx
}

func (tx *commonTx) btreeWriteTx() *btree.WriteTx {
	return tx.writeTx
}

func (tx *commonTx) markModified() {
	tx.modified.Store(true)
}

// heldTx is the transaction as a context marks it held: the enclosing call
// holds its lock, so a call made through that context runs under its turn
// instead of locking again — the context of an internal savepoint
// (savepointWrapper.Context) hands it to a callback, and a call that holds
// the turn and runs helpers that take their own passes it on (heldCtx).
// Unwrapped by lockCtxTx and ctxWriteTx.
type heldTx struct{ ReadTx }

func (tx *commonTx) instanceId() string {
	return tx.db.instanceId
}

func (tx *commonTx) dbRef() *db {
	return tx.db
}

// txHandle is a transaction's handle, one per transaction, which its
// wrappers, its context and its savepoints all point at: beyond the pooled
// state, the version that is this transaction's, the lock that is its own
// — made with it, never pooled, so a handle of an ended transaction locks
// nothing another transaction uses — and the context.
type txHandle struct {
	*commonTx
	version uint64
	mu      sync.Mutex
	ctx     context.Context
}

func (h *txHandle) Context() context.Context {
	return h.ctx
}

func (h *txHandle) Done() bool {
	return h.commonTx.version.Load() != h.version
}

func (h *txHandle) lock() {
	h.mu.Lock()
}

func (h *txHandle) unlock() {
	h.mu.Unlock()
}

// SetModified is a call on the transaction: it takes the turn, and marks
// nothing once the transaction has ended.
func (h *txHandle) SetModified() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.Done() {
		h.markModified()
	}
}

type readTx struct {
	*txHandle
}

func (r readTx) Commit() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.commonTx.version.CompareAndSwap(r.version, 0) {
		defer r.commonTx.release()
		r.commonTx.tripIters()
		return r.readTx.Rollback()
	}
	return nil
}

type writeTx struct {
	*txHandle
}

// unwind discards this tx's buffered full-text postings and its schema log,
// then rolls the btree tx back. The postings go at rollback, as FTS5
// discards its pending data in xRollback. Both go before the btree releases
// the global write lock inside Rollback: the log is writer state.
func (w writeTx) unwind() error {
	w.commonTx.tripIters()
	w.db.resetAllFtsPending(w.readTx)
	w.db.discardLog(&w.commonTx.schema, 0)
	return w.writeTx.Rollback()
}

func (w writeTx) Rollback() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.commonTx.version.CompareAndSwap(w.version, 0) {
		defer w.commonTx.release()
		return w.unwind()
	}
	return nil
}

// Commit takes the transaction's turn; commit is the work, kept apart so
// that its defers stay open-coded.
func (w writeTx) Commit() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.commit()
}

func (w writeTx) commit() error {
	if w.commonTx.version.CompareAndSwap(w.version, 0) {
		w.commonTx.tripIters()
		// The btree commit releases the global write lock before it
		// returns. A schema-changing commit announces the cookie it
		// produces (observeCookie), and the announcement of one that
		// publishes nothing — empty, or failed — is withdrawn only once it
		// returned; a log the commit did not publish — a pin-only one, or
		// one of a commit that failed — is discarded then too, and touches
		// the handles' marks. Hold the schema gate (paired with newWriteTx)
		// until then, so no writer of this process begins while another
		// process's commit could pass for this one's, or while the marks
		// move. Taken before the release is deferred: what the release has
		// left to discard, it discards under the gate.
		t := &w.commonTx.schema
		schemaChange := w.writeTx.SchemaChanged()
		if schemaChange || len(t.log) > 0 {
			w.db.schemaGate.Lock()
			defer w.db.schemaGate.Unlock()
		}
		defer w.commonTx.release()
		// Every non-committed exit below must discard the schema log
		// BEFORE its btree rollback releases the global write lock (see
		// unwind).
		if w.modified.Load() {
			w.writeTx.MarkDataChanged()
			// Flush the full-text write-back buffer into this same tx BEFORE the
			// btree commit, so postings commit atomically with the documents.
			if err := w.db.flushAllFtsPending(w.writeTx); err != nil {
				_ = w.unwind()
				return err
			}
			if err := w.db.persistAllDirtySketches(w.writeTx, w.commonTx); err != nil {
				_ = w.unwind()
				return err
			}
		}
		if w.writeTx.SchemaChanged() && w.writeTx.HasChanges() {
			// See formatSettledKey. An empty commit (EnsureIndex of an
			// existing index) bumps no cookie and must stay empty.
			if err := w.db.advanceFormatSettled(w.writeTx); err != nil {
				_ = w.unwind()
				return err
			}
		}
		if schemaChange {
			// The logged versions become the heads and the registry follows
			// them as the commit becomes visible, and the epoch follows the
			// cookie right after (commonTx.schemaCommitted). Withdrawn on
			// every exit, a panic in the commit included: an announcement
			// left standing would make the next process's commit at that
			// cookie pass for this one's.
			next := w.writeTx.DiskSchemaCookie() + 1
			w.db.announceCookie(next)
			defer w.db.endAnnouncement(next)
			w.writeTx.OnCommitted(w.commonTx.schemaCommitted)
		} else if len(w.commonTx.sketches) > 0 {
			w.writeTx.OnCommitted(w.commonTx.sketchesCommitted)
		}
		if testHookBeforeBtreeCommit != nil {
			testHookBeforeBtreeCommit(w.writeTx)
		}
		err := w.writeTx.Commit()
		if testHookAfterBtreeCommit != nil {
			testHookAfterBtreeCommit()
		}
		if err == nil && w.modified.Load() {
			w.db.recoveryController.OnWriteEvent()
		}
		// A log the commit did not publish is discarded, as after a rollback.
		if !t.installed {
			w.db.discardLog(t, 0)
		}
		return err
	}
	return nil
}

var savepointPool = &sync.Pool{
	New: func() any {
		return &savepointTx{}
	},
}

// newSavepointTx opens a savepoint on parent, whose lock the caller holds,
// for a call made with ctx. held marks a savepoint of an enclosing call's
// scope (enterWriteTx): its own calls lock nothing, and the context it
// hands out marks the turn as held for the calls made through it.
func newSavepointTx(ctx context.Context, parent WriteTx, held bool) (WriteTx, error) {
	btWtx := parent.btreeWriteTx()
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
	if err := parent.dbRef().flushAllFtsPending(btWtx); err != nil {
		return nil, err
	}
	spId, err := btWtx.Savepoint()
	if err != nil {
		return nil, err
	}
	sp := savepointPool.Get().(*savepointTx)
	version := sp.reset(spId, len(parent.schemaLog().log))
	parent.savepointOpened(sp)
	return savepointWrapper{WriteTx: parent, sp: sp, version: version, held: held, ctx: ctx}, nil
}

// savepointTx is a savepoint's pooled state: what the parent's stack marks
// (commonTx.savepoints) and the version that tells a handle its savepoint
// from the one the state serves next.
type savepointTx struct {
	savepointId int
	// logMark is the parent tx's schema log length at savepoint creation: a
	// rollback to this savepoint discards exactly the schema changes made
	// inside its scope (entries [logMark:]); a release keeps them on the
	// parent so an outer rollback still discards them.
	logMark int
	version atomic.Uint64
	// ended: the savepoint, or one enclosing it, has ended
	// (commonTx.savepointEnded). Read without the parent's lock by Done.
	ended atomic.Bool
}

func (sp *savepointTx) reset(spId, logMark int) uint64 {
	sp.savepointId = spId
	sp.logMark = logMark
	sp.ended.Store(false)
	version := newTxVersion()
	sp.version.Store(version)
	return version
}

// savepointWrapper is a savepoint's handle: the parent it was opened on —
// the pooled state and the lock are reached through it — and what is this
// handle's own, immutable: the savepoint's version, whether the handle is a
// held one (newSavepointTx), and the context it was opened with.
type savepointWrapper struct {
	WriteTx
	sp      *savepointTx
	version uint64
	held    bool
	ctx     context.Context
}

// Context is the context the savepoint was opened with: for a held
// savepoint, carrying the transaction as held (heldTx), so that the calls
// a callback makes through it run under the enclosing call's turn.
func (w savepointWrapper) Context() context.Context {
	if w.held {
		return context.WithValue(w.ctx, ctxKeyTx, heldTx{w.WriteTx})
	}
	return w.ctx
}

// orphaned reports that the savepoint no longer exists: the transaction it
// belongs to has ended, or a savepoint enclosing it has. SQLite frees every
// savepoint when the transaction ends (sqlite3CloseSavepoints) and the nested
// ones when a savepoint ends (see commonTx.savepoints); a later RELEASE or
// ROLLBACK TO fails with "no such savepoint". The parent's pooled state, the
// btree tx and the writer's buffers may already serve another transaction,
// and the btree savepoint id another savepoint: touch nothing.
func (w savepointWrapper) orphaned() bool {
	return w.WriteTx.Done() || w.sp.ended.Load()
}

// SetModified marks the parent modified, in the savepoint's turn.
func (w savepointWrapper) SetModified() {
	if w.held {
		if !w.orphaned() {
			w.markModified()
		}
		return
	}
	w.lock()
	defer w.unlock()
	if !w.Done() {
		w.markModified()
	}
}

// Commit takes its turn on the parent before it claims the savepoint: a
// caller that sees Done and then takes a turn finds the end complete.
func (w savepointWrapper) Commit() error {
	if !w.held {
		w.lock()
		defer w.unlock()
	}
	if !w.sp.version.CompareAndSwap(w.version, 0) {
		return nil
	}
	if w.orphaned() {
		savepointPool.Put(w.sp)
		return ErrTxIsUsed
	}
	w.savepointEnded(w.sp)
	if err := w.btreeWriteTx().ReleaseSavepoint(w.sp.savepointId); err != nil {
		return err
	}
	savepointPool.Put(w.sp)
	return nil
}

func (w savepointWrapper) Rollback() error {
	if !w.held {
		w.lock()
		defer w.unlock()
	}
	if !w.sp.version.CompareAndSwap(w.version, 0) {
		return nil
	}
	if w.orphaned() {
		savepointPool.Put(w.sp)
		return ErrTxIsUsed
	}
	w.savepointEnded(w.sp)
	btWtx := w.btreeWriteTx()
	db := w.dbRef()
	err := btWtx.RollbackToSavepoint(w.sp.savepointId)
	// The fts pending buffers hold only ops made inside this savepoint's
	// scope (they were flushed empty at its creation), and the btree state
	// those ops were derived from has just been reverted — discard them,
	// with the scope's schema log. Both run even when RollbackToSavepoint
	// fails: its error returns happen before any mutation, the outer tx is
	// doomed either way, and matching the in-memory schema state to the
	// last committed disk state is the conservative choice.
	// RollbackToSavepoint keeps the write lock in all cases, so the
	// discard runs inside the critical section.
	db.resetAllFtsPending(&btWtx.ReadTx)
	db.discardLog(w.schemaLog(), w.sp.logMark)
	if err != nil {
		return err
	}
	savepointPool.Put(w.sp)
	return nil
}

// Done takes no lock: a savepoint ends under its parent's lock, and marks
// its pooled state (savepointTx.ended) as it does.
func (w savepointWrapper) Done() bool {
	return w.sp.version.Load() != w.version || w.sp.ended.Load() || w.WriteTx.Done()
}

// noOpTx is the transaction a context carries as an operation gets it
// (getReadTx) when an enclosing call holds the turn: Commit leaves the
// transaction open and does nothing. turnTx is the same for an operation
// that took its own turn: Commit ends the turn.
type noOpTx struct {
	ReadTx
}

func (noOpTx) Commit() error {
	return nil
}

type turnTx struct {
	ReadTx
}

func (tx turnTx) Commit() error {
	tx.unlock()
	return nil
}
