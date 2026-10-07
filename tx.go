package anystore

import (
	"context"
	"runtime"
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
	// markModified is SetModified for a caller inside a call on the transaction.
	markModified()

	// schemaLog is the transaction's uncommitted schema changes; see
	// txSchema. Unexported: DDL-internal.
	schemaLog() *txSchema

	// The stack of open savepoints; see commonTx.savepoints. savepointEnded
	// returns the savepoint enclosing sp, nil for the outermost.
	// Unexported: savepoint-internal.
	savepointOpened(sp *savepointTx)
	savepointEnded(sp *savepointTx) *savepointTx
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

	// A call on the transaction begins and ends; see txHandle.enter. The
	// call that ends the transaction, or a savepoint, begins with enterEnd.
	enter()
	enterEnd(cbDepth int32) error
	exit()

	// A user callback the current call runs begins and ends, and how many
	// are running; see txHandle.enter.
	callbackBegin()
	callbackEnd()
	callbackDepth() int32

	// The open iterators that hold cursors on the transaction; see
	// commonTx.iters. Unexported: iterator-internal.
	iterOpened(pi *planIterator)
	iterClosed(pi *planIterator)
}

// commonTx is a transaction's pooled state (db.txPool): what the handles of
// one transaction share, and what a later transaction of the db takes over
// once this one ended. A handle tells the two apart by version, and it
// reads nothing else of a transaction that may have ended — its call
// counters and its context are the handle's own (txHandle).
//
// One goroutine at a time: what a transaction holds — the page cache of
// its btree tx, the full-text buffers, the schema log, the savepoint stack
// — is built for one caller, and every call
// made on it — an operation run with its context, a savepoint's, its
// commit or rollback, a method of an iterator — begins and ends as a call
// on its handle (txHandle.enter). Calls nest: a user callback the library
// runs — a query.Modifier — may call back into the transaction through its
// context, and the helpers of a call reach it through the context that
// marks the transaction held (heldTx). What the operation running the
// modifier holds — the collection's schema and document it loaded, the
// btree tx — the modifier must not pull away: a write to that collection
// (collection.writable) and an end of the transaction or of a savepoint
// enclosing the modifier (enterEnd) are refused. Two calls at once, from
// two goroutines, are a misuse the handle detects the way the runtime
// detects concurrent map writes: best effort, with a panic
// (ErrTxConcurrentCalls); an end waits the other call out instead.
type commonTx struct {
	db      *db
	readTx  *btree.ReadTx
	writeTx *btree.WriteTx
	version atomic.Uint64
	// modified: atomic, as SetModified is public and a handle of an ended
	// transaction may still call it while the commit reads it.
	modified atomic.Bool

	// schema is the transaction's own schema state (txSchema), reached
	// through the btree transaction's Aux slot, which holds the commonTx,
	// for the resolve of every operation. For a write transaction its log
	// holds the uncommitted DDL: the writes later
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
	// committed is onCommitted bound once (db.pooledTx): a method value
	// escapes, and a commit that persisted sketches would allocate it every
	// time. schemaChange is what the commit told the callback beyond the
	// sketches: the btree tx changed the schema (schemaCommitted).
	committed    func(fileChangeCounter, schemaCookie uint32)
	schemaChange bool

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
// marking each as ended. The nested ones hand their sketch images down,
// innermost first, so sp holds what its own end restores or hands on. Returns
// the savepoint enclosing sp, nil for the outermost.
func (tx *commonTx) savepointEnded(sp *savepointTx) (enclosing *savepointTx) {
	i := slices.Index(tx.savepoints, sp)
	if i < 0 {
		return nil
	}
	for k := len(tx.savepoints) - 1; k > i; k-- {
		inner := tx.savepoints[k]
		inner.ended.Store(true)
		inner.releaseImages(tx.savepoints[k-1])
	}
	sp.ended.Store(true)
	clear(tx.savepoints[i:])
	tx.savepoints = tx.savepoints[:i]
	if i > 0 {
		return tx.savepoints[i-1]
	}
	return nil
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
// ends: inside the call that ends it, before its btree tx ends.
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

// journalSketch takes the pre-image of idx's live sketch onto the innermost
// open savepoint, if any (savepointTx.journalSketch).
func (tx *commonTx) journalSketch(idx *index) {
	if n := len(tx.savepoints); n > 0 {
		tx.savepoints[n-1].journalSketch(idx)
	}
}

// schemaCommitted runs inside the btree commit as it becomes visible
// (btree.WriteTx.OnCommitted): the logged versions become the heads, the
// registry follows them, then the epoch's known passes the commit's cookie
// — so a transaction the epoch admits finds the handles and the registry
// as the commit left them.
func (tx *commonTx) schemaCommitted(fileChangeCounter, schemaCookie uint32) {
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

// onCommitted is the commit's callback (btree.WriteTx.OnCommitted, bound
// as commonTx.committed): the sketches the commit persisted settle, then
// the schema it changed installs.
func (tx *commonTx) onCommitted(fileChangeCounter, schemaCookie uint32) {
	tx.settleSketches()
	if tx.schemaChange {
		tx.schemaCommitted(fileChangeCounter, schemaCookie)
	}
}

// settleSketches publishes the live sketches the commit persisted
// (commonTx.sketches) as the reader snapshots and clears their flags: the
// committed row and the live object agree from here on. Inside the btree
// commit as it becomes visible, under the write lock: a commit that fails
// settles nothing, its indexes stay flagged and listed, and the next write
// transaction rebases them to the committed bytes
// (resetUncommittedSketches).
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
	// A savepoint still open ends with the transaction, as SQLite frees
	// every savepoint at the end of a transaction (pager.c
	// releaseAllSavepoints): its images go — a commit kept the writes, a
	// rollback is rebased at the next begin — before its handle can pool
	// the state for a savepoint of any database.
	for _, sp := range tx.savepoints {
		sp.releaseImages(nil)
	}
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
// counts for it, so a call made through that context begins no call of its
// own — the context of an internal savepoint (savepointWrapper.Context)
// hands it to the callback of a write scope, and a call that runs helpers
// which would begin their own passes it on (heldCtx). Unwrapped by
// enterCtxTx and ctxWriteTx.
type heldTx struct{ ReadTx }

func (tx *commonTx) instanceId() string {
	return tx.db.instanceId
}

func (tx *commonTx) dbRef() *db {
	return tx.db
}

// txHandle is a transaction's handle, one per transaction, which its
// wrappers, its context and its savepoints all point at: beyond the pooled
// state, the version that is this transaction's, the call counters that
// are its own — made with it, never pooled, so a handle of an ended
// transaction counts nothing another transaction uses — and the context.
type txHandle struct {
	*commonTx
	version uint64
	// calls is the calls in progress on the transaction, callbacks the
	// user callbacks running inside them; see enter.
	calls     atomic.Int32
	callbacks atomic.Int32
	ctx       context.Context
}

func (h *txHandle) Context() context.Context {
	return h.ctx
}

func (h *txHandle) Done() bool {
	return h.commonTx.version.Load() != h.version
}

// enter begins a call on the transaction. A call that begins while another
// is in progress and no callback runs is a misuse — a second goroutine's
// call, or one from a callback the library does not bracket (a Filter, a
// Sort, OnIntegrityError) — and panics with ErrTxConcurrentCalls, the way
// the runtime detects concurrent map writes: a best-effort check of the
// overlap, not of the goroutine, so a second goroutine's call that lands
// inside a callback passes for a nested one. The count is given back
// before the panic: the call in progress goes on, and the transaction can
// still be ended.
func (h *txHandle) enter() {
	if h.calls.Add(1) > 1 && h.callbacks.Load() == 0 {
		h.calls.Add(-1)
		panic(ErrTxConcurrentCalls)
	}
}

// enterEnd begins the call that ends the transaction, or a savepoint
// opened while cbDepth callbacks were running. From inside a callback the
// transaction or the savepoint encloses, the end is refused
// (ErrTxEndInModifier): the operation running the callback holds the btree
// tx. From inside a callback the savepoint was opened in, it nests. An
// overlap it waits out instead of refusing: a deferred Rollback must end
// the transaction after a refused call, or the writer lock is held for
// good.
func (h *txHandle) enterEnd(cbDepth int32) error {
	switch cb := h.callbacks.Load(); {
	case cb > cbDepth:
		return ErrTxEndInModifier
	case cb > 0:
		h.enter()
		return nil
	}
	for !h.calls.CompareAndSwap(0, 1) {
		runtime.Gosched()
	}
	return nil
}

func (h *txHandle) exit() {
	h.calls.Add(-1)
}

// callbackBegin marks a user callback running inside the current call: the
// calls it makes on the transaction nest in it (enter).
func (h *txHandle) callbackBegin() {
	h.callbacks.Add(1)
}

func (h *txHandle) callbackEnd() {
	h.callbacks.Add(-1)
}

func (h *txHandle) callbackDepth() int32 {
	return h.callbacks.Load()
}

// SetModified is a call on the transaction, and marks nothing once the
// transaction has ended.
func (h *txHandle) SetModified() {
	h.enter()
	defer h.exit()
	if !h.Done() {
		h.markModified()
	}
}

type readTx struct {
	*txHandle
}

func (r readTx) Commit() error {
	if err := r.enterEnd(0); err != nil {
		return err
	}
	defer r.exit()
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
	if err := w.enterEnd(0); err != nil {
		return err
	}
	defer w.exit()
	if w.commonTx.version.CompareAndSwap(w.version, 0) {
		defer w.commonTx.release()
		return w.unwind()
	}
	return nil
}

// Commit is the call that ends the transaction; commit is the work, kept
// apart so that its defers stay open-coded.
func (w writeTx) Commit() error {
	if err := w.enterEnd(0); err != nil {
		return err
	}
	defer w.exit()
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
		}
		w.commonTx.schemaChange = schemaChange
		if schemaChange || len(w.commonTx.sketches) > 0 {
			w.writeTx.OnCommitted(w.commonTx.committed)
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

// newSavepointTx opens a savepoint on parent, inside a call on it, for a
// call made with ctx. held marks a savepoint of an enclosing call's scope
// (enterWriteTx): its own methods begin no call, and the context it hands
// out marks the transaction as held for the calls made through it.
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
	return savepointWrapper{WriteTx: parent, sp: sp, version: version, held: held, cbDepth: parent.callbackDepth(), ctx: ctx}, nil
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
	// images are the pre-images of the live sketches first written inside
	// this savepoint's scope (journalSketch), in creation order: a rollback
	// restores them (restoreImages), a release hands them to the enclosing
	// savepoint, or drops the ones it holds an older image of
	// (releaseImages). The statement journal of SQLite's pager keeps a
	// page's pre-image the same way — once per savepoint (pager.c
	// subjRequiresPage), kept by RELEASE for the savepoint enclosing it
	// unless that one has it (bTruncateOnRelease), played back by ROLLBACK
	// TO (pagerPlaybackSavepoint). Nothing is journaled outside a
	// savepoint: a full rollback is rebased at the next write-tx begin
	// (db.resetUncommittedSketches), and a savepoint open as its
	// transaction ends drops them (commonTx.release). Buffers stay with the
	// pooled state.
	images []sketchImage
	// scope is the version the savepoint was opened with, kept once a
	// handle claimed its end (version is zeroed then): what the images of
	// its scope are marked with (index.sketchImaged).
	scope   uint64
	version atomic.Uint64
	// ended: the savepoint, or one enclosing it, has ended
	// (commonTx.savepointEnded). Read by Done outside any call.
	ended atomic.Bool
}

func (sp *savepointTx) reset(spId, logMark int) uint64 {
	sp.savepointId = spId
	sp.logMark = logMark
	sp.ended.Store(false)
	version := newTxVersion()
	sp.scope = version
	sp.version.Store(version)
	return version
}

// sketchImage is the pre-image of an index's live sketch, taken as a
// savepoint's scope first wrote it: the counters, and the flag and the
// marker the index had, all put back by the scope's rollback.
type sketchImage struct {
	idx *index
	// prev is idx.sketchImaged at the image: the version of the savepoint
	// holding the next older image, 0 for none.
	prev uint64
	// modified is idx.sketchModified at the image.
	modified bool
	buf      []uint64
}

// journalSketch takes the pre-image of idx's live sketch into this
// savepoint, unless its scope holds one already (idx.sketchImaged).
func (sp *savepointTx) journalSketch(idx *index) {
	if idx.sketchImaged == sp.scope {
		return
	}
	n := len(sp.images)
	if n < cap(sp.images) {
		sp.images = sp.images[:n+1]
	} else {
		sp.images = append(sp.images, sketchImage{})
	}
	img := &sp.images[n]
	img.idx = idx
	img.prev = idx.sketchImaged
	img.modified = idx.sketchModified
	img.buf = idx.sketch.Snapshot(img.buf)
	idx.sketchImaged = sp.scope
}

// restoreImages puts the live sketches back as the scope found them, newest
// image first: of two images of one sketch — a handle clone sharing the
// sketch (index.cloneWithNs) can leave an older one behind a newer — the
// older wins. The images stay in creation order: a savepoint takes one only
// while no savepoint is open inside it, and the nested ones hand theirs
// down as they end.
func (sp *savepointTx) restoreImages() {
	for i := len(sp.images) - 1; i >= 0; i-- {
		img := &sp.images[i]
		if img.idx.sketch.Restore(img.buf) {
			img.idx.sketchModified = img.modified
		} else {
			// Bytes of another shape, not restored: the deltas stay and
			// the flag keeps them for the next begin's rebase.
			img.idx.sketchModified = true
		}
		img.idx.sketchImaged = img.prev
		img.idx = nil
	}
	sp.images = sp.images[:0]
}

// releaseImages hands the images to the savepoint enclosing this one, which
// keeps the older image it holds of the same index (prev names it) and
// adopts the rest: nothing wrote that index between the two savepoints'
// creation, or the enclosing one would hold an image. With no enclosing
// savepoint the images are dropped. The marker follows the image that
// stays.
func (sp *savepointTx) releaseImages(enclosing *savepointTx) {
	for i := range sp.images {
		img := &sp.images[i]
		if enclosing != nil && img.prev != enclosing.scope {
			img.idx.sketchImaged = enclosing.scope
			enclosing.images = append(enclosing.images, *img)
			img.buf = nil
		} else {
			img.idx.sketchImaged = img.prev
		}
		img.idx = nil
	}
	sp.images = sp.images[:0]
}

// savepointWrapper is a savepoint's handle: the parent it was opened on —
// the pooled state and the call counters are reached through it — and
// what is this handle's own, immutable: the savepoint's version, whether
// the handle is a held one (newSavepointTx), the callbacks running when it
// was opened (enterEnd), and the context it was opened with.
type savepointWrapper struct {
	WriteTx
	sp      *savepointTx
	version uint64
	held    bool
	cbDepth int32
	ctx     context.Context
}

// Context is the context the savepoint was opened with: for a held
// savepoint, carrying the transaction as held (heldTx), so that the calls
// a callback makes through it nest in the enclosing call.
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

// SetModified marks the parent modified: a call on it, unless held.
func (w savepointWrapper) SetModified() {
	if w.held {
		if !w.orphaned() {
			w.markModified()
		}
		return
	}
	w.enter()
	defer w.exit()
	if !w.Done() {
		w.markModified()
	}
}

// Commit is the call that ends the savepoint (enterEnd), begun before it
// claims the savepoint.
func (w savepointWrapper) Commit() error {
	if !w.held {
		if err := w.enterEnd(w.cbDepth); err != nil {
			return err
		}
		defer w.exit()
	}
	if !w.sp.version.CompareAndSwap(w.version, 0) {
		return nil
	}
	if w.orphaned() {
		savepointPool.Put(w.sp)
		return ErrTxIsUsed
	}
	w.sp.releaseImages(w.savepointEnded(w.sp))
	if err := w.btreeWriteTx().ReleaseSavepoint(w.sp.savepointId); err != nil {
		return err
	}
	savepointPool.Put(w.sp)
	return nil
}

func (w savepointWrapper) Rollback() error {
	if !w.held {
		if err := w.enterEnd(w.cbDepth); err != nil {
			return err
		}
		defer w.exit()
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
	// with the scope's schema log, and put the sketches the scope wrote
	// back (before the log goes: the flags the discard propagates are the
	// restored ones). All three run even when RollbackToSavepoint
	// fails: its error returns happen before any mutation, the outer tx is
	// doomed either way, and matching the in-memory schema state to the
	// last committed disk state is the conservative choice.
	// RollbackToSavepoint keeps the write lock in all cases, so the
	// discard runs inside the critical section.
	db.resetAllFtsPending(&btWtx.ReadTx)
	w.sp.restoreImages()
	db.discardLog(w.schemaLog(), w.sp.logMark)
	if err != nil {
		return err
	}
	savepointPool.Put(w.sp)
	return nil
}

// Done begins no call: a savepoint ends inside a call on its parent, and
// marks its pooled state (savepointTx.ended) as it does.
func (w savepointWrapper) Done() bool {
	return w.sp.version.Load() != w.version || w.sp.ended.Load() || w.WriteTx.Done()
}

// noOpTx is the transaction a context carries as an operation gets it
// (getReadTx) inside an enclosing call: Commit leaves the transaction open
// and does nothing. callTx is the same for an operation that began a call
// of its own: Commit ends the call.
type noOpTx struct {
	ReadTx
}

func (noOpTx) Commit() error {
	return nil
}

type callTx struct {
	ReadTx
}

func (tx callTx) Commit() error {
	tx.exit()
	return nil
}
