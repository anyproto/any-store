package btree

import "sync/atomic"

// writerGate lets DB.Close wait for the write transaction's call in flight.
//
// DB.Close does not wait for a write transaction to end: it rolls back the
// one it finds, releases its locks and tears the pager down. The goroutine
// that owns the transaction can be anywhere at that moment, also in the
// middle of a b-tree operation: Put and balance hold no lock, and the page
// cache, the wal-index mapping and the files they use are what the teardown
// frees. So every call a write transaction makes into the pager is counted in
// and out here, and Close, before it rolls the transaction back, closes the
// gate and waits for the count to reach zero. From then on a call of that
// transaction is refused at the door with ErrClosed and touches nothing.
//
// Commit and Rollback are such calls, and so are a backup's writes to its
// destination (Backup.Step, Backup.update). Closing the gate is therefore
// the one decision that ends the transaction: a call that Close refused is
// never followed by a Commit that publishes what the calls before it wrote.
// A Commit or a Rollback in flight is waited for to its end, the unwind of a
// panic included: it leaves the gate once it has released everything but
// the write lock, and Close then waits for the write lock.
//
// SQLite needs none of this. In serialized mode, the default build,
// sqlite3_step holds the connection mutex, which sqlite3_close takes
// (vdbeapi.c:923, main.c:1263); sqlite3_close then refuses with SQLITE_BUSY
// while a statement is unfinalized, and sqlite3_close_v2 leaves the close to
// the last finalize (main.c:1283, 1371). In multi-thread mode an ordinary
// build gives the connection no mutex (main.c:3388-3393), and a close during
// another thread's step is the application's error.
// DRIFT: Close ends a write transaction under its goroutine, after waiting for the call in flight; C refuses or defers the close See docs/btree/NOTES.md#drift-2026-11-21-25-close-waits-for-the-writers-call-in-flight
//
// The count is of calls, not of transactions, and nests: a gated call may
// make another (a write saves the transaction's cursors, and each reads its
// key). Only the outermost call is refused once the gate is closed; a call
// that was let in before runs to its end, calls it makes included. A cursor
// counts the calls it makes on itself as one (Cursor.enterGate). Calls of
// two transactions are never in the count together: a Commit or a Rollback
// leaves the gate before the write lock passes on (releaseWriterLocks), as
// SQLite's COMMIT releases its locks inside the step that holds the
// connection mutex, and the next BEGIN is another step.
//
// Only the write transaction pays: two atomic adds per call, on a word no
// other goroutine writes until Close. A read transaction never comes here;
// Close waits for readers on db.mu. A writer cursor's read or step served
// entirely from the leaf it pins stays outside too (Cursor.readsPinnedLeaf).
type writerGate struct {
	// n is the number of calls in flight, with writerGateClosed set once
	// Close has closed the gate.
	n atomic.Int32
	// idle receives one token when the count reaches zero with the gate
	// closed. Buffered: the last call out does not wait for Close.
	idle chan struct{}
}

const writerGateClosed = 1 << 30

func newWriterGate() writerGate {
	return writerGate{idle: make(chan struct{}, 1)}
}

// enter counts a call of the write transaction in. It returns ErrClosed, and
// counts nothing, once Close has closed the gate and no call is in flight.
// A nil error is paired with leave, deferred: a b-tree operation can panic
// on a damaged page, and a count left behind would keep Close waiting.
func (g *writerGate) enter() error {
	if g.n.Add(1) == writerGateClosed+1 {
		g.leave()
		return ErrClosed
	}
	return nil
}

// leave counts a call out, and tells a waiting Close when it was the last.
func (g *writerGate) leave() {
	if g.n.Add(-1) == writerGateClosed {
		select {
		case g.idle <- struct{}{}:
		default:
		}
	}
}

// close closes the gate and returns once no call is in flight. Called by
// DB.Close, once. It must not run inside a call of the write transaction on
// that call's own goroutine: it would wait for itself.
func (g *writerGate) close() {
	if testWriterGateClosingHook != nil {
		testWriterGateClosingHook()
	}
	for {
		old := g.n.Load()
		if g.n.CompareAndSwap(old, old|writerGateClosed) {
			if old&^writerGateClosed == 0 {
				return
			}
			break
		}
	}
	if testWriterGateWaitingHook != nil {
		testWriterGateWaitingHook()
	}
	<-g.idle
}

// closed reports whether Close has closed the gate.
func (g *writerGate) closed() bool {
	return g.n.Load()&writerGateClosed != 0
}

// CloseState is a write transaction's view of DB.Close (ReadTx.CloseState).
// It holds the database's writer gate, not the transaction, so it can be
// read outside any call of the transaction: from a goroutine that holds
// what an earlier call returned while the transaction ends, or after it.
type CloseState struct {
	g *writerGate
}

// EndedByClose reports whether DB.Close has closed the writer gate of the
// transaction's database. For a transaction still open then, that is its
// end: Close rolled it back, and what its owner still holds of it (a
// document read earlier, the rows of a sort) is of a transaction that never
// committed. False for a read transaction's. Every storage call of an ended
// transaction reports ErrClosed by itself; this is for the callers that
// have no such call to make. Once true it stays true, also for a
// transaction that had ended by itself before Close came: a caller asks
// after it has checked for that end.
func (s CloseState) EndedByClose() bool {
	return s.g != nil && s.g.closed()
}

// Test hooks of the gate's close, test-only:
//   - testWriterGateClosingHook runs in DB.Close as it is about to close the
//     gate: the last point at which a call of the write transaction can
//     still get in;
//   - testWriterGateWaitingHook runs once the gate is closed with a call in
//     flight, before the wait for it;
//   - testWriterGateDrainedHook runs in DB.Close once the gate is closed and
//     empty, before the forced rollback.
var (
	testWriterGateClosingHook func()
	testWriterGateWaitingHook func()
	testWriterGateDrainedHook func()
)
