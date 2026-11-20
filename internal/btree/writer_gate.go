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
// A Commit or a Rollback in flight is waited for to its end, the release of
// the transaction's locks and the unwind of a panic included.
//
// SQLite needs none of this: sqlite3_step holds the connection mutex, which
// sqlite3_close takes (vdbeapi.c sqlite3_step, main.c sqlite3Close), and
// sqlite3Close then refuses with SQLITE_BUSY while a statement is active.
// DRIFT: Close ends a write transaction under its goroutine, after waiting for the call in flight; C refuses the close See docs/btree/NOTES.md#drift-2026-11-19-1-close-waits-for-the-writers-call-in-flight
//
// The count is of calls, not of transactions, and nests: a gated call may
// make another (a write saves the transaction's cursors, and each reads its
// key). Only the outermost call is refused once the gate is closed; a call
// that was let in before runs to its end, calls it makes included. A cursor
// counts the calls it makes on itself as one (Cursor.enterGate).
//
// Only the write transaction pays: two atomic adds per call, on a word no
// other goroutine writes until Close. A read transaction never comes here;
// Close waits for readers on db.mu. One kind of call of the write
// transaction stays outside: a cursor's Key or Value that reads nothing but
// the leaf the cursor pins (Cursor.readsPinnedLeaf).
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
