package anystore

import "context"

type ctxKey int

const (
	ctxKeyTx ctxKey = iota
)

// TxFromContext returns the transaction ctx carries and whether it carries
// one: the transaction whose Context ctx derives from, or the transaction a
// savepoint of it sits on, as a savepoint's Context carries that. A write
// transaction is returned as its WriteTx. The handle is returned as the
// operations that join it see it: it may have ended (Done reports it) and it
// may belong to another DB. For a caller that must not block while a
// transaction is open: an operation run with such a ctx joins the
// transaction, while one run with a ctx that carries none waits for the
// writer lock or a reader slot.
func TxFromContext(ctx context.Context) (ReadTx, bool) {
	v := ctx.Value(ctxKeyTx)
	if v == nil {
		return nil, false
	}
	if h, ok := v.(heldTx); ok {
		v = h.ReadTx
	}
	tx, ok := v.(ReadTx)
	return tx, ok
}
