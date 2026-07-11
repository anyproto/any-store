package query

import (
	"github.com/anyproto/any-store/v2/syncpool"
)

// RawFilter is an optional fast path implemented by filters that can evaluate
// against the raw encoded document (as stored, root already decoded — see
// anyenc.Parser.DecodedDoc) without building the full Value tree. Unindexed
// scans use it to REJECT documents cheaply: the field walk skips unrelated
// values length-only, so a low-selectivity scan avoids parsing ~everything it
// discards (the full parse dominated the scan profile). Accepted documents are
// re-parsed by the caller as before, so downstream semantics are unchanged.
//
// handled=false means this filter (or a required sub-filter) cannot decide
// from the raw bytes — the caller must fall back to Ok on the parsed document.
// Implementations must guarantee that when handled=true, ok equals what Ok
// would return on the parsed document (TestFilterOkRawParity).
type RawFilter interface {
	OkRaw(doc []byte, buf *syncpool.DocBuffer) (ok bool, handled bool)
}

// OkRaw extracts the field's raw bytes with a length-only walk and evaluates
// the inner filter on just that fragment. Falls back (handled=false) when the
// walk cannot mirror Value.Get exactly (array container on the path, malformed
// bytes) or when no scratch buffer is available.
func (e Key) OkRaw(doc []byte, buf *syncpool.DocBuffer) (bool, bool) {
	if buf == nil {
		return false, false
	}
	raw, exact, err := buf.Parser.RawByPathChecked(doc, e.Path...)
	if err != nil || !exact {
		return false, false
	}
	if raw == nil {
		// Absent path: identical to v.Get(...) returning nil.
		return e.Filter.Ok(nil, buf), true
	}
	// Parse only the referenced fragment (usually one scalar). RawByPathChecked
	// bounds it exactly, so ParseOwned's no-tail check holds. The fragment
	// Values alias the decoded doc and are consumed immediately by Ok.
	fv, perr := buf.Parser.ParseOwned(raw)
	if perr != nil {
		return false, false
	}
	return e.Filter.Ok(fv, buf), true
}

func (e And) OkRaw(doc []byte, buf *syncpool.DocBuffer) (bool, bool) {
	// A handled child that rejects decides the And regardless of the children
	// we cannot evaluate lazily — so an unhandled child defers the fallback
	// instead of bailing out, letting a mixed filter like
	// {"$and":[{"a":1},{"$text":...}]} still reject cheaply on the Key child.
	// Concluding true requires every child handled.
	allHandled := true
	for _, f := range e {
		rf, isRaw := f.(RawFilter)
		if !isRaw {
			allHandled = false
			continue
		}
		ok, handled := rf.OkRaw(doc, buf)
		if !handled {
			allHandled = false
			continue
		}
		if !ok {
			return false, true // short-circuit reject, like And.Ok
		}
	}
	if !allHandled {
		return false, false
	}
	return true, true
}

func (e Or) OkRaw(doc []byte, buf *syncpool.DocBuffer) (bool, bool) {
	// A handled child that matches proves the Or regardless of the children we
	// cannot evaluate lazily; concluding false requires every child handled.
	allHandled := true
	for _, f := range e {
		rf, isRaw := f.(RawFilter)
		if !isRaw {
			allHandled = false
			continue
		}
		ok, handled := rf.OkRaw(doc, buf)
		if !handled {
			allHandled = false
			continue
		}
		if ok {
			return true, true
		}
	}
	if !allHandled {
		return false, false
	}
	return false, true
}

func (e Nor) OkRaw(doc []byte, buf *syncpool.DocBuffer) (bool, bool) {
	// Nor is exactly not(Or); delegate so the lazy-evaluation logic (handled
	// short-circuits, unhandled bookkeeping) lives in one place.
	ok, handled := Or(e).OkRaw(doc, buf)
	if !handled {
		return false, false
	}
	return !ok, true
}

func (e Not) OkRaw(doc []byte, buf *syncpool.DocBuffer) (bool, bool) {
	rf, isRaw := e.Filter.(RawFilter)
	if !isRaw {
		return false, false
	}
	ok, handled := rf.OkRaw(doc, buf)
	if !handled {
		return false, false
	}
	return !ok, true
}
