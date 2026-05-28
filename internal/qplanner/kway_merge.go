package qplanner

import (
	"bytes"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/query"
)

// kWayDocIdMergeIter yields distinct docIds in ascending byte order across
// N PointLookup bounds of the same single-field index. Each input cursor
// is positioned on a different bound; the iterator runs them in parallel
// through an inline min-heap keyed by the current entry's docId. When the
// heap's minimum equals the previously emitted docId, it is skipped
// (deduplication). On emit, the producing cursor is advanced.
//
// Memory: O(k) for the heap + 2 × maxDocIdLen for the dedup-comparison
// buffers (see "slice lifetime" below). No per-emission alloc.
//
// Slice lifetime contract:
//
//	The []byte returned from Next aliases an INTERNAL two-buffer scheme
//	and is valid ONLY until the next call to Next on the same merge.
//	Callers that need the docId to outlive the next Next MUST copy.
//	Callers that consume in lock-step (e.g. countEntriesViaMerge
//	incrementing a counter then discarding; FetchIter using docId
//	synchronously for the point lookup) need no copy. This contract
//	is the alloc target: 75 K Next calls × 0 emission-slice allocs.
//
// Why two buffers (and not one):
//
//	The dedup check compares the just-extracted candidate docId against
//	the LAST EMITTED docId. If both alias the same buffer, overwriting
//	the buffer for the new candidate would also mutate "last emitted"
//	and break the equality check on the very next iteration. With two
//	buffers and a single bit of state (activeBuf), the just-overwritten
//	inactive buffer holds the candidate, the active buffer still holds
//	the previous emission; on emit we flip activeBuf. Total alloc is
//	2 × maxDocIdLen, one-time amortized.
//
// Ordering contract:
//
//	The merge emits in lexicographic bytes.Compare order of docIds.
//	For tuple-encoded docIds with mixed types (e.g. a collection with
//	both string-typed and number-typed primary keys), this is
//	type-tag-prefix order (TypeNumber < TypeString), NOT natural sort
//	order. Single-typed docIds (the common case) sort as expected.
//	Callers that need a specific user-visible order MUST request Sort —
//	buildIndexSeekChain bypasses the merge when needSort is true.
//
// Lifetime: the caller transfers ownership of the cursors. Close() releases
// all of them. It is safe to call Close more than once.
//
// Correctness invariants:
//  1. Within each cursor's bound, entries are sorted by (value, docId).
//     Because the bound is PointLookup (Start == End originally; End is
//     0xff-appended by AdjustBoundsForNonUnique), the value prefix is
//     constant, so entries iterate in ascending docId order.
//  2. The heap's minimum is the next docId to consider across all cursors.
//  3. The two-buffer scheme maintains the invariant that the buffer at
//     activeBuf holds the most recently EMITTED docId; the inactive
//     buffer is scratch space for the next candidate.
type kWayDocIdMergeIter struct {
	states        []cursorState   // backing array; the heap is states[:nActive]
	closedCursors []*btree.Cursor // cursors removed from the heap (exhausted or init-failed) but still owned for Close
	nActive       int
	buf0, buf1    []byte // two reusable buffers; one holds last-emitted, one is scratch
	activeBuf     int    // 0 or 1 — index of the buffer currently holding last-emitted
	haveEmitted   bool   // false until first emit
	fieldCount    int    // index field count, for docId extraction
	closed        bool
	initErr       error // first error observed in construction (surfaced from first Next)
}

// cursorState is one entry in the heap.
type cursorState struct {
	cursor       *btree.Cursor
	boundEnd     anyenc.Tuple // matches query.Bound.End type
	boundEndIncl bool
	curKey       []byte // current key bytes; aliased into the cursor's page
	curDocId     []byte // extracted docId suffix of curKey
	exhausted    bool
}

// newKWayDocIdMergeIter takes ownership of cursors. Each cursor MUST be
// positioned at its bound's start (e.g. via cursor.Seek(bound.Start)).
// fieldCount is the number of index fields (must be 1 in v1; assertion).
//
// Returns *kWayDocIdMergeIter unconditionally. If any cursor's first read
// fails during construction, the error is stashed in initErr and surfaced
// from the first Next() call. The caller then sees the error and calls
// Close() to release all cursors. This avoids the "constructor returns
// error" rewrite of every call site while still surfacing read errors
// deterministically.
func newKWayDocIdMergeIter(cursors []*btree.Cursor, bounds query.Bounds, fieldCount int) *kWayDocIdMergeIter {
	if fieldCount != 1 {
		panic("kWayDocIdMergeIter: only single-field indexes supported in v1")
	}
	if len(cursors) != len(bounds) {
		panic("kWayDocIdMergeIter: cursors and bounds length mismatch")
	}
	m := &kWayDocIdMergeIter{
		states:     make([]cursorState, 0, len(cursors)),
		fieldCount: fieldCount,
	}
	qpPerf.mergeDispatches.Add(1)
	// Keep an out-of-band slot for cursors that immediately exhaust so
	// Close still releases them; the heap only contains the active ones.
	m.closedCursors = make([]*btree.Cursor, 0, len(cursors))
	for i, c := range cursors {
		st := cursorState{
			cursor:       c,
			boundEnd:     bounds[i].End,
			boundEndIncl: bounds[i].EndInclude,
		}
		if err := st.readCurrent(fieldCount); err != nil {
			if m.initErr == nil {
				m.initErr = err
			}
			st.exhausted = true
		}
		if !st.exhausted {
			m.states = append(m.states, st)
		} else {
			// Track the cursor so Close releases it even though it didn't
			// enter the heap.
			m.closedCursors = append(m.closedCursors, c)
		}
	}
	m.nActive = len(m.states)
	// Floyd's heap construction, O(n).
	for i := m.nActive/2 - 1; i >= 0; i-- {
		m.siftDown(i)
	}
	return m
}

// Next yields the next distinct docId. The returned slice aliases an
// internal buffer and is INVALIDATED by the next call to Next on the
// same merge — copy if the value must outlive that. See the type-level
// "slice lifetime contract" doc.
//
// Returns (nil, false, nil) when the merge is drained.
func (m *kWayDocIdMergeIter) Next() ([]byte, bool, error) {
	if m.initErr != nil {
		err := m.initErr
		m.initErr = nil
		return nil, false, err
	}
	for m.nActive > 0 {
		top := &m.states[0]

		// CRITICAL: copy top.curDocId into the INACTIVE buffer BEFORE
		// calling advance(), because advance() may release the cursor's
		// current page and invalidate curDocId's underlying bytes.
		// Reading top.curDocId after advance would be a use-after-
		// invalidation in production cursors (the existing tests don't
		// catch it because small in-memory fixtures don't page-evict mid-
		// iteration; production page caches do).
		var inactive *[]byte
		if m.activeBuf == 0 {
			inactive = &m.buf1
		} else {
			inactive = &m.buf0
		}
		*inactive = append((*inactive)[:0], top.curDocId...)
		candidate := *inactive

		// Advance the producing cursor; readCurrent inside advance() may
		// mark top exhausted and may invalidate top.curDocId.
		if err := top.advance(m.fieldCount); err != nil {
			return nil, false, err
		}
		if top.exhausted {
			// Track the now-exhausted cursor for Close. It is the slot at
			// states[0]; we pop it by swapping with the tail.
			m.closedCursors = append(m.closedCursors, top.cursor)
			m.states[0] = m.states[m.nActive-1]
			// Zero out the trailing slot to drop the cursor reference so
			// Close doesn't see a stale duplicate.
			m.states[m.nActive-1] = cursorState{}
			m.nActive--
			if m.nActive > 0 {
				m.siftDown(0)
			}
		} else {
			m.siftDown(0)
		}

		// Dedup: candidate (inactive buffer) vs last emitted (active
		// buffer). If equal, skip — the inactive buffer is freely reused
		// on the next loop iteration without alloc.
		if m.haveEmitted {
			var active []byte
			if m.activeBuf == 0 {
				active = m.buf0
			} else {
				active = m.buf1
			}
			if bytes.Equal(candidate, active) {
				continue
			}
		}
		// Emit. Flip which buffer is "active" (= last emitted); the next
		// call to Next will overwrite the now-inactive (previously active)
		// buffer.
		m.activeBuf ^= 1
		m.haveEmitted = true
		return candidate, true, nil
	}
	return nil, false, nil
}

// Close releases all cursor resources. Safe to call multiple times.
func (m *kWayDocIdMergeIter) Close() {
	if m.closed {
		return
	}
	m.closed = true
	for i := range m.states[:m.nActive] {
		if m.states[i].cursor != nil {
			m.states[i].cursor.Close()
			m.states[i].cursor = nil
		}
	}
	for i, c := range m.closedCursors {
		if c != nil {
			c.Close()
			m.closedCursors[i] = nil
		}
	}
	m.nActive = 0
}

// siftDown maintains the min-heap invariant by sinking states[i] toward
// the leaves. Keyed by curDocId (bytes.Compare).
func (m *kWayDocIdMergeIter) siftDown(i int) {
	n := m.nActive
	for {
		l, r := 2*i+1, 2*i+2
		best := i
		if l < n && bytes.Compare(m.states[l].curDocId, m.states[best].curDocId) < 0 {
			best = l
		}
		if r < n && bytes.Compare(m.states[r].curDocId, m.states[best].curDocId) < 0 {
			best = r
		}
		if best == i {
			return
		}
		m.states[i], m.states[best] = m.states[best], m.states[i]
		i = best
	}
}

// readCurrent loads curKey/curDocId from the cursor's current position.
// Sets exhausted=true if the cursor is invalid or past boundEnd.
//
// NB: curDocId aliases the cursor's page. It is only valid until the next
// call to cursor.Next on the same cursor (= until advance is called).
// Next() handles this by copying into the inactive buffer before advancing.
func (st *cursorState) readCurrent(fieldCount int) error {
	if !st.cursor.Valid() {
		st.exhausted = true
		return nil
	}
	k, err := st.cursor.Key()
	if err != nil {
		return err
	}
	if len(st.boundEnd) > 0 {
		cmp := bytes.Compare(k, st.boundEnd)
		if cmp > 0 || (cmp == 0 && !st.boundEndIncl) {
			st.exhausted = true
			return nil
		}
	}
	st.curKey = k
	// For a single-field index, key = tuple(value, docId). Use the
	// existing helper to extract docId.
	st.curDocId = extractDocId(k, fieldCount)
	return nil
}

func (st *cursorState) advance(fieldCount int) error {
	if st.exhausted {
		return nil
	}
	if err := st.cursor.Next(); err != nil {
		return err
	}
	return st.readCurrent(fieldCount)
}
