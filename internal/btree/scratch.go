package btree

// scratchPool is a small bounded free-list of reusable []T scratch slices,
// generalizing the cell-buffer scratch pattern (SQLite Pager.pTmpSpace /
// sqlite3ScratchMalloc, pager.c). A free-list rather than a single slot because
// the balance path needs several alive AT ONCE: balanceNonroot's pooled cells
// coexist with the collect* buffers of up to NB gathered siblings, and a
// cascade up the tree (insertSepIntoAncestor → balanceNonroot on the parent)
// nests another full set before the child's set is recycled.
//
// RAM stays controllable: at most cellScratchMax entries are retained per pool,
// each converging on the working-set size of one balance (see put's
// keep-the-larger policy) — no sync.Pool, no unbounded growth. Writes are
// single-threaded, so no locking.
type scratchPool[T any] struct {
	free [][]T
	// clearOnPut zeroes the full capacity of a returned slice before pooling
	// it, releasing references (pointers, sub-slices) held by stale entries so
	// they do not pin their backing arrays in the GC. Enable for element types
	// that contain pointers; leave off for plain scalar buffers.
	clearOnPut bool
}

// take removes and returns a reusable slice (length 0) whose capacity is
// >= minCap. Returns nil if none qualifies — the caller then allocates its
// own, which put folds back into the pool, so the pool self-tunes to the
// workload's sizes. The caller owns the returned slice until it puts it back.
func (sp *scratchPool[T]) take(minCap int) []T {
	for i := len(sp.free) - 1; i >= 0; i-- {
		if cap(sp.free[i]) >= minCap {
			s := sp.free[i][:0]
			last := len(sp.free) - 1
			sp.free[i] = sp.free[last]
			sp.free[last] = nil
			sp.free = sp.free[:last]
			return s
		}
	}
	return nil
}

// takeOrMake is take with an allocation fallback: when no pooled slice
// qualifies it allocates a fresh one (length 0, capacity minCap), which put
// later folds back into the pool. Centralizes the take / if-nil-make idiom of
// the balance-path call sites.
func (sp *scratchPool[T]) takeOrMake(minCap int) []T {
	if s := sp.take(minCap); s != nil {
		return s
	}
	return make([]T, 0, minCap)
}

// put returns a slice to the free-list for reuse. When the pool is full it
// keeps the larger slices (replacing the smallest when the incoming one is
// bigger), so the pool converges on the workload's peak scratch sizes and
// never grows past cellScratchMax entries.
func (sp *scratchPool[T]) put(s []T) {
	if cap(s) == 0 {
		return
	}
	if sp.clearOnPut {
		clear(s[:cap(s)])
	}
	if len(sp.free) < cellScratchMax {
		sp.free = append(sp.free, s[:0])
		return
	}
	minI := 0
	for i := 1; i < len(sp.free); i++ {
		if cap(sp.free[i]) < cap(sp.free[minI]) {
			minI = i
		}
	}
	if cap(s) > cap(sp.free[minI]) {
		sp.free[minI] = s[:0]
	}
}

// reset drops all retained slices (used from the pager init/re-open paths).
func (sp *scratchPool[T]) reset() {
	sp.free = sp.free[:0]
}
