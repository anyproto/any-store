package qplanner

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/query"
)

// kWayMergeMax is the upper bound on len(Bounds) for the k-way merge path
// (Task 5). Above this, the dedup walk runs with a pre-sized seen-set.
// Default 64; can be overridden via SetKWayMergeMax (test/operator escape
// hatch). Setting to 0 disables the merge entirely (forces all multi-bound
// multi-key counts through the pre-sized seen-set path).
//
// Justification for 64: per-bound cursor setup is ~1 µs cold-page (Seek +
// first page fault). At k=64 that is ~64 µs of fixed setup against a merge
// body that processes ~50 ns/entry — so the merge wins only when the entry
// walk would otherwise be >>64 µs (entries >> 1k). The kWayMergeMinEntries
// gate covers the small-N case independently.
var kWayMergeMax atomic.Int32

// kWayMergeMinEntries is the minimum sum-of-sketch-estimates across bounds
// for which the merge is preferred. Below this, cursor setup cost dominates
// and the pre-sized seen-set is faster. Default 200. Sketch reads here are
// a COST HINT, not answer-determining — a wrong estimate produces a
// suboptimal plan choice, still-correct answer (see docs/known-issues.md
// I-03).
var kWayMergeMinEntries atomic.Int32

func init() {
	kWayMergeMax.Store(64)
	kWayMergeMinEntries.Store(200)
}

// SetKWayMergeMax overrides the merge dispatch upper bound for testing or
// production tuning. Pass 0 to disable the merge entirely. Returns the
// previous value, suitable for the restore-on-defer idiom:
//
//	prev := SetKWayMergeMax(0); defer SetKWayMergeMax(prev)
//
// Process-global; takes effect immediately for all subsequent queries
// (atomic swap). Internal/qplanner; downstream consumers should call
// anystore.SetKWayMergeMax instead.
func SetKWayMergeMax(n int) int {
	return int(kWayMergeMax.Swap(int32(n)))
}

// SetKWayMergeMinEntries overrides the merge dispatch lower bound on
// sum-of-sketch-estimates across the query's bounds. Below this value
// the dispatch picks the pre-sized seen-set walk instead of the merge.
// Returns the previous value; same restore-on-defer pattern as
// SetKWayMergeMax. Process-global, atomic, default 200.
func SetKWayMergeMinEntries(n int) int {
	return int(kWayMergeMinEntries.Swap(int32(n)))
}

// IndexIter iterates over an index namespace using bounds.
// key = indexFields + docId for both unique and non-unique indexes.
//
// Field layout note: pointers are packed first, then the slice header
// (Bounds), then the int, then all bools at the end. Without this layout
// the bools interleave with pointer fields and produce ~88 bytes after
// alignment padding; the current layout is 72 bytes. Saves 16 B/op on
// every query that builds an IndexIter (most index queries) because
// seekBatch embeds IndexIter inline (planner.go).
type IndexIter struct {
	Source  *CursorSource
	IdxInfo *IndexInfo
	cursor  *btree.Cursor

	// Sketch is the per-index frequency sketch. When non-nil, the pre-sized
	// seen-set fallback (countEntriesViaPreSizedSeenSet) and the merge
	// dispatch (Task 5) use Sketch.Estimate as a COST/CAPACITY hint —
	// sketch-stale-under or sketch-stale-over only affects allocation
	// efficiency and path choice, never the answer. May be nil for
	// freshly-created indexes; the fallback then runs a per-bound
	// CountUntil pre-pass to derive the capacity. See docs/known-issues.md
	// I-03 for the rule (sketch reads must never determine answers).
	Sketch *IndexSketch

	Bounds   query.Bounds
	boundIdx int

	Reverse bool
	started bool

	// PointLookup mirrors CBOIndex.PointLookup at iter-construction time.
	// It is true iff every bound was an equality (Start==End) before
	// AdjustBoundsForNonUnique appended 0xff to End in place. The post-
	// adjustment Start != End, so this flag is the ONLY reliable
	// PointLookup signal for IndexIter consumers.
	PointLookup bool

	// pendingCurrent is set by skipOffset after it positions the cursor
	// directly on the first row to emit. When true, the next Next() call
	// returns the entry at the current cursor position WITHOUT advancing
	// first (it then clears the flag). This makes the handoff from a
	// cursor-level offset skip to normal iteration off-by-one-safe across
	// page boundaries: skipOffset leaves the cursor on the target entry
	// rather than one-before it.
	pendingCurrent bool
}

func (it *IndexIter) Next() (key []byte, docId []byte, multiKey bool, err error) {
	perf := perfCountersEnabled()
	var start time.Time
	if perf {
		start = time.Now()
		qpPerf.indexNextCalls.Add(1)
	}
	defer func() {
		if perf {
			qpPerf.indexNextNs.Add(uint64(time.Since(start).Nanoseconds()))
			if docId != nil {
				qpPerf.indexYields.Add(1)
			}
		}
	}()

	if it.cursor == nil {
		it.cursor = it.Source.NewCursor()
	}

	// No bounds: full index scan
	if len(it.Bounds) == 0 {
		return it.nextNoBounds()
	}

	for {
		if it.boundIdx >= len(it.Bounds) {
			return nil, nil, false, nil
		}

		b := it.Bounds[it.boundIdx]

		if !it.started {
			it.started = true
			if it.Reverse {
				if len(b.End) > 0 {
					if err = it.cursor.Seek(b.End); err != nil {
						return nil, nil, false, err
					}
					if !it.cursor.Valid() {
						if err = it.cursor.Last(); err != nil {
							return nil, nil, false, err
						}
					} else {
						// Check if we need to back up (Seek finds >=)
						k, kerr := it.cursor.Key()
						if kerr != nil {
							return nil, nil, false, kerr
						}
						cmp := bytes.Compare(k, b.End)
						if cmp > 0 || (cmp == 0 && !b.EndInclude) {
							if err = it.cursor.Previous(); err != nil {
								return nil, nil, false, err
							}
						}
					}
				} else {
					if err = it.cursor.Last(); err != nil {
						return nil, nil, false, err
					}
				}
			} else {
				if len(b.Start) > 0 {
					if err = it.cursor.Seek(b.Start); err != nil {
						return nil, nil, false, err
					}
					if it.cursor.Valid() && !b.StartInclude {
						k, kerr := it.cursor.Key()
						if kerr != nil {
							return nil, nil, false, kerr
						}
						if bytes.Equal(k, b.Start) {
							if err = it.cursor.Next(); err != nil {
								return nil, nil, false, err
							}
						}
					}
				} else {
					if err = it.cursor.First(); err != nil {
						return nil, nil, false, err
					}
				}
			}
		} else {
			if it.Reverse {
				if err = it.cursor.Previous(); err != nil {
					return nil, nil, false, err
				}
			} else {
				if err = it.cursor.Next(); err != nil {
					return nil, nil, false, err
				}
			}
		}

		if !it.cursor.Valid() {
			it.boundIdx++
			it.started = false
			continue
		}

		k, err := it.cursor.Key()
		if err != nil {
			return nil, nil, false, err
		}

		// Check bounds
		if !it.Reverse && len(b.End) > 0 {
			cmp := bytes.Compare(k, b.End)
			if cmp > 0 || (cmp == 0 && !b.EndInclude) {
				it.boundIdx++
				it.started = false
				continue
			}
		}
		if it.Reverse && len(b.Start) > 0 {
			cmp := bytes.Compare(k, b.Start)
			if cmp < 0 || (cmp == 0 && !b.StartInclude) {
				it.boundIdx++
				it.started = false
				continue
			}
		}

		return it.extractResult(k)
	}
}

func (it *IndexIter) nextNoBounds() (key []byte, docId []byte, multiKey bool, err error) {
	switch {
	case it.pendingCurrent:
		// skipOffset positioned the cursor directly on the entry to emit;
		// return it without advancing. Clear the flag so the following
		// call advances normally.
		it.pendingCurrent = false
	case !it.started:
		it.started = true
		if it.Reverse {
			if err = it.cursor.Last(); err != nil {
				return nil, nil, false, err
			}
		} else {
			if err = it.cursor.First(); err != nil {
				return nil, nil, false, err
			}
		}
	default:
		if it.Reverse {
			if err = it.cursor.Previous(); err != nil {
				return nil, nil, false, err
			}
		} else {
			if err = it.cursor.Next(); err != nil {
				return nil, nil, false, err
			}
		}
	}
	if !it.cursor.Valid() {
		return nil, nil, false, nil
	}
	k, err := it.cursor.Key()
	if err != nil {
		return nil, nil, false, err
	}
	return it.extractResult(k)
}

// skipOffset advances the index cursor past up to n logical result rows
// WITHOUT fetching/parsing the skipped documents, implementing the
// offsetSkipper contract. See offsetSkipper for the full contract.
//
// Scope (correctness): only the unbounded full-index scan is fast-skipped,
// and only across entries the index records as scalar (multiKey==false —
// the doc's single entry in this index). On the first multi-key/legacy
// entry it stops and returns the unskipped remainder, leaving the cursor
// on that entry so the normal Next() path (FetchIter → CanonicalKeyDedup →
// LimitIter, or consumer-side DocDedup) resolves the offset correctly.
//
// For bounded scans it skips nothing (returns n): bounded index scans
// either carry a residual filter (which blocks the delegation chain
// upstream) or would require per-bound logical-row accounting that the
// safe fetch-then-skip path already handles correctly.
func (it *IndexIter) skipOffset(n int) (remaining int, err error) {
	if n <= 0 {
		return 0, nil
	}
	// Only the unbounded full-index scan is fast-skipped. With bounds we
	// fall back to the safe fetch-then-skip path (return the full offset).
	if len(it.Bounds) != 0 {
		return n, nil
	}
	// skipOffset is only ever invoked before iteration starts (LimitIter
	// calls it on its first Next, before pulling any row). Guard against a
	// mid-stream call to avoid corrupting cursor position.
	if it.started {
		return n, nil
	}
	if it.cursor == nil {
		it.cursor = it.Source.NewCursor()
	}
	it.started = true

	// Position at the first entry in scan order.
	if it.Reverse {
		if err = it.cursor.Last(); err != nil {
			return n, err
		}
	} else {
		if err = it.cursor.First(); err != nil {
			return n, err
		}
	}

	skipped := 0
	for skipped < n && it.cursor.Valid() {
		val, verr := it.cursor.Value()
		if verr != nil {
			return n - skipped, verr
		}
		if EntryValueIsMultiKey(val) {
			// A doc with >1 entries in this index (array/legacy). Entry
			// count no longer equals logical-row count: stop here and let
			// the dedup-aware path skip the remaining rows. Leave the
			// cursor on this entry; the next Next() emits it.
			it.pendingCurrent = true
			return n - skipped, nil
		}
		// Scalar entry == exactly one distinct doc == one logical row.
		skipped++
		if it.Reverse {
			if err = it.cursor.Previous(); err != nil {
				return n - skipped, err
			}
		} else {
			if err = it.cursor.Next(); err != nil {
				return n - skipped, err
			}
		}
	}

	// The next Next() should emit the entry at the current position (the
	// first un-skipped row), not advance past it. If the cursor ran out of
	// entries (skipped < n), the flag is harmless: Next() sees an invalid
	// cursor and returns end-of-stream.
	it.pendingCurrent = true
	return n - skipped, nil
}

// CountEntries counts distinct documents matching this index iterator's
// bounds. Two strategies based on bound count:
//
//	len(Bounds) <= 1: use cursor.CountUntil — page-batch count without
//	                  visiting individual cells. Within-doc dedup in
//	                  insertKeys guarantees ≤1 entry per distinct value
//	                  per doc, so the entry count equals the doc count.
//	                  Fast: no per-entry walk, no value reads.
//
//	len(Bounds) >  1: walk each entry, read the value byte, stream-count
//	                  scalar entries, and dedup multi-key (or legacy)
//	                  entries through a lazy seen-set. A doc with array
//	                  [v1,v2] would otherwise be counted twice when
//	                  $in:[v1,v2] crosses both bounds — the seen-set
//	                  reduces it to one. The seen-set is allocated on
//	                  the first multi-key entry and skipped entirely for
//	                  fully-scalar streams.
func (it *IndexIter) CountEntries() (int, error) {
	if it.cursor == nil {
		it.cursor = it.Source.NewCursor()
	}
	if len(it.Bounds) <= 1 {
		return it.countEntriesBatch()
	}

	// Shapes that can't ride the merge (compound multi-key, non-PointLookup,
	// range bounds) keep the existing lazy-alloc countEntriesWithDedup. The
	// pre-sized seen-set is only worth it for the PointLookup multi-key
	// case where every entry needs dedup; for compound/range, the seen-set
	// fills opportunistically and pre-sizing from sketch is unsound (range
	// bounds aren't pinned to a single sketch bucket).
	if !it.PointLookup || len(it.IdxInfo.FieldNames) != 1 {
		return it.countEntriesWithDedup()
	}

	// Two-phase dispatch for PointLookup + single-field. Phase 1 checks
	// the no-I/O gates (k bound, min-N sketch sum) and routes before
	// paying any cursor cost. Phase 2 (boundsAllScalar peek) only runs
	// when phase 1 leaves the merge as a candidate.
	//
	// Why phase ordering matters: the boundsAllScalar peek costs 1+ Seek;
	// for queries whose sketch sum is below the min-N gate (e.g., absent-
	// value $in like array_index/InEmpty), the merge is not the right
	// path anyway. Peeking before checking min-N adds an unnecessary Seek
	// to those queries; checking min-N first recovers alpha.6 baseline
	// behavior for low-N shapes.
	kMax := int(kWayMergeMax.Load())
	if kMax <= 0 || len(it.Bounds) > kMax {
		// k > kMax — too many bounds for the merge primitive. The
		// pre-sized seen-set (S2 capacity hint) is worth its setup cost
		// here because the walk is unbounded.
		return it.countEntriesViaPreSizedSeenSet()
	}
	if !passesMergeMinNGate(it.Bounds, it.Sketch) {
		// Sketch sum below kWayMergeMinEntries: the merge's k cursor
		// opens + heap setup don't amortize against this few entries,
		// and pre-sizing the seen-set is similarly wasteful (the lazy
		// growth of the alpha.6 countEntriesWithDedup is the cheapest
		// path). Skipping boundsAllScalar here is what fixes the
		// array_index/InEmpty regression: an absent-value $in (sketch
		// sum == 0) goes straight to countEntriesWithDedup with no
		// pre-pass and no over-eager peek.
		return it.countEntriesWithDedup()
	}

	// Merge is a viable candidate. Now peek to confirm there is at least
	// one multi-key bound — otherwise the existing peek-then-batch path
	// in countEntriesWithDedup is faster (CountUntil never visits the
	// seen-set; pre-allocating one would regress SimpleIndex/In).
	scalarOnly, err := it.boundsAllScalar()
	if err != nil {
		return 0, err
	}
	if scalarOnly {
		return it.countEntriesWithDedup()
	}
	return it.countEntriesViaMerge()
}

// passesMergeMinNGate decides whether the k-way merge is preferable to the
// pre-sized seen-set walk based on the sum of sketch estimates across
// bounds. Sketch reads here are a COST HINT only — a stale sketch produces
// a suboptimal path choice, still-correct answer (see docs/known-issues.md
// I-03).
//
// When the sketch is nil, return true: there is no cheap estimate, so
// committing to the merge avoids the CountUntil pre-pass that
// seenSetCapacityHint would otherwise run.
func (it *IndexIter) passesMergeMinNGate() bool {
	return passesMergeMinNGate(it.Bounds, it.Sketch)
}

// passesMergeMinNGate is the bounds+sketch-only variant used by both
// IndexIter.CountEntries and buildIndexSeekChain to keep their dispatch
// decisions in sync. See IndexIter.passesMergeMinNGate for semantics.
func passesMergeMinNGate(bounds query.Bounds, sketch *IndexSketch) bool {
	if sketch == nil {
		return true
	}
	var sum uint64
	for i := range bounds {
		sum += sketch.Estimate(bounds[i].Start)
	}
	return sum >= uint64(kWayMergeMinEntries.Load())
}

// canRunMergeStatic evaluates the static portion of the merge-dispatch
// gate — everything decidable without opening a cursor. Returns true iff
// PointLookup and single-field hold, the bound count is in [2, kMax], and
// the min-N sketch sum gate passes (sketch nil counts as "passes").
//
// The boundsAllScalar peek is NOT included here: it requires opening
// cursors and is therefore only practical inside CountEntries (which
// already has a working cursor) or deferred to first-Next on the Iter
// path. Today the Iter path ignores it — see the comment on the
// `useMerge` predicate in planner.go for the residual cost.
func canRunMergeStatic(bounds query.Bounds, fieldNames []string, pointLookup bool, sketch *IndexSketch) bool {
	if !pointLookup || len(fieldNames) != 1 || len(bounds) < 2 {
		return false
	}
	kMax := int(kWayMergeMax.Load())
	if kMax <= 0 || len(bounds) > kMax {
		return false
	}
	return passesMergeMinNGate(bounds, sketch)
}

// boundsAllScalar returns true if every bound's first entry has the
// IndexValueScalar value byte (i.e. confirmed not multi-key). Legacy
// nil-value entries return false (treat conservatively as multi-key).
// Reuses it.cursor — leaves it positioned arbitrarily; callers must
// re-seek before use.
//
// Empty bounds (Seek lands past the bound's End) are treated as
// neutral and skipped. Without the end-check, the cursor would land
// on whatever next key exists in the btree — possibly an unrelated
// value with its own value byte — and we'd classify the bound based on
// foreign data. This isn't a wrong-answer bug (every downstream path
// re-checks the bound's End during its own walk), but it would cause
// suboptimal path selection: a query with one empty bound + one scalar
// bound could read MULTI-key from the unrelated entry and engage the
// merge unnecessarily.
//
// Performance note: this is a k-Seek + k-Value-read peek. At cold-cache
// k=64 it can take ~50 µs which dominates the merge it gates. A future
// optimization (Task 8) caches HasMultiKey on IndexSketch to short-
// circuit this peek.
func (it *IndexIter) boundsAllScalar() (bool, error) {
	for _, b := range it.Bounds {
		if err := it.seekBoundStart(b); err != nil {
			return false, err
		}
		if !it.cursor.Valid() {
			continue
		}
		// Verify the cursor landed inside the bound. Seek positions the
		// cursor at the smallest key >= Start; if no such key exists
		// within (Start, End], the cursor is on an out-of-bound key for
		// the next existing value.
		if len(b.End) > 0 {
			k, kerr := it.cursor.Key()
			if kerr != nil {
				return false, kerr
			}
			cmp := bytes.Compare(k, b.End)
			if cmp > 0 || (cmp == 0 && !b.EndInclude) {
				continue // empty bound — read no value byte
			}
		}
		val, err := it.cursor.Value()
		if err != nil {
			return false, err
		}
		if EntryValueIsMultiKey(val) {
			return false, nil
		}
	}
	return true, nil
}

// countEntriesViaMerge runs the k-way docId-merge and counts emissions.
// Each bound gets a fresh cursor (the merge consumes them); the original
// it.cursor is reused for bound 0 to avoid one allocation.
func (it *IndexIter) countEntriesViaMerge() (int, error) {
	cursors := make([]*btree.Cursor, 0, len(it.Bounds))
	// Cleanup pattern: a deferred closure releases cursors unless ownership
	// has been transferred to the merge. We flip ownedByMerge once
	// newKWayDocIdMergeIter has taken the slice.
	ownedByMerge := false
	defer func() {
		if !ownedByMerge {
			for _, c := range cursors {
				if c != nil {
					c.Close()
				}
			}
		}
	}()
	for i, b := range it.Bounds {
		var c *btree.Cursor
		if i == 0 {
			c = it.cursor
			it.cursor = nil // ownership transferred to merge
		} else {
			c = it.Source.NewCursor()
		}
		cursors = append(cursors, c)
		if err := c.Seek(b.Start); err != nil {
			return 0, err
		}
		if c.Valid() && !b.StartInclude {
			k, kerr := c.Key()
			if kerr != nil {
				return 0, kerr
			}
			if bytes.Equal(k, b.Start) {
				if err := c.Next(); err != nil {
					return 0, err
				}
			}
		}
	}
	m := newKWayDocIdMergeIter(cursors, it.Bounds, len(it.IdxInfo.FieldNames))
	ownedByMerge = true
	defer m.Close()
	count := 0
	for {
		_, ok, err := m.Next()
		if err != nil {
			return 0, err
		}
		if !ok {
			return count, nil
		}
		count++
	}
}

// countEntriesBatch is the original page-batch fast path, used for
// single-bound (or unbounded — len(Bounds)==0 returns 0) counts.
func (it *IndexIter) countEntriesBatch() (int, error) {
	total := 0
	for _, b := range it.Bounds {
		if len(b.Start) > 0 {
			if err := it.cursor.Seek(b.Start); err != nil {
				return 0, err
			}
			if it.cursor.Valid() && !b.StartInclude {
				k, kerr := it.cursor.Key()
				if kerr != nil {
					return 0, kerr
				}
				if bytes.Equal(k, b.Start) {
					if err := it.cursor.Next(); err != nil {
						return 0, err
					}
				}
			}
		} else {
			if err := it.cursor.First(); err != nil {
				return 0, err
			}
		}

		if !it.cursor.Valid() {
			continue
		}

		n, err := it.cursor.CountUntil(b.End, b.EndInclude)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

// countEntriesWithDedup walks each bound. If the bound's first entry is
// scalar AND no multi-key entry has been seen across previous bounds,
// the bound is counted via the page-batch fast path (cursor.CountUntil)
// — same as the single-bound case. As soon as a multi-key (or legacy)
// entry appears anywhere, we fall back to the per-entry walk for the
// remainder of the iteration so docId-level dedup is applied.
//
// Rationale: in practice most indexes are entirely scalar OR entirely
// multi-key for a given query (the field is either array-typed or not).
// Mixed indexes are rare. The peek-then-batch shortcut means the common
// scalar-only case pays only one extra value-read per bound (vs the
// pure-batch path), recovering most of the alpha.2 SimpleIndex/In speed
// while preserving correctness for multi-key.
func (it *IndexIter) countEntriesWithDedup() (int, error) {
	return it.countEntriesWithDedupUsingSeen(nil) // nil → lazy alloc on first multi-key
}

// countEntriesWithDedupUsingSeen is countEntriesWithDedup parameterized
// over the seen-set so callers can pre-size it. seen may be nil — the
// inner loop allocates on the first multi-key entry, matching the
// existing lazy-allocation behavior. Pass a non-nil pre-sized map to
// avoid map-growth doublings on workloads where every entry is multi-key.
func (it *IndexIter) countEntriesWithDedupUsingSeen(seen map[string]struct{}) (int, error) {
	total := 0
	stickyMulti := false // once any multi-key seen, never re-engage batch fast path

	for _, b := range it.Bounds {
		if err := it.seekBoundStart(b); err != nil {
			return 0, err
		}
		if !it.cursor.Valid() {
			continue
		}

		// Peek the first entry's value. If it's scalar AND we haven't yet
		// seen a multi-key entry in this iteration, use batch counting:
		// the within-doc dedup invariant still holds (each doc has ≤1
		// entry per distinct value across the whole index, which means
		// ≤1 entry in any value-range covered by a single bound).
		if !stickyMulti {
			val, verr := it.cursor.Value()
			if verr != nil {
				return 0, verr
			}
			if !EntryValueIsMultiKey(val) {
				n, err := it.cursor.CountUntil(b.End, b.EndInclude)
				if err != nil {
					return 0, err
				}
				total += n
				continue
			}
			stickyMulti = true
		}

		// Per-entry walk with seen-set dedup.
		for it.cursor.Valid() {
			k, kerr := it.cursor.Key()
			if kerr != nil {
				return 0, kerr
			}
			if len(b.End) > 0 {
				cmp := bytes.Compare(k, b.End)
				if cmp > 0 || (cmp == 0 && !b.EndInclude) {
					break
				}
			}
			val, verr := it.cursor.Value()
			if verr != nil {
				return 0, verr
			}
			if EntryValueIsMultiKey(val) {
				if seen == nil {
					seen = make(map[string]struct{}, 64)
				}
				docId := extractDocId(k, len(it.IdxInfo.FieldNames))
				if _, dup := seen[string(docId)]; dup {
					if err := it.cursor.Next(); err != nil {
						return 0, err
					}
					continue
				}
				seen[string(docId)] = struct{}{}
			}
			total++
			if err := it.cursor.Next(); err != nil {
				return 0, err
			}
		}
	}
	return total, nil
}

// countEntriesViaPreSizedSeenSet is the dedup walk with the seen-set
// pre-allocated from Sketch.Estimate (S2). Identical correctness to
// countEntriesWithDedup; differs only in allocation profile (one map
// alloc up-front, no rehashing).
func (it *IndexIter) countEntriesViaPreSizedSeenSet() (int, error) {
	capHint := it.seenSetCapacityHint()
	seen := make(map[string]struct{}, capHint)
	return it.countEntriesWithDedupUsingSeen(seen)
}

// seenSetCapacityHint returns an upper-bound estimate of the seen-set's
// eventual size. Prefers Sketch.Estimate (no extra I/O); falls back to a
// per-bound cursor.CountUntil pre-pass when the sketch is unavailable.
// A wrong estimate only affects allocation efficiency, never the answer
// (see docs/known-issues.md I-03).
func (it *IndexIter) seenSetCapacityHint() int {
	if it.Sketch != nil {
		var sum uint64
		for i := range it.Bounds {
			// Only Equality bounds contribute a reliable estimate. Range
			// bounds aren't pinned to a single sketch bucket.
			if it.PointLookup && len(it.Bounds[i].Start) > 0 {
				sum += it.Sketch.Estimate(it.Bounds[i].Start)
			}
		}
		if sum > 1<<24 { // 16M cap — defensive against sketch over-estimate
			return 1 << 24
		}
		if sum > 0 {
			return int(sum)
		}
		// Sketch returned 0 (cold sketch or all-stale-zero). Fall through
		// to CountUntil pre-pass below; we MUST NOT short-circuit on the
		// zero sum per I-03.
	}
	var sum int
	for _, b := range it.Bounds {
		if err := it.seekBoundStart(b); err != nil {
			return 64 // best-effort hint; caller still walks
		}
		if !it.cursor.Valid() {
			continue
		}
		n, err := it.cursor.CountUntil(b.End, b.EndInclude)
		if err != nil {
			return 64
		}
		sum += n
	}
	if sum < 64 {
		sum = 64 // floor matching today's default cap
	}
	return sum
}

// seekBoundStart positions the cursor at the first entry of the given
// bound, accounting for StartInclude. Shared between
// countEntriesWithDedup and the existing countEntriesBatch.
func (it *IndexIter) seekBoundStart(b query.Bound) error {
	if len(b.Start) > 0 {
		if err := it.cursor.Seek(b.Start); err != nil {
			return err
		}
		if it.cursor.Valid() && !b.StartInclude {
			k, kerr := it.cursor.Key()
			if kerr != nil {
				return kerr
			}
			if bytes.Equal(k, b.Start) {
				if err := it.cursor.Next(); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return it.cursor.First()
}

// Close releases the underlying cursor resources.
func (it *IndexIter) Close() {
	if it.cursor != nil {
		it.cursor.Close()
		it.cursor = nil
	}
}

func (it *IndexIter) extractResult(k []byte) (key []byte, docId []byte, multiKey bool, err error) {
	// Both unique and non-unique: key = indexFields + docId.
	//
	// multiKey is read from the entry's value byte (set by insertKeys).
	// Legacy entries with empty values are treated as multi-key for
	// safety; see IndexEntryFlagMultiKey + EntryValueIsMultiKey.
	val, verr := it.cursor.Value()
	if verr != nil {
		return nil, nil, false, verr
	}
	docID := extractDocId(k, len(it.IdxInfo.FieldNames))
	return k, docID, EntryValueIsMultiKey(val), nil
}

func (it *IndexIter) String() string {
	s := fmt.Sprintf("IndexScan(%s)", it.IdxInfo.Name)
	if it.Reverse {
		s += "(reverse)"
	}
	if len(it.Bounds) > 0 {
		s += fmt.Sprintf("[bounds=%s]", it.Bounds.String())
	}
	return s
}

// extractDocId extracts the docId portion from a non-unique index key.
// The key is a tuple of (field1, field2, ..., docId).
func extractDocId(key anyenc.Tuple, numFields int) []byte {
	offset, err := key.OffsetAfter(numFields)
	if err != nil {
		// Corrupt tuple shouldn't crash planner path; keep previous behavior
		// and let downstream key lookups fail naturally.
		return key
	}
	if offset < len(key) {
		return key[offset:]
	}
	return key
}
