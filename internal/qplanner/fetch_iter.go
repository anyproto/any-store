package qplanner

import (
	"fmt"
	"time"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/syncpool"
)

// FetchIter wraps an index-yielding iterator (which produces docIds)
// and performs point-lookups in the data namespace to fetch full documents.
// It parses and caches the result in Plan.DocParsed to avoid double-fetch/double-parse.
type FetchIter struct {
	Source Iterator
	Data   *CursorSource
	Buf    *syncpool.DocBuffer
	Plan   *Plan // set by BuildPlan for doc value caching

	// cursor is the retained data-namespace cursor reused across rows. It is
	// lazily minted on the first Next() and lives for the whole scan, in the
	// SAME ReadTx as Data/Source (Data is a per-FetchIter CursorSource bound to
	// params.Tx). Reusing it lets SeekNear activate the same-leaf fast path —
	// when the next docId falls within the already-pinned leaf, the lookup
	// skips the full root-to-leaf descent. This mirrors IndexIter, which already
	// holds a long-lived *btree.Cursor for the whole index scan in this same tx
	// (index_iter.go), so same-tx retained-cursor validity is proven in
	// production, not theoretical. The cursor reads a frozen COW snapshot
	// (NewCursor pins walMaxFrame=tx.walHdr.mxFrame + the reader's private cache,
	// db.go), so the one retained leaf page is always the snapshot-correct page —
	// there is no in-tx writer to invalidate it (so SQLite's writer-only
	// saveAllCursors/CURSOR_REQUIRESEEK machinery, btree.c:806, has no analogue).
	// Released in Close (the single pinned leaf), so nothing leaks.
	cursor *btree.Cursor
}

func (it *FetchIter) Next() (key []byte, docId []byte, multiKey bool, err error) {
	perf := perfCountersEnabled()
	var start time.Time
	if perf {
		start = time.Now()
		qpPerf.fetchNextCalls.Add(1)
	}
	defer func() {
		if perf {
			qpPerf.fetchNextNs.Add(uint64(time.Since(start).Nanoseconds()))
			if docId != nil {
				qpPerf.fetchYields.Add(1)
			}
		}
	}()

	for {
		key, docId, multiKey, err = it.Source.Next()
		if err != nil || docId == nil {
			return nil, nil, false, err
		}

		// Lazily mint the retained data cursor on the first real lookup,
		// exactly like IndexIter (index_iter.go). One cursor reused for every
		// row of this scan. Minted here (not before the source loop) so a source
		// that errors before yielding any docId never touches it.Data — matching
		// the cursor-free path's contract.
		if it.cursor == nil {
			it.cursor = it.Data.NewCursor()
		}

		// Retained-cursor point lookup. AppendValueByKey -> SeekNear, whose
		// same-leaf window [firstKey,lastKey] (btree.go) mirrors
		// sqlite3BtreeIndexMoveto case-1 (sqlitec/src/btree.c:6065): when the
		// cursor is already positioned on the leaf that covers the target key,
		// it re-binary-searches the pinned leaf and SKIPS moveToRoot / the full
		// root-to-leaf descent. On a miss it falls back to a full root descent
		// (c.Seek), byte-identical to the old cursor-free AppendValue.
		//
		// DRIFT: v2 pins only the leaf frame (cursorFrame.pg, btree.go) versus
		// SQLite pinning every apPage[]. The deeper IndexMoveto case-2
		// bypass_moveto_root shortcut (btree.c:6072-6080, restart the search on
		// the retained interior stack instead of re-rooting) is NOT implemented:
		// because v2 does not pin interior pages, re-validating a retained
		// interior frame costs the same getPage + searchInterior as descending
		// into it, so under a warm cache it saves no page reads and adds CPU.
		// Measured (cache=20000, 10981-row real query): a bottom-up covering-
		// ancestor reseek dropped descendChild/row 4.00->3.43 but left
		// getPageReader/row flat at 5.10 and raised searchInterior CPU
		// ~20%->24%, so it was rejected. The same-leaf fast path here is the only
		// reuse that is genuinely free, and it rarely fires for this workload
		// (docIds arrive uncorrelated with data-key order); it pays off when the
		// fetch order is docId-sorted, which is out of scope here.
		var lookupStart time.Time
		if perf {
			lookupStart = time.Now()
		}
		it.Buf.DocBuf, err = it.cursor.AppendValueByKey(docId, it.Buf.DocBuf[:0])
		if perf {
			qpPerf.fetchLookupNs.Add(uint64(time.Since(lookupStart).Nanoseconds()))
		}
		if err != nil {
			if err == btree.ErrKeyNotFound {
				// doc may have been deleted from data but still in index; skip
				continue
			}
			return nil, nil, false, err
		}

		// Parse and cache the value to avoid re-fetching and re-parsing later
		if it.Plan != nil {
			var parseStart time.Time
			if perf {
				parseStart = time.Now()
			}
			doc, perr := it.Buf.Parser.ParseOwned(it.Buf.DocBuf)
			if perf {
				qpPerf.fetchParseNs.Add(uint64(time.Since(parseStart).Nanoseconds()))
			}
			if perr != nil {
				return nil, nil, false, perr
			}
			it.Plan.DocParsed = doc
		}

		return key, docId, multiKey, nil
	}
}

// skipOffset delegates a cursor-level offset skip to the source so skipped
// rows are never fetched/parsed (the whole point: avoid the data-namespace
// point lookup + ParseOwned for offset rows). FetchIter maps 1:1 onto its
// source for every index entry that has a corresponding data doc — which is
// every entry in a consistent committed snapshot, since deleteItem removes a
// doc's index entries and its data row in the same write transaction. (The
// ErrKeyNotFound branch in Next is a defensive guard for corruption that
// never occurs in a consistent snapshot; CountEntries likewise counts by
// index entry, so the skip stays consistent with Count.)
func (it *FetchIter) skipOffset(n int) (remaining int, err error) {
	if src, ok := it.Source.(offsetSkipper); ok {
		return src.skipOffset(n)
	}
	return n, nil
}

// Close releases resources by closing the retained cursor and the source
// iterator. The cursor pins exactly one leaf page across the whole scan
// (Cursor.Close -> releasePages, btree.go); without this it would leak that
// leaf's page ref and wedge cache eviction.
func (it *FetchIter) Close() {
	if it.cursor != nil {
		it.cursor.Close()
		it.cursor = nil
	}
	if it.Source != nil {
		it.Source.Close()
	}
}

func (it *FetchIter) String() string {
	return fmt.Sprintf("%s -> Fetch", it.Source)
}
