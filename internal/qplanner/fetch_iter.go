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

		// Cursor-free point lookup: avoids Cursor struct allocation and stack growth.
		var lookupStart time.Time
		if perf {
			lookupStart = time.Now()
		}
		it.Buf.DocBuf, err = it.Data.AppendValue(docId, it.Buf.DocBuf[:0])
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

// Close releases resources by closing the source iterator.
func (it *FetchIter) Close() {
	if it.Source != nil {
		it.Source.Close()
	}
}

func (it *FetchIter) String() string {
	return fmt.Sprintf("%s -> Fetch", it.Source)
}
