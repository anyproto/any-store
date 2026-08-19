package qplanner

import (
	"bytes"
	"sync"
	"unsafe"
)

// pooledSeenSet is a reusable, near-zero-allocation distinct-docId set for the
// multi-bound Count dedup path (IndexIter.countEntriesViaSeenSet).
//
// docId bytes are copied into fixed-size chunks that are NEVER reallocated, and
// the map is keyed on unsafe.String headers aliasing those chunk bytes. Because
// a chunk's backing array never moves once allocated, every key stays valid for
// the whole count even as later docIds are appended. The map is reused across
// counts via the clear() builtin (which retains its buckets), and the whole
// struct is pooled — so a warm set performs no allocations beyond the rare
// chunk/bucket growth.
//
// Reuse contract (reset): clear the map BEFORE rewinding the chunks, otherwise
// the now-stale keys would alias bytes the next count overwrites. Slices into a
// chunk (the unsafe.String keys) must never escape past the next reset(). Not
// safe for concurrent use; each count owns its set for the call's duration.
type pooledSeenSet struct {
	m      map[string]struct{}
	chunks [][]byte // fixed-capacity, never-moved backing arrays
	cur    int      // index of the chunk currently being filled
}

const seenChunkSize = 64 * 1024 // 64 KiB per chunk; far larger than any docId.

func newPooledSeenSet() *pooledSeenSet {
	return &pooledSeenSet{
		m:      make(map[string]struct{}),
		chunks: [][]byte{make([]byte, 0, seenChunkSize)},
	}
}

var seenSetPool = sync.Pool{New: func() any { return newPooledSeenSet() }}

// reset prepares the set for reuse. ORDER MATTERS: clear the map first, then
// rewind the chunks; otherwise stale keys would alias bytes we overwrite.
func (s *pooledSeenSet) reset() {
	clear(s.m)
	for i := range s.chunks {
		s.chunks[i] = s.chunks[i][:0]
	}
	s.cur = 0
}

// intern copies b into a non-moving chunk and returns a string aliasing those
// bytes, valid until the next reset().
func (s *pooledSeenSet) intern(b []byte) string {
	n := len(b)
	if n > seenChunkSize {
		// Oversized docId (never happens for real docIds, handled for safety):
		// give it a dedicated exact-size chunk so it still never moves.
		c := make([]byte, n)
		copy(c, b)
		s.chunks = append(s.chunks, nil)
		copy(s.chunks[s.cur+1:], s.chunks[s.cur:])
		s.chunks[s.cur] = c
		s.cur++
		return unsafe.String(&c[0], n)
	}
	c := s.chunks[s.cur]
	if len(c)+n > cap(c) {
		s.cur++
		if s.cur == len(s.chunks) {
			s.chunks = append(s.chunks, make([]byte, 0, seenChunkSize))
		}
		c = s.chunks[s.cur]
	}
	off := len(c)
	c = append(c, b...)
	s.chunks[s.cur] = c
	return unsafe.String(&c[off], n)
}

// add reports whether docId was newly seen (true) or a duplicate (false). The
// read-probe uses the compiler's no-alloc m[string(b)] form; only a genuinely
// new docId is interned into a chunk.
func (s *pooledSeenSet) add(docId []byte) bool {
	if _, ok := s.m[string(docId)]; ok {
		return false
	}
	s.m[s.intern(docId)] = struct{}{}
	return true
}

// countEntriesViaSeenSet counts distinct docIds across it.Bounds using the
// pooled seen-set. It backs CountEntries' two multi-bound dedup branches:
//
//   - skipScalar == false (Branch 4: single-field index the probe found to hold
//     array entries): every entry is interned/deduped. Such indexes are usually
//     array-dense (most entries multikey), so reading the value byte to skip the
//     few scalars wouldn't pay for itself.
//
//   - skipScalar == true (Branch 2: compound / non-PointLookup): the per-entry
//     value byte is read and SCALAR entries are counted directly without touching
//     the map — a scalar entry's doc has exactly one entry in the index, so it
//     matches at most one bound and can never be a cross-bound duplicate. This is
//     the same guarantee DocDedup.Accept relies on, and it makes a scalar-only
//     stream (e.g. a compound (a,b) index queried on its prefix) cost just the
//     walk: no hashing, no interning. Only multikey/legacy entries are deduped.
//
// Both modes return the identical distinct-doc count; they differ only in
// whether the value byte is consulted to skip provably-unique entries.
func (it *IndexIter) countEntriesViaSeenSet(skipScalar bool) (int, error) {
	if it.cursor == nil {
		it.cursor = it.Source.NewCursor()
	}
	numFields := len(it.IdxInfo.FieldNames)

	s := seenSetPool.Get().(*pooledSeenSet)
	s.reset()
	defer seenSetPool.Put(s)

	distinct := 0
	for _, b := range it.Bounds {
		if err := it.seekBoundStart(b); err != nil {
			return 0, err
		}
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
			counted := false
			if skipScalar {
				val, verr := it.cursor.Value()
				if verr != nil {
					return 0, verr
				}
				if !EntryValueIsMultiKey(val) {
					distinct++ // scalar: guaranteed unique, no dedup needed
					counted = true
				}
			}
			if !counted && s.add(extractDocId(k, numFields)) {
				distinct++
			}
			if err := it.cursor.Next(); err != nil {
				return 0, err
			}
		}
	}
	return distinct, nil
}
