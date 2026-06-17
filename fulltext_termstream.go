package anystore

import (
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/fts"
)

// fulltext_termstream.go provides a cross-chunk iterator over a single term's
// postings, used by the phrase zig-zag merge. The BM25 bag-of-words scan still
// streams chunks inline in fulltext_search.go (it never seeks); the phrase merge
// needs several terms advancing in lockstep by IntDocID, which is what termStream
// gives: ascending docIDs across all of a term's chunks, with chunk-skipping seek
// and lazy position decoding.

// termStream walks every posting of one term across its chunks, in ascending
// IntDocID. Not safe for concurrent use — one per term per query.
type termStream struct {
	cur    *btree.Cursor
	term   string
	prefix []byte // postingsTermPrefix(term): bounds the cursor to this term
	keyBuf []byte // scratch for seek keys

	it    fts.ChunkIterator
	docID uint64
	tf    uint32
	valid bool
	err   error
}

// newTermStream opens a stream over term's postings. A stream over a term with
// no postings is valid but immediately exhausted (valid()==false).
func newTermStream(tx *btree.ReadTx, ns *btree.Namespace, term string) *termStream {
	ts := &termStream{
		cur:    tx.NewCursor(ns),
		term:   term,
		prefix: postingsTermPrefix(nil, term),
	}
	if err := ts.cur.Seek(ts.prefix); err != nil {
		ts.err = err
		return ts
	}
	ts.loadChunk()
	return ts
}

// onTerm reports whether the cursor is positioned on a chunk of this stream's
// term (vs. having walked past it into the next term / end of namespace).
func (ts *termStream) onTerm() bool {
	if !ts.cur.Valid() {
		return false
	}
	key, err := ts.cur.Key()
	if err != nil {
		ts.err = err
		return false
	}
	return len(key) >= len(ts.prefix) && string(key[:len(ts.prefix)]) == string(ts.prefix)
}

// loadChunk builds an iterator over the chunk the cursor currently sits on and
// advances it to the first document. If the cursor is past this term, the stream
// becomes exhausted.
func (ts *termStream) loadChunk() {
	for {
		if ts.err != nil || !ts.onTerm() {
			ts.valid = false
			return
		}
		val, err := ts.cur.Value()
		if err != nil {
			ts.err = err
			ts.valid = false
			return
		}
		ts.it, err = fts.NewChunkIterator(val)
		if err != nil {
			ts.err = err
			ts.valid = false
			return
		}
		if ts.it.Next() {
			ts.docID = ts.it.DocID()
			ts.tf = ts.it.TF()
			ts.valid = true
			return
		}
		if err = ts.it.Err(); err != nil {
			ts.err = err
			ts.valid = false
			return
		}
		// Empty chunk (shouldn't happen — empty chunks are deleted) — skip it.
		if err = ts.cur.Next(); err != nil {
			ts.err = err
			ts.valid = false
			return
		}
	}
}

// next advances to the following document. Returns false at end of the term's
// postings or on error (check ts.err via valid()).
func (ts *termStream) next() bool {
	if !ts.valid {
		return false
	}
	if ts.it.Next() {
		ts.docID = ts.it.DocID()
		ts.tf = ts.it.TF()
		return true
	}
	if err := ts.it.Err(); err != nil {
		ts.err = err
		ts.valid = false
		return false
	}
	// Current chunk exhausted — move to the next chunk of this term.
	if err := ts.cur.Next(); err != nil {
		ts.err = err
		ts.valid = false
		return false
	}
	ts.loadChunk()
	return ts.valid
}

// seek advances the stream to the first document with docID >= target, jumping
// whole chunks when target lies ahead. A no-op if already at/after target.
func (ts *termStream) seek(target uint64) {
	if !ts.valid || ts.docID >= target {
		return
	}
	if fts.ChunkID(target) > fts.ChunkID(ts.docID) {
		// Jump the cursor straight to the chunk that would hold target.
		ts.keyBuf = postingsKey(ts.keyBuf, ts.term, fts.ChunkID(target))
		if err := ts.cur.Seek(ts.keyBuf); err != nil {
			ts.err = err
			ts.valid = false
			return
		}
		ts.loadChunk()
	}
	for ts.valid && ts.docID < target {
		ts.next()
	}
}

// positions decodes the current document's positions onto dst, returning the
// extended slice. Valid only while parked on a document (valid()==true).
func (ts *termStream) positions(dst []uint32) []uint32 {
	dst = ts.it.AppendPositions(dst)
	if err := ts.it.Err(); err != nil {
		ts.err = err
	}
	return dst
}

func (ts *termStream) close() {
	if ts.cur != nil {
		ts.cur.Close()
		ts.cur = nil
	}
}
