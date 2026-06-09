package fts

import (
	"encoding/binary"
	"errors"
)

// Postings chunk format (the value stored at fts:postings key Tuple(term, chunkID)):
//
//	[ version : 1 byte ]
//	repeated, one record per document, documents ascending by DocID:
//	    DocIDDelta : uvarint   // first record = absolute DocID; rest = DocID - prevDocID
//	    TF         : uvarint   // term frequency in this doc == len(Positions)
//	    PosDelta×TF: uvarint   // positions ascending; first = absolute, rest = delta
//
// Documents are delta-encoded against the previous document in the chunk, so a
// chunk is self-describing (it does not need to know its chunk base id to
// decode). Positions are delta-encoded within a document, so deltas are tiny.
//
// A chunk holds at most ChunkSize documents, keeping the blob well under a page
// so it never spills to an overflow page on read-modify-write.

const (
	// PostingsVersion is the first byte of every fts:postings value. It MUST be
	// present so the on-disk blob format can evolve in place. Do not remove.
	PostingsVersion = 1

	// ChunkSize is the number of documents per postings chunk. It is part of the
	// on-disk key layout (key = Tuple(term, IntDocID/ChunkSize)); changing it
	// rekeys the whole index and forces a full re-index. Power of two, fixed.
	ChunkSize = 128
)

// ErrCorruptChunk is returned when a postings blob cannot be decoded.
var ErrCorruptChunk = errors.New("fts: corrupt postings chunk")

// ErrUnknownVersion is returned when a postings blob carries a version this
// build does not understand.
var ErrUnknownVersion = errors.New("fts: unknown postings chunk version")

// Posting is one document's entry in a term's postings: the document's internal
// id and the ascending list of token positions at which the term occurs. The
// term frequency is len(Positions).
type Posting struct {
	DocID     uint64
	Positions []uint32
}

// ChunkID returns the chunk a document id belongs to.
func ChunkID(docID uint64) uint64 { return docID / ChunkSize }

// AppendChunk encodes postings into a fresh chunk blob appended to dst (dst may
// be nil) and returns it. postings must be sorted ascending by DocID, and each
// Posting's Positions must be sorted ascending; the caller guarantees this.
func AppendChunk(dst []byte, postings []Posting) []byte {
	dst = append(dst, PostingsVersion)
	var prevDoc uint64
	for i := range postings {
		p := &postings[i]
		if i == 0 {
			dst = binary.AppendUvarint(dst, p.DocID)
		} else {
			dst = binary.AppendUvarint(dst, p.DocID-prevDoc)
		}
		prevDoc = p.DocID

		dst = binary.AppendUvarint(dst, uint64(len(p.Positions)))
		var prevPos uint32
		for j, pos := range p.Positions {
			if j == 0 {
				dst = binary.AppendUvarint(dst, uint64(pos))
			} else {
				dst = binary.AppendUvarint(dst, uint64(pos-prevPos))
			}
			prevPos = pos
		}
	}
	return dst
}

// DecodeChunk decodes a whole chunk blob, appending its postings to dst (dst may
// be nil). It materializes positions; the read path should prefer ChunkReader to
// skip position decoding for documents it does not need.
func DecodeChunk(dst []Posting, blob []byte) ([]Posting, error) {
	r, err := NewChunkReader(blob)
	if err != nil {
		return dst, err
	}
	for r.Next() {
		positions := r.AppendPositions(nil)
		dst = append(dst, Posting{DocID: r.DocID(), Positions: positions})
	}
	return dst, r.Err()
}

// ChunkReader streams documents out of a chunk blob without allocating, decoding
// each document's positions only on demand. It is the read-path primitive: the
// zig-zag phrase merge advances DocIDs cheaply and only materializes positions
// for documents that survive the merge.
type ChunkReader struct {
	buf         []byte // remaining undecoded tail
	prevDoc     uint64
	docID       uint64
	tf          uint32
	started     bool // at least one doc decoded
	posConsumed bool // current doc's positions already read/skipped
	err         error
}

// NewChunkReader validates the version byte and returns a reader positioned
// before the first document.
func NewChunkReader(blob []byte) (*ChunkReader, error) {
	if len(blob) == 0 {
		return nil, ErrCorruptChunk
	}
	if blob[0] != PostingsVersion {
		return nil, ErrUnknownVersion
	}
	return &ChunkReader{buf: blob[1:]}, nil
}

// Next advances to the next document. It decodes the DocID and TF but leaves the
// positions in the buffer for AppendPositions/SkipPositions. Returns false at
// end of chunk or on error (check Err).
func (r *ChunkReader) Next() bool {
	if r.err != nil {
		return false
	}
	// If positions of the current doc were not consumed, skip them first.
	if r.started {
		if !r.skipCurrentPositions() {
			return false
		}
	}
	if len(r.buf) == 0 {
		return false
	}

	delta, n := binary.Uvarint(r.buf)
	if n <= 0 {
		r.err = ErrCorruptChunk
		return false
	}
	r.buf = r.buf[n:]
	if !r.started {
		r.docID = delta
	} else {
		r.docID = r.prevDoc + delta
	}
	r.prevDoc = r.docID
	r.started = true

	tf, n := binary.Uvarint(r.buf)
	if n <= 0 {
		r.err = ErrCorruptChunk
		return false
	}
	r.buf = r.buf[n:]
	r.tf = uint32(tf)
	r.posConsumed = false
	return true
}

// DocID returns the current document's internal id.
func (r *ChunkReader) DocID() uint64 { return r.docID }

// TF returns the current document's term frequency (number of positions).
func (r *ChunkReader) TF() uint32 { return r.tf }

// AppendPositions decodes the current document's positions, appending them to
// dst (dst may be nil) and returning the extended slice. Calling it consumes the
// positions for this document.
func (r *ChunkReader) AppendPositions(dst []uint32) []uint32 {
	if r.err != nil || r.posConsumed {
		return dst
	}
	var prev uint32
	for i := uint32(0); i < r.tf; i++ {
		d, n := binary.Uvarint(r.buf)
		if n <= 0 {
			r.err = ErrCorruptChunk
			return dst
		}
		r.buf = r.buf[n:]
		if i == 0 {
			prev = uint32(d)
		} else {
			prev += uint32(d)
		}
		dst = append(dst, prev)
	}
	r.posConsumed = true
	return dst
}

// skipCurrentPositions consumes the current document's position varints without
// decoding them into a slice.
func (r *ChunkReader) skipCurrentPositions() bool {
	if r.posConsumed {
		return true
	}
	for i := uint32(0); i < r.tf; i++ {
		_, n := binary.Uvarint(r.buf)
		if n <= 0 {
			r.err = ErrCorruptChunk
			return false
		}
		r.buf = r.buf[n:]
	}
	r.posConsumed = true
	return true
}

// Err returns the first error encountered while decoding.
func (r *ChunkReader) Err() error { return r.err }
