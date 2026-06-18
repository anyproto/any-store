package fts

import (
	"encoding/binary"
	"errors"
	"math/bits"
	"slices"
)

// Postings chunk format (the value stored at fts:postings key Tuple(term, chunkID)):
//
//	[ version : 1 byte = 2 ]
//	repeated, one record per document, documents ascending by DocID:
//	    DocIDDelta : uvarint   // first record = absolute DocID; rest = DocID - prevDocID
//	    FieldMask  : uvarint   // bit f set ⇒ the term occurs in indexed field f
//	    TF_f       : uvarint × popcount(FieldMask)  // per set bit, ascending field order
//	    PosDelta×ΣTF: uvarint  // GLOBAL (field-gapped) positions ascending; first
//	                           // absolute, rest delta. Σ TF_f of them.
//
// Documents are delta-encoded against the previous document in the chunk, so a
// chunk is self-describing. Positions are global (the indexer offsets each field
// by a fixed gap so a phrase cannot bridge fields) and delta-encoded within a
// document, so deltas are tiny. The per-field TF breakdown drives BM25F per-field
// weighting; positions stay global so the phrase merge is unchanged.
//
// A single-field index simply has FieldMask == 1 and one TF, so this format is
// uniform across one- and many-field indexes (no special case).
//
// A chunk holds at most ChunkSize documents, keeping the blob well under a page
// so it never spills to an overflow page on read-modify-write.

const (
	// PostingsVersion is the first byte of every fts:postings value. v2 added the
	// per-field TF breakdown (FieldMask + TF_f). The byte MUST be present so the
	// on-disk blob format can evolve in place. Old (v1) data is migrated by a full
	// re-index (the fts namespaces are cleared first), so this build only ever
	// decodes v2; a stray v1 byte is rejected with ErrUnknownVersion.
	PostingsVersion = 2

	// ChunkSize is the number of documents per postings chunk. It is part of the
	// on-disk key layout (key = Tuple(term, IntDocID/ChunkSize)); changing it
	// rekeys the whole index and forces a full re-index. Power of two, fixed.
	ChunkSize = 128

	// MaxFields is the number of indexed fields the FieldMask can represent.
	MaxFields = 64
)

// ErrCorruptChunk is returned when a postings blob cannot be decoded.
var ErrCorruptChunk = errors.New("fts: corrupt postings chunk")

// ErrUnknownVersion is returned when a postings blob carries a version this
// build does not understand (e.g. un-migrated v1 data).
var ErrUnknownVersion = errors.New("fts: unknown postings chunk version")

// Posting is one document's entry in a term's postings: the document's internal
// id, the set of indexed fields the term occurs in (Fields bitmask), the term
// frequency per occupied field (FieldTF, in ascending field order, one entry per
// set bit of Fields), and the ascending GLOBAL token positions. The total term
// frequency is len(Positions) == sum(FieldTF).
type Posting struct {
	DocID     uint64
	Fields    uint64
	FieldTF   []uint32
	Positions []uint32
}

// ChunkID returns the chunk a document id belongs to.
func ChunkID(docID uint64) uint64 { return docID / ChunkSize }

// AppendChunk encodes postings into a fresh chunk blob appended to dst (dst may
// be nil) and returns it. postings must be sorted ascending by DocID; each
// Posting's Positions must be sorted ascending; FieldTF must be in ascending
// field order with one entry per set bit of Fields and sum(FieldTF) ==
// len(Positions). The caller guarantees these.
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

		dst = binary.AppendUvarint(dst, p.Fields)
		for _, tf := range p.FieldTF {
			dst = binary.AppendUvarint(dst, uint64(tf))
		}

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
	dst, _, _, err := DecodeChunkInto(dst, nil, nil, blob)
	return dst, err
}

// DecodeChunkInto is DecodeChunk with caller-reused position and field-TF
// buffers: every posting's Positions and FieldTF slices reference posBuf / tfBuf,
// so a whole-chunk decode costs zero steady-state allocations. Both buffers are
// grown once up front to a safe upper bound (each encoded value is at least one
// byte), which guarantees the returned subslices are never invalidated by
// reallocation.
func DecodeChunkInto(dst []Posting, posBuf, tfBuf []uint32, blob []byte) ([]Posting, []uint32, []uint32, error) {
	r, err := NewChunkReader(blob)
	if err != nil {
		return dst, posBuf, tfBuf, err
	}
	if need := len(blob) - (cap(posBuf) - len(posBuf)); need > 0 {
		posBuf = slices.Grow(posBuf, len(blob))
	}
	if need := len(blob) - (cap(tfBuf) - len(tfBuf)); need > 0 {
		tfBuf = slices.Grow(tfBuf, len(blob))
	}
	for r.Next() {
		tfStart := len(tfBuf)
		tfBuf = r.AppendFieldTFs(tfBuf)
		posStart := len(posBuf)
		posBuf = r.AppendPositions(posBuf)
		dst = append(dst, Posting{
			DocID:     r.DocID(),
			Fields:    r.FieldMask(),
			FieldTF:   tfBuf[tfStart:len(tfBuf):len(tfBuf)],
			Positions: posBuf[posStart:len(posBuf):len(posBuf)],
		})
	}
	return dst, posBuf, tfBuf, r.Err()
}

// ChunkReader streams documents out of a chunk blob without allocating, decoding
// each document's FieldMask + per-field TF eagerly (cheap, needed for scoring)
// but its positions only on demand. It is the read-path primitive: the BM25(F)
// scan reads DocID + FieldMask + TFs and skips positions; the zig-zag phrase
// merge advances DocIDs cheaply and materializes positions only for documents
// that survive the merge. It implements ChunkIterator.
type ChunkReader struct {
	buf         []byte // remaining undecoded tail
	prevDoc     uint64
	docID       uint64
	fields      uint64   // FieldMask of the current document
	fieldTF     []uint32 // per-set-bit TFs of the current document (ascending field order)
	tf          uint32   // total TF of the current document (sum of fieldTF)
	started     bool     // at least one doc decoded
	posConsumed bool     // current doc's positions already read/skipped
	err         error
}

// ChunkIterator streams documents out of a postings chunk in ascending DocID,
// exposing field-aware term frequencies eagerly and positions lazily. v1/v2
// chunks (only v2 exists today) implement it, so the search code is blind to the
// blob layout.
type ChunkIterator interface {
	// Next advances to the next document, returning false at end of chunk or on
	// error (check Err). It skips any unconsumed positions of the prior document.
	Next() bool
	// DocID returns the current document's internal id.
	DocID() uint64
	// TF returns the current document's total term frequency (sum over fields).
	TF() uint32
	// FieldMask returns the bitmask of indexed fields the term occurs in.
	FieldMask() uint64
	// FieldTF returns the term frequency in indexed field fieldIdx (0 if the term
	// does not occur there).
	FieldTF(fieldIdx int) uint32
	// AppendFieldTFs appends the per-occupied-field TFs (ascending field order) to
	// dst — the sparse representation matching the set bits of FieldMask.
	AppendFieldTFs(dst []uint32) []uint32
	// AppendPositions decodes the current document's global positions onto dst.
	// Calling it consumes the positions for this document.
	AppendPositions(dst []uint32) []uint32
	// Reset re-initializes the iterator over a new chunk blob (same format
	// version), so a cross-chunk walk can reuse one iterator instead of
	// allocating per chunk. Returns the same errors as the constructor.
	Reset(blob []byte) error
	// Err returns the first decode error encountered.
	Err() error
}

// NewChunkIterator returns a ChunkIterator for a postings chunk blob, dispatching
// on the leading version byte.
func NewChunkIterator(blob []byte) (ChunkIterator, error) {
	return NewChunkReader(blob)
}

// NewChunkReader validates the version byte and returns a reader positioned
// before the first document.
func NewChunkReader(blob []byte) (*ChunkReader, error) {
	r := &ChunkReader{}
	if err := r.Reset(blob); err != nil {
		return nil, err
	}
	return r, nil
}

// Reset re-initializes the reader over a new chunk blob, clearing all decode
// state, so a cross-chunk walk (termStream) can reuse one reader instead of
// allocating a fresh one per chunk.
func (r *ChunkReader) Reset(blob []byte) error {
	if len(blob) == 0 {
		return ErrCorruptChunk
	}
	if blob[0] != PostingsVersion {
		return ErrUnknownVersion
	}
	tf := r.fieldTF[:0] // keep the field-TF scratch across resets
	*r = ChunkReader{buf: blob[1:], fieldTF: tf}
	return nil
}

// Next advances to the next document. It decodes the DocID, FieldMask, and
// per-field TFs but leaves the positions in the buffer for
// AppendPositions/SkipPositions. Returns false at end of chunk or on error.
func (r *ChunkReader) Next() bool {
	if r.err != nil {
		return false
	}
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

	mask, n := binary.Uvarint(r.buf)
	if n <= 0 {
		r.err = ErrCorruptChunk
		return false
	}
	r.buf = r.buf[n:]
	r.fields = mask

	k := bits.OnesCount64(mask)
	r.fieldTF = r.fieldTF[:0]
	var total uint64
	for i := 0; i < k; i++ {
		tf, n := binary.Uvarint(r.buf)
		if n <= 0 {
			r.err = ErrCorruptChunk
			return false
		}
		r.buf = r.buf[n:]
		r.fieldTF = append(r.fieldTF, uint32(tf))
		total += tf
	}
	r.tf = uint32(total)
	r.posConsumed = false
	return true
}

// DocID returns the current document's internal id.
func (r *ChunkReader) DocID() uint64 { return r.docID }

// TF returns the current document's total term frequency (sum over fields).
func (r *ChunkReader) TF() uint32 { return r.tf }

// FieldMask returns the bitmask of indexed fields the term occurs in.
func (r *ChunkReader) FieldMask() uint64 { return r.fields }

// FieldTF returns the term frequency in indexed field fieldIdx, or 0 if the term
// does not occur there.
func (r *ChunkReader) FieldTF(fieldIdx int) uint32 {
	if fieldIdx < 0 || fieldIdx >= MaxFields {
		return 0
	}
	bit := uint64(1) << uint(fieldIdx)
	if r.fields&bit == 0 {
		return 0
	}
	ord := bits.OnesCount64(r.fields & (bit - 1))
	return r.fieldTF[ord]
}

// AppendFieldTFs appends the per-occupied-field TFs (ascending field order) to dst.
func (r *ChunkReader) AppendFieldTFs(dst []uint32) []uint32 {
	return append(dst, r.fieldTF...)
}

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
