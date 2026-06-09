package anystore

import (
	"errors"
	"slices"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/fts"
)

// fulltext_pending.go implements the per-transaction write-back buffer for the
// full-text index. Instead of read-modify-writing a postings chunk (and the
// vocab df) for every term of every document, the maintenance path accumulates
// postings adds/removes and df deltas in memory for the whole write tx, then
// flushes each TOUCHED chunk/term to the B-tree exactly once at commit, in
// sorted key order (for B-tree locality). A batch of N docs that all contain
// "the" thus rewrites the "the" chunk once, not N times.
//
// The buffer is WRITER-OWNED: it is mutated only by the single writer (the
// btree write lock is held for the whole tx), reset at tx start, and flushed at
// commit — mirroring the range-index sketch lifecycle. See docs/fts/DESIGN.md.

// pendingChunk holds the in-memory mutations for one (term, chunkID) posting
// chunk within a write tx.
type pendingChunk struct {
	term    string
	chunkID uint64
	adds    map[uint64][]uint32 // IntDocID -> positions (ascending)
	dels    map[uint64]struct{} // IntDocIDs to remove
}

// ftsPending is the per-tx accumulator for one full-text index.
type ftsPending struct {
	chunks map[string]*pendingChunk // key = string(postingsKey(term, chunkID))
	vocab  map[string]int64         // term -> df delta
	count  int                      // buffered posting ops (adds+dels), for the spill cap
}

// ftsSpillPostings bounds the buffer's RAM: once this many posting ops are
// buffered, the accumulator is flushed to the B-tree mid-tx (safe — same tx,
// just written earlier) so a single huge batch can't grow the buffer without
// bound. ~200k postings ≈ a few MB of buffered positions.
const ftsSpillPostings = 200_000

// ftsPendingShrink: if the chunks map grew past this after a big batch, drop it
// on reset so a one-off bulk load doesn't pin large map capacity for the life of
// the writer.
const ftsPendingShrink = 4096

func (p *ftsPending) empty() bool {
	return len(p.chunks) == 0 && len(p.vocab) == 0
}

func (p *ftsPending) reset() {
	if len(p.chunks) > ftsPendingShrink {
		p.chunks = nil // release capacity back to GC after a bulk load
	} else if p.chunks != nil {
		clear(p.chunks)
	}
	if len(p.vocab) > ftsPendingShrink {
		p.vocab = nil
	} else if p.vocab != nil {
		clear(p.vocab)
	}
	p.count = 0
}

// maybeSpill flushes the buffer mid-transaction if it has grown past the RAM cap.
func (fx *ftsIndex) maybeSpill(tx *btree.WriteTx) error {
	if fx.pending.count >= ftsSpillPostings {
		return fx.flushPending(tx)
	}
	return nil
}

// addPosting buffers an insertion of (docID, positions) into the term's chunk.
func (fx *ftsIndex) addPostingPending(term string, docID uint64, positions []uint32) {
	pc := fx.pendingChunkFor(term, docID)
	if pc.adds == nil {
		pc.adds = make(map[uint64][]uint32, 4)
	}
	pc.adds[docID] = slices.Clone(positions)
	delete(pc.dels, docID) // an add supersedes a same-tx delete
	fx.pending.count++
}

// removePosting buffers a removal of docID from the term's chunk.
func (fx *ftsIndex) removePostingPending(term string, docID uint64) {
	pc := fx.pendingChunkFor(term, docID)
	if _, ok := pc.adds[docID]; ok {
		delete(pc.adds, docID) // cancel a same-tx add
	}
	if pc.dels == nil {
		pc.dels = make(map[uint64]struct{}, 4)
	}
	pc.dels[docID] = struct{}{}
	fx.pending.count++
}

func (fx *ftsIndex) pendingChunkFor(term string, docID uint64) *pendingChunk {
	if fx.pending.chunks == nil {
		fx.pending.chunks = make(map[string]*pendingChunk, 16)
	}
	chunkID := fts.ChunkID(docID)
	fx.keyBuf = postingsKey(fx.keyBuf, term, chunkID)
	key := string(fx.keyBuf)
	pc := fx.pending.chunks[key]
	if pc == nil {
		pc = &pendingChunk{term: term, chunkID: chunkID}
		fx.pending.chunks[key] = pc
	}
	return pc
}

func (fx *ftsIndex) vocabDeltaPending(term string, delta int64) {
	if fx.pending.vocab == nil {
		fx.pending.vocab = make(map[string]int64, 16)
	}
	fx.pending.vocab[term] += delta
}

// flushPending writes the accumulated postings and vocab deltas to the B-tree in
// sorted key order, then clears the buffer. Called once per write tx at commit.
func (fx *ftsIndex) flushPending(tx *btree.WriteTx) error {
	if fx.pending.empty() {
		return nil
	}

	// Postings chunks, flushed in sorted key order for B-tree locality.
	keys := make([]string, 0, len(fx.pending.chunks))
	for k := range fx.pending.chunks {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		if err := fx.flushChunk(tx, fx.pending.chunks[k]); err != nil {
			return err
		}
	}

	// Vocab df deltas, also sorted.
	terms := make([]string, 0, len(fx.pending.vocab))
	for t := range fx.pending.vocab {
		terms = append(terms, t)
	}
	slices.Sort(terms)
	for _, t := range terms {
		if err := fx.flushVocab(tx, t, fx.pending.vocab[t]); err != nil {
			return err
		}
	}

	fx.pending.reset()
	return nil
}

// flushChunk applies a pendingChunk's adds/removes to its B-tree chunk: it reads
// and decodes the existing chunk, merges in the buffered changes keeping DocIDs
// ascending, and writes the re-encoded chunk back (or deletes it if empty).
func (fx *ftsIndex) flushChunk(tx *btree.WriteTx, pc *pendingChunk) error {
	if len(pc.adds) == 0 && len(pc.dels) == 0 {
		return nil
	}
	fx.keyBuf = postingsKey(fx.keyBuf, pc.term, pc.chunkID)

	fx.chunkPL = fx.chunkPL[:0]
	existing, err := tx.Get(fx.nsPost, fx.keyBuf)
	if err != nil {
		if !errors.Is(err, btree.ErrKeyNotFound) {
			return err
		}
	} else if fx.chunkPL, err = fts.DecodeChunk(fx.chunkPL, existing); err != nil {
		return err
	}

	// Sorted list of added DocIDs (adds override existing postings).
	addIDs := make([]uint64, 0, len(pc.adds))
	for id := range pc.adds {
		addIDs = append(addIDs, id)
	}
	slices.Sort(addIDs)

	// Two-way merge of the existing postings (ascending) and the added postings
	// (ascending), dropping any DocID that is deleted or re-added.
	merged := fx.mergeBuf[:0]
	ai := 0
	for _, p := range fx.chunkPL {
		for ai < len(addIDs) && addIDs[ai] < p.DocID {
			merged = append(merged, fts.Posting{DocID: addIDs[ai], Positions: pc.adds[addIDs[ai]]})
			ai++
		}
		if ai < len(addIDs) && addIDs[ai] == p.DocID {
			// re-added: take the new positions, skip the old
			merged = append(merged, fts.Posting{DocID: addIDs[ai], Positions: pc.adds[addIDs[ai]]})
			ai++
			continue
		}
		if _, deleted := pc.dels[p.DocID]; deleted {
			continue
		}
		merged = append(merged, p)
	}
	for ; ai < len(addIDs); ai++ {
		merged = append(merged, fts.Posting{DocID: addIDs[ai], Positions: pc.adds[addIDs[ai]]})
	}
	fx.mergeBuf = merged

	if len(merged) == 0 {
		return deleteIfExists(tx, fx.nsPost, fx.keyBuf)
	}
	fx.valBuf = fts.AppendChunk(fx.valBuf[:0], merged)
	return tx.Put(fx.nsPost, fx.keyBuf, fx.valBuf)
}

// flushVocab applies a buffered df delta to a term's vocab entry, deleting the
// key when df reaches zero.
func (fx *ftsIndex) flushVocab(tx *btree.WriteTx, term string, delta int64) error {
	if delta == 0 {
		return nil
	}
	return fx.bumpVocab(tx, term, delta)
}
