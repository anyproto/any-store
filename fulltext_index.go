package anystore

import (
	"encoding/binary"
	"errors"
	"slices"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/fts"
)

// fulltext_index.go implements the write side of the full-text index: the five
// B-tree namespaces and the per-document maintenance run inside the write tx.
// See docs/fts/DESIGN.md for the format and rationale. The pure algorithms
// (analysis, postings codec) live in internal/fts.

// ftsPosGap is the position gap inserted between two indexed fields so a phrase
// query cannot match across a field boundary (title end → body start).
const ftsPosGap = 100

// fts namespace parts (per index): ftx:<coll>:<index>:<part>
const (
	ftsPartMap     = "map"  // StringDocID ⇄ IntDocID
	ftsPartMeta    = "meta" // global counters (seq, N, tokens)
	ftsPartVocab   = "vocab"
	ftsPartDocinfo = "info" // IntDocID → doc length (tokens)
	ftsPartPost    = "post" // Tuple(term, chunkID) → postings chunk
)

func ftsNsName(collName, indexName, part string) string {
	return "ftx:" + collName + ":" + indexName + ":" + part
}

// docmap key prefixes (the forward and reverse maps share one namespace).
const (
	ftsMapForward = 'f' // 'f' + stringID  → uvarint(IntDocID)
	ftsMapReverse = 'r' // 'r' + IntDocID8 → stringID
)

// meta keys.
var (
	ftsMetaSeq    = []byte("seq") // last allocated IntDocID
	ftsMetaCount  = []byte("N")   // number of indexed documents
	ftsMetaTokens = []byte("tok") // total tokens across all documents
)

// ftsIndex is a live full-text index bound to its five namespaces.
type ftsIndex struct {
	c    *collection
	info IndexInfo

	fieldPaths [][]string

	nsMap     *btree.Namespace
	nsMeta    *btree.Namespace
	nsVocab   *btree.Namespace
	nsDocinfo *btree.Namespace
	nsPost    *btree.Namespace

	az *fts.Analyzer

	// pending is the per-tx write-back buffer (postings + vocab deltas), flushed
	// at commit. See fulltext_pending.go.
	pending ftsPending

	// scratch buffers, writer-owned (single writer, serialized by btree writeMu)
	keyBuf   []byte
	valBuf   []byte
	tokBuf   []fts.Token         // analyze() output (also old-doc tokens in updateDoc)
	tokBufB  []fts.Token         // new-doc tokens in updateDoc
	termBufA map[string][]uint32 // reused term->positions for insert/delete + old-doc
	termBufB map[string][]uint32 // reused term->positions for new-doc in updateDoc
	chunkPL  []fts.Posting
	mergeBuf []fts.Posting // flushChunk merge scratch

	// flushChunk scratch: decoded-positions arena, existing-chunk value buffer,
	// sorted added-id buffer — all reused across chunks and transactions.
	posDecode   []uint32
	chunkValBuf []byte
	addIDsBuf   []uint64

	// posFree recycles per-term position slices harvested by termPostingsInto
	// when it clears its map (clear() would otherwise drop their capacity and
	// force one allocation per distinct term per document).
	posFree [][]uint32
}

func newFtsIndex(c *collection, info IndexInfo) (*ftsIndex, error) {
	fx := &ftsIndex{c: c, info: info, az: fts.NewAnalyzer()}
	for _, field := range info.Fields {
		fields, _ := parseIndexField(field)
		for _, f := range fields {
			if f == "" {
				return nil, errors.New("fts: invalid index field: '" + field + "'")
			}
		}
		fx.fieldPaths = append(fx.fieldPaths, fields)
	}
	if len(fx.fieldPaths) == 0 {
		return nil, errors.New("fts: index requires at least one field")
	}
	return fx, nil
}

// bindNamespaces resolves (does not create) the five namespaces. resolve is the
// writer/reader-aware GetNamespace closure used at collection init.
func (fx *ftsIndex) bindNamespaces(resolve func(name string) (*btree.Namespace, error)) (err error) {
	parts := []struct {
		name string
		dst  **btree.Namespace
	}{
		{ftsPartMap, &fx.nsMap},
		{ftsPartMeta, &fx.nsMeta},
		{ftsPartVocab, &fx.nsVocab},
		{ftsPartDocinfo, &fx.nsDocinfo},
		{ftsPartPost, &fx.nsPost},
	}
	for _, p := range parts {
		ns, e := resolve(ftsNsName(fx.c.name, fx.info.Name, p.name))
		if e != nil {
			return e
		}
		*p.dst = ns
	}
	return nil
}

func (fx *ftsIndex) Info() IndexInfo { return fx.info }

// ---- analysis -------------------------------------------------------------

// analyzeInto runs the analyzer over all indexed fields of the document,
// appending the flat token stream to dst (positions are offset per field by
// ftsPosGap so phrases cannot bridge fields) and returning the extended slice.
func (fx *ftsIndex) analyzeInto(dst []fts.Token, it item) []fts.Token {
	dst = dst[:0]
	val := it.Value()
	var base uint32
	for _, path := range fx.fieldPaths {
		fv := val.Get(path...)
		before := len(dst)
		dst = fx.appendFieldTokens(dst, fv, base)
		emitted := len(dst) - before
		if emitted > 0 {
			base += uint32(emitted) + ftsPosGap
		}
	}
	return dst
}

// appendFieldTokens analyzes a single field value (string, or array of strings)
// and appends its tokens to dst with positions shifted by base.
func (fx *ftsIndex) appendFieldTokens(dst []fts.Token, fv *anyenc.Value, base uint32) []fts.Token {
	if fv == nil {
		return dst
	}
	switch fv.Type() {
	case anyenc.TypeString:
		sb, err := fv.StringBytes()
		if err != nil {
			return dst
		}
		dst = fx.appendText(dst, string(sb), base)
	case anyenc.TypeArray:
		arr, err := fv.Array()
		if err != nil {
			return dst
		}
		for _, av := range arr {
			if av.Type() != anyenc.TypeString {
				continue
			}
			sb, err := av.StringBytes()
			if err != nil {
				continue
			}
			dst = fx.appendText(dst, string(sb), base)
			// keep array elements on separate position runs too
			base += ftsPosGap
		}
	}
	return dst
}

func (fx *ftsIndex) appendText(dst []fts.Token, text string, base uint32) []fts.Token {
	start := len(dst)
	dst = fx.az.Append(dst, text)
	if base != 0 {
		for i := start; i < len(dst); i++ {
			dst[i].Pos += base
		}
	}
	return dst
}

// termPostingsInto groups a token stream into per-term ascending position lists
// for a single document, reusing the provided map (cleared first) to avoid a
// per-document map allocation on the hot write path. Returns the (same) map and
// the document length. Tokens arrive in ascending position order, so the lists
// are already sorted.
func (fx *ftsIndex) termPostingsInto(dst map[string][]uint32, tokens []fts.Token) (map[string][]uint32, uint32) {
	if dst == nil {
		dst = make(map[string][]uint32, len(tokens))
	} else {
		// Harvest the previous document's position slices for reuse before
		// clearing the map.
		for _, v := range dst {
			if cap(v) > 0 {
				fx.posFree = append(fx.posFree, v[:0])
			}
		}
		clear(dst)
	}
	for _, t := range tokens {
		cur, ok := dst[t.Term]
		if !ok {
			if n := len(fx.posFree); n > 0 {
				cur = fx.posFree[n-1]
				fx.posFree = fx.posFree[:n-1]
			}
		}
		dst[t.Term] = append(cur, t.Pos)
	}
	return dst, uint32(len(tokens))
}

// ---- IntDocID allocation & docmap -----------------------------------------

func (fx *ftsIndex) readMetaUint(tx *btree.WriteTx, key []byte) (uint64, error) {
	v, err := tx.Get(fx.nsMeta, key)
	if err != nil {
		if errors.Is(err, btree.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}
	n, _ := binary.Uvarint(v)
	return n, nil
}

func (fx *ftsIndex) putMetaUint(tx *btree.WriteTx, key []byte, n uint64) error {
	fx.valBuf = binary.AppendUvarint(fx.valBuf[:0], n)
	return tx.Put(fx.nsMeta, key, fx.valBuf)
}

// addMetaDelta applies a signed delta to a meta counter (clamped at 0).
func (fx *ftsIndex) addMetaDelta(tx *btree.WriteTx, key []byte, delta int64) error {
	cur, err := fx.readMetaUint(tx, key)
	if err != nil {
		return err
	}
	next := int64(cur) + delta
	if next < 0 {
		next = 0
	}
	return fx.putMetaUint(tx, key, uint64(next))
}

// allocDocID returns the next monotonic IntDocID (never reused).
func (fx *ftsIndex) allocDocID(tx *btree.WriteTx) (uint64, error) {
	seq, err := fx.readMetaUint(tx, ftsMetaSeq)
	if err != nil {
		return 0, err
	}
	seq++
	if err = fx.putMetaUint(tx, ftsMetaSeq, seq); err != nil {
		return 0, err
	}
	return seq, nil
}

func ftsMapForwardKey(dst, stringID []byte) []byte {
	dst = append(dst[:0], ftsMapForward)
	return append(dst, stringID...)
}

func ftsMapReverseKey(dst []byte, docID uint64) []byte {
	dst = append(dst[:0], ftsMapReverse)
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], docID)
	return append(dst, b[:]...)
}

// lookupDocID returns the IntDocID for a string id, or ok=false if absent.
func (fx *ftsIndex) lookupDocID(tx *btree.WriteTx, stringID []byte) (uint64, bool, error) {
	fx.keyBuf = ftsMapForwardKey(fx.keyBuf, stringID)
	v, err := tx.Get(fx.nsMap, fx.keyBuf)
	if err != nil {
		if errors.Is(err, btree.ErrKeyNotFound) {
			return 0, false, nil
		}
		return 0, false, err
	}
	n, _ := binary.Uvarint(v)
	return n, true, nil
}

// ---- postings key ---------------------------------------------------------

// postingsKey builds the key Tuple(term, chunkID) as
// uvarint(len(term)) | term | chunkID(8 BE). The length prefix keeps all chunks
// of one term contiguous and ordered by chunkID (we only ever scan within a
// single term, so cross-term ordering is irrelevant).
func postingsKey(dst []byte, term string, chunkID uint64) []byte {
	dst = binary.AppendUvarint(dst[:0], uint64(len(term)))
	dst = append(dst, term...)
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], chunkID)
	return append(dst, b[:]...)
}

// ---- maintenance ----------------------------------------------------------

// insertDoc indexes a document: allocates a stable IntDocID, writes the docmap,
// docinfo and counters directly, and BUFFERS the postings + vocab deltas in the
// per-tx write-back buffer (flushed once per touched chunk/term at commit).
func (fx *ftsIndex) insertDoc(tx *btree.WriteTx, it item) error {
	fx.tokBuf = fx.analyzeInto(fx.tokBuf, it)
	var docLen uint32
	fx.termBufA, docLen = fx.termPostingsInto(fx.termBufA, fx.tokBuf)
	terms := fx.termBufA
	if docLen == 0 {
		// Nothing to index (no text / empty fields). The doc is simply absent
		// from this fts index.
		return nil
	}

	stringID := it.appendId(nil)
	docID, err := fx.allocDocID(tx)
	if err != nil {
		return err
	}

	// docmap forward + reverse, docinfo — direct point Puts (no decode, cheap).
	fx.keyBuf = ftsMapForwardKey(fx.keyBuf, stringID)
	fx.valBuf = binary.AppendUvarint(fx.valBuf[:0], docID)
	if err = tx.Put(fx.nsMap, fx.keyBuf, fx.valBuf); err != nil {
		return err
	}
	fx.keyBuf = ftsMapReverseKey(fx.keyBuf, docID)
	if err = tx.Put(fx.nsMap, fx.keyBuf, stringID); err != nil {
		return err
	}
	if err = fx.putDocLen(tx, docID, docLen); err != nil {
		return err
	}

	// postings + vocab — buffered, flushed at commit.
	for term, positions := range terms {
		fx.addPostingPending(term, docID, positions)
		fx.vocabDeltaPending(term, +1)
	}

	// global counters — direct (same meta page, coalesced in the page cache).
	if err = fx.addMetaDelta(tx, ftsMetaCount, +1); err != nil {
		return err
	}
	if err = fx.addMetaDelta(tx, ftsMetaTokens, int64(docLen)); err != nil {
		return err
	}
	return fx.maybeSpill(tx)
}

// deleteDoc removes a document from the index. No-op if it was never indexed.
func (fx *ftsIndex) deleteDoc(tx *btree.WriteTx, it item) error {
	stringID := it.appendId(nil)
	docID, ok, err := fx.lookupDocID(tx, stringID)
	if err != nil || !ok {
		return err
	}
	fx.tokBuf = fx.analyzeInto(fx.tokBuf, it)
	var docLen uint32
	fx.termBufA, docLen = fx.termPostingsInto(fx.termBufA, fx.tokBuf)
	return fx.removeIndexedDoc(tx, stringID, docID, fx.termBufA, docLen)
}

// removeIndexedDoc buffers removal of all of a doc's postings/df and deletes its
// docmap/docinfo + counters. Shared by deleteDoc and the "update to empty" path.
func (fx *ftsIndex) removeIndexedDoc(tx *btree.WriteTx, stringID []byte, docID uint64, terms map[string][]uint32, docLen uint32) error {
	for term := range terms {
		fx.removePostingPending(term, docID)
		fx.vocabDeltaPending(term, -1)
	}
	fx.keyBuf = appendDocIDKey(fx.keyBuf, docID)
	if err := deleteIfExists(tx, fx.nsDocinfo, fx.keyBuf); err != nil {
		return err
	}
	fx.keyBuf = ftsMapForwardKey(fx.keyBuf, stringID)
	if err := deleteIfExists(tx, fx.nsMap, fx.keyBuf); err != nil {
		return err
	}
	fx.keyBuf = ftsMapReverseKey(fx.keyBuf, docID)
	if err := deleteIfExists(tx, fx.nsMap, fx.keyBuf); err != nil {
		return err
	}
	if err := fx.addMetaDelta(tx, ftsMetaCount, -1); err != nil {
		return err
	}
	if err := fx.addMetaDelta(tx, ftsMetaTokens, -int64(docLen)); err != nil {
		return err
	}
	return fx.maybeSpill(tx)
}

// updateDoc re-indexes a changed document by DIFFING its old and new token
// streams, keeping the IntDocID STABLE and touching only the chunks/terms that
// actually changed. Editing one word in a long note touches a couple of chunks,
// not all of them — and never accretes tombstones (the IntDocID is reused).
func (fx *ftsIndex) updateDoc(tx *btree.WriteTx, oldIt, newIt item) error {
	stringID := newIt.appendId(nil)
	docID, ok, err := fx.lookupDocID(tx, stringID)
	if err != nil {
		return err
	}
	if !ok {
		// The previous version had no indexable text → this is a fresh insert.
		return fx.insertDoc(tx, newIt)
	}

	fx.tokBuf = fx.analyzeInto(fx.tokBuf, oldIt)
	var oldLen, newLen uint32
	fx.termBufA, oldLen = fx.termPostingsInto(fx.termBufA, fx.tokBuf)
	oldTerms := fx.termBufA
	fx.tokBufB = fx.analyzeInto(fx.tokBufB, newIt)
	fx.termBufB, newLen = fx.termPostingsInto(fx.termBufB, fx.tokBufB)
	newTerms := fx.termBufB

	if newLen == 0 {
		// Doc lost all indexable text → drop it from the index entirely.
		return fx.removeIndexedDoc(tx, stringID, docID, oldTerms, oldLen)
	}

	// Terms gone or whose positions changed.
	for term, oldPos := range oldTerms {
		newPos, inNew := newTerms[term]
		switch {
		case !inNew:
			fx.removePostingPending(term, docID)
			fx.vocabDeltaPending(term, -1)
		case !slices.Equal(oldPos, newPos):
			// term still present, positions changed: re-add (df unchanged).
			fx.removePostingPending(term, docID)
			fx.addPostingPending(term, docID, newPos)
		}
		// identical positions → nothing to do.
	}
	// Newly added terms.
	for term, newPos := range newTerms {
		if _, inOld := oldTerms[term]; !inOld {
			fx.addPostingPending(term, docID, newPos)
			fx.vocabDeltaPending(term, +1)
		}
	}

	if newLen != oldLen {
		if err = fx.putDocLen(tx, docID, newLen); err != nil {
			return err
		}
		if err = fx.addMetaDelta(tx, ftsMetaTokens, int64(newLen)-int64(oldLen)); err != nil {
			return err
		}
	}
	return fx.maybeSpill(tx)
}

func (fx *ftsIndex) putDocLen(tx *btree.WriteTx, docID uint64, docLen uint32) error {
	fx.keyBuf = appendDocIDKey(fx.keyBuf, docID)
	fx.valBuf = binary.AppendUvarint(fx.valBuf[:0], uint64(docLen))
	return tx.Put(fx.nsDocinfo, fx.keyBuf, fx.valBuf)
}

func appendDocIDKey(dst []byte, docID uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], docID)
	return append(dst[:0], b[:]...)
}

// bumpVocab adjusts a term's document frequency by delta, deleting the key when
// it reaches zero.
func (fx *ftsIndex) bumpVocab(tx *btree.WriteTx, term string, delta int64) error {
	key := []byte(term)
	var cur uint64
	v, err := tx.Get(fx.nsVocab, key)
	if err != nil {
		if !errors.Is(err, btree.ErrKeyNotFound) {
			return err
		}
	} else {
		cur, _ = binary.Uvarint(v)
	}
	next := int64(cur) + delta
	if next <= 0 {
		return deleteIfExists(tx, fx.nsVocab, key)
	}
	fx.valBuf = binary.AppendUvarint(fx.valBuf[:0], uint64(next))
	return tx.Put(fx.nsVocab, key, fx.valBuf)
}

func deleteIfExists(tx *btree.WriteTx, ns *btree.Namespace, key []byte) error {
	if err := tx.Delete(ns, key); err != nil && !errors.Is(err, btree.ErrKeyNotFound) {
		return err
	}
	return nil
}

// ftsIndexNames returns the five namespace names for an fts index (creation /
// drop helper).
func ftsIndexNames(collName, indexName string) []string {
	parts := []string{ftsPartMap, ftsPartMeta, ftsPartVocab, ftsPartDocinfo, ftsPartPost}
	names := make([]string, len(parts))
	for i, p := range parts {
		names[i] = ftsNsName(collName, indexName, p)
	}
	return names
}

// isFulltext reports whether an IndexInfo declares a full-text index.
func isFulltext(info IndexInfo) bool { return info.Kind == IndexKindFulltext }
