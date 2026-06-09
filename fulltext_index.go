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

	// scratch buffers, writer-owned (single writer, serialized by btree writeMu)
	keyBuf  []byte
	valBuf  []byte
	tokBuf  []fts.Token
	chunkPL []fts.Posting
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

// analyze runs the analyzer over all indexed fields of the document, returning
// the flat token stream (positions are offset per field by ftsPosGap so phrases
// cannot bridge fields). Tokens are appended to fx.tokBuf, which is returned.
func (fx *ftsIndex) analyze(it item) []fts.Token {
	fx.tokBuf = fx.tokBuf[:0]
	val := it.Value()
	var base uint32
	for _, path := range fx.fieldPaths {
		fv := val.Get(path...)
		before := len(fx.tokBuf)
		fx.appendFieldTokens(fv, base)
		emitted := len(fx.tokBuf) - before
		if emitted > 0 {
			base += uint32(emitted) + ftsPosGap
		}
	}
	return fx.tokBuf
}

// appendFieldTokens analyzes a single field value (string, or array of strings)
// and appends its tokens to fx.tokBuf with positions shifted by base.
func (fx *ftsIndex) appendFieldTokens(fv *anyenc.Value, base uint32) {
	if fv == nil {
		return
	}
	switch fv.Type() {
	case anyenc.TypeString:
		sb, err := fv.StringBytes()
		if err != nil {
			return
		}
		fx.appendText(string(sb), base)
	case anyenc.TypeArray:
		arr, err := fv.Array()
		if err != nil {
			return
		}
		for _, av := range arr {
			if av.Type() != anyenc.TypeString {
				continue
			}
			sb, err := av.StringBytes()
			if err != nil {
				continue
			}
			fx.appendText(string(sb), base)
			// keep array elements on separate position runs too
			base += ftsPosGap
		}
	}
}

func (fx *ftsIndex) appendText(text string, base uint32) {
	start := len(fx.tokBuf)
	fx.tokBuf = fx.az.Append(fx.tokBuf, text)
	if base != 0 {
		for i := start; i < len(fx.tokBuf); i++ {
			fx.tokBuf[i].Pos += base
		}
	}
}

// termPostings groups a token stream into per-term ascending position lists for
// a single document. Returns a map term→positions and the document length.
func termPostings(tokens []fts.Token) (map[string][]uint32, uint32) {
	if len(tokens) == 0 {
		return nil, 0
	}
	m := make(map[string][]uint32, len(tokens))
	for _, t := range tokens {
		m[t.Term] = append(m[t.Term], t.Pos)
	}
	for _, ps := range m {
		slices.Sort(ps)
	}
	return m, uint32(len(tokens))
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

// insertDoc indexes a document: allocates an IntDocID, writes the docmap,
// docinfo, vocab and postings entries, and bumps the global counters.
func (fx *ftsIndex) insertDoc(tx *btree.WriteTx, it item) error {
	tokens := fx.analyze(it)
	terms, docLen := termPostings(tokens)
	if docLen == 0 {
		// Nothing to index (no text / empty fields). Skip entirely — the doc is
		// simply absent from this fts index.
		return nil
	}

	stringID := it.appendId(nil)
	docID, err := fx.allocDocID(tx)
	if err != nil {
		return err
	}

	// docmap forward + reverse
	fx.keyBuf = ftsMapForwardKey(fx.keyBuf, stringID)
	fx.valBuf = binary.AppendUvarint(fx.valBuf[:0], docID)
	if err = tx.Put(fx.nsMap, fx.keyBuf, fx.valBuf); err != nil {
		return err
	}
	fx.keyBuf = ftsMapReverseKey(fx.keyBuf, docID)
	if err = tx.Put(fx.nsMap, fx.keyBuf, stringID); err != nil {
		return err
	}

	// docinfo: doc length
	if err = fx.putDocLen(tx, docID, docLen); err != nil {
		return err
	}

	// postings + vocab
	for term, positions := range terms {
		if err = fx.addPosting(tx, term, docID, positions); err != nil {
			return err
		}
		if err = fx.bumpVocab(tx, term, +1); err != nil {
			return err
		}
	}

	// global counters
	if err = fx.addMetaDelta(tx, ftsMetaCount, +1); err != nil {
		return err
	}
	return fx.addMetaDelta(tx, ftsMetaTokens, int64(docLen))
}

// deleteDoc removes a document from the index. It is a no-op if the document was
// never indexed (e.g. had no text).
func (fx *ftsIndex) deleteDoc(tx *btree.WriteTx, it item) error {
	stringID := it.appendId(nil)
	docID, ok, err := fx.lookupDocID(tx, stringID)
	if err != nil || !ok {
		return err
	}

	tokens := fx.analyze(it)
	terms, docLen := termPostings(tokens)

	for term := range terms {
		if err = fx.removePosting(tx, term, docID); err != nil {
			return err
		}
		if err = fx.bumpVocab(tx, term, -1); err != nil {
			return err
		}
	}

	// docinfo
	fx.keyBuf = appendDocIDKey(fx.keyBuf, docID)
	if err = deleteIfExists(tx, fx.nsDocinfo, fx.keyBuf); err != nil {
		return err
	}
	// docmap forward + reverse
	fx.keyBuf = ftsMapForwardKey(fx.keyBuf, stringID)
	if err = deleteIfExists(tx, fx.nsMap, fx.keyBuf); err != nil {
		return err
	}
	fx.keyBuf = ftsMapReverseKey(fx.keyBuf, docID)
	if err = deleteIfExists(tx, fx.nsMap, fx.keyBuf); err != nil {
		return err
	}

	// global counters
	if err = fx.addMetaDelta(tx, ftsMetaCount, -1); err != nil {
		return err
	}
	return fx.addMetaDelta(tx, ftsMetaTokens, -int64(docLen))
}

// updateDoc re-indexes a changed document. v1 = delete old + insert new (which
// allocates a fresh IntDocID and rewrites the affected chunks). The delta-update
// optimization (diff old/new terms, touch only changed chunks, keep the
// IntDocID) is a deferred v2 improvement — see DESIGN.md.
func (fx *ftsIndex) updateDoc(tx *btree.WriteTx, oldIt, newIt item) error {
	if err := fx.deleteDoc(tx, oldIt); err != nil {
		return err
	}
	return fx.insertDoc(tx, newIt)
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

// addPosting inserts (docID, positions) into the term's chunk. Because docID is
// the largest yet allocated, it sorts last within its chunk, so we decode the
// chunk, append, and re-encode.
func (fx *ftsIndex) addPosting(tx *btree.WriteTx, term string, docID uint64, positions []uint32) error {
	chunkID := fts.ChunkID(docID)
	fx.keyBuf = postingsKey(fx.keyBuf, term, chunkID)

	fx.chunkPL = fx.chunkPL[:0]
	existing, err := tx.Get(fx.nsPost, fx.keyBuf)
	if err != nil {
		if !errors.Is(err, btree.ErrKeyNotFound) {
			return err
		}
	} else {
		fx.chunkPL, err = fts.DecodeChunk(fx.chunkPL, existing)
		if err != nil {
			return err
		}
	}
	fx.chunkPL = append(fx.chunkPL, fts.Posting{
		DocID:     docID,
		Positions: slices.Clone(positions),
	})
	// keyBuf is reused by encoding below; rebuild key after the value buffer.
	fx.valBuf = fts.AppendChunk(fx.valBuf[:0], fx.chunkPL)
	fx.keyBuf = postingsKey(fx.keyBuf, term, chunkID)
	return tx.Put(fx.nsPost, fx.keyBuf, fx.valBuf)
}

// removePosting drops docID from the term's chunk, deleting the chunk key if it
// becomes empty.
func (fx *ftsIndex) removePosting(tx *btree.WriteTx, term string, docID uint64) error {
	chunkID := fts.ChunkID(docID)
	fx.keyBuf = postingsKey(fx.keyBuf, term, chunkID)
	existing, err := tx.Get(fx.nsPost, fx.keyBuf)
	if err != nil {
		if errors.Is(err, btree.ErrKeyNotFound) {
			return nil
		}
		return err
	}
	fx.chunkPL = fx.chunkPL[:0]
	if fx.chunkPL, err = fts.DecodeChunk(fx.chunkPL, existing); err != nil {
		return err
	}
	out := fx.chunkPL[:0]
	for _, p := range fx.chunkPL {
		if p.DocID != docID {
			out = append(out, p)
		}
	}
	fx.keyBuf = postingsKey(fx.keyBuf, term, chunkID)
	if len(out) == 0 {
		return deleteIfExists(tx, fx.nsPost, fx.keyBuf)
	}
	fx.valBuf = fts.AppendChunk(fx.valBuf[:0], out)
	return tx.Put(fx.nsPost, fx.keyBuf, fx.valBuf)
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
