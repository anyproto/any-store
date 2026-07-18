package anystore

import (
	"context"
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
	ftsMetaFormat = []byte("fmt") // on-disk postings format version (see fts.PostingsVersion)
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

	// nsNames holds the five namespace names in bindNamespaces order, and
	// catalogKey the index's system-namespace key — the identity of the
	// generation this handle was built from, captured at construction/bind and
	// immutable: visibleTo's slow path reads them lock-free, so it must never
	// consult c.name, which a concurrent Rename mutates under c.mu (see
	// index.nsName).
	nsNames    [5]string
	catalogKey []byte

	az *fts.Analyzer

	// pending is the per-tx write-back buffer (postings + vocab deltas), flushed
	// at commit. See fulltext_pending.go.
	pending ftsPending

	// validFromCookie: earliest schema cookie at which this handle is KNOWN
	// visible. Same contract as index.validFromCookie — see there, visibleTo
	// and visibleIndexes.
	validFromCookie uint32

	// nFields is the number of indexed fields (== len(fieldPaths)); each token's
	// field index keys into the FieldMask / per-field TF of the v2 postings.
	nFields int

	// scratch buffers, writer-owned (single writer, serialized by btree writeMu)
	keyBuf   []byte
	valBuf   []byte
	tokBuf   []fts.Token         // analyze() output (also old-doc tokens in updateDoc)
	tokBufB  []fts.Token         // new-doc tokens in updateDoc
	termBufA map[string][]uint32 // reused term->positions for insert/delete + old-doc
	termBufB map[string][]uint32 // reused term->positions for new-doc in updateDoc
	chunkPL  []fts.Posting
	mergeBuf []fts.Posting // flushChunk merge scratch

	// fieldStarts[f] is the global position at which indexed field f begins (after
	// the per-field gap); fieldStarts[nFields] is the end. analyzeInto fills it so
	// fieldBreakdown can attribute each term position to its field. fieldTFBuf is
	// the reused dense per-field TF scratch.
	fieldStarts []uint32
	// fieldStartsOld snapshots the OLD document's fieldStarts in updateDoc (analyzeInto
	// overwrites fieldStarts with the new doc's). When old and new starts differ, the
	// field ranges moved, so identical positions no longer imply identical field
	// attribution and the unchanged-positions skip must be disabled.
	fieldStartsOld []uint32
	fieldTFBuf     []uint32

	// flushChunk scratch: decoded-positions arena, decoded per-field-TF arena,
	// existing-chunk value buffer, sorted added-id buffer — reused across chunks.
	posDecode   []uint32
	tfDecode    []uint32
	chunkValBuf []byte
	addIDsBuf   []uint64

	// posFree recycles per-term position slices harvested by termPostingsInto
	// when it clears its map (clear() would otherwise drop their capacity and
	// force one allocation per distinct term per document).
	posFree [][]uint32
}

func newFtsIndex(c *collection, info IndexInfo) (*ftsIndex, error) {
	// Construction sites (init, reconcile, createFtsIndex) run on the writer
	// or under c.mu, so reading c.name here is race-free (see catalogKey).
	fx := &ftsIndex{c: c, info: info, az: fts.NewAnalyzer(),
		catalogKey: indexKey(c.name, info.Name)}
	for _, field := range info.Fields {
		fields, _ := parseIndexField(field)
		if slices.Contains(fields, "") {
			return nil, errors.New("fts: invalid index field: '" + field + "'")
		}
		fx.fieldPaths = append(fx.fieldPaths, fields)
	}
	if len(fx.fieldPaths) == 0 {
		return nil, errors.New("fts: index requires at least one field")
	}
	if len(fx.fieldPaths) > fts.MaxFields {
		return nil, errors.New("fts: too many full-text fields (max 64)")
	}
	fx.nFields = len(fx.fieldPaths)
	if p := info.Fulltext; p != nil {
		if p.B < 0 || p.B > 1 {
			return nil, errors.New("fts: Fulltext.B must be in [0,1]")
		}
		if p.K1 < 0 {
			return nil, errors.New("fts: Fulltext.K1 must be >= 0")
		}
		for field, w := range p.Weights {
			if !slices.Contains(info.Fields, field) {
				return nil, errors.New("fts: Fulltext.Weights references unknown field: " + field)
			}
			if w < 0 {
				return nil, errors.New("fts: Fulltext.Weights must be >= 0 for field: " + field)
			}
		}
	}
	return fx, nil
}

// Len returns the number of indexed documents — the maintained N counter, not
// a namespace scan; documents with no indexable text are excluded (Index
// interface).
func (fx *ftsIndex) Len(ctx context.Context) (count int, err error) {
	err = fx.c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		n, txErr := ftsGetUint(tx, fx.nsMeta, ftsMetaCount)
		count = int(n)
		return txErr
	})
	return
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
	for i, p := range parts {
		name := ftsNsName(fx.c.name, fx.info.Name, p.name)
		ns, e := resolve(name)
		if e != nil {
			return e
		}
		*p.dst = ns
		fx.nsNames[i] = name
	}
	return nil
}

func (fx *ftsIndex) Info() IndexInfo { return fx.info }

// visibleTo reports whether the given tx may search through this handle — the
// visibility gate of visibleIndexes, fts-shaped (see index.visibleTo): fast
// path on the write-tx view or a snapshot cookie at or past validFromCookie;
// an older reader admits the handle only if its OWN snapshot still carries
// this exact index — the catalog row matches the handle's full definition
// AND all five namespaces resolve at the bound roots. All five, not just
// meta: freelist reuse can land one recreated root on its freed predecessor's
// page number, and a partial match would search this generation's postings
// through another generation's vocab/docinfo. When everything matches, the
// trees are the reader's own generation of this definition — exact. Anything
// less → invisible → ErrNoFulltextIndex at the call sites, exactly as before
// the CreateIndex began. (metaRootUnchanged has the opposite error bias —
// keep a working index over a transient view — so the two checks stay
// separate.)
func (fx *ftsIndex) visibleTo(tx *btree.ReadTx) bool {
	if tx.IsWriteTx() || tx.DiskSchemaCookie() >= fx.validFromCookie {
		return true
	}
	raw, err := tx.AppendValue(fx.c.db.systemNS, fx.catalogKey, nil)
	if err != nil || !indexDefMatches(raw, fx.info) {
		return false
	}
	for i, bound := range [...]*btree.Namespace{fx.nsMap, fx.nsMeta, fx.nsVocab, fx.nsDocinfo, fx.nsPost} {
		if bound == nil || fx.nsNames[i] == "" {
			return false
		}
		ns, nsErr := tx.GetNamespace(fx.nsNames[i])
		if nsErr != nil || ns.RootPage() != bound.RootPage() {
			return false
		}
	}
	return true
}

// metaRootUnchanged reports whether the ftx: meta namespace still resolves to
// the btree root this handle was bound against — false after a peer
// drop+recreate moved the roots, which means the handles are stale and the
// index must be rebound. A transient resolution failure returns true so a
// working index is not dropped over a momentary view.
func (fx *ftsIndex) metaRootUnchanged(tx *btree.ReadTx) bool {
	if fx.nsMeta == nil {
		return true
	}
	// nsNames[1] = meta (bindNamespaces order); the captured name, not
	// c.name, for consistency with visibleTo — reconcile holds c.mu, but the
	// handle's own generation name is the one its roots belong to.
	ns, err := tx.GetNamespace(fx.nsNames[1])
	if err != nil {
		return true
	}
	return ns.RootPage() == fx.nsMeta.RootPage()
}

// reconcileFtsIndexesLocked rebuilds the full-text-index set from on-disk infos
// after a peer committed full-text DDL. Caller holds c.mu. Mirrors
// reconcileVectorIndexesLocked: adopt peer-created indexes (trusting the peer's
// backfill, as the range/vector adoptions do), reuse live objects while their
// definition and meta root are unchanged, and evict peer-dropped ones so a
// stale handle can never flush postings into freed-and-reused ftx: pages.
// Never touches disk — reconcile may run inside a read tx, and the peer
// already performed the DDL; eviction is purely local handle release.
func (c *collection) reconcileFtsIndexesLocked(tx *btree.ReadTx, infos []IndexInfo) {
	cur := c.loadFtsIndexes()
	byName := make(map[string]*ftsIndex, len(cur))
	for _, fx := range cur {
		byName[fx.info.Name] = fx
	}

	var want int
	for _, info := range infos {
		if isFulltext(info) {
			want++
		}
	}
	rebuilt := make([]*ftsIndex, 0, want)
	changed := want != len(cur)
	for _, info := range infos {
		if !isFulltext(info) {
			continue
		}
		if existing, ok := byName[info.Name]; ok &&
			indexInfoEqual(existing.info, info) && existing.metaRootUnchanged(tx) {
			rebuilt = append(rebuilt, existing)
			continue
		}
		// New index, or a drop+recreate moved the roots under the same name so
		// the existing object's handles are stale — bind fresh handles.
		fx, err := newFtsIndex(c, info)
		if err == nil {
			err = fx.bindNamespaces(tx.GetNamespace)
			// Reconcile runs at tx begin (checkStale), before any of this tx's
			// writes: the bound state is committed as of this snapshot, so its
			// cookie is the exact visibility bound. An older concurrent reader
			// resolves per-snapshot in visibleTo and correctly skips a peer
			// index its snapshot predates.
			fx.validFromCookie = tx.DiskSchemaCookie()
		}
		if err != nil {
			// Not resolvable in this snapshot. With no existing object this is
			// a peer create racing our view — skip until a later reconcile.
			// With an existing object we only got here because its definition
			// or meta root no longer matches the on-disk index (peer
			// drop+recreate): the handle is KNOWN-stale and must not be
			// republished — a later flush through it would write into freed,
			// reused pages. Evict it; $text fails cleanly until a rebind
			// succeeds. (Keeping a working index over a merely-transient
			// resolution failure is handled in metaRootUnchanged, which
			// returns true when it cannot resolve.)
			changed = true
			continue
		}
		rebuilt = append(rebuilt, fx)
		changed = true
	}
	if !changed {
		// Same set, same definitions, same roots — every live object was
		// carried forward, so there is nothing to evict or publish.
		return
	}

	// Eviction is publication-by-omission only — an evicted handle's pending
	// buffer is deliberately NOT touched. Resetting it here would race a
	// concurrent writer goroutine still buffering into it (inserts read the
	// snapshot lock-free), and there is nothing to save anyway: the commit
	// flush enumerates the published snapshot, so an evicted (peer-dropped)
	// index's buffered postings are dropped with the object — exactly right
	// for an index that no longer exists. (A writer's own uncommitted index
	// can never be evicted here: the indexSetDDLTxs guard in reconcileIndexes
	// skips the whole pass while local DDL is in flight.)
	c.storeFtsIndexes(rebuilt)
}

// ---- analysis -------------------------------------------------------------

// analyzeInto runs the analyzer over all indexed fields of the document,
// appending the flat token stream to dst (positions are offset per field by
// ftsPosGap so phrases cannot bridge fields) and returning the extended slice. It
// also records fx.fieldStarts: the global position at which each field begins (and
// fieldStarts[nFields] = the end), so fieldBreakdown can attribute every term
// position to its field for the per-field TF of the v2 postings. The next field's
// base is the current field's max position + 1 + gap, which keeps field position
// ranges contiguous and non-overlapping even for multi-element array fields.
func (fx *ftsIndex) analyzeInto(dst []fts.Token, it item) []fts.Token {
	dst = dst[:0]
	val := it.Value()
	fx.fieldStarts = append(fx.fieldStarts[:0], make([]uint32, fx.nFields+1)...)
	var base uint32
	for fieldIdx, path := range fx.fieldPaths {
		fx.fieldStarts[fieldIdx] = base
		fv := val.Get(path...)
		before := len(dst)
		dst = fx.appendFieldTokens(dst, fv, base)
		if len(dst) > before {
			base = dst[len(dst)-1].Pos + 1 + ftsPosGap
		}
	}
	fx.fieldStarts[fx.nFields] = base
	return dst
}

// fieldBreakdown attributes a term's ascending global positions to their indexed
// fields (using fieldStarts) and returns the FieldMask plus the dense per-field
// TF (length nFields, reusing fieldTFBuf). Positions are ascending and field
// ranges are contiguous, so a single forward walk suffices.
func (fx *ftsIndex) fieldBreakdown(positions []uint32) (mask uint64, denseTF []uint32) {
	tf := fx.fieldTFBuf[:0]
	for i := 0; i < fx.nFields; i++ {
		tf = append(tf, 0)
	}
	f := 0
	for _, p := range positions {
		for f+1 < fx.nFields && p >= fx.fieldStarts[f+1] {
			f++
		}
		tf[f]++
	}
	for i, c := range tf {
		if c > 0 {
			mask |= uint64(1) << uint(i)
		}
	}
	fx.fieldTFBuf = tf
	return mask, tf
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
			// Keep array elements on separate position runs too. Advance past the
			// element's last emitted position (not by a fixed step): an element with
			// >= ftsPosGap tokens would otherwise overlap the next element's range,
			// producing false cross-element phrase matches and non-ascending
			// per-term position lists (breaking AppendChunk's ascending contract
			// and countAdjacent's binary search).
			if len(dst) > 0 {
				base = dst[len(dst)-1].Pos + 1 + ftsPosGap
			} else {
				base += ftsPosGap
			}
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
	return ftsGetUint(&tx.ReadTx, fx.nsMeta, key)
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
	next := max(int64(cur)+delta, 0)
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
	return ftsGetUintOk(&tx.ReadTx, fx.nsMap, fx.keyBuf)
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

	stringID := fx.c.appendId(nil, it.Value())
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

	// postings + vocab — buffered, flushed at commit. fieldBreakdown reuses
	// fieldTFBuf, so consume it immediately inside addPostingPending (it copies
	// the dense TF into the per-tx arena).
	for term, positions := range terms {
		_, denseTF := fx.fieldBreakdown(positions)
		fx.addPostingPending(term, docID, positions, denseTF)
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
	stringID := fx.c.appendId(nil, it.Value())
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
	stringID := fx.c.appendId(nil, newIt.Value())
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
	fx.fieldStartsOld = append(fx.fieldStartsOld[:0], fx.fieldStarts...)
	fx.tokBufB = fx.analyzeInto(fx.tokBufB, newIt)
	fx.termBufB, newLen = fx.termPostingsInto(fx.termBufB, fx.tokBufB)
	newTerms := fx.termBufB
	// Field ranges moved (e.g. an earlier field emptied, so a later field inherits
	// its base): identical positions may now map to different fields, so every
	// surviving term must be re-attributed even when its positions are unchanged.
	startsChanged := !slices.Equal(fx.fieldStartsOld, fx.fieldStarts)

	if newLen == 0 {
		// Doc lost all indexable text → drop it from the index entirely.
		return fx.removeIndexedDoc(tx, stringID, docID, oldTerms, oldLen)
	}

	// fx.fieldStarts now reflects newIt (analyzed last), so fieldBreakdown below
	// attributes new positions to the new document's fields.
	// Terms gone or whose positions changed.
	for term, oldPos := range oldTerms {
		newPos, inNew := newTerms[term]
		switch {
		case !inNew:
			fx.removePostingPending(term, docID)
			fx.vocabDeltaPending(term, -1)
		case startsChanged || !slices.Equal(oldPos, newPos):
			// term still present, positions changed (or the field ranges moved):
			// re-add (df unchanged). With unchanged fieldStarts, identical positions
			// imply identical field attribution, so the skip below is safe.
			fx.removePostingPending(term, docID)
			_, denseTF := fx.fieldBreakdown(newPos)
			fx.addPostingPending(term, docID, newPos, denseTF)
		}
		// identical positions with unchanged field ranges → nothing to do.
	}
	// Newly added terms.
	for term, newPos := range newTerms {
		if _, inOld := oldTerms[term]; !inOld {
			_, denseTF := fx.fieldBreakdown(newPos)
			fx.addPostingPending(term, docID, newPos, denseTF)
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
