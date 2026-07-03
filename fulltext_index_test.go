package anystore

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/fts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ftsTestColl creates a collection with a full-text index over the given fields.
func ftsTestColl(t *testing.T, fields ...string) (*fixture, *collection) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Kind:   IndexKindFulltext,
		Fields: fields,
	}))
	return fx, coll.(*collection)
}

func insertJSON(t *testing.T, coll Collection, jsons ...string) {
	for _, j := range jsons {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(j)))
	}
}

// readMeta reads a meta counter from the fts index.
func ftsReadMeta(t *testing.T, c *collection, key []byte) uint64 {
	fxs := c.loadFtsIndexes()
	require.Len(t, fxs, 1)
	fx := fxs[0]
	var n uint64
	require.NoError(t, c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		v, err := tx.Get(fx.nsMeta, key)
		if err != nil {
			if err == btree.ErrKeyNotFound {
				return nil
			}
			return err
		}
		n, _ = binary.Uvarint(v)
		return nil
	}))
	return n
}

// ftsVocabDF reads the document frequency of a term, 0 if absent.
func ftsVocabDF(t *testing.T, c *collection, term string) uint64 {
	fx := c.loadFtsIndexes()[0]
	var df uint64
	require.NoError(t, c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		v, err := tx.Get(fx.nsVocab, []byte(term))
		if err != nil {
			if err == btree.ErrKeyNotFound {
				return nil
			}
			return err
		}
		df, _ = binary.Uvarint(v)
		return nil
	}))
	return df
}

// ftsTermDocIDs returns the sorted DocIDs in a term's chunk(s). Single chunk
// assumed (small tests).
func ftsTermDocIDs(t *testing.T, c *collection, term string) []uint64 {
	fx := c.loadFtsIndexes()[0]
	var ids []uint64
	require.NoError(t, c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		cur := tx.NewCursor(fx.nsPost)
		defer cur.Close()
		prefix := postingsKey(nil, term, 0)
		prefix = prefix[:len(prefix)-8] // strip the chunkID suffix → term prefix
		if err := cur.Seek(prefix); err != nil {
			return err
		}
		for cur.Valid() {
			k, err := cur.Key()
			if err != nil {
				return err
			}
			if len(k) < len(prefix) || string(k[:len(prefix)]) != string(prefix) {
				break
			}
			v, err := cur.Value()
			if err != nil {
				return err
			}
			ps, err := fts.DecodeChunk(nil, v)
			if err != nil {
				return err
			}
			for _, p := range ps {
				ids = append(ids, p.DocID)
			}
			if err := cur.Next(); err != nil {
				return err
			}
		}
		return nil
	}))
	return ids
}

func TestFts_InsertPopulatesIndex(t *testing.T) {
	fx, coll := ftsTestColl(t, "title", "body")
	defer fx.finish()

	insertJSON(t, coll,
		`{"id":"a","title":"Hello World","body":"the quick brown fox"}`,
		`{"id":"b","title":"Hello again","body":"lazy dog sleeps"}`,
	)

	// N = 2 documents indexed
	assert.Equal(t, uint64(2), ftsReadMeta(t, coll, ftsMetaCount))
	// seq advanced to 2
	assert.Equal(t, uint64(2), ftsReadMeta(t, coll, ftsMetaSeq))

	// "hello" appears in both docs → df 2
	assert.Equal(t, uint64(2), ftsVocabDF(t, coll, "hello"))
	// "fox" only in doc a → df 1
	assert.Equal(t, uint64(1), ftsVocabDF(t, coll, "fox"))
	// stop words are indexed (not removed)
	assert.Equal(t, uint64(1), ftsVocabDF(t, coll, "the"))

	// "hello" postings reference both IntDocIDs (1 and 2)
	assert.Equal(t, []uint64{1, 2}, ftsTermDocIDs(t, coll, "hello"))
	assert.Equal(t, []uint64{1}, ftsTermDocIDs(t, coll, "fox"))
}

func TestFts_GetIndexesIncludesFulltext(t *testing.T) {
	fx, coll := ftsTestColl(t, "title", "body")
	defer fx.finish()
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"year"}}))

	insertJSON(t, coll,
		`{"id":"a","title":"Hello World","body":"the quick brown fox","year":2001}`,
		`{"id":"b","year":2002}`, // no indexable text — excluded from the fts corpus
	)

	indexes := coll.GetIndexes()
	require.Len(t, indexes, 2)
	// Range indexes first, then full-text.
	assert.Equal(t, IndexKindRange, indexes[0].Info().Kind)
	rangeLen, err := indexes[0].Len(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, rangeLen)

	info := indexes[1].Info()
	assert.Equal(t, IndexKindFulltext, info.Kind)
	assert.Equal(t, []string{"title", "body"}, info.Fields)
	ftsLen, err := indexes[1].Len(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, ftsLen)
}

func TestFts_DeleteRemovesPostings(t *testing.T) {
	fx, coll := ftsTestColl(t, "body")
	defer fx.finish()

	insertJSON(t, coll,
		`{"id":"a","body":"shared token alpha"}`,
		`{"id":"b","body":"shared token beta"}`,
	)
	require.NoError(t, coll.DeleteId(ctx, "a"))

	assert.Equal(t, uint64(1), ftsReadMeta(t, coll, ftsMetaCount))
	// "alpha" was only in a → vocab key gone
	assert.Equal(t, uint64(0), ftsVocabDF(t, coll, "alpha"))
	// "shared" still in b → df 1, postings has only b's id (2)
	assert.Equal(t, uint64(1), ftsVocabDF(t, coll, "shared"))
	assert.Equal(t, []uint64{2}, ftsTermDocIDs(t, coll, "shared"))
	// seq is monotonic — never reused even though a was deleted
	assert.Equal(t, uint64(2), ftsReadMeta(t, coll, ftsMetaSeq))
}

func TestFts_UpdateReindexes(t *testing.T) {
	fx, coll := ftsTestColl(t, "body")
	defer fx.finish()

	insertJSON(t, coll, `{"id":"a","body":"original content here"}`)
	assert.Equal(t, uint64(1), ftsVocabDF(t, coll, "original"))

	// Replace the document's body.
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":"a","body":"replaced text entirely"}`)))

	// Old terms gone, new terms present.
	assert.Equal(t, uint64(0), ftsVocabDF(t, coll, "original"))
	assert.Equal(t, uint64(0), ftsVocabDF(t, coll, "content"))
	assert.Equal(t, uint64(1), ftsVocabDF(t, coll, "replaced"))
	assert.Equal(t, uint64(1), ftsVocabDF(t, coll, "entirely"))
	// still exactly one indexed document
	assert.Equal(t, uint64(1), ftsReadMeta(t, coll, ftsMetaCount))
	// Delta-update keeps the IntDocID STABLE across edits (id 1, not a fresh 2)
	// and does NOT advance the seq counter.
	assert.Equal(t, []uint64{1}, ftsTermDocIDs(t, coll, "replaced"))
	assert.Equal(t, uint64(1), ftsReadMeta(t, coll, ftsMetaSeq))
}

func TestFts_DeltaUpdateKeepsChunksDense(t *testing.T) {
	fx, coll := ftsTestColl(t, "body")
	defer fx.finish()

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"x","body":"alpha beta gamma"}`)))
	// Edit the doc many times. The delta-update must keep IntDocID stable, never
	// advance seq, and never accrete tombstones in the term chunks.
	for i := 0; i < 50; i++ {
		body := fmt.Sprintf("alpha beta gamma rev%d", i)
		require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":"x","body":%q}`, body))))
	}
	assert.Equal(t, uint64(1), ftsReadMeta(t, coll, ftsMetaSeq), "seq must not grow with edits")
	assert.Equal(t, uint64(1), ftsReadMeta(t, coll, ftsMetaCount))
	// Terms present in every revision keep a single live posting (docID 1) — the
	// chunk is dense, not 50 tombstones + 1 live.
	assert.Equal(t, []uint64{1}, ftsTermDocIDs(t, coll, "alpha"))
	assert.Equal(t, []uint64{1}, ftsTermDocIDs(t, coll, "gamma"))
	// Per-revision unique terms: only the latest survives.
	assert.Equal(t, uint64(0), ftsVocabDF(t, coll, "rev0"))
	assert.Equal(t, uint64(0), ftsVocabDF(t, coll, "rev48"))
	assert.Equal(t, uint64(1), ftsVocabDF(t, coll, "rev49"))
}

func TestFts_CJKBigramsIndexed(t *testing.T) {
	fx, coll := ftsTestColl(t, "body")
	defer fx.finish()

	insertJSON(t, coll, `{"id":"a","body":"東京都"}`)
	// 東京都 → bigrams 東京, 京都
	assert.Equal(t, uint64(1), ftsVocabDF(t, coll, "東京"))
	assert.Equal(t, uint64(1), ftsVocabDF(t, coll, "京都"))
}

// ftsSearchIDs runs a $text query and returns the matched ids in result order.
func ftsSearchIDs(t *testing.T, coll Collection, search string, opts ...func(Query) Query) []string {
	q := coll.Find(map[string]any{"$text": map[string]any{"$search": search}})
	for _, o := range opts {
		q = o(q)
	}
	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var ids []string
	for iter.Next() {
		doc, derr := iter.Doc()
		require.NoError(t, derr)
		ids = append(ids, doc.Value().GetString("id"))
	}
	require.NoError(t, iter.Err())
	return ids
}

func TestFtsSearch_BasicMatchAndMiss(t *testing.T) {
	fx, coll := ftsTestColl(t, "body")
	defer fx.finish()
	insertJSON(t, coll,
		`{"id":"a","body":"the quick brown fox"}`,
		`{"id":"b","body":"lazy dog sleeps"}`,
		`{"id":"c","body":"quick green turtle"}`,
	)
	assert.ElementsMatch(t, []string{"a", "c"}, ftsSearchIDs(t, coll, "quick"))
	assert.ElementsMatch(t, []string{"b"}, ftsSearchIDs(t, coll, "dog"))
	assert.Empty(t, ftsSearchIDs(t, coll, "nonexistent"))
}

func TestFtsSearch_RanksByRelevance(t *testing.T) {
	fx, coll := ftsTestColl(t, "body")
	defer fx.finish()
	insertJSON(t, coll,
		`{"id":"low","body":"apple orange banana grape melon kiwi"}`,
		`{"id":"high","body":"apple apple apple"}`,
		`{"id":"mid","body":"apple apple pear"}`,
	)
	// "high" (tf=3, short doc) should outrank "mid" (tf=2) and "low" (tf=1, long doc).
	got := ftsSearchIDs(t, coll, "apple")
	require.Equal(t, []string{"high", "mid", "low"}, got)
}

func TestFtsSearch_MultiTermOr(t *testing.T) {
	fx, coll := ftsTestColl(t, "body")
	defer fx.finish()
	insertJSON(t, coll,
		`{"id":"a","body":"london fog"}`,
		`{"id":"b","body":"paris sun"}`,
		`{"id":"c","body":"london and paris"}`,
	)
	// bag-of-words OR: all docs containing london OR paris; c matches both → ranks first.
	got := ftsSearchIDs(t, coll, "london paris")
	require.Len(t, got, 3)
	assert.Equal(t, "c", got[0])
	assert.ElementsMatch(t, []string{"a", "b", "c"}, got)
}

func TestFtsSearch_ResidualFilter(t *testing.T) {
	fx, coll := ftsTestColl(t, "body")
	defer fx.finish()
	insertJSON(t, coll,
		`{"id":"a","body":"crash report","year":1920}`,
		`{"id":"b","body":"crash report","year":1937}`,
	)
	// $text combined with a normal predicate via implicit AND.
	q := coll.Find(map[string]any{
		"$text": map[string]any{"$search": "crash"},
		"year":  1937,
	})
	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var ids []string
	for iter.Next() {
		doc, _ := iter.Doc()
		ids = append(ids, doc.Value().GetString("id"))
	}
	require.NoError(t, iter.Err())
	assert.Equal(t, []string{"b"}, ids)
}

func TestFtsSearch_SortByTextScore(t *testing.T) {
	fx, coll := ftsTestColl(t, "body")
	defer fx.finish()
	insertJSON(t, coll,
		`{"id":"low","body":"apple orange banana grape"}`,
		`{"id":"high","body":"apple apple apple"}`,
	)
	// The documented Mongo-style relevance sort must parse and keep score order.
	iter, err := coll.Find(`{"$text":{"$search":"apple"}}`).
		Sort(`{"score":{"$meta":"textScore"}}`).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var ids []string
	for iter.Next() {
		doc, _ := iter.Doc()
		ids = append(ids, doc.Value().GetString("id"))
	}
	require.Equal(t, []string{"high", "low"}, ids)
}

func TestFtsSearch_LimitOffset(t *testing.T) {
	fx, coll := ftsTestColl(t, "body")
	defer fx.finish()
	insertJSON(t, coll,
		`{"id":"a","body":"term term term term"}`,
		`{"id":"b","body":"term term term"}`,
		`{"id":"c","body":"term term"}`,
		`{"id":"d","body":"term"}`,
	)
	full := ftsSearchIDs(t, coll, "term")
	require.Equal(t, []string{"a", "b", "c", "d"}, full)
	top2 := ftsSearchIDs(t, coll, "term", func(q Query) Query { return q.Limit(2) })
	assert.Equal(t, []string{"a", "b"}, top2)
	skip := ftsSearchIDs(t, coll, "term", func(q Query) Query { return q.Offset(1).Limit(2) })
	assert.Equal(t, []string{"b", "c"}, skip)
}

func TestFtsSearch_Count(t *testing.T) {
	fx, coll := ftsTestColl(t, "body")
	defer fx.finish()
	insertJSON(t, coll,
		`{"id":"a","body":"alpha beta"}`,
		`{"id":"b","body":"beta gamma"}`,
		`{"id":"c","body":"gamma delta"}`,
	)
	n, err := coll.Find(`{"$text":{"$search":"beta"}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
}

func TestFtsSearch_CJKQuery(t *testing.T) {
	fx, coll := ftsTestColl(t, "body")
	defer fx.finish()
	insertJSON(t, coll,
		`{"id":"a","body":"東京都に住んでいる"}`,
		`{"id":"b","body":"大阪は良い街です"}`,
	)
	// Query 東京 (a bigram) should find doc a.
	assert.Equal(t, []string{"a"}, ftsSearchIDs(t, coll, "東京"))
}

func TestFtsSearch_NoIndexError(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish()
	coll, err := fx.CreateCollection(ctx, "plain")
	require.NoError(t, err)
	_, err = coll.Find(`{"$text":{"$search":"x"}}`).Iter(ctx)
	assert.ErrorIs(t, err, ErrNoFulltextIndex)
}

func TestFtsSearch_RejectsTextUnderOr(t *testing.T) {
	fx, coll := ftsTestColl(t, "body")
	defer fx.finish()
	insertJSON(t, coll, `{"id":"a","body":"hello"}`)
	_, err := coll.Find(map[string]any{
		"$or": []any{
			map[string]any{"$text": map[string]any{"$search": "hello"}},
			map[string]any{"body": "world"},
		},
	}).Iter(ctx)
	require.Error(t, err)
}

func TestFts_SurvivesReopen(t *testing.T) {
	tmpDir := t.TempDir()
	func() {
		fxLocal := newFixturePath(t, tmpDir)
		coll, err := fxLocal.CreateCollection(ctx, "docs")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Kind: IndexKindFulltext, Fields: []string{"body"}}))
		insertJSON(t, coll, `{"id":"a","body":"persisted across reopen"}`)
		require.NoError(t, fxLocal.Close())
	}()

	// Reopen and confirm the index is reconstructed as full-text and still queries.
	db, err := Open(ctx, tmpDir+"/any-store-test.db", nil)
	require.NoError(t, err)
	defer db.Close()
	collI, err := db.OpenCollection(ctx, "docs")
	require.NoError(t, err)
	c := collI.(*collection)

	fxs := c.loadFtsIndexes()
	require.Len(t, fxs, 1, "fts index should be rebuilt on reopen")
	assert.Equal(t, uint64(1), ftsVocabDF(t, c, "persisted"))

	// A further insert must keep allocating monotonic ids from persisted seq.
	insertJSON(t, c, `{"id":"b","body":"another persisted doc"}`)
	assert.Equal(t, uint64(2), ftsReadMeta(t, c, ftsMetaSeq))
	assert.Equal(t, uint64(2), ftsVocabDF(t, c, "persisted"))
}

// A savepoint rollback (an op failing inside an outer WriteTx) must not leak
// buffered postings from the rolled-back op: the seq/docmap writes it depends
// on are reverted, so leaked postings would attach to whichever document later
// reuses the IntDocID.
func TestFtsSavepointRollbackDiscardsPending(t *testing.T) {
	fx, coll := ftsTestColl(t, "text")
	defer fx.finish()

	insertJSON(t, coll, `{"id":"a","text":"hello"}`)

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	// second doc duplicates "a" → whole Insert call rolls back its savepoint
	err = coll.Insert(tx.Context(),
		anyenc.MustParseJson(`{"id":"b","text":"ghostterm"}`),
		anyenc.MustParseJson(`{"id":"a","text":"dup"}`))
	require.ErrorIs(t, err, ErrDocExists)
	// a successful op afterwards reuses the rolled-back IntDocID
	require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":"c","text":"celery"}`)))
	require.NoError(t, tx.Commit())

	_, err = coll.FindId(ctx, "b")
	assert.ErrorIs(t, err, ErrDocNotFound)

	ids, _ := collectIter(t, coll.Find(`{"$text":{"$search":"ghostterm"}}`))
	assert.Empty(t, ids, "rolled-back postings leaked: %v", ids)
	assert.EqualValues(t, 0, ftsVocabDF(t, coll, "ghostterm"))

	ids, _ = collectIter(t, coll.Find(`{"$text":{"$search":"celery"}}`))
	assert.Equal(t, []string{"c"}, ids)
}

// The inverse leak: a delete inside a rolled-back savepoint must not strip
// postings from a document that stays live.
func TestFtsSavepointRollbackKeepsLiveDocPostings(t *testing.T) {
	fx, coll := ftsTestColl(t, "text")
	defer fx.finish()

	insertJSON(t, coll, `{"id":"a","text":"alpha keeper"}`)

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	// delete succeeds, then the same op's tx is rolled back by the caller
	sp, err := fx.WriteTx(tx.Context())
	require.NoError(t, err)
	require.NoError(t, coll.DeleteId(sp.Context(), "a"))
	require.NoError(t, sp.Rollback())
	require.NoError(t, tx.Commit())

	ids, _ := collectIter(t, coll.Find(`{"$text":{"$search":"keeper"}}`))
	assert.Equal(t, []string{"a"}, ids, "postings of a live doc were stripped by a rolled-back delete")
	assert.EqualValues(t, 1, ftsVocabDF(t, coll, "keeper"))
}

// Ops buffered BEFORE a savepoint must survive that savepoint's rollback (they
// are flushed outside its scope at creation).
func TestFtsSavepointRollbackKeepsEarlierPending(t *testing.T) {
	fx, coll := ftsTestColl(t, "text")
	defer fx.finish()

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":"a","text":"early bird"}`)))
	// failing op after the buffered one
	err = coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":"a","text":"dup"}`))
	require.ErrorIs(t, err, ErrDocExists)
	require.NoError(t, tx.Commit())

	ids, _ := collectIter(t, coll.Find(`{"$text":{"$search":"early"}}`))
	assert.Equal(t, []string{"a"}, ids, "pre-savepoint buffered postings were lost")
	assert.EqualValues(t, 1, ftsVocabDF(t, coll, "early"))
}
