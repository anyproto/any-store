package anystore

import (
	"encoding/binary"
	"fmt"
	"os"
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

// TestFtsPendingSurvivesCollectionCloseMidTx:
// closing a collection between Insert and Commit removed it from
// openedCollections, so the commit-time flushAllFtsPending never saw its
// pending buffer — the document committed but its postings were silently
// dropped, leaving it permanently invisible to $text.
func TestFtsPendingSurvivesCollectionCloseMidTx(t *testing.T) {
	t.Run("close then commit flushes postings", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "docs")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Kind: IndexKindFulltext, Fields: []string{"body"}}))

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":"a","body":"needleterm here"}`)))
		require.NoError(t, coll.Close())
		require.NoError(t, tx.Commit())

		coll2, err := fx.OpenCollection(ctx, "docs")
		require.NoError(t, err)
		count, err := coll2.Find(`{"$text":{"$search":"needleterm"}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "postings buffered before Close must flush at Commit")
	})

	t.Run("close then rollback discards postings", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "docs")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Kind: IndexKindFulltext, Fields: []string{"body"}}))

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":"a","body":"needleterm here"}`)))
		require.NoError(t, coll.Close())
		require.NoError(t, tx.Rollback())

		coll2, err := fx.OpenCollection(ctx, "docs")
		require.NoError(t, err)
		count, err := coll2.Find(`{"$text":{"$search":"needleterm"}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, count, "orphaned buffer of a rolled-back tx must be discarded")
	})

	t.Run("drop mid-tx discards postings and commit succeeds", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "docs")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Kind: IndexKindFulltext, Fields: []string{"body"}}))

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":"a","body":"needleterm here"}`)))
		require.NoError(t, coll.Drop(tx.Context()))
		require.NoError(t, tx.Commit(), "flushing into dropped namespaces must not fail the commit")
	})

	t.Run("reopen after close in same tx keeps both writes searchable", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "docs")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Kind: IndexKindFulltext, Fields: []string{"body"}}))

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":"a","body":"needleterm here"}`)))
		require.NoError(t, coll.Close())
		collB, err := fx.OpenCollection(tx.Context(), "docs")
		require.NoError(t, err)
		require.NoError(t, collB.EnsureIndex(tx.Context(), IndexInfo{Kind: IndexKindFulltext, Fields: []string{"body"}}))
		require.NoError(t, collB.Insert(tx.Context(), anyenc.MustParseJson(`{"id":"b","body":"otherterm here"}`)))
		require.NoError(t, tx.Commit())

		count, err := collB.Find(`{"$text":{"$search":"needleterm"}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "orphaned buffer must flush")
		count, err = collB.Find(`{"$text":{"$search":"otherterm"}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "reopened collection's buffer must flush")
	})
}

// A $text clause names no index. So if a collection carried two full-text
// indexes, detectFtsQuery's `fxs[0]` would search whichever one came first in
// the collection's slice — creation order in the session that created them,
// catalog (name) order after a reopen. The same query then returned different
// rows before and after a restart, and a term that WAS indexed could come back
// empty with no error:
//
//	LIVE      order=[zbody atitle]   $text "alpha" -> 0    $text "beta" -> 50
//	REOPENED  order=[atitle zbody]   $text "alpha" -> 50   $text "beta" -> 0
//
// The ambiguity is now unrepresentable: a second full-text index is rejected at
// creation. Multiple fields belong in ONE full-text index, with per-field
// Weights — which is what BM25F is for.

func TestFts_SecondIndexRejected(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "zbody", Fields: []string{"body"}, Kind: IndexKindFulltext}))

	err = coll.EnsureIndex(ctx, IndexInfo{
		Name: "atitle", Fields: []string{"title"}, Kind: IndexKindFulltext})
	require.ErrorIs(t, err, ErrMultipleFulltextIndexes)

	// Rejected, not half-created: the collection still has exactly one.
	var fts []string
	for _, idx := range coll.GetIndexes() {
		if idx.Info().Kind == IndexKindFulltext {
			fts = append(fts, idx.Info().Name)
		}
	}
	assert.Equal(t, []string{"zbody"}, fts)
}

// Two full-text indexes in a single EnsureIndex call are rejected too — the
// second is not yet published to the collection when the first is created.
func TestFts_SecondIndexRejectedInSameBatch(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)

	err = coll.EnsureIndex(ctx,
		IndexInfo{Name: "zbody", Fields: []string{"body"}, Kind: IndexKindFulltext},
		IndexInfo{Name: "atitle", Fields: []string{"title"}, Kind: IndexKindFulltext})
	require.ErrorIs(t, err, ErrMultipleFulltextIndexes)
}

// The guard must not break the idempotent and conflicting redefinition paths,
// which are about ONE index, not two.
func TestFts_SameNameRedefinitionUnaffected(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)

	info := IndexInfo{Name: "ft", Fields: []string{"title", "body"}, Kind: IndexKindFulltext}
	require.NoError(t, coll.EnsureIndex(ctx, info))
	// Same name, same definition: idempotent.
	require.NoError(t, coll.EnsureIndex(ctx, info))
	// Same name, different definition: still a mismatch, not an ambiguity.
	err = coll.EnsureIndex(ctx, IndexInfo{
		Name: "ft", Fields: []string{"title"}, Kind: IndexKindFulltext})
	require.ErrorIs(t, err, ErrIndexMismatch)

	// Dropping frees the slot.
	require.NoError(t, coll.DropIndex(ctx, "ft"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "other", Fields: []string{"body"}, Kind: IndexKindFulltext}))
}

// One full-text index over several fields is the supported way to search more
// than one field, and it finds terms from every field it covers.
func TestFts_OneIndexCoversSeveralFields(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "ft", Fields: []string{"title", "body"}, Kind: IndexKindFulltext}))
	for i := range 50 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"title":"alpha doc %d","body":"beta text %d"}`, i, i, i))))
	}

	for _, search := range []string{"alpha", "beta"} {
		count, err := coll.Find(fmt.Sprintf(`{"$text":{"$search":%q}}`, search)).Count(ctx)
		require.NoError(t, err)
		assert.Equalf(t, 50, count, "$text %q", search)
	}
}

// A database written before the guard can still carry two full-text indexes.
// Opening it must keep working, but a $text query must refuse rather than
// silently search whichever index load order puts first.
func TestFts_LegacyTwoIndexesRefuseQuery(t *testing.T) {
	skipIfInMemory(t, "a legacy on-disk database is written and reopened")
	tmpDir, err := os.MkdirTemp("", "fts-legacy-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "zbody", Fields: []string{"body"}, Kind: IndexKindFulltext}))
	// Bypass the DDL guard to forge the pre-guard on-disk state.
	require.NoError(t, forceSecondFtsIndex(t, coll))
	require.NoError(t, fx1.Close())

	fx2 := newFixturePath(t, tmpDir)
	coll2, err := fx2.OpenCollection(ctx, "docs")
	require.NoError(t, err)

	_, err = coll2.Find(`{"$text":{"$search":"alpha"}}`).Count(ctx)
	require.ErrorIs(t, err, ErrMultipleFulltextIndexes)

	// Non-$text queries on such a collection keep working.
	count, err := coll2.Find(`{}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

// forceSecondFtsIndex creates a second full-text index bypassing
// checkSingleFulltextIndex, to reproduce the on-disk state a database written
// before that guard can have. Test-only.
func forceSecondFtsIndex(t *testing.T, coll Collection) error {
	t.Helper()
	c := coll.(*collection)
	return c.db.doWriteTxW(ctx, func(wtx WriteTx, tx *btree.WriteTx) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		fx, err := c.createFtsIndex(ctx, tx, IndexInfo{
			Name: "atitle", Fields: []string{"title"}, Kind: IndexKindFulltext})
		if err != nil {
			return err
		}
		c.storeFtsIndexes(append(c.loadFtsIndexes(), fx))
		return nil
	})
}

func TestFtsBM25_ParamsSurviveReopen(t *testing.T) {
	tmpDir := t.TempDir()
	func() {
		fxLocal := newFixturePath(t, tmpDir)
		coll, err := fxLocal.CreateCollection(ctx, "docs")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
			Kind: IndexKindFulltext, Fields: []string{"body"}, Fulltext: &FulltextParams{B: 0.33, K1: 1.7},
		}))
		insertJSON(t, coll, `{"id":"a","body":"alpha beta"}`)
		require.NoError(t, fxLocal.Close())
	}()

	db, err := Open(ctx, tmpDir+"/any-store-test.db", nil)
	require.NoError(t, err)
	defer db.Close()
	collI, err := db.OpenCollection(ctx, "docs")
	require.NoError(t, err)
	c := collI.(*collection)

	fxs := c.loadFtsIndexes()
	require.Len(t, fxs, 1)
	assert.Equal(t, 0.33, fxs[0].info.Fulltext.B)
	assert.Equal(t, 1.7, fxs[0].info.Fulltext.K1)
}

func TestFtsWeights_SurviveReopen(t *testing.T) {
	tmpDir := t.TempDir()
	func() {
		fxLocal := newFixturePath(t, tmpDir)
		coll, err := fxLocal.CreateCollection(ctx, "docs")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
			Kind: IndexKindFulltext, Fields: []string{"title", "body"},
			Fulltext: &FulltextParams{Weights: map[string]float64{"title": 4, "body": 1}},
		}))
		insertJSON(t, coll, `{"id":"a","title":"alpha","body":"beta"}`)
		require.NoError(t, fxLocal.Close())
	}()

	db, err := Open(ctx, tmpDir+"/any-store-test.db", nil)
	require.NoError(t, err)
	defer db.Close()
	collI, err := db.OpenCollection(ctx, "docs")
	require.NoError(t, err)
	c := collI.(*collection)
	fxs := c.loadFtsIndexes()
	require.Len(t, fxs, 1)
	assert.Equal(t, 4.0, fxs[0].info.Fulltext.Weights["title"])
	assert.Equal(t, 1.0, fxs[0].info.Fulltext.Weights["body"])
}

func ftsPipelineColl(t *testing.T) (*fixture, Collection) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "p")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Kind: IndexKindFulltext, Fields: []string{"body"}}))
	insertJSON(t, coll,
		`{"id":"a","body":"london crash report","year":1920,"status":"open"}`,
		`{"id":"b","body":"london fog landing","year":1937,"status":"closed"}`,
		`{"id":"c","body":"london london tower","year":1950,"status":"open"}`,
		`{"id":"d","body":"paris sunshine","year":1960,"status":"open"}`,
	)
	return fx, coll
}

// collectIter runs a query and returns (ids, scores) in result order.
func collectIter(t *testing.T, q Query) ([]string, []float64) {
	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var ids []string
	var scores []float64
	for iter.Next() {
		doc, derr := iter.Doc()
		require.NoError(t, derr)
		ids = append(ids, doc.Value().GetString("id"))
		scores = append(scores, iter.Score())
	}
	require.NoError(t, iter.Err())
	return ids, scores
}

// A concurrent read tx's staleness pass must not rebuild a collection's index
// sets while a local write tx has uncommitted index DDL published in them —
// its older snapshot would evict the writer's uncommitted indexes. The
// indexSetDDLTxs counter carries that in-flight state.

func TestIndexSetDDLTxs_BalancedAcrossCommitAndRollback(t *testing.T) {
	fx := newFixture(t)
	collIface, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	coll := collIface.(*collection)

	ddlTxs := func() int {
		coll.mu.Lock()
		defer coll.mu.Unlock()
		return coll.indexSetDDLTxs
	}
	require.Zero(t, ddlTxs())

	// Committed DDL: counter is 1 while the tx is open, 0 after commit.
	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(tx.Context(), IndexInfo{Fields: []string{"a"}, Kind: IndexKindFulltext}))
	assert.Equal(t, 1, ddlTxs(), "uncommitted DDL must be marked in flight")
	require.NoError(t, tx.Commit())
	assert.Zero(t, ddlTxs(), "commit must release the in-flight marker")

	// Rolled-back DDL: same lifecycle through the undo path.
	tx2, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(tx2.Context(), IndexInfo{Fields: []string{"b"}}))
	assert.Equal(t, 1, ddlTxs())
	require.NoError(t, tx2.Rollback())
	assert.Zero(t, ddlTxs(), "rollback must release the in-flight marker")
}

func TestReconcileSkipsWhileLocalIndexDDLInFlight(t *testing.T) {
	fx := newFixture(t)
	collIface, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	coll := collIface.(*collection)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"text"}, Kind: IndexKindFulltext}))

	// Plant a ghost fts handle that no on-disk catalog entry backs — the
	// stand-in for a writer's just-published, not-yet-committed index as seen
	// by a reconcile running on an older snapshot.
	ghost := &ftsIndex{c: coll, info: IndexInfo{Name: "ghost", Kind: IndexKindFulltext, Fields: []string{"g"}}}
	coll.mu.Lock()
	coll.storeFtsIndexes(append(coll.loadFtsIndexes(), ghost))
	coll.indexSetDDLTxs++
	coll.mu.Unlock()

	inSnapshot := func() bool {
		for _, fxi := range coll.loadFtsIndexes() {
			if fxi == ghost {
				return true
			}
		}
		return false
	}

	// With DDL in flight, reconcile must leave the sets untouched.
	require.NoError(t, fx.DB.(*db).doReadTx(ctx, func(tx *btree.ReadTx) error {
		coll.reconcileIndexes(tx)
		return nil
	}))
	assert.True(t, inSnapshot(), "reconcile evicted an index while local DDL was in flight")

	// Once the writer resolved, the same reconcile evicts the ghost by
	// omission (it has no catalog entry in the snapshot).
	coll.mu.Lock()
	coll.indexSetDDLTxs--
	coll.mu.Unlock()
	require.NoError(t, fx.DB.(*db).doReadTx(ctx, func(tx *btree.ReadTx) error {
		coll.reconcileIndexes(tx)
		return nil
	}))
	assert.False(t, inSnapshot(), "reconcile must evict a catalog-less handle once no DDL is in flight")
}
