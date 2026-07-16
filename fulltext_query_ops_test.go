package anystore

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// These tests exercise the Phase 1 query operators added on top of the v1
// bag-of-words BM25: phrases, required (AND) / $defaultOperator, negation,
// prefix expansion, and CJK implicit-phrase matching. See docs/fts/FTS-V2-PLAN.md.

func ftsOpsColl(t *testing.T) (*fixture, Collection) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "ops")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Kind: IndexKindFulltext, Fields: []string{"body"}}))
	insertJSON(t, coll,
		`{"id":"a","body":"east tokyo bound train"}`, // east tokyo (adjacent, in order)
		`{"id":"b","body":"tokyo east district"}`,    // both words, reversed → not the phrase
		`{"id":"c","body":"east side story"}`,        // east only
		`{"id":"d","body":"west tokyo tower"}`,       // tokyo only
		`{"id":"e","body":"tokens of affection"}`,    // tok* completion, no tokyo/east
	)
	return fx, coll
}

func sortedIDs(ids []string) []string {
	out := append([]string(nil), ids...)
	sort.Strings(out)
	return out
}

func TestFtsOps_Phrase(t *testing.T) {
	fx, coll := ftsOpsColl(t)
	defer fx.finish()
	// "east tokyo" as a phrase: only a has them adjacent and in order. b has both
	// words but reversed; c/d have only one.
	ids, _ := collectIter(t, coll.Find(`{"$text":{"$search":"\"east tokyo\""}}`))
	assert.Equal(t, []string{"a"}, ids)
}

func TestFtsOps_BagOfWordsStillOR(t *testing.T) {
	fx, coll := ftsOpsColl(t)
	defer fx.finish()
	// Unquoted: default OR. east∪tokyo → a,b,c,d (not e).
	ids, _ := collectIter(t, coll.Find(`{"$text":{"$search":"east tokyo"}}`))
	assert.Equal(t, []string{"a", "b", "c", "d"}, sortedIDs(ids))
}

func TestFtsOps_RequiredAnd(t *testing.T) {
	fx, coll := ftsOpsColl(t)
	defer fx.finish()
	// $require both → a,b (both contain east and tokyo, regardless of order).
	ids, _ := collectIter(t, coll.Find(map[string]any{
		"$text": map[string]any{"$search": "", "$require": []any{"east", "tokyo"}},
	}))
	assert.Equal(t, []string{"a", "b"}, sortedIDs(ids))
}

func TestFtsOps_DefaultOperatorAnd(t *testing.T) {
	fx, coll := ftsOpsColl(t)
	defer fx.finish()
	// $defaultOperator:"and" makes bare $search terms required → a,b.
	ids, _ := collectIter(t, coll.Find(map[string]any{
		"$text": map[string]any{"$search": "east tokyo", "$defaultOperator": "and"},
	}))
	assert.Equal(t, []string{"a", "b"}, sortedIDs(ids))
}

func TestFtsOps_Exclude(t *testing.T) {
	fx, coll := ftsOpsColl(t)
	defer fx.finish()
	// search east, $exclude tokyo: east docs (a,b,c) minus tokyo docs (a,b) → c.
	ids, _ := collectIter(t, coll.Find(map[string]any{
		"$text": map[string]any{"$search": "east", "$exclude": "tokyo"},
	}))
	assert.Equal(t, []string{"c"}, ids)
}

func TestFtsOps_LeadingDashIsLiteral(t *testing.T) {
	fx, coll := ftsOpsColl(t)
	defer fx.finish()
	// "-tokyo" in $search is NOT an exclusion — it is the term "tokyo" (the dash
	// is dropped by the analyzer). So it matches tokyo docs, not excludes them.
	ids, _ := collectIter(t, coll.Find(`{"$text":{"$search":"-tokyo"}}`))
	assert.Equal(t, []string{"a", "b", "d"}, sortedIDs(ids))
}

func TestFtsOps_PrefixExpansion(t *testing.T) {
	fx, coll := ftsOpsColl(t)
	defer fx.finish()
	// tok*: expands to "tokyo" (a,b,d) and "tokens" (e).
	ids, _ := collectIter(t, coll.Find(`{"$text":{"$search":"tok*"}}`))
	assert.Equal(t, []string{"a", "b", "d", "e"}, sortedIDs(ids))
}

func TestFtsOps_PrefixRequiredCombo(t *testing.T) {
	fx, coll := ftsOpsColl(t)
	defer fx.finish()
	// $require east, plus a tok* should-term in $search. east docs = a,b,c; east
	// is required (tok* is optional), so a,b,c — scoring favours a,b (also tok*).
	ids, _ := collectIter(t, coll.Find(map[string]any{
		"$text": map[string]any{"$search": "tok*", "$require": "east"},
	}))
	assert.Equal(t, []string{"a", "b", "c"}, sortedIDs(ids))
}

func TestFtsOps_OnlyExcludeMatchesNothing(t *testing.T) {
	fx, coll := ftsOpsColl(t)
	defer fx.finish()
	// An exclude with no positive source → empty (no docs to keep).
	ids, _ := collectIter(t, coll.Find(map[string]any{
		"$text": map[string]any{"$search": "", "$exclude": "tokyo"},
	}))
	assert.Empty(t, ids)
}

func ftsCJKColl(t *testing.T) (*fixture, Collection) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "cjk")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Kind: IndexKindFulltext, Fields: []string{"body"}}))
	insertJSON(t, coll,
		`{"id":"j","body":"東京都"}`,  // bigrams 東京, 京都 (adjacent, in order)
		`{"id":"k","body":"京都東京"}`, // bigrams 京都, 都東, 東京 — has 東京 and 京都 but NOT adjacent as 東京→京都
	)
	return fx, coll
}

func TestFtsOps_CJKImplicitPhrase(t *testing.T) {
	fx, coll := ftsCJKColl(t)
	defer fx.finish()
	// "東京都" analyzes to the bigrams 東京,京都 and is matched as an implicit
	// phrase: only j has them at consecutive positions in order.
	ids, _ := collectIter(t, coll.Find(`{"$text":{"$search":"東京都"}}`))
	assert.Equal(t, []string{"j"}, ids)
}

func TestFtsOps_CJKSingleBigramOR(t *testing.T) {
	fx, coll := ftsCJKColl(t)
	defer fx.finish()
	// A single bigram "東京" is a plain term → both j and k contain it.
	ids, _ := collectIter(t, coll.Find(`{"$text":{"$search":"東京"}}`))
	assert.Equal(t, []string{"j", "k"}, sortedIDs(ids))
}

func TestFtsOps_PhraseSpansChunkBoundary(t *testing.T) {
	// A phrase must still match when the two terms' postings live in different
	// chunks for the same doc (exercises the termStream cross-chunk seek). We
	// can't easily force >128 docs cheaply here, so instead verify the phrase
	// merge over many docs that all contain the phrase.
	fx := newFixture(t)
	defer fx.finish()
	coll, err := fx.CreateCollection(ctx, "many")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Kind: IndexKindFulltext, Fields: []string{"body"}}))
	docs := make([]string, 0, 300)
	for i := 0; i < 300; i++ {
		// alternate: half contain the phrase, half contain only reversed words
		if i%2 == 0 {
			docs = append(docs, `{"id":"p`+itoa(i)+`","body":"alpha beta gamma"}`)
		} else {
			docs = append(docs, `{"id":"q`+itoa(i)+`","body":"beta alpha gamma"}`)
		}
	}
	insertJSON(t, coll, docs...)
	ids, _ := collectIter(t, coll.Find(`{"$text":{"$search":"\"alpha beta\""}}`))
	require.Len(t, ids, 150)
	for _, id := range ids {
		assert.Equal(t, byte('p'), id[0], "only p-docs have the adjacent phrase")
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [12]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}

func TestFtsPhrase_NoMatchAcrossArrayElements(t *testing.T) {
	// Array elements sit on separate position runs. An element with >= ftsPosGap
	// tokens used to overlap the next element's range (base advanced by a fixed
	// gap), producing false cross-element phrase matches and non-ascending
	// per-term position lists.
	fx, coll := ftsTestColl(t, "tags")
	defer fx.finish()

	words := make([]string, 150)
	for i := range words {
		words[i] = fmt.Sprintf("w%03d", i)
	}
	insertJSON(t, coll, `{"id":"d1","tags":["`+strings.Join(words, " ")+`", "aaa bbb"]}`)

	// "w100 bbb" is not a phrase anywhere in the doc.
	ids, _ := collectIter(t, coll.Find(`{"$text":{"$search":"\"w100 bbb\""}}`))
	assert.Empty(t, ids)

	// A phrase within one element still matches.
	ids, _ = collectIter(t, coll.Find(`{"$text":{"$search":"\"w100 w101\""}}`))
	assert.Equal(t, []string{"d1"}, ids)
	ids, _ = collectIter(t, coll.Find(`{"$text":{"$search":"\"aaa bbb\""}}`))
	assert.Equal(t, []string{"d1"}, ids)
}

// Count must denote the same document set as Iter on the identical $text
// query — including under Limit/Offset and with a residual predicate. Count
// compiles a native FTS plan (no score sidecar, no public iterator); these
// pin that it cannot drift from the read path.
func TestFtsCount_MatchesIterAcrossWindows(t *testing.T) {
	fx, coll := ftsOpsColl(t)
	defer fx.finish()

	for name, q := range map[string]func() Query{
		"plain":         func() Query { return coll.Find(`{"$text":{"$search":"tokyo"}}`) },
		"limit":         func() Query { return coll.Find(`{"$text":{"$search":"tokyo"}}`).Limit(1) },
		"offset":        func() Query { return coll.Find(`{"$text":{"$search":"tokyo"}}`).Offset(1) },
		"limit_offset":  func() Query { return coll.Find(`{"$text":{"$search":"tokyo"}}`).Limit(2).Offset(1) },
		"residual":      func() Query { return coll.Find(`{"$text":{"$search":"tokyo"},"id":{"$in":["a","d"]}}`) },
		"match_nothing": func() Query { return coll.Find(`{"$text":{"$search":"zzznope"}}`) },
	} {
		t.Run(name, func(t *testing.T) {
			ids, _ := collectIter(t, q())
			count, err := q().Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, len(ids), count, "Count and Iter must agree; iter=%v", ids)
		})
	}
}


// $text inside an open write tx must see the tx's own uncommitted full-text
// writes — the same view on every verb — and a rollback must discard them.
// Previously Count/Iter matched against stale committed postings while
// Update/Delete (whose savepoint path flushes the pending buffer) matched
// current ones: one predicate, contradictory answers within one tx.
func TestFtsReadYourWrites_InsideWriteTx(t *testing.T) {
	fx, coll := ftsOpsColl(t)
	defer fx.finish()
	const q = `{"$text":{"$search":"zanzibar"}}`

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":"z1","body":"zanzibar ferry"}`)))

	// In-tx Iter sees the tx's own write.
	iter, err := coll.Find(q).Iter(tx.Context())
	require.NoError(t, err)
	var ids []string
	for iter.Next() {
		doc, derr := iter.Doc()
		require.NoError(t, derr)
		ids = append(ids, doc.Value().GetString("id"))
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	assert.Equal(t, []string{"z1"}, ids, "in-tx $text Iter must see the tx's own write")

	// In-tx Count agrees with in-tx Iter.
	count, err := coll.Find(q).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, count, "in-tx $text Count must agree with Iter")

	require.NoError(t, tx.Rollback())

	// Rollback discards the pending postings entirely.
	after, _ := collectIter(t, coll.Find(q))
	assert.Empty(t, after, "rollback must discard the pending postings")
	count, err = coll.Find(q).Count(ctx)
	require.NoError(t, err)
	assert.Zero(t, count)
}
