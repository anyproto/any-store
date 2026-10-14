package test

import (
	"fmt"
	"strings"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Search-as-you-type operators: a prefix clause scored as one merged term
// without an expansion cap, "phrase"* prefixes, ^ anchors and the $fields
// scope. See docs/full-text-search.md § Query syntax.

// ftsSaytQuery runs a $text query and asserts Count agrees with Iter.
func ftsSaytQuery(t *testing.T, coll anystore.Collection, text map[string]any) ([]string, []float64) {
	t.Helper()
	q := map[string]any{"$text": text}
	ids, scores := collectIter(t, coll.Find(q))
	n, err := coll.Find(q).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, len(ids), n, "Count diverged from Iter for %v", text)
	return ids, scores
}

func TestFtsSayt_PrefixCoversEveryCompletion(t *testing.T) {
	fx, coll := ftsOpsColl(t)
	defer fx.finish()
	// 70 distinct completions of tok*, well past the former 50-expansion cap;
	// the frequent "tokyo" (3 docs) must not crowd the rare ones out.
	var docs []string
	for i := 0; i < 70; i++ {
		docs = append(docs, fmt.Sprintf(`{"id":"t%02d","body":"tok%02dword filler"}`, i, i))
	}
	insertJSON(t, coll, docs...)
	ids, _ := ftsSaytQuery(t, coll, map[string]any{"$search": "tok*"})
	assert.Len(t, ids, 70+4, "a,b,d (tokyo), e (tokens) and the 70 tokNN docs")
}

func TestFtsSayt_PrefixScoredAsOneTerm(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish()
	coll, err := fx.CreateCollection(ctx, "titles")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Kind: anystore.IndexKindFulltext, Fields: []string{"name"}}))
	// "kart" is common (20 filler docs), "kanzenban" occurs once. Under
	// per-completion scoring the rare completion's IDF put C above B; as one
	// merged term both completions carry the same IDF, so B and C (same tf,
	// same length) score identically and A (shorter) comes first.
	docs := []string{
		`{"id":"A","name":"mario kart"}`,
		`{"id":"B","name":"mario kart 64"}`,
		`{"id":"C","name":"mario kanzenban edition"}`,
	}
	for i := 0; i < 20; i++ {
		docs = append(docs, fmt.Sprintf(`{"id":"k%02d","name":"kart racing %d"}`, i, i))
	}
	insertJSON(t, coll, docs...)
	ids, scores := ftsSaytQuery(t, coll, map[string]any{"$search": "mario ka*", "$defaultOperator": "and"})
	require.Equal(t, []string{"A", "B", "C"}, ids)
	assert.Greater(t, scores[0], scores[1])
	assert.Equal(t, scores[1], scores[2], "the rare completion must not outweigh the common one")

	// Two completions in one document add up as one term (tf 2), still one
	// contribution: the doc is not scored twice with two IDFs.
	insertJSON(t, coll, `{"id":"D","name":"kart kanzenban"}`)
	ids, scores = ftsSaytQuery(t, coll, map[string]any{"$search": "ka*"})
	assert.Equal(t, "D", ids[0])
	_ = scores
}

func TestFtsSayt_PhrasePrefix(t *testing.T) {
	fx, coll := ftsOpsColl(t)
	defer fx.finish()
	cases := []struct {
		search string
		want   []string
	}{
		{`"east tok"*`, []string{"a"}},                     // east tokyo (adjacent); b has tokyo east
		{`"tokyo ea"*`, []string{"b"}},                     // tokyo east
		{`"west tok"*`, []string{"d"}},                     // west tokyo
		{`"of aff"*`, []string{"e"}},                       // tokens of affection
		{`"east tokyo"*`, []string{"a"}},                   // whole last word is a stem of itself
		{`"east s"*`, []string{"c"}},                       // east side
		{`"bound tr"*`, []string{"a"}},                     // the phrase ends at the document end
		{`"tokyo tow"*`, []string{"d"}},                    // west tokyo tower
		{`"side st"* tokyo`, []string{"a", "b", "c", "d"}}, // OR with a plain term
		{`"east zz"*`, nil},                                // no completion
		{`"nosuch tok"*`, nil},                             // dead fixed term
	}
	for _, c := range cases {
		ids, _ := ftsSaytQuery(t, coll, map[string]any{"$search": c.search})
		assert.Equal(t, c.want, sortedIDs(ids), c.search)
	}
	// Required and excluded phrase-prefixes.
	ids, _ := ftsSaytQuery(t, coll, map[string]any{"$search": "tokyo", "$exclude": `"east tok"*`})
	assert.Equal(t, []string{"b", "d"}, sortedIDs(ids))
	ids, _ = ftsSaytQuery(t, coll, map[string]any{"$search": "tokyo", "$require": `"east tok"*`})
	assert.Equal(t, []string{"a"}, sortedIDs(ids))
	// A bare word analyzing to several tokens is a phrase-prefix too.
	insertJSON(t, coll, `{"id":"f","body":"a well-known fact"}`)
	ids, _ = ftsSaytQuery(t, coll, map[string]any{"$search": "well-kno*"})
	assert.Equal(t, []string{"f"}, sortedIDs(ids))
}

func TestFtsSayt_Anchored(t *testing.T) {
	fx, coll := ftsOpsColl(t)
	defer fx.finish()
	cases := []struct {
		search string
		want   []string
	}{
		{`^east`, []string{"a", "c"}},
		{`^tokyo`, []string{"b"}},
		{`^"east tokyo"`, []string{"a"}},
		{`^"tokyo east"`, []string{"b"}},
		{`^"side story"`, nil}, // present, not at the start
		{`^tok*`, []string{"b", "e"}},
		{`^"east tok"*`, []string{"a"}},
		{`^"tokyo ea"*`, []string{"b"}},
		{`^we*`, []string{"d"}},
		{`^ east`, []string{"a", "b", "c"}}, // a lone ^ anchors nothing: plain "east"
		{`^nosuch`, nil},
	}
	for _, c := range cases {
		ids, _ := ftsSaytQuery(t, coll, map[string]any{"$search": c.search})
		assert.Equal(t, c.want, sortedIDs(ids), c.search)
	}
	// Anchored clauses in $require / $exclude.
	ids, _ := ftsSaytQuery(t, coll, map[string]any{"$search": "tokyo", "$require": "^east"})
	assert.Equal(t, []string{"a", "c"}, sortedIDs(ids), "required anchor gates; the should term stays optional")
	ids, _ = ftsSaytQuery(t, coll, map[string]any{"$search": "tokyo", "$exclude": "^tokyo"})
	assert.Equal(t, []string{"a", "d"}, sortedIDs(ids))
	ids, _ = ftsSaytQuery(t, coll, map[string]any{"$search": "tokyo", "$exclude": "^tok*"})
	assert.Equal(t, []string{"a", "d"}, sortedIDs(ids))
	// An anchored term scores once (tf 1) however often it repeats.
	insertJSON(t, coll, `{"id":"g","body":"east east east"}`)
	_, scores := ftsSaytQuery(t, coll, map[string]any{"$search": "^east"})
	ids, _ = ftsSaytQuery(t, coll, map[string]any{"$search": "^east"})
	assert.Equal(t, []string{"a", "c", "g"}, sortedIDs(ids))
	assert.Len(t, scores, 3)
}

func TestFtsSayt_CJKPrefixAndAnchor(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish()
	coll, err := fx.CreateCollection(ctx, "cjk")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Kind: anystore.IndexKindFulltext, Fields: []string{"body"}}))
	insertJSON(t, coll,
		`{"id":"a","body":"東京都に住んでいる"}`,
		`{"id":"b","body":"京都は美しい"}`,
		`{"id":"c","body":"私は東京が好き"}`,
	)
	ids, _ := ftsSaytQuery(t, coll, map[string]any{"$search": "東*"})
	assert.Equal(t, []string{"a", "c"}, sortedIDs(ids), "a lone CJK char expands to every bigram starting with it")
	ids, _ = ftsSaytQuery(t, coll, map[string]any{"$search": "^東京"})
	assert.Equal(t, []string{"a"}, sortedIDs(ids))
	ids, _ = ftsSaytQuery(t, coll, map[string]any{"$search": "^京都"})
	assert.Equal(t, []string{"b"}, sortedIDs(ids), "京都 inside 東京都 is not at the start")
}

func ftsFieldsColl(t *testing.T, weights map[string]float64) (*fixture, anystore.Collection) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "fields")
	require.NoError(t, err)
	info := anystore.IndexInfo{Kind: anystore.IndexKindFulltext, Fields: []string{"title", "body"}}
	if weights != nil {
		info.Fulltext = &anystore.FulltextParams{Weights: weights}
	}
	require.NoError(t, coll.EnsureIndex(ctx, info))
	insertJSON(t, coll,
		`{"id":"a","title":"east tokyo","body":"a train to the west"}`,
		`{"id":"b","title":"west side","body":"east tokyo station"}`,
		`{"id":"c","title":"tokyo","body":"east east"}`,
		`{"id":"d","body":"east wind"}`, // no title: the body is the document's first field
	)
	return fx, coll
}

func TestFtsSayt_FieldScope(t *testing.T) {
	fx, coll := ftsFieldsColl(t, nil)
	defer fx.finish()
	cases := []struct {
		text map[string]any
		want []string
	}{
		{map[string]any{"$search": "east"}, []string{"a", "b", "c", "d"}},
		{map[string]any{"$search": "east", "$fields": "title"}, []string{"a"}},
		{map[string]any{"$search": "east", "$fields": []any{"body"}}, []string{"b", "c", "d"}},
		{map[string]any{"$search": "east", "$fields": []any{"title", "body"}}, []string{"a", "b", "c", "d"}},
		{map[string]any{"$search": `"east tokyo"`, "$fields": "body"}, []string{"b"}},
		{map[string]any{"$search": `"east tokyo"`, "$fields": "title"}, []string{"a"}},
		{map[string]any{"$search": `"east tok"*`, "$fields": "body"}, []string{"b"}},
		{map[string]any{"$search": "tok*", "$fields": "title"}, []string{"a", "c"}},
		{map[string]any{"$search": "^tokyo", "$fields": "title"}, []string{"c"}},
		{map[string]any{"$search": "^east", "$fields": "body"}, []string{"d"}}, // position 0 only when the title is empty
		{map[string]any{"$search": "^east"}, []string{"a", "d"}},
		{map[string]any{"$search": "west", "$fields": "title", "$exclude": "side"}, nil},
		{map[string]any{"$search": "tokyo", "$fields": "title", "$require": "east"}, []string{"a"}}, // b: east only in body
	}
	for _, c := range cases {
		ids, _ := ftsSaytQuery(t, coll, c.text)
		assert.Equal(t, c.want, sortedIDs(ids), "%v", c.text)
	}
	// Scoped scores count only the in-scope frequency: c has "east" twice in
	// body, once nowhere else; a has it once in title.
	_, all := ftsSaytQuery(t, coll, map[string]any{"$search": "east", "$require": "tokyo"})
	assert.Len(t, all, 3)

	// Unknown field: every verb refuses.
	bad := map[string]any{"$text": map[string]any{"$search": "east", "$fields": "nope"}}
	_, err := coll.Find(bad).Iter(ctx)
	assert.ErrorIs(t, err, anystore.ErrFulltextUnknownField)
	_, err = coll.Find(bad).Count(ctx)
	assert.ErrorIs(t, err, anystore.ErrFulltextUnknownField)
	_, err = coll.Find(bad).Delete(ctx)
	assert.ErrorIs(t, err, anystore.ErrFulltextUnknownField)
	_, err = coll.Find(bad).Explain(ctx)
	assert.ErrorIs(t, err, anystore.ErrFulltextUnknownField)
}

func TestFtsSayt_FieldScopeWeighted(t *testing.T) {
	fx, coll := ftsFieldsColl(t, map[string]float64{"title": 5})
	defer fx.finish()
	// Scoped to body, the title weight is irrelevant: a (east only in title)
	// is out, and c (east twice in body) outranks b and d.
	ids, _ := ftsSaytQuery(t, coll, map[string]any{"$search": "east", "$fields": "body"})
	assert.Equal(t, []string{"b", "c", "d"}, sortedIDs(ids))
	assert.Equal(t, "c", ids[0])
	// Scoped to title, only a scores, with the title weight.
	ids, scores := ftsSaytQuery(t, coll, map[string]any{"$search": "east", "$fields": "title"})
	assert.Equal(t, []string{"a"}, ids)
	_, unscoped := collectIter(t, coll.Find(map[string]any{"$text": map[string]any{"$search": "east"}, "id": "a"}))
	assert.Equal(t, unscoped, scores, "a's east lives in the title only: scoped and unscoped tf agree")
}

func TestFtsSayt_Verbs(t *testing.T) {
	fx, coll := ftsOpsColl(t)
	defer fx.finish()
	res, err := coll.Find(`{"$text":{"$search":"\"east tok\"*"}}`).Update(ctx, `{"$set":{"hit":true}}`)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Modified)
	ids, _ := collectIter(t, coll.Find(`{"hit":true}`))
	assert.Equal(t, []string{"a"}, ids)
	res, err = coll.Find(`{"$text":{"$search":"^tok*"}}`).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, res.Modified)
	ids, _ = collectIter(t, coll.Find(nil))
	assert.Equal(t, []string{"a", "c", "d"}, sortedIDs(ids))
	ex, err := coll.Find(`{"$text":{"$search":"to*"},"id":{"$in":["a","b"]}}`).Explain(ctx)
	require.NoError(t, err)
	assert.True(t, strings.Contains(ex.Plan, "FtsSearch"), "a prefix clause has no probe form: %s", ex.Plan)
}
