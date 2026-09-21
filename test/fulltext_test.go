package test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests exercise the Phase 1 query operators added on top of the v1
// bag-of-words BM25: phrases, required (AND) / $defaultOperator, negation,
// prefix expansion, and CJK implicit-phrase matching. See any-store-tests:docs/any-store/fts/FTS-V2-PLAN.md.

func ftsOpsColl(t *testing.T) (*fixture, anystore.Collection) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "ops")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Kind: anystore.IndexKindFulltext, Fields: []string{"body"}}))
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

func ftsCJKColl(t *testing.T) (*fixture, anystore.Collection) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "cjk")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Kind: anystore.IndexKindFulltext, Fields: []string{"body"}}))
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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Kind: anystore.IndexKindFulltext, Fields: []string{"body"}}))
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

	for name, q := range map[string]func() anystore.Query{
		"plain":         func() anystore.Query { return coll.Find(`{"$text":{"$search":"tokyo"}}`) },
		"limit":         func() anystore.Query { return coll.Find(`{"$text":{"$search":"tokyo"}}`).Limit(1) },
		"offset":        func() anystore.Query { return coll.Find(`{"$text":{"$search":"tokyo"}}`).Offset(1) },
		"limit_offset":  func() anystore.Query { return coll.Find(`{"$text":{"$search":"tokyo"}}`).Limit(2).Offset(1) },
		"residual":      func() anystore.Query { return coll.Find(`{"$text":{"$search":"tokyo"},"id":{"$in":["a","d"]}}`) },
		"match_nothing": func() anystore.Query { return coll.Find(`{"$text":{"$search":"zzznope"}}`) },
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

// A $text plan driven by a sparse index over a path through an array of
// objects: the index-order dedup must elect among the entries the sparse
// index wrote, never a missing leaf.
func TestFtsOps_SparseTraversedIndexKeepsDocs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		sparse bool
	}{{"sparse", true}, {"dense", false}} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t)
			coll, err := fx.CreateCollection(ctx, "ops")
			require.NoError(t, err)
			require.NoError(t, coll.EnsureIndex(ctx,
				anystore.IndexInfo{Kind: anystore.IndexKindFulltext, Fields: []string{"text"}},
				anystore.IndexInfo{Name: "a.b", Fields: []string{"a.b"}, Sparse: tc.sparse},
			))
			insertJSON(t, coll,
				`{"id":"d1","text":"alpha","a":[{"c":1},{"b":[1,2]}]}`,
				`{"id":"d2","text":"alpha","a":[{"b":[3,4]},{"c":2}]}`,
				`{"id":"d3","text":"alpha","a":[{"b":[5,6]},{"b":[7,8]}]}`,
			)
			filter := `{"$text":{"$search":"alpha"},"a.b":{"$size":2}}`
			hint := anystore.IndexHint{IndexName: "a.b", Boost: 1_000_000}
			for _, sort := range []string{"a.b", "-a.b"} {
				q := coll.Find(filter).IndexHint(hint).Sort(sort)
				assert.Equal(t, []string{"d1", "d2", "d3"}, sortedIDs(collectIdsString(t, q)), sort)
			}
			n, err := coll.Find(filter).IndexHint(hint).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, 3, n)
		})
	}
}

// The $text plan applies the same sparse-completeness gate: a presence
// predicate admits a sparse index as the text plan's driver, and the rows
// must match the same query with the index absent.
func TestFtsOps_SparsePresencePredicates(t *testing.T) {
	for _, filter := range []string{
		`{"$text":{"$search":"alpha"},"opt":{"$exists":true}}`,
		`{"$text":{"$search":"alpha"},"opt":{"$type":"null"}}`,
		`{"$text":{"$search":"alpha"},"opt":null}`,
		`{"$text":{"$search":"alpha"},"opt":{"$exists":false}}`,
	} {
		fx := newFixture(t)
		plain, err := fx.CreateCollection(ctx, "plain")
		require.NoError(t, err)
		sparse, err := fx.CreateCollection(ctx, "sparse")
		require.NoError(t, err)
		require.NoError(t, plain.EnsureIndex(ctx, anystore.IndexInfo{Kind: anystore.IndexKindFulltext, Fields: []string{"text"}}))
		require.NoError(t, sparse.EnsureIndex(ctx,
			anystore.IndexInfo{Kind: anystore.IndexKindFulltext, Fields: []string{"text"}},
			anystore.IndexInfo{Name: "opt", Fields: []string{"opt"}, Sparse: true},
		))
		var docs []string
		for i := range 200 {
			d := fmt.Sprintf(`{"id":"d%d","text":"alpha"}`, i)
			switch i % 40 {
			case 0:
				d = fmt.Sprintf(`{"id":"d%d","text":"alpha","opt":%d}`, i, i)
			case 1:
				d = fmt.Sprintf(`{"id":"d%d","text":"alpha","opt":null}`, i)
			}
			docs = append(docs, d)
		}
		insertJSON(t, plain, docs...)
		insertJSON(t, sparse, docs...)

		want := sortedIDs(collectIdsString(t, plain.Find(filter)))
		hint := anystore.IndexHint{IndexName: "opt", Boost: 1_000_000}
		for _, q := range []anystore.Query{sparse.Find(filter), sparse.Find(filter).IndexHint(hint)} {
			assert.Equal(t, want, sortedIDs(collectIdsString(t, q)), filter)
			n, err := q.Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, len(want), n, filter)
		}
	}
}

// A sorted $text query whose residual guarantees a sparse index's field probes
// from that index, priced from the index's own population, not the
// collection's.
func TestFtsOps_SparsePresenceSortedProbe(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx,
		anystore.IndexInfo{Kind: anystore.IndexKindFulltext, Fields: []string{"text"}},
		anystore.IndexInfo{Name: "p", Fields: []string{"p"}, Sparse: true},
	))
	var docs, want []string
	for i := range 2000 {
		d := fmt.Sprintf(`{"id":"d%04d","text":"alpha"}`, i)
		if i%50 == 0 {
			d = fmt.Sprintf(`{"id":"d%04d","text":"alpha","p":%d}`, i, i)
			want = append(want, fmt.Sprintf("d%04d", i))
		}
		docs = append(docs, d)
	}
	insertJSON(t, coll, docs...)

	q := coll.Find(`{"$text":{"$search":"alpha"},"p":{"$exists":true}}`).Sort("p")
	explain, err := q.Explain(ctx)
	require.NoError(t, err)
	assert.True(t, plannerIndexUsed(explain, "p"), explain.Sql)
	assert.Contains(t, explain.Plan, "FtsProbeSeek(p)")
	assert.Equal(t, want, collectIdsString(t, q))
}

// Without a sort, a $text query whose residual guarantees a sparse index's
// field probes the documents that index holds instead of scoring every text
// match; a Count of it fetches nothing.
func TestFtsOps_SparsePresenceDrivesProbe(t *testing.T) {
	fx := newFixture(t)
	plain, err := fx.CreateCollection(ctx, "plain")
	require.NoError(t, err)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, plain.EnsureIndex(ctx, anystore.IndexInfo{Kind: anystore.IndexKindFulltext, Fields: []string{"text"}}))
	require.NoError(t, coll.EnsureIndex(ctx,
		anystore.IndexInfo{Kind: anystore.IndexKindFulltext, Fields: []string{"text"}},
		anystore.IndexInfo{Name: "p", Fields: []string{"p"}, Sparse: true},
	))
	var docs []string
	for i := range 2000 {
		d := fmt.Sprintf(`{"id":"d%04d","text":"alpha"}`, i)
		switch i % 100 {
		case 0:
			d = fmt.Sprintf(`{"id":"d%04d","text":"alpha","p":[%d,null]}`, i, i)
		case 1:
			d = fmt.Sprintf(`{"id":"d%04d","text":"beta","p":null}`, i)
		}
		docs = append(docs, d)
	}
	insertJSON(t, plain, docs...)
	insertJSON(t, coll, docs...)

	for filter, probes := range map[string]bool{
		`{"$text":{"$search":"alpha"},"p":{"$exists":true}}`: true,
		`{"$text":{"$search":"alpha"},"p":{"$type":"null"}}`: true,
		// A term rarer than the field: the text driver is the cheaper source.
		`{"$text":{"$search":"beta"},"p":{"$exists":true}}`: false,
	} {
		explain, err := coll.Find(filter).Explain(ctx)
		require.NoError(t, err)
		assert.Equal(t, probes, plannerIndexUsed(explain, "p"), "%s: %s", filter, explain.Sql)

		want := sortedIDs(collectIdsString(t, plain.Find(filter)))
		assert.Equal(t, want, sortedIDs(collectIdsString(t, coll.Find(filter))), filter)
		n, err := coll.Find(filter).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, len(want), n, filter)
		for _, page := range [][2]uint{{1, 5}, {3, 0}} {
			wantN, err := plain.Find(filter).Offset(page[0]).Limit(page[1]).Count(ctx)
			require.NoError(t, err)
			gotN, err := coll.Find(filter).Offset(page[0]).Limit(page[1]).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, wantN, gotN, "%s offset %d limit %d", filter, page[0], page[1])
		}
	}

	_, err = coll.Find(`{"$text":{"$search":"alpha"},"p":{"$exists":true}}`).Count(ctx)
	require.NoError(t, err)
	qplannerEnableCounters(t)
	_, err = coll.Find(`{"$text":{"$search":"alpha"},"p":{"$exists":true}}`).Count(ctx)
	require.NoError(t, err)
	pc := qplannerSnapshot().Planner
	assert.Zero(t, pc.FetchNextCalls, "a presence-covered probe Count must not fetch documents")
}

// A compound sparse index whose fields share an array holds exactly the
// documents {$exists:true} matches on every field: a $text Count covered by
// presence agrees with the rows, one per document whatever its fan-out.
func TestFtsOps_SparsePresenceCountSharedArray(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx,
		anystore.IndexInfo{Kind: anystore.IndexKindFulltext, Fields: []string{"text"}},
		anystore.IndexInfo{Name: "xyz", Fields: []string{"x.y", "x.z"}, Sparse: true},
	))
	insertJSON(t, coll,
		`{"id":"a","text":"alpha","x":[{"y":null},[{"z":null}]]}`,
		`{"id":"b","text":"alpha","x":[{"y":1,"z":1}]}`,
		`{"id":"c","text":"alpha","x":[{"y":1,"z":1},{"y":2},{"z":3}]}`,
		`{"id":"d","text":"beta","x":[{"y":1,"z":1}]}`,
	)
	// Text matches far outnumber the index's documents: the index probes.
	var filler []string
	for i := range 2000 {
		filler = append(filler, fmt.Sprintf(`{"id":"f%04d","text":"alpha"}`, i))
	}
	insertJSON(t, coll, filler...)
	filter := `{"$text":{"$search":"alpha"},"x.y":{"$exists":true},"x.z":{"$exists":true}}`
	explain, err := coll.Find(filter).Explain(ctx)
	require.NoError(t, err)
	assert.True(t, plannerIndexUsed(explain, "xyz"), explain.Sql)
	for _, q := range []anystore.Query{coll.Find(filter), coll.Find(filter).IndexHint(anystore.IndexHint{IndexName: "xyz", Boost: 1 << 30})} {
		assert.ElementsMatch(t, []string{"b", "c"}, collectIdsString(t, q))
		n, err := q.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, n)
	}
	qplannerEnableCounters(t)
	_, err = coll.Find(filter).Count(ctx)
	require.NoError(t, err)
	assert.Zero(t, qplannerSnapshot().Planner.FetchNextCalls, "a presence-covered probe Count must not fetch documents")
}

// These tests exercise the first-class FTS pipeline: $text combined with custom
// residual filters and custom sorts, plus the BM25 _score sidecar, Explain, and
// Count/Delete — all flowing through qplanner.BuildPlan(Fts) → FtsIter.

func ftsPipelineColl(t *testing.T) (*fixture, anystore.Collection) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "p")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Kind: anystore.IndexKindFulltext, Fields: []string{"body"}}))
	insertJSON(t, coll,
		`{"id":"a","body":"london crash report","year":1920,"status":"open"}`,
		`{"id":"b","body":"london fog landing","year":1937,"status":"closed"}`,
		`{"id":"c","body":"london london tower","year":1950,"status":"open"}`,
		`{"id":"d","body":"paris sunshine","year":1960,"status":"open"}`,
	)
	return fx, coll
}

// collectIter runs a query and returns (ids, scores) in result order.
func collectIter(t *testing.T, q anystore.Query) ([]string, []float64) {
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

func TestFtsPipeline_ResidualEqualityFilter(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	// $text + an equality predicate on a non-indexed field (residual FilterIter).
	ids, _ := collectIter(t, coll.Find(map[string]any{
		"$text":  map[string]any{"$search": "london"},
		"status": "open",
	}))
	// b is "closed" → filtered out; a and c remain (both contain london).
	assert.ElementsMatch(t, []string{"a", "c"}, ids)
}

func TestFtsPipeline_ResidualRangeFilter(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	ids, _ := collectIter(t, coll.Find(map[string]any{
		"$text": map[string]any{"$search": "london"},
		"year":  map[string]any{"$gte": 1940},
	}))
	// Only c (1950) matches london AND year>=1940.
	assert.Equal(t, []string{"c"}, ids)
}

func TestFtsPipeline_ScoreSidecar(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	// Default relevance order: c (london×2, short doc) should outrank a and b,
	// and every hit must carry a positive BM25 score via Score().
	ids, scores := collectIter(t, coll.Find(`{"$text":{"$search":"london"}}`))
	require.Equal(t, []string{"c", "a", "b"}, ids)
	require.Len(t, scores, 3)
	// c (london×2, short doc) strictly outranks; a and b tie (london×1, same len).
	assert.Greater(t, scores[0], scores[1], "c must outrank a")
	assert.GreaterOrEqual(t, scores[1], scores[2], "non-increasing")
	assert.InDelta(t, scores[1], scores[2], 1e-12, "a and b tie")
	for _, s := range scores {
		assert.Greater(t, s, 0.0)
	}
}

func TestFtsPipeline_SortByRealFieldAscending(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	// $text matches a,b,c; an explicit real-field sort (year asc) must override
	// the relevance order — i.e. the planner inserts a SortIter.
	ids, _ := collectIter(t, coll.Find(`{"$text":{"$search":"london"}}`).Sort("year"))
	assert.Equal(t, []string{"a", "b", "c"}, ids) // 1920, 1937, 1950
}

func TestFtsPipeline_SortByRealFieldDescending(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	ids, _ := collectIter(t, coll.Find(`{"$text":{"$search":"london"}}`).Sort("-year"))
	assert.Equal(t, []string{"c", "b", "a"}, ids)
}

func TestFtsPipeline_TextScoreSortIsRelevanceOrder(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	// Explicit {$meta:textScore} sort == default relevance order.
	idsDefault, _ := collectIter(t, coll.Find(`{"$text":{"$search":"london"}}`))
	idsMeta, _ := collectIter(t, coll.Find(`{"$text":{"$search":"london"}}`).
		Sort(`{"score":{"$meta":"textScore"}}`))
	assert.Equal(t, idsDefault, idsMeta)
}

func TestFtsPipeline_FilterAndSortCombined(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	// Residual filter (status=open) AND a real-field sort (-year) over $text.
	ids, _ := collectIter(t, coll.Find(map[string]any{
		"$text":  map[string]any{"$search": "london"},
		"status": "open",
	}).Sort("-year"))
	assert.Equal(t, []string{"c", "a"}, ids) // open londons, 1950 then 1920
}

func TestFtsPipeline_LimitAfterFilter(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	// Limit applies AFTER the residual filter (status=open leaves a,c; limit 1).
	ids, _ := collectIter(t, coll.Find(map[string]any{
		"$text":  map[string]any{"$search": "london"},
		"status": "open",
	}).Sort("year").Limit(1))
	assert.Equal(t, []string{"a"}, ids) // earliest open london
}

func TestFtsPipeline_CountWithResidual(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	n, err := coll.Find(map[string]any{
		"$text":  map[string]any{"$search": "london"},
		"status": "open",
	}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, n) // a, c
}

func TestFtsPipeline_DeleteByText(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	res, err := coll.Find(map[string]any{
		"$text":  map[string]any{"$search": "london"},
		"status": "closed",
	}).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Modified) // only b (closed london)
	// b is gone; a and c (open londons) and d remain.
	ids, _ := collectIter(t, coll.Find(`{"$text":{"$search":"london"}}`))
	assert.ElementsMatch(t, []string{"a", "c"}, ids)
}

func TestFtsPipeline_UpdateByText(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	res, err := coll.Find(map[string]any{"$text": map[string]any{"$search": "paris"}}).
		Update(ctx, `{"$set":{"status":"archived"}}`)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Modified)
	doc, err := coll.FindId(ctx, "d")
	require.NoError(t, err)
	assert.Equal(t, "archived", doc.Value().GetString("status"))
}

func TestFtsPipeline_Explain(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	exp, err := coll.Find(map[string]any{
		"$text":  map[string]any{"$search": "london"},
		"status": "open",
	}).Explain(ctx)
	require.NoError(t, err)
	// The plan must be driven by the FtsSearch source with the residual filter
	// downstream — no full scan.
	assert.Contains(t, exp.Sql, "FtsSearch")
}

func TestFtsPipeline_ScoreZeroForNonText(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	iter, err := coll.Find(`{"status":"open"}`).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	require.True(t, iter.Next())
	assert.Equal(t, 0.0, iter.Score(), "Score() is 0 for non-$text queries")
}

func TestFtsStats_CorrectAndHelpful(t *testing.T) {
	fx, coll := ftsTestColl(t, "body")
	defer fx.finish()

	// Known corpus: distinct terms = {alpha, beta, gamma, delta, the} = 5.
	// tokens: doc1=3, doc2=3, doc3=2, empty doc indexed as 0 (excluded).
	insertJSON(t, coll,
		`{"id":"a","body":"alpha beta gamma"}`, // 3 tokens
		`{"id":"b","body":"alpha beta the"}`,   // 3 tokens
		`{"id":"c","body":"delta the"}`,        // 2 tokens
		`{"id":"d","body":""}`,                 // no text → not indexed
	)

	st, err := coll.Stats(ctx)
	require.NoError(t, err)

	require.Len(t, st.FtsIndexes, 1, "the full-text index must appear in Stats")
	fs := st.FtsIndexes[0]

	// Corpus stats are correct.
	assert.Equal(t, []string{"body"}, fs.Fields)
	assert.Equal(t, 3, fs.DocCount, "empty doc d is excluded")
	assert.Equal(t, 8, fs.TotalTokens, "3+3+2")
	assert.InDelta(t, 8.0/3.0, fs.AvgDocLen, 1e-9)
	// distinct terms: alpha, beta, gamma, the, delta
	assert.Equal(t, 5, fs.VocabSize)

	// Storage is reported and non-trivial, and the postings dominate.
	assert.Greater(t, fs.SizeBytes, 0)
	assert.Equal(t, fs.PostingsBytes+fs.VocabBytes+fs.DocmapBytes+fs.DocinfoBytes+fs.MetaBytes, fs.SizeBytes)
	assert.Greater(t, fs.PostingsBytes, 0)

	// The collection total includes the fts index.
	assert.Equal(t, fs.SizeBytes, st.FtsIndexesSizeBytes)
	assert.Equal(t, st.DocsSizeBytes+st.IndexesSizeBytes+st.VectorIndexesSizeBytes+st.FtsIndexesSizeBytes, st.TotalSizeBytes)
	assert.GreaterOrEqual(t, st.TotalSizeBytes, fs.SizeBytes)
}

func TestFtsStats_TracksMutations(t *testing.T) {
	fx, coll := ftsTestColl(t, "body")
	defer fx.finish()

	insertJSON(t, coll,
		`{"id":"a","body":"one two three"}`,
		`{"id":"b","body":"four five"}`,
	)
	st, _ := coll.Stats(ctx)
	assert.Equal(t, 2, st.FtsIndexes[0].DocCount)
	assert.Equal(t, 5, st.FtsIndexes[0].VocabSize)
	assert.Equal(t, 5, st.FtsIndexes[0].TotalTokens)

	// Delete one doc: counts shrink, vocab drops its now-orphaned terms.
	require.NoError(t, coll.DeleteId(ctx, "b"))
	st, _ = coll.Stats(ctx)
	assert.Equal(t, 1, st.FtsIndexes[0].DocCount)
	assert.Equal(t, 3, st.FtsIndexes[0].VocabSize, "four/five gone")
	assert.Equal(t, 3, st.FtsIndexes[0].TotalTokens)

	// Delta-update keeps DocCount stable but adjusts tokens/vocab.
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":"a","body":"one two three four"}`)))
	st, _ = coll.Stats(ctx)
	assert.Equal(t, 1, st.FtsIndexes[0].DocCount)
	assert.Equal(t, 4, st.FtsIndexes[0].VocabSize, "four re-added")
	assert.Equal(t, 4, st.FtsIndexes[0].TotalTokens)
}

func TestFtsStats_EmptyIndex(t *testing.T) {
	fx, coll := ftsTestColl(t, "body")
	defer fx.finish()
	st, err := coll.Stats(ctx)
	require.NoError(t, err)
	require.Len(t, st.FtsIndexes, 1)
	fs := st.FtsIndexes[0]
	assert.Equal(t, 0, fs.DocCount)
	assert.Equal(t, 0, fs.VocabSize)
	assert.Equal(t, 0.0, fs.AvgDocLen, "no divide-by-zero on an empty index")
}

// Phase 2: configurable BM25 b / k1 (FulltextParams). Default params reproduce
// the original scoring exactly (covered by the parity tests); these cover that a
// custom b actually changes length normalization and that params persist.

func ftsScoreByID(t *testing.T, coll anystore.Collection, search string) map[string]float64 {
	ids, scores := collectIter(t, coll.Find(map[string]any{"$text": map[string]any{"$search": search}}))
	m := make(map[string]float64, len(ids))
	for i, id := range ids {
		m[id] = scores[i]
	}
	return m
}

func TestFtsBM25_Validation(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish()
	coll, err := fx.CreateCollection(ctx, "v")
	require.NoError(t, err)
	assert.Error(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Kind: anystore.IndexKindFulltext, Fields: []string{"body"}, Fulltext: &anystore.FulltextParams{B: 1.5},
	}), "B>1 must be rejected")
	assert.Error(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Kind: anystore.IndexKindFulltext, Fields: []string{"body"}, Fulltext: &anystore.FulltextParams{K1: -1},
	}), "negative K1 must be rejected")
}

// withB builds a fresh collection whose fts index uses the given b, then inserts
// one short and one long doc that each contain "alpha" exactly once.
func withB(t *testing.T, fx *fixture, name string, b float64) anystore.Collection {
	coll, err := fx.CreateCollection(ctx, name)
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Kind: anystore.IndexKindFulltext, Fields: []string{"body"}, Fulltext: &anystore.FulltextParams{B: b},
	}))
	insertJSON(t, coll,
		`{"id":"s","body":"alpha"}`,
		`{"id":"l","body":"alpha beta gamma delta epsilon zeta eta theta iota kappa"}`,
	)
	return coll
}

func TestFtsBM25_BAffectsLengthNorm(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish()

	// b high (default 0.75) penalizes the long doc more than b low (0.1), so the
	// short/long score ratio is larger at high b.
	highB := withB(t, fx, "high", 0.75)
	lowB := withB(t, fx, "low", 0.1)

	sh := ftsScoreByID(t, highB, "alpha")
	sl := ftsScoreByID(t, lowB, "alpha")
	require.Contains(t, sh, "s")
	require.Contains(t, sh, "l")

	ratioHigh := sh["s"] / sh["l"]
	ratioLow := sl["s"] / sl["l"]
	assert.Greater(t, ratioHigh, ratioLow,
		"higher b must favour the short doc more (ratioHigh=%.4f ratioLow=%.4f)", ratioHigh, ratioLow)
	// Sanity: at b=0.1 the two docs score nearly the same (length barely matters).
	assert.InDelta(t, 1.0, ratioLow, 0.15)
}

// Phase 3: per-field weights (BM25F). The v2 postings store per-field TF; the
// scorer combines them as Σ weight_f · tf_f. An unweighted index reduces exactly
// to classic BM25 (covered by the parity tests); these cover the weighting.

func ftsWeightedColl(t *testing.T, fx *fixture, name string, weights map[string]float64) anystore.Collection {
	coll, err := fx.CreateCollection(ctx, name)
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Kind:     anystore.IndexKindFulltext,
		Fields:   []string{"title", "body"},
		Fulltext: &anystore.FulltextParams{Weights: weights},
	}))
	insertJSON(t, coll,
		`{"id":"t","title":"alpha","body":"filler filler filler"}`,      // match in title
		`{"id":"b","title":"filler","body":"alpha filler filler"}`,      // match in body
		`{"id":"x","title":"nothing","body":"unrelated text entirely"}`, // no match
	)
	return coll
}

func TestFtsWeights_TitleBoostChangesRanking(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish()

	// With a strong title boost, the title match (t) must outrank the body match (b).
	boosted := ftsWeightedColl(t, fx, "boost", map[string]float64{"title": 8})
	ids, _ := collectIter(t, boosted.Find(`{"$text":{"$search":"alpha"}}`))
	assert.Equal(t, []string{"t", "b"}, ids)

	// With the opposite boost (body >> title), the body match wins.
	bodyBoost := ftsWeightedColl(t, fx, "bodyboost", map[string]float64{"body": 8})
	ids2, _ := collectIter(t, bodyBoost.Find(`{"$text":{"$search":"alpha"}}`))
	assert.Equal(t, []string{"b", "t"}, ids2)
}

func TestFtsWeights_UnweightedUnchanged(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish()
	// No weights → default BM25; both match, x does not. (Sanity that the v2 format
	// + default scoring still returns the right match set.)
	plain := ftsWeightedColl(t, fx, "plain", nil)
	ids, scores := collectIter(t, plain.Find(`{"$text":{"$search":"alpha"}}`))
	assert.ElementsMatch(t, []string{"t", "b"}, ids)
	for _, s := range scores {
		assert.Greater(t, s, 0.0)
	}
}

func TestFtsWeights_Validation(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish()
	coll, err := fx.CreateCollection(ctx, "v")
	require.NoError(t, err)
	assert.Error(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Kind: anystore.IndexKindFulltext, Fields: []string{"title", "body"},
		Fulltext: &anystore.FulltextParams{Weights: map[string]float64{"nosuchfield": 2}},
	}), "weight on unknown field must be rejected")
	assert.Error(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Kind: anystore.IndexKindFulltext, Fields: []string{"title"},
		Fulltext: &anystore.FulltextParams{Weights: map[string]float64{"title": -1}},
	}), "negative weight must be rejected")
}

func TestFtsWeights_UpdateMovesTermBetweenFields(t *testing.T) {
	// A term moving from title to body with IDENTICAL positions (possible when
	// the earlier field empties, so the later field inherits its base) must be
	// re-attributed: the diff skip used to keep the stale title FieldMask, so the
	// doc kept scoring with the title weight forever.
	fx := newFixture(t)
	defer fx.finish()

	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Kind:     anystore.IndexKindFulltext,
		Fields:   []string{"title", "body"},
		Fulltext: &anystore.FulltextParams{Weights: map[string]float64{"title": 8}},
	}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"x","title":"alpha beta gamma","body":""}`)))
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":"x","title":"","body":"alpha beta gamma"}`)))
	// control doc with the same final content inserted directly
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"y","title":"","body":"alpha beta gamma"}`)))

	ids, scores := collectIter(t, coll.Find(`{"$text":{"$search":"alpha"}}`))
	require.ElementsMatch(t, []string{"x", "y"}, ids)
	require.Len(t, scores, 2)
	assert.InDelta(t, scores[0], scores[1], 1e-9,
		"identical-content docs must score identically (stale FieldMask if not): ids=%v scores=%v", ids, scores)
}
