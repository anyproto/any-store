package anystore

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/internal/qplanner"
)

func qplannerEnableCounters(t *testing.T) {
	qplanner.ResetPerfCounters()
	qplanner.EnablePerfCounters(true)
	t.Cleanup(func() { qplanner.EnablePerfCounters(false) })
}

func qplannerSnapshot() qplanner.PerfCounters {
	return qplanner.SnapshotPerfCounters()
}

// Differential tests for the $text driver/probe plan duality: every legal plan
// for the same query must produce byte-identical rows, order, and scores. A
// bounded write selects rows by (score desc, IntDocID asc), so any divergence
// here is cross-device data divergence, not a perf bug. Plans are forced via
// IndexHint: the fulltext index name forces the driver, the primary-key field
// name the pk probe, and a secondary index name its probe.

const ftsProbeBoost = 1 << 30

// ftsProbeColl builds a corpus exercising every clause kind: zipf-ish plain
// terms, phrase pairs, a prefix family, docs with identical text (score ties),
// docs missing the text field, and a multikey tags index.
func ftsProbeColl(t *testing.T) (*fixture, Collection) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "probe")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "ft", Kind: IndexKindFulltext, Fields: []string{"title", "body"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "tags", Fields: []string{"tags"}}))

	vocab := []string{"alpha", "beta", "gamma", "delta", "omega", "prefixone", "prefixtwo", "prefixthree"}
	var docs []string
	// 300 docs: IntDocIDs span 3 postings chunks (chunk size 128), so the
	// prober's single-chunk point-get is exercised against multi-chunk terms.
	for i := 0; i < 300; i++ {
		var words []string
		for w, word := range vocab {
			if i%(w+2) == 0 {
				words = append(words, word)
			}
		}
		if i%3 == 0 {
			words = append(words, "alpha", "beta") // adjacent → phrase "alpha beta"
		}
		if i%17 == 0 {
			words = append(words, fmt.Sprintf("uniq%d", i))
		}
		body := strings.Join(words, " ")
		if i%29 == 0 {
			body = "" // empty text
		}
		title := "doc"
		if i%4 == 0 {
			title = "alpha title"
		}
		docs = append(docs, fmt.Sprintf(
			`{"id":"d%03d","a":%d,"s":%d,"tags":["t%d","t%d"],"title":%q,"body":%q}`,
			i, i%10, (i*37)%50, i%5, i%7, title, body))
	}
	// A tie group: identical text, distinct ids — order must fall back to
	// IntDocID (insertion order) identically under every plan.
	for i := 0; i < 8; i++ {
		docs = append(docs, fmt.Sprintf(`{"id":"tie%d","a":%d,"s":0,"tags":["t0"],"title":"doc","body":"omega omega gamma"}`, i, i%10))
	}
	// A doc with no text fields at all.
	docs = append(docs, `{"id":"notext","a":1,"s":1,"tags":["t1"]}`)
	insertJSON(t, coll, docs...)
	return fx, coll
}

// ftsProbePlans are the plans to force for every query: the driver, the pk
// probe, and the two secondary-index probes. A hint that has no matching
// candidate for a given query is a no-op — the comparison then just re-checks
// the cheapest plan, which is harmless.
var ftsProbePlans = []struct {
	name string
	hint []IndexHint
}{
	{"cbo", nil},
	{"driver", []IndexHint{{IndexName: "ft", Boost: ftsProbeBoost}}},
	{"probe-ids", []IndexHint{{IndexName: "id", Boost: ftsProbeBoost}}},
	{"probe-a", []IndexHint{{IndexName: "a", Boost: ftsProbeBoost}}},
	{"probe-tags", []IndexHint{{IndexName: "tags", Boost: ftsProbeBoost}}},
}

func ftsProbeQueries() []map[string]any {
	text := func(kv ...any) map[string]any {
		m := map[string]any{}
		for i := 0; i < len(kv); i += 2 {
			m[kv[i].(string)] = kv[i+1]
		}
		return m
	}
	searches := []map[string]any{
		text("$search", "alpha"),
		text("$search", "omega"),
		text("$search", "uniq17"),
		text("$search", "alpha beta gamma"),
		text("$search", `"alpha beta"`),
		text("$search", `"alpha"`), // quoted SINGLE token: driver routes to scanTerm
		text("$search", "prefix*"),
		text("$search", "alpha prefixon*"), // expansion overlapping a plain-term family
		text("$search", "alpha", "$exclude", "beta"),
		text("$search", "alpha", "$exclude", "prefix*"), // negated prefix
		text("$search", "alpha", "$exclude", `"beta gamma"`),
		text("$search", "alpha", "$require", "gamma"),
		text("$search", "alpha", "$require", "nosuchterm"), // dead required clause: zero rows
		text("$search", "alpha beta", "$defaultOperator", "and"),
		text("$search", "alpha alpha beta"), // repeated term dedup
		text("$search", "nosuchterm"),
		text("$search", ""),
	}
	restrictions := []map[string]any{
		nil,
		{"id": map[string]any{"$in": []any{"d001", "d004", "d012", "tie3", "nope"}}},
		{"id": map[string]any{"$in": manyIds(40)}},
		{"id": map[string]any{"$gte": "d010", "$lt": "d060"}},
		{"a": 3},
		{"a": map[string]any{"$in": []any{1, 2}}},
		{"tags": "t2"},
		{"a": 3, "s": map[string]any{"$lt": 30}},
	}
	var out []map[string]any
	for _, s := range searches {
		for _, r := range restrictions {
			q := map[string]any{"$text": s}
			for k, v := range r {
				q[k] = v
			}
			out = append(out, q)
		}
	}
	return out
}

func manyIds(n int) []any {
	out := make([]any, 0, n+2)
	for i := 0; i < n; i++ {
		out = append(out, fmt.Sprintf("d%03d", i*3))
	}
	out = append(out, "tie0", "tie5")
	return out
}

// assertSamePlanResults runs the query under every forced plan and asserts
// rows, order, and bit-exact scores match the driver plan.
func assertSamePlanResults(t *testing.T, coll Collection, cond map[string]any, limit, offset uint, sort any) {
	t.Helper()
	var baseIds []string
	var baseScores []float64
	for _, p := range ftsProbePlans {
		q := coll.Find(cond)
		if len(p.hint) > 0 {
			q = q.IndexHint(p.hint...)
		}
		if limit > 0 {
			q = q.Limit(limit)
		}
		if offset > 0 {
			q = q.Offset(offset)
		}
		if sort != nil {
			q = q.Sort(sort)
		}
		ids, scores := collectIter(t, q)
		if p.name == "cbo" {
			baseIds, baseScores = ids, scores
			continue
		}
		if !assert.Equal(t, baseIds, ids, "plan %s: rows/order diverged for %v limit=%d offset=%d sort=%v", p.name, cond, limit, offset, sort) {
			ex, _ := coll.Find(cond).IndexHint(p.hint...).Explain(ctx)
			t.Logf("diverging plan: %s", ex.Plan)
			return
		}
		require.Equal(t, len(baseScores), len(scores))
		for i := range scores {
			assert.Equal(t, math.Float64bits(baseScores[i]), math.Float64bits(scores[i]),
				"plan %s: score not bit-identical for %v row %d (%s): %v vs %v", p.name, cond, i, ids[i], baseScores[i], scores[i])
		}

		// Count must agree with the row count of the unwindowed query.
		if limit == 0 && offset == 0 {
			n, err := coll.Find(cond).IndexHint(p.hint...).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, len(baseIds), n, "plan %s: count diverged for %v", p.name, cond)
		}
	}
}

func TestFtsProbe_DifferentialAllPlans(t *testing.T) {
	fx, coll := ftsProbeColl(t)
	defer fx.finish()
	for _, cond := range ftsProbeQueries() {
		assertSamePlanResults(t, coll, cond, 0, 0, nil)
		assertSamePlanResults(t, coll, cond, 5, 0, nil)
		assertSamePlanResults(t, coll, cond, 5, 3, nil)
	}
}

func TestFtsProbe_DifferentialSorted(t *testing.T) {
	fx, coll := ftsProbeColl(t)
	defer fx.finish()
	conds := []map[string]any{
		{"$text": map[string]any{"$search": "alpha"}, "a": 3},
		{"$text": map[string]any{"$search": "omega"}, "id": map[string]any{"$in": manyIds(30)}},
		{"$text": map[string]any{"$search": "alpha beta"}},
	}
	for _, cond := range conds {
		for _, sort := range []any{"s", "-s", "a"} {
			assertSamePlanResults(t, coll, cond, 0, 0, sort)
			assertSamePlanResults(t, coll, cond, 4, 0, sort)
			assertSamePlanResults(t, coll, cond, 4, 2, sort)
		}
		// Explicit relevance sort maps to the intrinsic order.
		assertSamePlanResults(t, coll, cond, 6, 0, `{"score":{"$meta":"textScore"}}`)
	}
}

// Score ties must break by IntDocID (insertion order) under every plan — this
// is what keeps bounded writes device-deterministic.
func TestFtsProbe_TieBreakOrder(t *testing.T) {
	fx, coll := ftsProbeColl(t)
	defer fx.finish()
	cond := map[string]any{
		"$text": map[string]any{"$search": "omega omega gamma", "$defaultOperator": "and"},
		"id":    map[string]any{"$in": []any{"tie7", "tie5", "tie3", "tie1", "tie0"}},
	}
	assertSamePlanResults(t, coll, cond, 3, 0, nil)
	ids, _ := collectIter(t, coll.Find(cond).Limit(3))
	assert.Equal(t, []string{"tie0", "tie1", "tie3"}, ids)
}

// A bounded write under $text must modify the same rows whichever plan runs.
func TestFtsProbe_BoundedUpdateSameRows(t *testing.T) {
	fx, coll := ftsProbeColl(t)
	defer fx.finish()
	cond := map[string]any{
		"$text": map[string]any{"$search": "alpha"},
		"id":    map[string]any{"$in": manyIds(40)},
	}
	expected, _ := collectIter(t, coll.Find(cond).Limit(3))
	require.Len(t, expected, 3)

	res, err := coll.Find(cond).IndexHint(IndexHint{IndexName: "id", Boost: ftsProbeBoost}).
		Limit(3).Update(ctx, `{"$set":{"marked":1}}`)
	require.NoError(t, err)
	require.Equal(t, 3, res.Modified)

	marked, _ := collectIter(t, coll.Find(`{"marked":1}`))
	assert.ElementsMatch(t, expected, marked)
}

// The CBO must route the SYN-191 shapes to probe plans and broad queries to
// the driver, and Count on a covered probe plan must fetch no documents.
func TestFtsProbe_PlanChoice(t *testing.T) {
	fx, coll := ftsProbeColl(t)
	defer fx.finish()

	explain := func(cond map[string]any, limit uint) string {
		q := coll.Find(cond)
		if limit > 0 {
			q = q.Limit(limit)
		}
		ex, err := q.Explain(ctx)
		require.NoError(t, err)
		return ex.Plan + " " + ex.Sql
	}

	selective := map[string]any{
		"$text": map[string]any{"$search": "alpha"},
		"id":    map[string]any{"$in": []any{"d000", "d004", "d008"}},
	}
	assert.Contains(t, explain(selective, 10), "FtsProbe", "3-id restriction must probe")

	broad := map[string]any{"$text": map[string]any{"$search": "alpha"}}
	assert.Contains(t, explain(broad, 10), "FtsSearch", "unrestricted must keep the driver")

	eqA := map[string]any{"$text": map[string]any{"$search": "alpha"}, "a": 3}
	assert.Contains(t, explain(eqA, 10), "FtsProbe", "selective index equality must probe")
}

// Weighted (BM25F) parity: per-field weights change the scored tf, a
// zero-weight field contributes tf 0 yet still counts as presence for
// matching, required bits, and exclusion — the prober must reproduce all of
// it bit-exactly (a quoted single token routes through scanTerm, not the
// phrase path). Non-default K1/B pin ftsResolveBM25 parity.
func TestFtsProbe_WeightedDifferential(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish()
	coll, err := fx.CreateCollection(ctx, "wprobe")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "ft", Kind: IndexKindFulltext, Fields: []string{"title", "body"},
		Fulltext: &FulltextParams{Weights: map[string]float64{"title": 3, "body": 0}, K1: 1.6, B: 0.6},
	}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
	var docs []string
	for i := 0; i < 150; i++ {
		title, body := "doc", "pad filler"
		if i%2 == 0 {
			title = "alpha common"
		}
		if i%3 == 0 {
			body = "beta only in body" // zero-weight field: presence without score
		}
		if i%5 == 0 {
			title = "alpha beta adjacent"
		}
		docs = append(docs, fmt.Sprintf(`{"id":"w%03d","a":%d,"title":%q,"body":%q}`, i, i%10, title, body))
	}
	insertJSON(t, coll, docs...)

	conds := []map[string]any{
		{"$text": map[string]any{"$search": "beta"}},
		{"$text": map[string]any{"$search": `"alpha"`}},
		{"$text": map[string]any{"$search": "alpha", "$exclude": "beta"}},
		{"$text": map[string]any{"$search": "alpha", "$require": "beta"}},
		{"$text": map[string]any{"$search": `"alpha beta"`}},
		{"$text": map[string]any{"$search": "alpha beta", "$defaultOperator": "and"}},
	}
	restrictions := []map[string]any{
		{"a": 3},
		{"id": map[string]any{"$in": func() []any {
			var out []any
			for i := 0; i < 40; i++ {
				out = append(out, fmt.Sprintf("w%03d", i*3))
			}
			return out
		}()}},
	}
	for _, c := range conds {
		for _, r := range restrictions {
			cond := map[string]any{}
			for k, v := range c {
				cond[k] = v
			}
			for k, v := range r {
				cond[k] = v
			}
			assertSamePlanResults(t, coll, cond, 0, 0, nil)
			assertSamePlanResults(t, coll, cond, 5, 0, nil)
		}
	}
}

// >64 required clauses exhaust the 64-bit mask; the tail degrades to ungated
// OR identically on both sides.
func TestFtsProbe_RequiredBitExhaustion(t *testing.T) {
	fx, coll := ftsProbeColl(t)
	defer fx.finish()
	req := make([]any, 0, 70)
	req = append(req, "alpha", "gamma")
	for i := 0; i < 68; i++ {
		req = append(req, fmt.Sprintf("filler%d", i)) // df==0 fillers
	}
	cond := map[string]any{
		"$text": map[string]any{"$search": "", "$require": req},
		"id":    map[string]any{"$in": manyIds(40)},
	}
	assertSamePlanResults(t, coll, cond, 0, 0, nil)
}

// Logical-work guard: a Count whose residual is covered by the probe driver
// must fetch zero documents — the structural fix for the Count sibling defect.
func TestFtsProbe_CountFetchesNothing(t *testing.T) {
	fx, coll := ftsProbeColl(t)
	defer fx.finish()
	cond := map[string]any{
		"$text": map[string]any{"$search": "alpha"},
		"id":    map[string]any{"$in": []any{"d000", "d004", "d008"}},
	}
	// Warm plan/visibility state outside the measured window.
	_, err := coll.Find(cond).Count(ctx)
	require.NoError(t, err)

	qplannerEnableCounters(t)
	n, err := coll.Find(cond).Count(ctx)
	require.NoError(t, err)
	require.Greater(t, n, 0)
	pc := qplannerSnapshot()
	assert.Zero(t, pc.FetchNextCalls, "covered probe Count must not fetch documents")
	assert.Zero(t, pc.FilterNextCalls, "covered probe Count must not run the residual filter")
}

// The driver plan's primary-key gate: an id range restriction must not change
// results, and stays on the driver when the range is broad.
func TestFtsProbe_DriverIdGate(t *testing.T) {
	fx, coll := ftsProbeColl(t)
	defer fx.finish()
	cond := map[string]any{
		"$text": map[string]any{"$search": "alpha"},
		"id":    map[string]any{"$gte": "d010", "$lt": "d040"},
	}
	ids, _ := collectIter(t, coll.Find(cond).IndexHint(IndexHint{IndexName: "ft", Boost: ftsProbeBoost}))
	for _, id := range ids {
		assert.GreaterOrEqual(t, id, "d010")
		assert.Less(t, id, "d040")
	}
	require.NotEmpty(t, ids)
}
