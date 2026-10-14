package anystore

import (
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
)

// TestFtsResidual_NoTextSurvives mirrors TestKnnResidual_NoKnnSurvives: the
// residual handed to the FilterIter must never contain the Text node, in any
// $and nesting — detection (findTextInAnd) recurses, so the strip must too.
// A leaked Text fails open (Text.Ok returns true), so the leak is invisible
// in results; this test pins the structural post-condition instead.
func TestFtsResidual_NoTextSurvives(t *testing.T) {
	td := `{"$text":{"$search":"x"}}`
	cases := []string{
		td,
		fmt.Sprintf(`{"$and":[%s,{"a":1}]}`, td),
		fmt.Sprintf(`{"$and":[{"$and":[%s,{"a":1}]},{"b":2}]}`, td),
		fmt.Sprintf(`{"$and":[{"$and":[%s]},{"$and":[{"a":1},{"b":2}]}]}`, td),
		fmt.Sprintf(`{"$and":[{"$and":[{"$and":[%s]}]},{"a":1}]}`, td),
	}
	for _, c := range cases {
		f := query.MustParseCondition(c)
		res := ftsResidualFilter(f)
		require.NotNil(t, res, c)
		assert.False(t, query.ContainsText(res), "text survived: %s", c)
	}

	// The surviving conjuncts keep their semantics: for the nested shape the
	// residual must still require a==1 AND b==2.
	res := ftsResidualFilter(query.MustParseCondition(cases[2]))
	buf := &syncpool.DocBuffer{}
	assert.True(t, res.Ok(anyenc.MustParseJson(`{"a":1,"b":2}`), buf))
	assert.False(t, res.Ok(anyenc.MustParseJson(`{"a":1,"b":3}`), buf))
	assert.False(t, res.Ok(anyenc.MustParseJson(`{"a":0,"b":2}`), buf))

	// A text-only query leaves the match-all residual.
	assert.Equal(t, query.All{}, ftsResidualFilter(query.MustParseCondition(td)))

	// Pointer forms: *And nodes and *Text leaves must be stripped too.
	ptr := &query.And{&query.Text{}, query.MustParseCondition(`{"a":1}`)}
	res = ftsResidualFilter(query.And{ptr, query.MustParseCondition(`{"b":2}`)})
	require.NotNil(t, res)
	assert.False(t, query.ContainsText(res))
	assert.True(t, res.Ok(anyenc.MustParseJson(`{"a":1,"b":2}`), buf))
	assert.False(t, res.Ok(anyenc.MustParseJson(`{"a":2,"b":2}`), buf))
}

// TestFtsPipeline_NestedAndResidual drives the leak shape end-to-end: the
// nested conjuncts must actually FILTER the BM25 candidates (guards against
// over-stripping, the opposite failure of the leak).
func TestFtsPipeline_NestedAndResidual(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	cond := `{"$and":[{"$and":[{"$text":{"$search":"london"}},{"status":"open"}]},{"year":{"$lt":1940}}]}`

	// a: open/1920 matches; b: closed; c: 1950; d: no "london".
	ids, _ := collectIter(t, coll.Find(cond))
	assert.Equal(t, []string{"a"}, ids)

	cnt, err := coll.Find(cond).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
}

// TestFtsPlacementRejections pins the placement errors around the residual
// path: $text under $or/$nor/$not and duplicate $text fail with the named
// errors on every entry point, they don't silently degrade.
func TestFtsPlacementRejections(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	for _, c := range []string{
		`{"$or":[{"$text":{"$search":"london"}},{"status":"open"}]}`,
		`{"$nor":[{"$text":{"$search":"london"}}]}`,
	} {
		_, err := coll.Find(c).Count(ctx)
		assert.ErrorIs(t, err, errFtsBadPlacement, c)
	}
	_, err := coll.Find(`{"$and":[{"$text":{"$search":"london"}},{"$and":[{"$text":{"$search":"fog"}}]}]}`).Count(ctx)
	assert.ErrorIs(t, err, errFtsMultiple)
}

// TestFtsPointerFilterForms pins detection symmetry for programmatic filters
// (ParseCondition passes a query.Filter through verbatim): pointer-built
// nodes must behave exactly like their value forms — a *Text is a real $text
// (searched, not silently ignored fail-open), and a Text inside a pointer
// composite is a placement error, not a match-all.
func TestFtsPointerFilterForms(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	tf := query.MustParseCondition(`{"$text":{"$search":"london"}}`).(query.Text)

	// *Text inside $and: the BM25 search must run — only the three "london"
	// docs match, not all four.
	ids, _ := collectIter(t, coll.Find(query.And{&tf, query.MustParseCondition(`{"status":"open"}`)}))
	assert.ElementsMatch(t, []string{"a", "c"}, ids)

	// Text inside pointer composites: same placement rejection as the value
	// forms.
	for _, f := range []query.Filter{
		&query.Or{tf, query.MustParseCondition(`{"status":"open"}`)},
		&query.Nor{tf},
		&query.Not{Filter: tf},
		query.And{tf, &query.Key{Path: []string{"a"}, Filter: tf}},
	} {
		_, err := coll.Find(f).Count(ctx)
		assert.ErrorIs(t, err, errFtsBadPlacement, "%T", f)
	}
}

// An independent $text oracle: it tokenizes each field with FtsAnalyzeTerms,
// keeps every token's (field, array element, index in element), and computes
// matches and BM25 scores from those alone — no positions, no postings. Phrase
// adjacency is "same field, same element, consecutive indexes"; the anchor is
// the document's first token (the first token of the first field that yields
// tokens; inside an array field, of the first element that yields tokens). It
// sums each document's contributions in the engine's order (plain terms in
// first-seen order, then groups in clause order), so its scores are compared
// bit for bit.

var ftoFields = []string{"tags", "title", "body"}

type ftoTok struct {
	term             string
	field, elem, idx int
}

type ftoDoc struct {
	id   string
	json string
	toks []ftoTok // document order
}

func ftoBuildDoc(id string, vals map[string]any) ftoDoc {
	m := map[string]any{"id": id}
	var toks []ftoTok
	for f, name := range ftoFields {
		v, ok := vals[name]
		if !ok {
			continue
		}
		m[name] = v
		switch tv := v.(type) {
		case string:
			for i, t := range FtsAnalyzeTerms(tv) {
				toks = append(toks, ftoTok{t, f, 0, i})
			}
		case []any:
			for e, ev := range tv {
				s, ok := ev.(string)
				if !ok {
					continue
				}
				for i, t := range FtsAnalyzeTerms(s) {
					toks = append(toks, ftoTok{t, f, e, i})
				}
			}
		}
	}
	return ftoDoc{id: id, json: ftoJSON(m), toks: toks}
}

// ftoJSON renders a document with sorted keys (values: ASCII strings, ints,
// arrays of those).
func ftoJSON(m map[string]any) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	var sb strings.Builder
	sb.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "%q:", k)
		ftoWriteVal(&sb, m[k])
	}
	sb.WriteByte('}')
	return sb.String()
}

func ftoWriteVal(sb *strings.Builder, v any) {
	switch tv := v.(type) {
	case string:
		fmt.Fprintf(sb, "%q", tv)
	case int:
		fmt.Fprintf(sb, "%d", tv)
	case []any:
		sb.WriteByte('[')
		for i, e := range tv {
			if i > 0 {
				sb.WriteByte(',')
			}
			ftoWriteVal(sb, e)
		}
		sb.WriteByte(']')
	}
}

type ftoOracle struct {
	docs    []ftoDoc // indexed docs (at least one token)
	n       float64
	avgdl   float64
	df      map[string]int
	vocab   []string  // sorted
	weights []float64 // per field; nil = unweighted
	k1, b   float64
}

func newFtoOracle(docs []ftoDoc, weights []float64) *ftoOracle {
	o := &ftoOracle{df: map[string]int{}, weights: weights, k1: bm25K1, b: bm25B}
	total := 0
	for _, d := range docs {
		if len(d.toks) == 0 {
			continue // not indexed: no tokens
		}
		o.docs = append(o.docs, d)
		total += len(d.toks)
		seen := map[string]bool{}
		for _, t := range d.toks {
			if !seen[t.term] {
				seen[t.term] = true
				o.df[t.term]++
			}
		}
	}
	o.n = float64(len(o.docs))
	o.avgdl = float64(total) / o.n
	for t := range o.df {
		o.vocab = append(o.vocab, t)
	}
	slices.Sort(o.vocab)
	return o
}

func (o *ftoOracle) idf(df int) float64 {
	return math.Log(1 + (o.n-float64(df)+0.5)/(float64(df)+0.5))
}

func (o *ftoOracle) bm25(idf, tf, dl float64) float64 {
	denom := tf + o.k1*(1-o.b+o.b*dl/o.avgdl)
	return idf * (tf * (o.k1 + 1)) / denom
}

// tfOf is the (weighted) in-scope term frequency from per-field counts.
func (o *ftoOracle) tfOf(perField []int) float64 {
	var tf float64
	for f, n := range perField {
		if n == 0 {
			continue
		}
		if o.weights != nil {
			if w := o.weights[f]; w != 0 {
				tf += w * float64(n)
			}
		} else {
			tf += float64(n)
		}
	}
	return tf
}

type ftoClause struct {
	terms    []string // phrase terms; for a prefix the terms before the stem
	stem     string
	prefix   bool
	anchored bool
	single   bool // one unanchored token: plain-term semantics
	reqBit   uint64
}

type ftoAcc struct {
	score float64
	mask  uint64
	tomb  bool
}

// eval returns the matching documents' scores for the parsed clauses under
// scope (nil = every field).
func (o *ftoOracle) eval(clauses []query.TextClause, defaultAnd bool, scope map[int]bool) map[string]float64 {
	inScope := func(f int) bool { return scope == nil || scope[f] }

	// Clause compilation: required bits, plain-term dedup, group order.
	var nextBit uint
	var reqAll uint64
	alloc := func() uint64 {
		if nextBit >= 64 {
			return 0
		}
		b := uint64(1) << nextBit
		nextBit++
		reqAll |= b
		return b
	}
	var plainOrder []string
	plainBit := map[string]uint64{}
	var pos, neg []ftoClause
	for _, c := range clauses {
		terms := FtsAnalyzeTerms(c.Raw)
		if len(terms) == 0 {
			continue
		}
		required := c.Op == query.TextMust || (c.Op == query.TextShould && defaultAnd)
		negated := c.Op == query.TextMustNot
		switch {
		case c.Prefix:
			g := ftoClause{terms: terms[:len(terms)-1], stem: terms[len(terms)-1], prefix: true, anchored: c.Anchored}
			if negated {
				neg = append(neg, g)
			} else {
				if required {
					g.reqBit = alloc()
				}
				pos = append(pos, g)
			}
		case len(terms) == 1 && !c.Phrase && !c.Anchored:
			if negated {
				neg = append(neg, ftoClause{terms: terms, single: true})
				continue
			}
			var rb uint64
			if required {
				rb = alloc()
			}
			if _, ok := plainBit[terms[0]]; ok {
				plainBit[terms[0]] |= rb
			} else {
				plainBit[terms[0]] = rb
				plainOrder = append(plainOrder, terms[0])
			}
		default:
			g := ftoClause{terms: terms, anchored: c.Anchored, single: len(terms) == 1 && !c.Anchored}
			if negated {
				neg = append(neg, g)
			} else {
				if required {
					g.reqBit = alloc()
				}
				pos = append(pos, g)
			}
		}
	}

	res := map[string]*ftoAcc{}
	add := func(id string, s float64, rb uint64) {
		if r := res[id]; r != nil {
			r.score += s
			r.mask |= rb
			return
		}
		res[id] = &ftoAcc{score: s, mask: rb}
	}
	tomb := func(id string) {
		if r := res[id]; r != nil {
			r.tomb = true
		}
	}
	dl := func(d ftoDoc) float64 { return float64(len(d.toks)) }

	// scoreTerms scores a single term or a merged prefix (match(term) picks
	// the term or the completions) with idf from df.
	scoreTerms := func(match func(string) bool, df int, rb uint64, neg bool) {
		idf := o.idf(df)
		for _, d := range o.docs {
			per := make([]int, len(ftoFields))
			hit := false
			for _, t := range d.toks {
				if match(t.term) && inScope(t.field) {
					per[t.field]++
					hit = true
				}
			}
			switch {
			case !hit:
			case neg:
				tomb(d.id)
			default:
				add(d.id, o.bm25(idf, o.tfOf(per), dl(d)), rb)
			}
		}
	}
	// occurrences of terms (then a completion when stemOK != nil) at
	// consecutive indexes of one element of one in-scope field; anchored: only
	// the one starting at the document's first token.
	occurrences := func(d ftoDoc, terms []string, stemOK func(string) bool, anchored bool) int {
		k := len(terms)
		need := k
		if stemOK != nil {
			need++
		}
		cnt := 0
		for i := 0; i+need <= len(d.toks); i++ {
			if anchored && i > 0 {
				break
			}
			t0 := d.toks[i]
			if !inScope(t0.field) {
				continue
			}
			ok := true
			for j := 0; j < need && ok; j++ {
				tj := d.toks[i+j]
				switch {
				case tj.field != t0.field || tj.elem != t0.elem || tj.idx != t0.idx+j:
					ok = false
				case j < k:
					ok = tj.term == terms[j]
				default:
					ok = stemOK(tj.term)
				}
			}
			if ok {
				cnt++
			}
		}
		return cnt
	}
	scoreGroup := func(g ftoClause, rb uint64, neg bool) {
		if g.single {
			term := g.terms[0]
			if df := o.df[term]; df > 0 {
				scoreTerms(func(t string) bool { return t == term }, df, rb, neg)
			}
			return
		}
		idfSum := 0.0
		for _, t := range g.terms {
			df := o.df[t]
			if df == 0 {
				return // a dead constituent: no occurrence anywhere
			}
			idfSum += o.idf(df)
		}
		var stemOK func(string) bool
		if g.prefix {
			stemOK = func(t string) bool { return strings.HasPrefix(t, g.stem) }
			// df_u: documents holding any completion, in scope or not — the
			// merged term's IDF is corpus-wide like a plain term's.
			dfu := 0
			for _, d := range o.docs {
				for _, t := range d.toks {
					if stemOK(t.term) {
						dfu++
						break
					}
				}
			}
			if dfu == 0 {
				return
			}
			if len(g.terms) == 0 && !g.anchored {
				scoreTerms(stemOK, dfu, rb, neg)
				return
			}
			idfSum += o.idf(dfu)
		}
		for _, d := range o.docs {
			c := occurrences(d, g.terms, stemOK, g.anchored)
			switch {
			case c == 0:
			case neg:
				tomb(d.id)
			default:
				add(d.id, o.bm25(idfSum, float64(c), dl(d)), rb)
			}
		}
	}

	for _, t := range plainOrder {
		if df := o.df[t]; df > 0 {
			term := t
			scoreTerms(func(s string) bool { return s == term }, df, plainBit[t], false)
		}
	}
	for _, g := range pos {
		scoreGroup(g, g.reqBit, false)
	}
	for _, g := range neg {
		scoreGroup(g, 0, true)
	}
	out := map[string]float64{}
	for id, r := range res {
		if r.tomb || (reqAll != 0 && r.mask&reqAll != reqAll) {
			continue
		}
		out[id] = r.score
	}
	return out
}

// ---- random corpus and queries -------------------------------------------

var ftoVocab = []string{"ab", "abc", "abd", "b", "bc", "c", "x"}

func ftoWords(r *rand.Rand, max int) string {
	w := make([]string, r.Intn(max+1))
	for i := range w {
		w[i] = ftoVocab[r.Intn(len(ftoVocab))]
	}
	return strings.Join(w, " ")
}

// ftoFieldVal draws a field value: missing, empty, token-less, an array
// (with empty, numeric and worded elements) or words.
func ftoFieldVal(r *rand.Rand) (any, bool) {
	switch r.Intn(7) {
	case 0:
		return nil, false
	case 1:
		return "", true
	case 2:
		return "!!", true
	case 3:
		arr := make([]any, r.Intn(4))
		for i := range arr {
			switch r.Intn(5) {
			case 0:
				arr[i] = ""
			case 1:
				arr[i] = 7
			default:
				arr[i] = ftoWords(r, 3)
			}
		}
		return arr, true
	default:
		return ftoWords(r, 5), true
	}
}

func ftoCorpus(r *rand.Rand, n int) []ftoDoc {
	docs := make([]ftoDoc, 0, n+8)
	for i := 0; i < n; i++ {
		vals := map[string]any{}
		for _, f := range ftoFields {
			if v, ok := ftoFieldVal(r); ok {
				vals[f] = v
			}
		}
		docs = append(docs, ftoBuildDoc(fmt.Sprintf("d%03d", i), vals))
	}
	return append(docs,
		ftoBuildDoc("e_arr_empty_first", map[string]any{"tags": []any{"", "ab b"}, "title": "c"}),
		ftoBuildDoc("e_arr_bang_first", map[string]any{"tags": []any{"!!", "ab"}}),
		ftoBuildDoc("e_arr_num_first", map[string]any{"tags": []any{7, "ab"}}),
		ftoBuildDoc("e_arr_empty_only", map[string]any{"tags": []any{""}, "title": "ab b"}),
		ftoBuildDoc("e_gaps", map[string]any{"tags": []any{"ab"}, "title": "b", "body": "c"}),
		ftoBuildDoc("e_elems", map[string]any{"tags": []any{"ab", "b c"}}),
		ftoBuildDoc("e_two_fields", map[string]any{"title": "ab b x b ab", "body": "b ab"}),
		ftoBuildDoc("e_mid_empty", map[string]any{"tags": "ab", "title": "", "body": "b"}),
	)
}

// ftoClauseStr draws one clause: plain, quoted single, phrase, anchored term
// or phrase, joined word, and (allowPrefix) every prefix form.
func ftoClauseStr(r *rand.Rand, allowPrefix bool) string {
	w := func() string { return ftoVocab[r.Intn(len(ftoVocab))] }
	stems := []string{"a", "ab", "abc", "b", "z", "x"}
	st := func() string { return stems[r.Intn(len(stems))] }
	kinds := 10
	if allowPrefix {
		kinds = 18
	}
	switch r.Intn(kinds) {
	case 0, 1:
		return w()
	case 2:
		return `"` + w() + `"`
	case 3:
		return `"` + w() + " " + w() + `"`
	case 4:
		return `"` + w() + " " + w() + " " + w() + `"`
	case 5:
		return "^" + w()
	case 6:
		return `^"` + w() + " " + w() + `"`
	case 7:
		return `^"` + w() + `"`
	case 8:
		return w() + "-" + w()
	case 9:
		return "zz" // absent term
	case 10:
		return st() + "*"
	case 11:
		return "^" + st() + "*"
	case 12:
		return `"` + w() + " " + st() + `"*`
	case 13:
		return `^"` + w() + " " + st() + `"*`
	case 14:
		return `"` + w() + " " + w() + " " + st() + `"*`
	case 15:
		return `^"` + w() + " " + w() + " " + st() + `"*`
	case 16:
		return w() + "-" + st() + "*"
	default:
		return "^" + w() + "-" + st() + "*"
	}
}

func ftoQuery(r *rand.Rand, allowPrefix bool) map[string]any {
	q := map[string]any{}
	parts := make([]string, 1+r.Intn(3))
	for i := range parts {
		parts[i] = ftoClauseStr(r, allowPrefix)
	}
	q["$search"] = strings.Join(parts, " ")
	if r.Intn(3) == 0 {
		q["$defaultOperator"] = "and"
	}
	for _, key := range []string{"$require", "$exclude"} {
		if k := r.Intn(4); k >= 2 {
			var entries []any
			for i := 0; i < k-1; i++ {
				entries = append(entries, ftoClauseStr(r, allowPrefix))
			}
			q[key] = entries
		}
	}
	scopes := [][]any{nil, nil, {"tags"}, {"title"}, {"body"}, {"tags", "body"}, {"title", "body"}, {"tags", "title"}, {"tags", "title", "body"}}
	if s := scopes[r.Intn(len(scopes))]; s != nil {
		q["$fields"] = s
	}
	return q
}

func ftoScope(q map[string]any) map[int]bool {
	v, ok := q["$fields"]
	if !ok {
		return nil
	}
	m := map[int]bool{}
	for _, f := range v.([]any) {
		m[slices.Index(ftoFields, f.(string))] = true
	}
	if len(m) == len(ftoFields) {
		return nil
	}
	return m
}

func ftoParse(t *testing.T, text map[string]any) query.Text {
	f, err := query.ParseCondition(map[string]any{"$text": text})
	require.NoError(t, err)
	return f.(query.Text)
}

// ---- engine side -----------------------------------------------------------

type ftoColl struct {
	c       *collection
	docs    []ftoDoc
	weights []float64
}

func ftoSetup(t *testing.T, fx *fixture, name string, weights map[string]float64, docs []ftoDoc) ftoColl {
	coll, err := fx.CreateCollection(ctx, name)
	require.NoError(t, err)
	info := IndexInfo{Kind: IndexKindFulltext, Fields: ftoFields}
	var w []float64
	if weights != nil {
		info.Fulltext = &FulltextParams{Weights: weights}
		for _, f := range ftoFields {
			v, ok := weights[f]
			if !ok {
				v = 1
			}
			w = append(w, v)
		}
	}
	require.NoError(t, coll.EnsureIndex(ctx, info))
	for _, d := range docs {
		insertJSON(t, coll, d.json)
	}
	return ftoColl{c: coll.(*collection), docs: docs, weights: w}
}

// ftoDriver runs the driver scan and returns score by document id.
func ftoDriver(t *testing.T, fxi *ftsIndex, tx *btree.ReadTx, text query.Text) map[string]float64 {
	s, err := fxi.searchCandidates(tx, text)
	require.NoError(t, err)
	out := map[string]float64{}
	if s == nil {
		return out
	}
	defer s.Close()
	for {
		key, score, ok, nerr := s.Next()
		require.NoError(t, nerr)
		if !ok {
			return out
		}
		id, serr := anyenc.MustParse(key).StringBytes()
		require.NoError(t, serr)
		out[string(id)] = score
	}
}

// ftoProber probes every document (indexed or not) and returns score by id.
func ftoProber(t *testing.T, c *collection, fxi *ftsIndex, tx *btree.ReadTx, text query.Text, docs []ftoDoc) map[string]float64 {
	p, err := fxi.newFtsProber(tx, text)
	require.NoError(t, err)
	out := map[string]float64{}
	for _, d := range docs {
		key := c.appendId(nil, anyenc.MustParseJson(d.json))
		score, _, ok, perr := p.Probe(key)
		require.NoError(t, perr)
		if ok {
			out[d.id] = score
		}
	}
	return out
}

func ftoHasPrefix(text query.Text) bool {
	for _, c := range text.ParsedClauses() {
		if c.Prefix {
			return true
		}
	}
	return false
}

// ftoDiff describes how got differs from want (verdicts and score bits);
// empty when identical.
func ftoDiff(want, got map[string]float64) []string {
	var out []string
	for id, w := range want {
		g, ok := got[id]
		switch {
		case !ok:
			out = append(out, fmt.Sprintf("%s: missing (want %v)", id, w))
		case math.Float64bits(w) != math.Float64bits(g):
			out = append(out, fmt.Sprintf("%s: score %v, want %v", id, g, w))
		}
	}
	for id, g := range got {
		if _, ok := want[id]; !ok {
			out = append(out, fmt.Sprintf("%s: unexpected (score %v)", id, g))
		}
	}
	slices.Sort(out)
	return out
}

// ftoCorpora are the random corpora both random tests run over: unweighted
// and BM25F with a zero weight; 300 docs span three postings chunks.
func ftoCorpora(t *testing.T, fx *fixture) []ftoColl {
	var colls []ftoColl
	for i, n := range []int{100, 300} {
		docs := ftoCorpus(rand.New(rand.NewSource(int64(i+1))), n)
		colls = append(colls,
			ftoSetup(t, fx, fmt.Sprintf("plain%d", i), nil, docs),
			ftoSetup(t, fx, fmt.Sprintf("weighted%d", i), map[string]float64{"tags": 0, "title": 3}, docs))
	}
	return colls
}

// TestFtsSearch_Oracle checks the driver scan against the oracle over random
// queries of every clause form (prefixes included), $require/$exclude,
// $defaultOperator and $fields: same documents, bit-identical scores.
func TestFtsSearch_Oracle(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish()
	r := rand.New(rand.NewSource(7))
	failures := 0
	for _, zc := range ftoCorpora(t, fx) {
		o := newFtoOracle(zc.docs, zc.weights)
		fxi := zc.c.loadFtsIndexes()[0]
		require.NoError(t, zc.c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
			n, err := ftsGetUint(tx, fxi.nsMeta, ftsMetaCount)
			require.NoError(t, err)
			require.Equal(t, o.n, float64(n), "indexed document count")
			for i := 0; i < 1500 && failures < 10; i++ {
				q := ftoQuery(r, true)
				text := ftoParse(t, q)
				want := o.eval(text.ParsedClauses(), text.DefaultAnd, ftoScope(q))
				if d := ftoDiff(want, ftoDriver(t, fxi, tx, text)); len(d) > 0 {
					failures++
					t.Errorf("%s %v: driver diverges from the oracle:\n  %s", zc.c.Name(), q, strings.Join(d, "\n  "))
				}
			}
			return nil
		}))
	}
}
