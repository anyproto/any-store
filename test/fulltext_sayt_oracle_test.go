package test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/stretchr/testify/require"
)

// TestFtsSayt_FieldScopeOracle checks $fields scoping against an independent
// reference: 400 documents over three indexed fields, each missing, a string
// or an array of 1-3 strings, queried under five scopes with nine clause kinds
// (term, phrase, phrase-prefix, prefix, anchored term / phrase / prefix /
// phrase-prefix). The reference evaluates every clause per field element, so
// it does not share positionsOf / postingTF with the engine the way the
// driver/probe differential does. Every element holds at least one token;
// token-less elements are covered by the root package's scoring oracle.
func TestFtsSayt_FieldScopeOracle(t *testing.T) {
	rng := &oracleRand{s: 7}
	vocab := []string{"a", "b", "c", "ab"}
	fields := []string{"f0", "f1", "f2"}
	type doc struct {
		id    string
		elems [3][][]string // per field, per array element, its terms
	}
	var docs []doc
	var jsons []string
	for i := 0; i < 400; i++ {
		d := doc{id: fmt.Sprintf("d%03d", i)}
		js := fmt.Sprintf(`{"id":%q`, d.id)
		for f := 0; f < 3; f++ {
			kind := rng.Intn(3) // 0 missing, 1 string, 2 array
			if kind == 0 {
				continue
			}
			ne := 1
			if kind == 2 {
				ne = 1 + rng.Intn(3)
			}
			var strs []string
			for e := 0; e < ne; e++ {
				nt := 1 + rng.Intn(3)
				var terms []string
				for k := 0; k < nt; k++ {
					terms = append(terms, vocab[rng.Intn(len(vocab))])
				}
				d.elems[f] = append(d.elems[f], terms)
				strs = append(strs, fmt.Sprintf("%q", strings.Join(terms, " ")))
			}
			if kind == 1 {
				js += fmt.Sprintf(`,%q:%s`, fields[f], strs[0])
			} else {
				js += fmt.Sprintf(`,%q:[%s]`, fields[f], strings.Join(strs, ","))
			}
		}
		js += "}"
		docs = append(docs, d)
		jsons = append(jsons, js)
	}

	fx := newFixture(t)
	defer fx.finish()
	coll, err := fx.CreateCollection(ctx, "oracle")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Kind: anystore.IndexKindFulltext, Fields: fields}))
	insertJSON(t, coll, jsons...)

	scopes := [][]string{{"f0"}, {"f1"}, {"f2"}, {"f0", "f2"}, {"f1", "f2"}}
	queries := []struct {
		search   string
		terms    []string
		prefix   bool // the last term is a stem
		anchored bool
	}{
		{`"a b"`, []string{"a", "b"}, false, false},
		{`"b a c"`, []string{"b", "a", "c"}, false, false},
		{`"c a"*`, []string{"c", "a"}, true, false},
		{`^a`, []string{"a"}, false, true},
		{`^"a b"`, []string{"a", "b"}, false, true},
		{`^"b a"*`, []string{"b", "a"}, true, true},
		{`^a*`, []string{"a"}, true, true},
		{`a*`, []string{"a"}, true, false},
		{`c`, []string{"c"}, false, false},
	}

	// matches reports whether d holds the clause inside one in-scope array
	// element (elements never join into a phrase); an anchored clause must
	// start at the document's first token: the first element of the first
	// present field, and that field must be in scope.
	matches := func(d doc, scope []string, terms []string, prefix, anchored bool) bool {
		termEq := func(have, want string, last bool) bool {
			if last && prefix {
				return strings.HasPrefix(have, want)
			}
			return have == want
		}
		first := true
		for f := 0; f < 3; f++ {
			inScope := slices.Contains(scope, fields[f])
			for _, el := range d.elems[f] {
				for st := 0; st+len(terms) <= len(el); st++ {
					if anchored && !(first && st == 0) {
						break
					}
					ok := inScope
					for k := 0; ok && k < len(terms); k++ {
						ok = termEq(el[st+k], terms[k], k == len(terms)-1)
					}
					if ok {
						return true
					}
				}
				first = false
			}
		}
		return false
	}

	nonEmpty := 0
	for _, sc := range scopes {
		scAny := make([]any, len(sc))
		for i := range sc {
			scAny[i] = sc[i]
		}
		for _, q := range queries {
			var want []string
			for _, d := range docs {
				if matches(d, sc, q.terms, q.prefix, q.anchored) {
					want = append(want, d.id)
				}
			}
			if len(want) > 0 {
				nonEmpty++
			}
			ids, _ := collectIter(t, coll.Find(map[string]any{"$text": map[string]any{"$search": q.search, "$fields": scAny}}))
			got := sortedIDs(ids)
			if !slices.Equal(got, want) {
				t.Errorf("$fields %v, %s: engine %d rows, oracle %d rows\n  engine: %v\n  oracle: %v", sc, q.search, len(got), len(want), got, want)
			}
		}
	}
	require.Greater(t, nonEmpty, len(scopes)*len(queries)*3/4, "the corpus must give most cases a non-empty answer")
}

// oracleRand is a fixed-sequence LCG, so the corpus is the same on every run.
type oracleRand struct{ s uint64 }

func (r *oracleRand) Intn(n int) int {
	r.s = r.s*6364136223846793005 + 1442695040888963407
	return int((r.s >> 33) % uint64(n))
}
