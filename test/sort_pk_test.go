package test

import (
	"cmp"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
)

// Every index key ends with the document's primary key, so a sort that
// continues an index's fields with the primary key reads in index order. These
// tests check the plan shape and compare every result with an order computed
// in Go from the inserted rows.

const (
	spRows = 600
	spTies = 5 // rows per distinct a
)

// spDoc is one row of the sort-pk fixtures.
type spDoc struct {
	id   string
	a, b int
	u    int
	name string
	text string
}

// spDocs maps a to ids through a permutation, so primary-key order differs
// from every sort order under test; name runs opposite to id.
func spDocs() []spDoc {
	rng := rand.New(rand.NewSource(1))
	perm := rng.Perm(spRows)
	uperm := rng.Perm(spRows)
	docs := make([]spDoc, spRows)
	for i := range docs {
		text := "alpha"
		if i%3 == 0 {
			text = "beta"
		}
		docs[i] = spDoc{
			id:   fmt.Sprintf("d%04d", i),
			a:    perm[i] / spTies,
			b:    rng.Intn(3),
			u:    uperm[i],
			name: fmt.Sprintf("n%04d", spRows-1-i),
			text: text,
		}
	}
	return docs
}

// spCollection creates a collection with the given indexes and inserts docs
// in a shuffled order.
func spCollection(t testing.TB, fx *fixture, name string, docs []spDoc, idx ...anystore.IndexInfo) anystore.Collection {
	t.Helper()
	coll, err := fx.CreateCollection(ctx, name)
	require.NoError(t, err)
	if len(idx) > 0 {
		require.NoError(t, coll.EnsureIndex(ctx, idx...))
	}
	vals := make([]*anyenc.Value, 0, len(docs))
	for _, i := range rand.New(rand.NewSource(2)).Perm(len(docs)) {
		d := docs[i]
		vals = append(vals, anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%q,"a":%d,"b":%d,"u":%d,"name":%q,"text":%q}`, d.id, d.a, d.b, d.u, d.name, d.text)))
	}
	require.NoError(t, coll.Insert(ctx, vals...))
	return coll
}

// spWant is the reference: the rows keep accepts, in order, cut to limit.
func spWant[D any](docs []D, keep func(D) bool, order func(x, y D) int, limit uint, id func(D) string) []string {
	var rows []D
	for _, d := range docs {
		if keep == nil || keep(d) {
			rows = append(rows, d)
		}
	}
	slices.SortFunc(rows, order)
	if limit > 0 && int(limit) < len(rows) {
		rows = rows[:limit]
	}
	out := make([]string, len(rows))
	for i, d := range rows {
		out[i] = id(d)
	}
	return out
}

func spID(d spDoc) string { return d.id }

// iteratorLine is the plan's iterator chain as Explain prints it.
func iteratorLine(t testing.TB, ex anystore.Explain) string {
	t.Helper()
	for line := range strings.SplitSeq(ex.Plan, "\n") {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "Iterator: "); ok {
			return rest
		}
	}
	require.Failf(t, "no Iterator line", "plan:\n%s", ex.Plan)
	return ""
}

// sortsInMemory reports whether the chain orders rows with a SortIter.
func sortsInMemory(line string) bool {
	return strings.Contains(line, "TopK(") || strings.Contains(line, "-> Sort")
}

type spPlan int

const (
	spIndexOrder spPlan = iota // an index scan yields the sort order
	spInMemory                 // a SortIter orders the rows
)

type spCase struct {
	filter any
	sort   []any
	plan   spPlan
	keep   func(spDoc) bool
	order  func(x, y spDoc) int
}

// spCheck runs tc with no limit and with a limit that cuts a tie group, and
// asserts the plan shape and the row order.
func spCheck(t *testing.T, coll anystore.Collection, docs []spDoc, tc spCase) {
	t.Helper()
	for _, limit := range []uint{0, 7} {
		name := fmt.Sprintf("filter=%v sort=%v limit=%d", tc.filter, tc.sort, limit)
		q := coll.Find(tc.filter).Sort(tc.sort...)
		if limit > 0 {
			q = q.Limit(limit)
		}
		ex, err := q.Explain(ctx)
		require.NoError(t, err, name)
		line := iteratorLine(t, ex)
		t.Logf("%s: %s", name, line)
		switch tc.plan {
		case spIndexOrder:
			assert.Contains(t, line, "IndexScan(", name)
			assert.False(t, sortsInMemory(line), "%s: %s", name, line)
		case spInMemory:
			assert.True(t, sortsInMemory(line), "%s: %s", name, line)
		}
		assert.Equal(t, spWant(docs, tc.keep, tc.order, limit, spID), collectIdsString(t, q), name)
	}
}

func sortArgs(fields ...string) []any {
	out := make([]any, len(fields))
	for i, f := range fields {
		out[i] = f
	}
	return out
}

// A single-field index serves a sort that continues with the primary key in
// the same relation to the index direction; a mixed relation sorts in memory.
func TestSortPk_SingleIndex(t *testing.T) {
	docs := spDocs()
	fx := newFixture(t)
	coll := spCollection(t, fx, "single", docs, anystore.IndexInfo{Fields: []string{"a"}})
	const pinned = 42
	for _, tc := range []spCase{
		{sort: sortArgs("a", "id"), plan: spIndexOrder, order: func(x, y spDoc) int {
			return cmp.Or(cmp.Compare(x.a, y.a), cmp.Compare(x.id, y.id))
		}},
		{sort: sortArgs("-a", "-id"), plan: spIndexOrder, order: func(x, y spDoc) int {
			return cmp.Or(cmp.Compare(y.a, x.a), cmp.Compare(y.id, x.id))
		}},
		{sort: sortArgs("-a", "id"), plan: spInMemory, order: func(x, y spDoc) int {
			return cmp.Or(cmp.Compare(y.a, x.a), cmp.Compare(x.id, y.id))
		}},
		{sort: sortArgs("a", "-id"), plan: spInMemory, order: func(x, y spDoc) int {
			return cmp.Or(cmp.Compare(x.a, y.a), cmp.Compare(y.id, x.id))
		}},
		// The primary key is unique: the fields after it never reorder rows.
		{sort: sortArgs("a", "id", "name"), plan: spIndexOrder, order: func(x, y spDoc) int {
			return cmp.Or(cmp.Compare(x.a, y.a), cmp.Compare(x.id, y.id))
		}},
		// Every declared field pinned: the seek yields primary-key order.
		{filter: fmt.Sprintf(`{"a":%d}`, pinned), sort: sortArgs("id"), plan: spIndexOrder,
			keep:  func(d spDoc) bool { return d.a == pinned },
			order: func(x, y spDoc) int { return cmp.Compare(x.id, y.id) }},
		{filter: fmt.Sprintf(`{"a":%d}`, pinned), sort: sortArgs("-id"), plan: spIndexOrder,
			keep:  func(d spDoc) bool { return d.a == pinned },
			order: func(x, y spDoc) int { return cmp.Compare(y.id, x.id) }},
		// A range keeps the tiebreak: rows stay in (a, id) order inside the bounds.
		{filter: `{"a":{"$gte":10,"$lt":30}}`, sort: sortArgs("-a", "-id"), plan: spIndexOrder,
			keep:  func(d spDoc) bool { return d.a >= 10 && d.a < 30 },
			order: func(x, y spDoc) int { return cmp.Or(cmp.Compare(y.a, x.a), cmp.Compare(y.id, x.id)) }},
	} {
		spCheck(t, coll, docs, tc)
	}
}

// A descending index stores a inverted and the primary key ascending, so its
// forward scan yields (-a, id) and its reverse scan (a, -id).
func TestSortPk_DescendingIndex(t *testing.T) {
	docs := spDocs()
	fx := newFixture(t)
	coll := spCollection(t, fx, "desc", docs, anystore.IndexInfo{Fields: []string{"-a"}})
	for _, tc := range []spCase{
		{sort: sortArgs("-a", "id"), plan: spIndexOrder, order: func(x, y spDoc) int {
			return cmp.Or(cmp.Compare(y.a, x.a), cmp.Compare(x.id, y.id))
		}},
		{sort: sortArgs("a", "-id"), plan: spIndexOrder, order: func(x, y spDoc) int {
			return cmp.Or(cmp.Compare(x.a, y.a), cmp.Compare(y.id, x.id))
		}},
		{sort: sortArgs("-a", "-id"), plan: spInMemory, order: func(x, y spDoc) int {
			return cmp.Or(cmp.Compare(y.a, x.a), cmp.Compare(y.id, x.id))
		}},
		{sort: sortArgs("a", "id"), plan: spInMemory, order: func(x, y spDoc) int {
			return cmp.Or(cmp.Compare(x.a, y.a), cmp.Compare(x.id, y.id))
		}},
	} {
		spCheck(t, coll, docs, tc)
	}
}

// On a compound index the primary key is the column after the last declared
// field: it continues a full-width sort or one past an equality prefix, and
// skipping a declared field sorts in memory.
func TestSortPk_CompoundIndex(t *testing.T) {
	docs := spDocs()
	fx := newFixture(t)
	coll := spCollection(t, fx, "compound", docs, anystore.IndexInfo{Fields: []string{"a", "b"}})
	const pa, pb = 42, 1
	keepA := func(d spDoc) bool { return d.a == pa }
	for _, tc := range []spCase{
		{sort: sortArgs("a", "b", "id"), plan: spIndexOrder, order: func(x, y spDoc) int {
			return cmp.Or(cmp.Compare(x.a, y.a), cmp.Compare(x.b, y.b), cmp.Compare(x.id, y.id))
		}},
		{sort: sortArgs("-a", "-b", "-id"), plan: spIndexOrder, order: func(x, y spDoc) int {
			return cmp.Or(cmp.Compare(y.a, x.a), cmp.Compare(y.b, x.b), cmp.Compare(y.id, x.id))
		}},
		{sort: sortArgs("a", "id"), plan: spInMemory, order: func(x, y spDoc) int {
			return cmp.Or(cmp.Compare(x.a, y.a), cmp.Compare(x.id, y.id))
		}},
		{filter: fmt.Sprintf(`{"a":%d}`, pa), sort: sortArgs("b", "id"), plan: spIndexOrder, keep: keepA,
			order: func(x, y spDoc) int { return cmp.Or(cmp.Compare(x.b, y.b), cmp.Compare(x.id, y.id)) }},
		{filter: fmt.Sprintf(`{"a":%d}`, pa), sort: sortArgs("-b", "-id"), plan: spIndexOrder, keep: keepA,
			order: func(x, y spDoc) int { return cmp.Or(cmp.Compare(y.b, x.b), cmp.Compare(y.id, x.id)) }},
		// b is free within a=pa, so the index order is (b, id), not id.
		{filter: fmt.Sprintf(`{"a":%d}`, pa), sort: sortArgs("id"), plan: spInMemory, keep: keepA,
			order: func(x, y spDoc) int { return cmp.Compare(x.id, y.id) }},
		// Both declared fields pinned: the seek yields primary-key order.
		{filter: fmt.Sprintf(`{"a":%d,"b":%d}`, pa, pb), sort: sortArgs("id"), plan: spIndexOrder,
			keep:  func(d spDoc) bool { return d.a == pa && d.b == pb },
			order: func(x, y spDoc) int { return cmp.Compare(x.id, y.id) }},
	} {
		spCheck(t, coll, docs, tc)
	}
}

// A unique index serves a sort continued by the primary key.
func TestSortPk_UniqueIndex(t *testing.T) {
	docs := spDocs()
	fx := newFixture(t)
	coll := spCollection(t, fx, "unique", docs, anystore.IndexInfo{Fields: []string{"u"}, Unique: true})
	for _, tc := range []spCase{
		{sort: sortArgs("u", "id"), plan: spIndexOrder, order: func(x, y spDoc) int {
			return cmp.Or(cmp.Compare(x.u, y.u), cmp.Compare(x.id, y.id))
		}},
		{sort: sortArgs("-u", "-id"), plan: spIndexOrder, order: func(x, y spDoc) int {
			return cmp.Or(cmp.Compare(y.u, x.u), cmp.Compare(y.id, x.id))
		}},
	} {
		spCheck(t, coll, docs, tc)
	}
}

// The trailing index column is the collection's own primary key: with
// PrimaryKey "uuid" a sort continued by uuid is index-served, and one
// continued by a plain field named id sorts in memory.
func TestSortPk_CustomPrimaryKey(t *testing.T) {
	type doc struct {
		uuid, id string
		a        int
	}
	rng := rand.New(rand.NewSource(3))
	perm, idPerm := rng.Perm(spRows), rng.Perm(spRows)
	docs := make([]doc, spRows)
	vals := make([]*anyenc.Value, spRows)
	for i := range docs {
		docs[i] = doc{uuid: fmt.Sprintf("u%04d", i), id: fmt.Sprintf("x%04d", idPerm[i]), a: perm[i] / spTies}
		vals[i] = anyenc.MustParseJson(fmt.Sprintf(`{"uuid":%q,"id":%q,"a":%d}`, docs[i].uuid, docs[i].id, docs[i].a))
	}
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "custompk", anystore.CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx, vals...))

	for _, tc := range []struct {
		sort  []any
		plan  spPlan
		order func(x, y doc) int
	}{
		{sortArgs("a", "uuid"), spIndexOrder, func(x, y doc) int {
			return cmp.Or(cmp.Compare(x.a, y.a), cmp.Compare(x.uuid, y.uuid))
		}},
		{sortArgs("-a", "-uuid"), spIndexOrder, func(x, y doc) int {
			return cmp.Or(cmp.Compare(y.a, x.a), cmp.Compare(y.uuid, x.uuid))
		}},
		{sortArgs("a", "id"), spInMemory, func(x, y doc) int {
			return cmp.Or(cmp.Compare(x.a, y.a), cmp.Compare(x.id, y.id))
		}},
		{sortArgs("-a", "-id"), spInMemory, func(x, y doc) int {
			return cmp.Or(cmp.Compare(y.a, x.a), cmp.Compare(y.id, x.id))
		}},
	} {
		for _, limit := range []uint{0, 7} {
			name := fmt.Sprintf("sort=%v limit=%d", tc.sort, limit)
			q := coll.Find(nil).Sort(tc.sort...)
			if limit > 0 {
				q = q.Limit(limit)
			}
			ex, err := q.Explain(ctx)
			require.NoError(t, err, name)
			line := iteratorLine(t, ex)
			t.Logf("%s: %s", name, line)
			if tc.plan == spIndexOrder {
				assert.Contains(t, line, "IndexScan(a)", name)
				assert.False(t, sortsInMemory(line), "%s: %s", name, line)
			} else {
				assert.True(t, sortsInMemory(line), "%s: %s", name, line)
			}
			want := spWant(docs, nil, tc.order, limit, func(d doc) string { return d.uuid })
			assert.Equal(t, want, spStrings(t, q, "uuid"), name)
		}
	}
}

// spStrings collects one string field per result row.
func spStrings(t testing.TB, q anystore.Query, field string) []string {
	t.Helper()
	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var out []string
	for iter.Next() {
		d, err := iter.Doc()
		require.NoError(t, err)
		out = append(out, d.Value().GetString(field))
	}
	require.NoError(t, iter.Err())
	return out
}

// Numeric primary keys order as numbers inside the index tiebreak, negative
// ids and ids of different digit counts included.
func TestSortPk_NumericIds(t *testing.T) {
	type doc struct{ id, a int }
	perm := rand.New(rand.NewSource(4)).Perm(spRows)
	docs := make([]doc, spRows)
	vals := make([]*anyenc.Value, spRows)
	for i := range docs {
		docs[i] = doc{id: i*7 - 1500, a: perm[i] / spTies}
		vals[i] = anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, docs[i].id, docs[i].a))
	}
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "numeric")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx, vals...))

	for _, tc := range []struct {
		sort  []any
		order func(x, y doc) int
	}{
		{sortArgs("a", "id"), func(x, y doc) int { return cmp.Or(cmp.Compare(x.a, y.a), cmp.Compare(x.id, y.id)) }},
		{sortArgs("-a", "-id"), func(x, y doc) int { return cmp.Or(cmp.Compare(y.a, x.a), cmp.Compare(y.id, x.id)) }},
	} {
		for _, limit := range []uint{0, 7} {
			name := fmt.Sprintf("sort=%v limit=%d", tc.sort, limit)
			q := coll.Find(nil).Sort(tc.sort...)
			if limit > 0 {
				q = q.Limit(limit)
			}
			ex, err := q.Explain(ctx)
			require.NoError(t, err, name)
			line := iteratorLine(t, ex)
			t.Logf("%s: %s", name, line)
			assert.Contains(t, line, "IndexScan(a)", name)
			assert.False(t, sortsInMemory(line), "%s: %s", name, line)

			rows := slices.Clone(docs)
			slices.SortFunc(rows, tc.order)
			if limit > 0 {
				rows = rows[:limit]
			}
			want := make([]int, len(rows))
			for i, d := range rows {
				want[i] = d.id
			}
			assert.Equal(t, want, collectIds(t, q), name)
		}
	}
}

// Over a multikey index a sort continued by the primary key returns what a
// full scan returns, whatever plan the index gets.
func TestSortPk_MultiKey(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	vals := make([]*anyenc.Value, spRows)
	for i := range vals {
		var tags string
		switch {
		case i%97 == 0:
			tags = `,"tags":[]`
		case i%89 == 0:
			tags = "" // field missing
		case i%4 == 0:
			tags = fmt.Sprintf(`,"tags":%d`, rng.Intn(20))
		default:
			parts := make([]string, 1+rng.Intn(3))
			for j := range parts {
				parts[j] = fmt.Sprint(rng.Intn(20))
			}
			tags = `,"tags":[` + strings.Join(parts, ",") + `]`
		}
		vals[i] = anyenc.MustParseJson(fmt.Sprintf(`{"id":"d%04d"%s}`, i, tags))
	}
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "multikey")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"tags"}}))
	require.NoError(t, coll.Insert(ctx, vals...))

	noIndex := anystore.IndexHint{IndexName: "tags", Boost: -1_000_000}
	for _, sort := range [][]any{sortArgs("tags", "id"), sortArgs("-tags", "-id")} {
		for _, limit := range []uint{0, 7} {
			name := fmt.Sprintf("sort=%v limit=%d", sort, limit)
			q := coll.Find(nil).Sort(sort...)
			ref := coll.Find(nil).Sort(sort...).IndexHint(noIndex)
			if limit > 0 {
				q, ref = q.Limit(limit), ref.Limit(limit)
			}
			ex, err := q.Explain(ctx)
			require.NoError(t, err, name)
			refEx, err := ref.Explain(ctx)
			require.NoError(t, err, name)
			line, refLine := iteratorLine(t, ex), iteratorLine(t, refEx)
			t.Logf("%s: %s | reference: %s", name, line, refLine)
			require.True(t, strings.HasPrefix(refLine, "FullScan"), "%s: reference plan %s", name, refLine)

			want := collectIdsString(t, ref)
			if limit == 0 {
				require.Len(t, want, spRows, name)
			}
			assert.Equal(t, want, collectIdsString(t, q), name)
		}
	}
}

// An aggregation's pushed $sort continued by the primary key reads the index
// in order and keeps the $limit off a TopK.
func TestSortPk_Aggregate(t *testing.T) {
	docs := spDocs()
	fx := newFixture(t)
	coll := spCollection(t, fx, "agg", docs, anystore.IndexInfo{Fields: []string{"a"}})
	agg := coll.Aggregate(`[{"$sort":{"a":-1,"id":-1}},{"$limit":10}]`)
	ex, err := agg.Explain(ctx)
	require.NoError(t, err)
	line := iteratorLine(t, ex)
	t.Logf("aggregate: %s", line)
	assert.Contains(t, line, "IndexScan(a)(reverse)")
	assert.False(t, sortsInMemory(line), line)

	iter, err := agg.Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var got []string
	for iter.Next() {
		d, err := iter.Doc()
		require.NoError(t, err)
		got = append(got, d.Value().GetString("id"))
	}
	require.NoError(t, iter.Err())
	want := spWant(docs, nil, func(x, y spDoc) int {
		return cmp.Or(cmp.Compare(y.a, x.a), cmp.Compare(y.id, x.id))
	}, 10, spID)
	assert.Equal(t, want, got)
}

// A $text query with a range on the sort field keeps the (a, id) order.
func TestSortPk_Text(t *testing.T) {
	docs := spDocs()
	fx := newFixture(t)
	coll := spCollection(t, fx, "text", docs,
		anystore.IndexInfo{Kind: anystore.IndexKindFulltext, Fields: []string{"text"}},
		anystore.IndexInfo{Fields: []string{"a"}},
	)
	keep := func(d spDoc) bool { return d.text == "alpha" && d.a > 30 }
	for _, tc := range []struct {
		sort  []any
		order func(x, y spDoc) int
	}{
		{sortArgs("a", "id"), func(x, y spDoc) int { return cmp.Or(cmp.Compare(x.a, y.a), cmp.Compare(x.id, y.id)) }},
		{sortArgs("-a", "-id"), func(x, y spDoc) int { return cmp.Or(cmp.Compare(y.a, x.a), cmp.Compare(y.id, x.id)) }},
	} {
		for _, limit := range []uint{0, 7} {
			name := fmt.Sprintf("sort=%v limit=%d", tc.sort, limit)
			q := coll.Find(`{"$text":{"$search":"alpha"},"a":{"$gt":30}}`).Sort(tc.sort...)
			if limit > 0 {
				q = q.Limit(limit)
			}
			ex, err := q.Explain(ctx)
			require.NoError(t, err, name)
			t.Logf("%s: %s", name, iteratorLine(t, ex))
			assert.Equal(t, spWant(docs, keep, tc.order, limit, spID), collectIdsString(t, q), name)
		}
	}
}

// A sort that starts with the primary key is the full scan's own order, so no
// SortIter follows it whatever fields come after.
func TestSortPk_FullScanPkPrefix(t *testing.T) {
	docs := spDocs()
	fx := newFixture(t)
	coll := spCollection(t, fx, "pkprefix", docs, anystore.IndexInfo{Fields: []string{"a"}})
	for _, tc := range []struct {
		sort []any
		desc bool
	}{
		{sortArgs("id", "a"), false},
		{sortArgs("-id", "a"), true},
		{sortArgs("id", "-a", "name"), false},
	} {
		for _, limit := range []uint{0, 7} {
			name := fmt.Sprintf("sort=%v limit=%d", tc.sort, limit)
			q := coll.Find(nil).Sort(tc.sort...)
			if limit > 0 {
				q = q.Limit(limit)
			}
			ex, err := q.Explain(ctx)
			require.NoError(t, err, name)
			line := iteratorLine(t, ex)
			t.Logf("%s: %s", name, line)
			assert.True(t, strings.HasPrefix(line, "FullScan"), "%s: %s", name, line)
			assert.False(t, sortsInMemory(line), "%s: %s", name, line)
			order := func(x, y spDoc) int { return cmp.Compare(x.id, y.id) }
			if tc.desc {
				order = func(x, y spDoc) int { return cmp.Compare(y.id, x.id) }
			}
			assert.Equal(t, spWant(docs, nil, order, limit, spID), collectIdsString(t, q), name)
		}
	}
}

// keysetDoc is one row of the keyset-pagination fixtures.
type keysetDoc struct {
	id string
	at int
}

// keysetDocs builds n rows with ties rows per modifiedAt, the modifiedAt-to-id
// mapping permuted.
func keysetDocs(n, ties int) []keysetDoc {
	perm := rand.New(rand.NewSource(6)).Perm(n)
	docs := make([]keysetDoc, n)
	for i := range docs {
		docs[i] = keysetDoc{id: fmt.Sprintf("k%05d", i), at: 1_700_000_000 + perm[i]/ties}
	}
	return docs
}

func keysetCollection(t testing.TB, docs []keysetDoc) anystore.Collection {
	t.Helper()
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "keyset")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"modifiedAt"}}))
	order := rand.New(rand.NewSource(7)).Perm(len(docs))
	for start := 0; start < len(order); start += 1000 {
		var vals []*anyenc.Value
		for _, i := range order[start:min(start+1000, len(order))] {
			vals = append(vals, anyenc.MustParseJson(fmt.Sprintf(`{"id":%q,"modifiedAt":%d}`, docs[i].id, docs[i].at)))
		}
		require.NoError(t, coll.Insert(ctx, vals...))
	}
	return coll
}

func keysetOrder(desc bool) func(x, y keysetDoc) int {
	if desc {
		return func(x, y keysetDoc) int { return cmp.Or(cmp.Compare(y.at, x.at), cmp.Compare(y.id, x.id)) }
	}
	return func(x, y keysetDoc) int { return cmp.Or(cmp.Compare(x.at, y.at), cmp.Compare(x.id, y.id)) }
}

// keysetQuery is the page after cursor (the first page when cursor is nil).
func keysetQuery(coll anystore.Collection, cursor *keysetDoc, desc bool, limit uint) anystore.Query {
	var filter any
	if cursor != nil {
		op := "$gt"
		if desc {
			op = "$lt"
		}
		filter = fmt.Sprintf(`{"$or":[{"modifiedAt":{%q:%d}},{"modifiedAt":%d,"id":{%q:%q}}]}`,
			op, cursor.at, cursor.at, op, cursor.id)
	}
	sort := sortArgs("modifiedAt", "id")
	if desc {
		sort = sortArgs("-modifiedAt", "-id")
	}
	return coll.Find(filter).Sort(sort...).Limit(limit)
}

func keysetRows(t testing.TB, q anystore.Query) []keysetDoc {
	t.Helper()
	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var out []keysetDoc
	for iter.Next() {
		d, err := iter.Doc()
		require.NoError(t, err)
		out = append(out, keysetDoc{id: d.Value().GetString("id"), at: d.Value().GetInt("modifiedAt")})
	}
	require.NoError(t, iter.Err())
	return out
}

// Keyset pages over (modifiedAt, id) read the index in order: they concatenate
// to the reference order with no gap or duplicate, no page sorts in memory,
// and a page fetches at most its rows, the rest of the cursor's tie group and
// the one row the limit pulls past its end.
func TestKeyset_Pagination(t *testing.T) {
	const n, ties, limit = 2000, 5, 50
	docs := keysetDocs(n, ties)
	coll := keysetCollection(t, docs)
	qplannerEnableCounters(t) // process-global: this test must not run in parallel

	for _, desc := range []bool{true, false} {
		t.Run(fmt.Sprintf("desc=%v", desc), func(t *testing.T) {
			want := slices.Clone(docs)
			slices.SortFunc(want, keysetOrder(desc))

			var got []keysetDoc
			var cursor *keysetDoc
			var maxFetched uint64
			for page := 0; ; page++ {
				require.LessOrEqual(t, page, n/limit, "pagination does not end")
				q := keysetQuery(coll, cursor, desc, limit)
				ex, err := q.Explain(ctx)
				require.NoError(t, err)
				line := iteratorLine(t, ex)
				if page < 2 {
					t.Logf("page %d: %s", page, line)
				}
				assert.Contains(t, line, "IndexScan(modifiedAt)", "page %d", page)
				assert.False(t, sortsInMemory(line), "page %d: %s", page, line)

				anystore.ResetPipelinePerfCounters()
				rows := keysetRows(t, q)
				fetched := qplannerSnapshot().Planner.FetchYields
				assert.GreaterOrEqual(t, fetched, uint64(len(rows)), "page %d: every returned row is fetched", page)
				assert.LessOrEqual(t, fetched, uint64(limit+ties+1), "page %d fetched %d rows", page, fetched)
				maxFetched = max(maxFetched, fetched)

				got = append(got, rows...)
				if len(rows) < limit {
					break
				}
				cursor = &rows[len(rows)-1]
			}

			t.Logf("%d pages, at most %d rows fetched per page", len(got)/limit+1, maxFetched)
			seen := make(map[string]bool, len(got))
			for _, d := range got {
				assert.False(t, seen[d.id], "duplicate %s", d.id)
				seen[d.id] = true
			}
			assert.Equal(t, want, got)
		})
	}
}

// BenchmarkKeysetPage reads one descending keyset page from the middle of a
// 20k-row collection.
func BenchmarkKeysetPage(b *testing.B) {
	const n, ties, limit = 20000, 5, 50
	docs := keysetDocs(n, ties)
	coll := keysetCollection(b, docs)
	ref := slices.Clone(docs)
	slices.SortFunc(ref, keysetOrder(true))
	cursor := ref[n/2+2] // inside a tie group
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		iter, err := keysetQuery(coll, &cursor, true, limit).Iter(ctx)
		if err != nil {
			b.Fatal(err)
		}
		rows := 0
		for iter.Next() {
			if _, err := iter.Doc(); err != nil {
				b.Fatal(err)
			}
			rows++
		}
		if err := iter.Err(); err != nil {
			b.Fatal(err)
		}
		iter.Close()
		if rows != limit {
			b.Fatalf("page has %d rows, want %d", rows, limit)
		}
	}
}
