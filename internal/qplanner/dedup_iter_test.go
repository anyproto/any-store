package qplanner

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/query"
)

// fakeIter is a minimal Iterator used to feed canned (key, docId) hits.
// It also emulates FetchIter's contract of populating Plan.DocParsed.
type fakeIter struct {
	hits []fakeHit
	i    int
	plan *Plan
}

type fakeHit struct {
	key   []byte
	docId []byte
	doc   *anyenc.Value
}

func (f *fakeIter) Next() ([]byte, []byte, error) {
	if f.i >= len(f.hits) {
		return nil, nil, nil
	}
	h := f.hits[f.i]
	f.i++
	if f.plan != nil {
		f.plan.DocParsed = h.doc
	}
	return h.key, h.docId, nil
}

func (f *fakeIter) Close()         {}
func (f *fakeIter) String() string { return "fake" }

// encodeKey encodes a single scalar anyenc value as an index-key prefix +
// a raw docId suffix, matching the layout produced by IndexIter for a
// single-field index.
func encodeKey(val *anyenc.Value, docId string) []byte {
	out := val.MarshalTo(nil)
	return append(out, []byte(docId)...)
}

func TestCanonicalKeyDedupIter_SingleDocMultipleMatches(t *testing.T) {
	a := &anyenc.Arena{}
	arr := a.NewArray()
	arr.SetArrayItem(0, a.NewString("ai"))
	arr.SetArrayItem(1, a.NewString("theory"))
	arr.SetArrayItem(2, a.NewString("philosophy"))
	doc := a.NewObject()
	doc.Set("id", a.NewString("p1"))
	doc.Set("tags", arr)

	k1 := encodeKey(a.NewString("ai"), "p1")
	k2 := encodeKey(a.NewString("theory"), "p1")

	bs := query.Bounds{
		{Start: a.NewString("ai").MarshalTo(nil), End: a.NewString("ai").MarshalTo(nil), StartInclude: true, EndInclude: true},
		{Start: a.NewString("theory").MarshalTo(nil), End: a.NewString("theory").MarshalTo(nil), StartInclude: true, EndInclude: true},
	}

	plan := &Plan{}
	upstream := &fakeIter{
		plan: plan,
		hits: []fakeHit{
			{key: k1, docId: []byte("p1"), doc: doc},
			{key: k2, docId: []byte("p1"), doc: doc},
		},
	}

	it := &CanonicalKeyDedupIter{
		Source:    upstream,
		Plan:      plan,
		Bounds:    bs,
		FieldPath: []string{"tags"},
	}

	var got [][]byte
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, append([]byte(nil), docId...))
	}
	assert.Equal(t, [][]byte{[]byte("p1")}, got,
		"doc p1 must be emitted exactly once (at the canonical 'ai' hit)")
}

func TestCanonicalKeyDedupIter_ReverseScan(t *testing.T) {
	a := &anyenc.Arena{}
	arr := a.NewArray()
	arr.SetArrayItem(0, a.NewString("ai"))
	arr.SetArrayItem(1, a.NewString("theory"))
	doc := a.NewObject()
	doc.Set("id", a.NewString("p1"))
	doc.Set("tags", arr)

	k2 := encodeKey(a.NewString("theory"), "p1")
	k1 := encodeKey(a.NewString("ai"), "p1")

	bs := query.Bounds{
		{Start: a.NewString("ai").MarshalTo(nil), End: a.NewString("ai").MarshalTo(nil), StartInclude: true, EndInclude: true},
		{Start: a.NewString("theory").MarshalTo(nil), End: a.NewString("theory").MarshalTo(nil), StartInclude: true, EndInclude: true},
	}

	plan := &Plan{}
	// Reverse scan order: theory first, ai second.
	upstream := &fakeIter{
		plan: plan,
		hits: []fakeHit{
			{key: k2, docId: []byte("p1"), doc: doc},
			{key: k1, docId: []byte("p1"), doc: doc},
		},
	}

	it := &CanonicalKeyDedupIter{
		Source: upstream, Plan: plan, Bounds: bs,
		FieldPath: []string{"tags"}, Reverse: true,
	}

	var got [][]byte
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, append([]byte(nil), docId...))
	}
	assert.Equal(t, [][]byte{[]byte("p1")}, got,
		"reverse scan: emit at 'theory' (max), skip 'ai'")
}

func TestCanonicalKeyDedupIter_ScalarPassthrough(t *testing.T) {
	a := &anyenc.Arena{}
	doc := a.NewObject()
	doc.Set("id", a.NewString("p1"))
	doc.Set("status", a.NewString("draft")) // scalar, not array
	k := encodeKey(a.NewString("draft"), "p1")

	plan := &Plan{}
	upstream := &fakeIter{plan: plan, hits: []fakeHit{
		{key: k, docId: []byte("p1"), doc: doc},
	}}
	it := &CanonicalKeyDedupIter{
		Source: upstream, Plan: plan,
		FieldPath: []string{"status"},
	}

	_, docId, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, []byte("p1"), docId, "scalar field: always emit")

	_, docId2, err := it.Next()
	require.NoError(t, err)
	assert.Nil(t, docId2)
}

func TestCanonicalKeyDedupIter_RangeBounds(t *testing.T) {
	a := &anyenc.Arena{}
	arr := a.NewArray()
	arr.SetArrayItem(0, a.NewString("a"))
	arr.SetArrayItem(1, a.NewString("b"))
	arr.SetArrayItem(2, a.NewString("c"))
	doc := a.NewObject()
	doc.Set("id", a.NewString("p1"))
	doc.Set("tags", arr)

	// $gte "a", $lte "c" — single inclusive range hitting all three tags.
	bs := query.Bounds{{
		Start:        a.NewString("a").MarshalTo(nil),
		End:          a.NewString("c").MarshalTo(nil),
		StartInclude: true, EndInclude: true,
	}}

	plan := &Plan{}
	upstream := &fakeIter{plan: plan, hits: []fakeHit{
		{key: encodeKey(a.NewString("a"), "p1"), docId: []byte("p1"), doc: doc},
		{key: encodeKey(a.NewString("b"), "p1"), docId: []byte("p1"), doc: doc},
		{key: encodeKey(a.NewString("c"), "p1"), docId: []byte("p1"), doc: doc},
	}}
	it := &CanonicalKeyDedupIter{Source: upstream, Plan: plan, Bounds: bs, FieldPath: []string{"tags"}}

	var got []string
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, string(docId))
	}
	assert.Equal(t, []string{"p1"}, got, "range bounds: emit only at canonical 'a'")
}

func TestCanonicalKeyDedupIter_NoBounds(t *testing.T) {
	// A pure Sort("tags") with no filter produces an IndexScan with empty
	// bounds. Every array element qualifies; canonical = min of the whole array.
	a := &anyenc.Arena{}
	arr := a.NewArray()
	arr.SetArrayItem(0, a.NewString("c"))
	arr.SetArrayItem(1, a.NewString("a"))
	arr.SetArrayItem(2, a.NewString("b"))
	doc := a.NewObject()
	doc.Set("id", a.NewString("p1"))
	doc.Set("tags", arr)

	plan := &Plan{}
	// Scan order: a, b, c (index entries are sorted).
	upstream := &fakeIter{plan: plan, hits: []fakeHit{
		{key: encodeKey(a.NewString("a"), "p1"), docId: []byte("p1"), doc: doc},
		{key: encodeKey(a.NewString("b"), "p1"), docId: []byte("p1"), doc: doc},
		{key: encodeKey(a.NewString("c"), "p1"), docId: []byte("p1"), doc: doc},
	}}
	it := &CanonicalKeyDedupIter{Source: upstream, Plan: plan, FieldPath: []string{"tags"}}

	var got []string
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, string(docId))
	}
	assert.Equal(t, []string{"p1"}, got,
		"no bounds: emit only at canonical 'a' (min of whole array)")
}

func TestCanonicalKeyDedupIter_MultipleDocs(t *testing.T) {
	a := &anyenc.Arena{}

	mkDoc := func(id string, tags ...string) *anyenc.Value {
		arr := a.NewArray()
		for i, tg := range tags {
			arr.SetArrayItem(i, a.NewString(tg))
		}
		d := a.NewObject()
		d.Set("id", a.NewString(id))
		d.Set("tags", arr)
		return d
	}
	p1 := mkDoc("p1", "a", "b", "c")
	p2 := mkDoc("p2", "a")

	// Bounds {a,b,c}. p1 produces 3 hits, p2 produces 1.
	bs := query.Bounds{
		{Start: a.NewString("a").MarshalTo(nil), End: a.NewString("a").MarshalTo(nil), StartInclude: true, EndInclude: true},
		{Start: a.NewString("b").MarshalTo(nil), End: a.NewString("b").MarshalTo(nil), StartInclude: true, EndInclude: true},
		{Start: a.NewString("c").MarshalTo(nil), End: a.NewString("c").MarshalTo(nil), StartInclude: true, EndInclude: true},
	}

	// Scan order (sorted by key): a/p1, a/p2, b/p1, c/p1.
	plan := &Plan{}
	upstream := &fakeIter{plan: plan, hits: []fakeHit{
		{key: encodeKey(a.NewString("a"), "p1"), docId: []byte("p1"), doc: p1},
		{key: encodeKey(a.NewString("a"), "p2"), docId: []byte("p2"), doc: p2},
		{key: encodeKey(a.NewString("b"), "p1"), docId: []byte("p1"), doc: p1},
		{key: encodeKey(a.NewString("c"), "p1"), docId: []byte("p1"), doc: p1},
	}}
	it := &CanonicalKeyDedupIter{
		Source: upstream, Plan: plan, Bounds: bs,
		FieldPath: []string{"tags"},
	}

	var got []string
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, string(docId))
	}
	assert.Equal(t, []string{"p1", "p2"}, got)
}

func TestSeenSetDedupIter_RemovesDuplicates(t *testing.T) {
	a := &anyenc.Arena{}
	p1 := a.NewObject()
	p1.Set("id", a.NewString("p1"))
	p2 := a.NewObject()
	p2.Set("id", a.NewString("p2"))

	plan := &Plan{}
	upstream := &fakeIter{plan: plan, hits: []fakeHit{
		{key: []byte("k1"), docId: []byte("p1"), doc: p1},
		{key: []byte("k2"), docId: []byte("p2"), doc: p2},
		{key: []byte("k3"), docId: []byte("p1"), doc: p1}, // dup
		{key: []byte("k4"), docId: []byte("p2"), doc: p2}, // dup
	}}

	it := &SeenSetDedupIter{Source: upstream}

	var got []string
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, string(docId))
	}
	assert.Equal(t, []string{"p1", "p2"}, got)
}

// --- Coverage tests from dedup_iter_coverage_test.go ---

// TestCanonicalKeyDedupIter_Coverage_NilPlanPassthrough verifies that when
// Plan is nil (no access to DocParsed) the iterator passes upstream hits
// through unchanged. Covers the defensive nil-Plan branch at
// internal/qplanner/dedup_iter.go:59-61.
func TestCanonicalKeyDedupIter_Coverage_NilPlanPassthrough(t *testing.T) {
	a := &anyenc.Arena{}

	// Three arbitrary hits on two docs, no Plan/DocParsed available.
	upstream := &fakeIter{
		hits: []fakeHit{
			{key: encodeKey(a.NewString("a"), "p1"), docId: []byte("p1"), doc: nil},
			{key: encodeKey(a.NewString("b"), "p2"), docId: []byte("p2"), doc: nil},
			{key: encodeKey(a.NewString("c"), "p1"), docId: []byte("p1"), doc: nil},
		},
	}

	it := &CanonicalKeyDedupIter{
		Source:    upstream,
		Plan:      nil, // the scenario under test
		FieldPath: []string{"tags"},
	}

	var got []string
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, string(docId))
	}
	assert.Equal(t, []string{"p1", "p2", "p1"}, got,
		"nil Plan: every upstream hit must pass through unchanged")
}

// TestCanonicalKeyDedupIter_Coverage_NilDocParsedPassthrough verifies the
// defensive branch when Plan is non-nil but Plan.DocParsed is nil — the
// iterator cannot see the array, so it passes through.
func TestCanonicalKeyDedupIter_Coverage_NilDocParsedPassthrough(t *testing.T) {
	a := &anyenc.Arena{}

	// fakeIter populates Plan.DocParsed from hit.doc — here we set doc=nil
	// so that DocParsed remains nil after the upstream call.
	plan := &Plan{}
	upstream := &fakeIter{
		plan: plan,
		hits: []fakeHit{
			{key: encodeKey(a.NewString("a"), "p1"), docId: []byte("p1"), doc: nil},
			{key: encodeKey(a.NewString("b"), "p2"), docId: []byte("p2"), doc: nil},
		},
	}

	it := &CanonicalKeyDedupIter{
		Source:    upstream,
		Plan:      plan,
		FieldPath: []string{"tags"},
	}

	var got []string
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, string(docId))
	}
	assert.Equal(t, []string{"p1", "p2"}, got,
		"nil DocParsed: every upstream hit must pass through unchanged")
}

// TestCanonicalKeyDedupIter_Coverage_EmptyArrayWithBounds verifies the
// length-0 array combined with non-empty bounds branch at
// internal/qplanner/dedup_iter.go:68-88.
// When the array is [] and bounds are non-empty, no element can match;
// the iterator must pass through conservatively (it.best stays empty).
func TestCanonicalKeyDedupIter_Coverage_EmptyArrayWithBounds(t *testing.T) {
	a := &anyenc.Arena{}

	// Build doc: {"id":"p1","tags":[]}
	arr := a.NewArray()
	doc := a.NewObject()
	doc.Set("id", a.NewString("p1"))
	doc.Set("tags", arr)

	// bounds: tags $in ["a"]
	bs := query.Bounds{
		{
			Start:        a.NewString("a").MarshalTo(nil),
			End:          a.NewString("a").MarshalTo(nil),
			StartInclude: true, EndInclude: true,
		},
	}

	plan := &Plan{}
	// Upstream emits one hit (as a real btree would not — but tests the
	// defensive path). Key prefix = "a", docId = "p1".
	upstream := &fakeIter{
		plan: plan,
		hits: []fakeHit{
			{key: encodeKey(a.NewString("a"), "p1"), docId: []byte("p1"), doc: doc},
		},
	}

	it := &CanonicalKeyDedupIter{
		Source:    upstream,
		Plan:      plan,
		Bounds:    bs,
		FieldPath: []string{"tags"},
	}

	// Per dedup_iter.go:85-88, when no array element hits the bounds the
	// iterator passes through conservatively (best stays empty).
	_, docId, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, []byte("p1"), docId,
		"empty array vs non-empty bounds: conservative passthrough")

	// Ensure drained.
	_, docId2, err := it.Next()
	require.NoError(t, err)
	assert.Nil(t, docId2)
}

// TestCanonicalKeyDedupIter_Coverage_ArrayAllOutsideBounds verifies graceful
// handling when the upstream emits a doc whose array values all fall outside
// the query bounds. Covers the "no element in bounds" path at
// internal/qplanner/dedup_iter.go:69-88.
func TestCanonicalKeyDedupIter_Coverage_ArrayAllOutsideBounds(t *testing.T) {
	a := &anyenc.Arena{}

	// Doc tags = ["a","b","c"], bounds $in ["x","y"] — no overlap.
	arr := a.NewArray()
	arr.SetArrayItem(0, a.NewString("a"))
	arr.SetArrayItem(1, a.NewString("b"))
	arr.SetArrayItem(2, a.NewString("c"))
	doc := a.NewObject()
	doc.Set("id", a.NewString("p1"))
	doc.Set("tags", arr)

	bs := query.Bounds{
		{Start: a.NewString("x").MarshalTo(nil), End: a.NewString("x").MarshalTo(nil), StartInclude: true, EndInclude: true},
		{Start: a.NewString("y").MarshalTo(nil), End: a.NewString("y").MarshalTo(nil), StartInclude: true, EndInclude: true},
	}

	plan := &Plan{}
	// Stage a hit whose fieldVal differs from any bound entry.
	upstream := &fakeIter{
		plan: plan,
		hits: []fakeHit{
			{key: encodeKey(a.NewString("a"), "p1"), docId: []byte("p1"), doc: doc},
		},
	}

	it := &CanonicalKeyDedupIter{
		Source:    upstream,
		Plan:      plan,
		Bounds:    bs,
		FieldPath: []string{"tags"},
	}

	// Conservative passthrough: emit the hit even though the doc's array
	// has no in-bounds element.
	_, docId, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, []byte("p1"), docId,
		"all array values outside bounds: conservative passthrough")

	_, docId2, err := it.Next()
	require.NoError(t, err)
	assert.Nil(t, docId2)
}

// TestCanonicalKeyDedupIter_Coverage_ReverseExclusiveBounds verifies that
// reverse-scan with exclusive bounds (tags $gt "a" $lt "z") emits exactly
// the maximum in-bounds element and suppresses duplicates.
// Covers internal/qplanner/dedup_iter.go:80-83.
func TestCanonicalKeyDedupIter_Coverage_ReverseExclusiveBounds(t *testing.T) {
	a := &anyenc.Arena{}

	// Doc tags include an element strictly below "a", two in-range, and
	// one strictly at the excluded end "z".
	arr := a.NewArray()
	arr.SetArrayItem(0, a.NewString("a")) // excluded by $gt
	arr.SetArrayItem(1, a.NewString("b")) // included
	arr.SetArrayItem(2, a.NewString("y")) // included (canonical max in reverse)
	arr.SetArrayItem(3, a.NewString("z")) // excluded by $lt
	doc := a.NewObject()
	doc.Set("id", a.NewString("p1"))
	doc.Set("tags", arr)

	// tags $gt "a" $lt "z": exclusive on both ends.
	bs := query.Bounds{
		{
			Start:        a.NewString("a").MarshalTo(nil),
			End:          a.NewString("z").MarshalTo(nil),
			StartInclude: false,
			EndInclude:   false,
		},
	}

	plan := &Plan{}
	// Reverse scan order: btree yields keys in descending order, so for a
	// single doc with tags b,y the cursor sees y first, then b.
	upstream := &fakeIter{
		plan: plan,
		hits: []fakeHit{
			{key: encodeKey(a.NewString("y"), "p1"), docId: []byte("p1"), doc: doc},
			{key: encodeKey(a.NewString("b"), "p1"), docId: []byte("p1"), doc: doc},
		},
	}

	it := &CanonicalKeyDedupIter{
		Source:    upstream,
		Plan:      plan,
		Bounds:    bs,
		FieldPath: []string{"tags"},
		Reverse:   true,
	}

	// Emit at the canonical (max-in-bounds) hit, skip the rest.
	k1, docId, err := it.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("p1"), docId)
	// The emitted key must be the "y" hit (canonical maximum under reverse).
	expectedKey := encodeKey(a.NewString("y"), "p1")
	assert.Equal(t, expectedKey, k1,
		"reverse + exclusive bounds: emit at canonical max ('y'), skip 'b'")

	_, docId2, err := it.Next()
	require.NoError(t, err)
	assert.Nil(t, docId2, "'b' must be skipped as non-canonical in reverse")
}

// TestSeenSetDedupIter_Coverage_HashSetStress exercises the SeenSet with
// enough hits and distinct keys to trigger map growth and hash collisions.
// Covers internal/qplanner/dedup_iter.go:119-133.
func TestSeenSetDedupIter_Coverage_HashSetStress(t *testing.T) {
	a := &anyenc.Arena{}

	const distinctDocs = 100
	const hitsPerDoc = 15 // 1500 total hits, 100 distinct docIds

	hits := make([]fakeHit, 0, distinctDocs*hitsPerDoc)
	for r := 0; r < hitsPerDoc; r++ {
		for d := 0; d < distinctDocs; d++ {
			docId := fmt.Sprintf("doc%03d", d)
			// Ensure each hit has a different key so it isn't trivially deduped
			// on key identity (SeenSet dedups on docId).
			keyStr := fmt.Sprintf("r%02d_%s", r, docId)
			obj := a.NewObject()
			obj.Set("id", a.NewString(docId))
			hits = append(hits, fakeHit{
				key:   encodeKey(a.NewString(keyStr), docId),
				docId: []byte(docId),
				doc:   obj,
			})
		}
	}

	upstream := &fakeIter{hits: hits}
	it := &SeenSetDedupIter{Source: upstream}

	seen := make(map[string]int)
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		seen[string(docId)]++
	}

	require.Len(t, seen, distinctDocs, "must yield exactly 100 distinct docIds")
	for docId, count := range seen {
		require.Equal(t, 1, count, "docId %q must be emitted exactly once (got %d)", docId, count)
	}
}
