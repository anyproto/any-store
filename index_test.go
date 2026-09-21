package anystore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"os"
	"path/filepath"
	"sort"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/qplanner"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
)

func mustParseItem(t testing.TB, s string) item {
	it, err := newItem(anyenc.MustParseJson(s))
	require.NoError(t, err)
	return it
}

func assertIdxKeyBuf(t *testing.T, idx *index, keyCase fillKeysCase) {
	// A single-field reverse index stores keys bitwise-inverted, which
	// Tuple.String cannot decode. Each `expected` entry is valid JSON, so
	// compare the raw key bytes against the inverted marshal of that value.
	if len(idx.reverse) == 1 && idx.reverse[0] {
		require.Equal(t, len(keyCase.expected), len(idx.keysBuf), keyCase.doc)
		for i, ej := range keyCase.expected {
			want := anyenc.Tuple{}.AppendInverted(anyenc.MustParseJson(ej))
			assert.Equal(t, []byte(want), []byte(idx.keysBuf[i]), "%s key %d (inverted %s)", keyCase.doc, i, ej)
		}
		return
	}
	var keysStrings = make([]string, len(idx.keysBuf))
	for i, k := range idx.keysBuf {
		keysStrings[i] = k.String()
	}
	require.Equal(t, len(keyCase.expected), len(idx.keysBuf), keyCase.doc, strings.Join(keysStrings, ","))
	for i, k := range keysStrings {
		assert.Equal(t, keyCase.expected[i], k, keyCase.doc)
	}
}

func assertIndexLen(t testing.TB, idx Index, expected int) bool {
	count, err := idx.Len(ctx)
	require.NoError(t, err)
	return assert.Equal(t, expected, count)
}

type fillKeysCaseIndex struct {
	name  string
	info  IndexInfo
	cases []fillKeysCase
}

type fillKeysCase struct {
	doc      string
	expected []string
}

var fillKeysCases = []fillKeysCaseIndex{
	{
		name: "one field",
		info: IndexInfo{Fields: []string{"a"}},
		cases: []fillKeysCase{
			{`{"id":1,"a":"b"}`, []string{`"b"`}},
			{`{"id":1,"a":["b","c"]}`, []string{`"b"`, `"c"`, `["b","c"]`}},
			{`{"id":1,"a":["a", "a", "b", "c", "b"]}`, []string{`"a"`, `"b"`, `"c"`, `["a","a","b","c","b"]`}},
			{`{"id":1}`, []string{"null"}},
			{`{"id":1,"a":null}`, []string{"null"}},
		},
	},
	{
		name: "one field sparse",
		info: IndexInfo{Fields: []string{"a"}, Sparse: true},
		cases: []fillKeysCase{
			{`{"id":1,"a":"b"}`, []string{`"b"`}},
			{`{"id":1,"a":["b","c"]}`, []string{`"b"`, `"c"`, `["b","c"]`}},
			{`{"id":1,"a":["a", "a", "b", "c", "b"]}`, []string{`"a"`, `"b"`, `"c"`, `["a","a","b","c","b"]`}},
			{`{"id":1}`, []string{}},
			{`{"id":1,"a":null}`, []string{`null`}},
			{`{"id":1,"a":[]}`, []string{`[]`}},
		},
	},
	{
		// A missing leaf and an explicit null encode alike; only the null is
		// a key, whichever element comes first.
		name: "traversed sparse",
		info: IndexInfo{Fields: []string{"x.y"}, Sparse: true},
		cases: []fillKeysCase{
			{`{"id":1,"x":[{"y":null},{"z":9}]}`, []string{`null`}},
			{`{"id":1,"x":[{"z":9},{"y":null}]}`, []string{`null`}},
			{`{"id":1,"x":[{"z":9},{"y":null},{"y":5},{"y":null}]}`, []string{`null`, `5`}},
			{`{"id":1,"x":[{"z":9},{"w":1}]}`, []string{}},
		},
	},
	{
		name: "traversed compound sparse",
		info: IndexInfo{Fields: []string{"x.y", "x.z"}, Sparse: true},
		cases: []fillKeysCase{
			{`{"id":1,"x":[{"z":2},{"y":null,"z":2}]}`, []string{`null/2`}},
			{`{"id":1,"x":[{"y":null,"z":2},{"z":2}]}`, []string{`null/2`}},
			{`{"id":1,"x":[{"w":1},{"y":null},{"z":3}]}`, []string{`null/null`, `null/3`}},
		},
	},
	{
		// A non-object element of the shared array is a missing leaf for
		// every field running through it, as in path matching.
		name: "shared array non-object element",
		info: IndexInfo{Fields: []string{"x.y", "x.z"}},
		cases: []fillKeysCase{
			{`{"id":1,"x":[[{"y":1,"z":2}]]}`, []string{`null/null`}},
			{`{"id":1,"x":[[{"z":5}],{"y":1,"z":2}]}`, []string{`null/null`, `1/2`}},
			{`{"id":1,"x":[3,null,[{"z":5}],{"y":1}]}`, []string{`null/null`, `1/null`}},
		},
	},
	{
		name: "shared array non-object element positional",
		info: IndexInfo{Fields: []string{"x.0.y", "x.0.z"}},
		cases: []fillKeysCase{
			{`{"id":1,"x":[[{"y":1,"z":2}]]}`, []string{`1/2`}},
			{`{"id":1,"x":[[[{"y":1,"z":2}]]]}`, []string{`null/null`}},
		},
	},
	{
		name: "shared array non-object element sparse",
		info: IndexInfo{Fields: []string{"x.y", "x.z"}, Sparse: true},
		cases: []fillKeysCase{
			{`{"id":1,"x":[[{"z":1}],{"y":2}]}`, []string{}},
			{`{"id":1,"x":[[{"y":1,"z":2}],{"y":3,"z":4}]}`, []string{`3/4`}},
		},
	},
	{
		// Reverse single-field index: keys are stored bitwise-inverted. Each
		// `expected` entry is valid JSON; assertIdxKeyBuf detects the reverse
		// field (idx.reverse[0]) and compares the raw key bytes against the
		// inverted marshal of MustParseJson(expected[i]) — Tuple.String cannot
		// decode inverted bytes. The per-doc shape matches the forward case.
		name: "reverse",
		info: IndexInfo{Fields: []string{"-a"}},
		cases: []fillKeysCase{
			{`{"id":1,"a":"b"}`, []string{`"b"`}},
			{`{"id":1,"a":["b","c"]}`, []string{`"b"`, `"c"`, `["b","c"]`}},
			{`{"id":1,"a":["a", "a", "b", "c", "b"]}`, []string{`"a"`, `"b"`, `"c"`, `["a","a","b","c","b"]`}},
		},
	},
	{
		name: "two fields",
		info: IndexInfo{Fields: []string{"a", "b"}},
		cases: []fillKeysCase{
			{`{"id":1,"a":1}`, []string{"1/null"}},
			{`{"id":1,"a":1,"b":2}`, []string{"1/2"}},
			{`{"id":1,"a":[1,2],"b":2}`, []string{"1/2", "2/2", "[1,2]/2"}},
			{`{"id":1,"a":[1,2,1],"b":[2,1,2]}`, []string{
				"1/2", "1/1", "1/[2,1,2]",
				"2/2", "2/1", "2/[2,1,2]",
				"[1,2,1]/2", "[1,2,1]/1", "[1,2,1]/[2,1,2]"}},
		},
	},
	{
		name: "two fields sparse",
		info: IndexInfo{Fields: []string{"a", "b"}, Sparse: true},
		cases: []fillKeysCase{
			{`{"id":1,"a":"1"}`, []string{}},
			{`{"id":1,"b":"2"}`, []string{}},
			{`{"id":1,"a":[1,2]}`, []string{}},
		},
	},
}

func TestIndex_fillKeysBuf(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	newIdx := func(i IndexInfo) *index {
		i.Name = i.createName()
		idx := &index{info: i, c: coll.(*collection)}
		require.NoError(t, idx.init())
		return idx
	}
	for _, idxCase := range fillKeysCases[:] {
		t.Run(idxCase.name, func(t *testing.T) {
			idx := newIdx(idxCase.info)
			for _, keyCase := range idxCase.cases[:] {
				idx.fillKeysBuf(mustParseItem(t, keyCase.doc))
				assertIdxKeyBuf(t, idx, keyCase)
			}
		})
	}
}

// TestIndex_fillKeysBuf_CompoundMixedDirection verifies that for an (a,-b) index
// only the reverse-flagged field (b) is bitwise-inverted in the stored key,
// while the forward field (a) and the per-element array expansion keep their
// normal encoding. This is the element-wise half of the invert-on-write design.
func TestIndex_fillKeysBuf_CompoundMixedDirection(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	idx := &index{info: IndexInfo{Name: "a,-b", Fields: []string{"a", "-b"}}, c: coll.(*collection)}
	require.NoError(t, idx.init())

	fwd := func(j string) anyenc.Tuple { return anyenc.Tuple{}.Append(anyenc.MustParseJson(j)) }
	inv := func(j string) anyenc.Tuple { return anyenc.Tuple{}.AppendInverted(anyenc.MustParseJson(j)) }
	key := func(aj, bj string) []byte { return append(append([]byte{}, fwd(aj)...), inv(bj)...) }

	t.Run("scalar", func(t *testing.T) {
		idx.fillKeysBuf(mustParseItem(t, `{"id":1,"a":1,"b":2}`))
		require.Len(t, idx.keysBuf, 1)
		assert.Equal(t, key("1", "2"), []byte(idx.keysBuf[0]))
	})

	t.Run("array_on_a_only_b_inverted", func(t *testing.T) {
		// a=[1,2], b=2 → keys (1,^2),(2,^2),([1,2],^2); a stays forward, b inverted.
		idx.fillKeysBuf(mustParseItem(t, `{"id":1,"a":[1,2],"b":2}`))
		require.Len(t, idx.keysBuf, 3)
		assert.Equal(t, key("1", "2"), []byte(idx.keysBuf[0]))
		assert.Equal(t, key("2", "2"), []byte(idx.keysBuf[1]))
		assert.Equal(t, key("[1,2]", "2"), []byte(idx.keysBuf[2]))
	})

	t.Run("missing_reverse_field_is_inverted_null", func(t *testing.T) {
		// b missing → stored as inverted null (0xFE), a forward.
		idx.fillKeysBuf(mustParseItem(t, `{"id":1,"a":1}`))
		require.Len(t, idx.keysBuf, 1)
		assert.Equal(t, key("1", "null"), []byte(idx.keysBuf[0]))
		// Sanity: the b segment is exactly the inverted null tag.
		assert.Equal(t, []byte{^byte(anyenc.TypeNull)}, []byte(idx.keysBuf[0][len(fwd("1")):]))
	})
}

func TestIndex_Insert(t *testing.T) {
	fx := newFixture(t)
	t.Run("uniq", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_uniq")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, coll.Close())
		}()
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1,"a":1}`),
			anyenc.MustParseJson(`{"id":2,"a":2}`),
			anyenc.MustParseJson(`{"id":3,"a":3}`),
		))
		assertIndexLen(t, coll.GetIndexes()[0], 3)
		// Unique constraint should be enforced
		require.ErrorIs(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":2}`)), ErrUniqueConstraint)
		assertCollCount(t, coll, 3)
		assertIndexLen(t, coll.GetIndexes()[0], 3)
	})
	t.Run("sparse", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_sparse")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, coll.Close())
		}()
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true}))

		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1,"a":1}`),
			anyenc.MustParseJson(`{"id":2,"a":2}`),
			anyenc.MustParseJson(`{"id":3,"b":3}`),
		))
		assertCollCount(t, coll, 3)
		assertIndexLen(t, coll.GetIndexes()[0], 2)
	})
	t.Run("simple", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_simple")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, coll.Close())
		}()
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`), anyenc.MustParseJson(`{"id":2,"a":1}`), anyenc.MustParseJson(`{"id":3,"b":3}`)))
		assertCollCount(t, coll, 3)
		assertIndexLen(t, coll.GetIndexes()[0], 3)
	})
}

func TestIndex_Update(t *testing.T) {
	fx := newFixture(t)
	t.Run("uniq", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_uniq")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, coll.Close())
		}()
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "a":1}`), anyenc.MustParseJson(`{"id":2, "a":2}`), anyenc.MustParseJson(`{"id":3,"a":3}`)))
		assertIndexLen(t, coll.GetIndexes()[0], 3)
		require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":2,"a":4}`)))
		assertIndexLen(t, coll.GetIndexes()[0], 3)
		// Unique constraint should be enforced on update
		require.ErrorIs(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":2, "a":1}`)), ErrUniqueConstraint)
		res, err := coll.FindId(ctx, 2)
		require.NoError(t, err)
		assert.Equal(t, `{"id":2,"a":4}`, res.Value().String())
	})
	t.Run("sparse", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_sparse")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, coll.Close())
		}()
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true}))

		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "a":1}`), anyenc.MustParseJson(`{"id":2, "a":2}`), anyenc.MustParseJson(`{"id":3, "b":3}`)))
		assertIndexLen(t, coll.GetIndexes()[0], 2)
		require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1, "b":1}`)))
		assertIndexLen(t, coll.GetIndexes()[0], 1)
		require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":3, "a":1}`)))
		assertIndexLen(t, coll.GetIndexes()[0], 2)
	})
}

func TestIndex_Delete(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_simple")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, coll.Close())
	}()
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "a":1}`), anyenc.MustParseJson(`{"id":2, "a":1}`), anyenc.MustParseJson(`{"id":3, "b":3}`)))
	assertIndexLen(t, coll.GetIndexes()[0], 3)

	require.NoError(t, coll.DeleteId(ctx, 1))
	assertIndexLen(t, coll.GetIndexes()[0], 2)

	require.NoError(t, coll.DeleteId(ctx, 2))
	assertIndexLen(t, coll.GetIndexes()[0], 1)

	require.NoError(t, coll.DeleteId(ctx, 3))
	assertIndexLen(t, coll.GetIndexes()[0], 0)
}

func Benchmark_fillKeysBuf(b *testing.B) {
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(b, err)
	newIdx := func(i IndexInfo) *index {
		i.Name = i.createName()
		idx := &index{info: i, c: coll.(*collection)}
		require.NoError(b, idx.init())
		return idx
	}

	b.Run("simple", func(b *testing.B) {
		idx := newIdx(IndexInfo{Fields: []string{"a"}})
		it := mustParseItem(b, `{"id":1,"a":"b"}`)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			idx.fillKeysBuf(it)
		}
	})
	b.Run("two fields", func(b *testing.B) {
		idx := newIdx(IndexInfo{Fields: []string{"a", "b"}})
		it := mustParseItem(b, `{"id":1,"a":1,"b":2}`)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			idx.fillKeysBuf(it)
		}
	})
}

// TestValidateIndexField covers every branch of validateIndexField.
func TestValidateIndexField(t *testing.T) {
	t.Run("empty_is_error", func(t *testing.T) {
		err := validateIndexField("")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty")
	})
	t.Run("dash_only_is_error", func(t *testing.T) {
		// A lone "-" is the reverse-marker with no field name after it.
		err := validateIndexField("-")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty")
	})
	t.Run("dollar_prefix_is_error", func(t *testing.T) {
		err := validateIndexField("$meta")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid")
		// The error must surface the rejected input so callers can see what
		// was wrong — a bare "invalid" is not enough.
		assert.Contains(t, err.Error(), "$meta",
			"error message must include the literal rejected input")
	})
	t.Run("dollar_mid_string_ok", func(t *testing.T) {
		// Dollar not at prefix is legal — proves the reject rule is a
		// HasPrefix check, not a substring/Contains check.
		require.NoError(t, validateIndexField("a$b"))
	})
	t.Run("valid_field", func(t *testing.T) {
		require.NoError(t, validateIndexField("name"))
		require.NoError(t, validateIndexField("-createdAt"))
		require.NoError(t, validateIndexField("nested.path"))
	})
}

// TestIndex_InsertKeys_IdempotentSameDoc directly calls insertKeys twice with
// the same item in the same transaction to exercise the "same doc, idempotent"
// branch at index.go:157. This branch is not reachable via UpsertOne because
// collection.update short-circuits on anyencutil.Equal before insertKeys runs.
func TestIndex_InsertKeys_IdempotentSameDoc(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "idx_idempotent_direct")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "ix_uq", Fields: []string{"a"}, Unique: true,
	}))

	idx := coll.GetIndexes()[0].(*index)
	it, itErr := newItem(anyenc.MustParseJson(`{"id":1,"a":42}`))
	require.NoError(t, itErr)

	wrTx, err := coll.WriteTx(ctx)
	require.NoError(t, err)
	btWtx := wrTx.btreeWriteTx()

	// First insert: populates the unique index.
	require.NoError(t, idx.insertKeys(btWtx, it))
	countAfterFirst, err := btWtx.Count(idx.ns)
	require.NoError(t, err)
	require.Equal(t, 1, countAfterFirst, "first insert must produce exactly one index row")

	// Capture sketch state pre-re-insert. If sketch is nil in this test
	// setup, we simply skip the sketch assertion — no harm done.
	//
	// Note: we only assert on the per-key bucket, NOT on DocCount. The
	// production insertKeys calls IncrementDocCount() unconditionally after
	// the per-key loop (index.go:169-172), so DocCount does advance on the
	// idempotent path too — that's an existing quirk, not something the
	// idempotent branch is expected to suppress. The per-key Increment IS
	// inside the loop and IS skipped via `continue` on idempotent re-insert,
	// so that's the counter we can meaningfully pin here.
	var sketchKeyCountBefore uint64
	// Snapshot the encoded key by value — keysBuf is reused by the second
	// insertKeys call and its backing storage will be overwritten in place.
	var keyCopy []byte
	if idx.sketch != nil {
		keyCopy = append(keyCopy, idx.keysBuf[0]...)
		// keysBuf[0] is the full field-tuple → the full-key (deepest) level.
		sketchKeyCountBefore = idx.sketch.Estimate(idx.sketch.NumLevels()-1, keyCopy)
	}

	// Second insert with the same item: unique seek finds the existing entry
	// matching fullKeyBuf → takes the idempotent continue at index.go:157.
	require.NoError(t, idx.insertKeys(btWtx, it),
		"re-inserting the same (key, docId) pair must hit the idempotent branch")

	// Core idempotency proof: the namespace still holds exactly one row.
	// Nothing was re-inserted, nothing was duplicated.
	countAfterSecond, err := btWtx.Count(idx.ns)
	require.NoError(t, err)
	require.Equal(t, 1, countAfterSecond,
		"idempotent re-insert must not add a second index row")

	// If the index has a sketch, the per-key bucket must not advance on the
	// idempotent path — otherwise per-value selectivity estimates would drift
	// on every duplicate re-insert.
	if idx.sketch != nil {
		assert.Equal(t, sketchKeyCountBefore, idx.sketch.Estimate(idx.sketch.NumLevels()-1, keyCopy),
			"sketch per-key bucket must not increment on idempotent re-insert")
	}

	require.NoError(t, wrTx.Rollback())

	// Cross-doc collision via the public API still rejects.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":99}`)))
	require.ErrorIs(t,
		coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"a":99}`)),
		ErrUniqueConstraint,
		"cross-doc duplicate must be rejected through the full pipeline")
}

// TestIndex_InsertKeys_MultiLevelSketch_PrefixDedup verifies the multi-level
// sketch maintenance: a deeper multikey (array) field fans one document into
// several index entries, but the shallow (leading-field) level must be counted
// only ONCE per distinct prefix — otherwise prefix selectivity for the leading
// field would be inflated by the array length of a trailing field.
func TestIndex_InsertKeys_MultiLevelSketch_PrefixDedup(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "idx_multilevel_dedup")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "ab", Fields: []string{"a", "b"},
	}))

	idx := coll.GetIndexes()[0].(*index)
	require.Equal(t, 2, idx.sketch.NumLevels(), "compound (a,b) sketch must have 2 levels")

	// a is scalar; b is an array → the trailing field fans out into multiple
	// entries while the leading field a=7 has a single distinct prefix.
	it, itErr := newItem(anyenc.MustParseJson(`{"id":1,"a":7,"b":[10,20]}`))
	require.NoError(t, itErr)

	wrTx, err := coll.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, idx.insertKeys(wrTx.btreeWriteTx(), it))

	assert.Equal(t, uint64(1), idx.sketch.EntryCount(0),
		"leading field a must be counted once despite b's array fan-out")
	assert.Greater(t, idx.sketch.EntryCount(1), uint64(1),
		"trailing field level must reflect the array fan-out (>1 entries)")
	assert.Equal(t, uint64(1), idx.sketch.GetDocCount(),
		"docCount must advance once per document, independent of fan-out")

	require.NoError(t, wrTx.Rollback())
}

// TestIndex_DeleteKeys_SwallowsErrKeyNotFound directly calls deleteKeys on an
// item whose index entry was never inserted, so tx.Delete returns
// btree.ErrKeyNotFound. The function must swallow that error (index.go:184-187).
// This branch is not reachable via the public update path because the sparse
// code path produces an empty keysBuf and skips the Delete call entirely.
func TestIndex_DeleteKeys_SwallowsErrKeyNotFound(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "idx_delete_missing_direct")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "ix_a", Fields: []string{"a"},
	}))

	idx := coll.GetIndexes()[0].(*index)

	// Populate one real doc so the index has an entry. If we skipped this,
	// we could not distinguish "deleteKeys swallowed ErrKeyNotFound" from
	// "deleteKeys short-circuited because the namespace was empty".
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"other","a":"real"}`)))

	// Build an item with a present field "a" that was NEVER inserted.
	// fillKeysBuf will produce a real key for it, and tx.Delete on that
	// key returns ErrKeyNotFound.
	it, itErr := newItem(anyenc.MustParseJson(`{"id":"never-inserted-id","a":"never-inserted"}`))
	require.NoError(t, itErr)

	wrTx, err := coll.WriteTx(ctx)
	require.NoError(t, err)
	btWtx := wrTx.btreeWriteTx()

	// Must succeed without surfacing ErrKeyNotFound.
	require.NoError(t, idx.deleteKeys(btWtx, it),
		"deleteKeys must swallow ErrKeyNotFound from tx.Delete")

	// The "other" doc's index entry must still be present — we didn't
	// accidentally blast unrelated keys while swallowing ErrKeyNotFound.
	countAfter, err := btWtx.Count(idx.ns)
	require.NoError(t, err)
	require.Equal(t, 1, countAfter,
		"deleteKeys on a never-inserted doc must not touch unrelated index rows")

	require.NoError(t, wrTx.Rollback())
}

// rawIndexEntry pairs an index key with its raw on-disk value byte(s).
// Used by audit_*_test.go to assert the per-entry value byte (bit 0 =
// multi-key) written by insertKeys.
type rawIndexEntry struct {
	Key   []byte
	Value []byte
}

// readRawIndexEntries walks every entry of the named index in the named
// collection and returns (key, value) pairs as-is. Test-only helper —
// reaches into private collection/index fields and uses the btree
// cursor directly so tests can assert what insertKeys actually wrote.
func readRawIndexEntries(t *testing.T, anyDB DB, collName, indexName string) []rawIndexEntry {
	t.Helper()
	ctx := context.Background()
	impl := anyDB.(*db)
	coll, err := impl.Collection(ctx, collName)
	require.NoError(t, err)
	c := coll.(*collection)

	var idx *index
	c.mu.Lock()
	for _, i := range c.loadIndexes() {
		if i.info.Name == indexName {
			idx = i
			break
		}
	}
	c.mu.Unlock()
	require.NotNil(t, idx, "index %q not found in collection %q", indexName, collName)

	var out []rawIndexEntry
	require.NoError(t, impl.doReadTx(ctx, func(tx *btree.ReadTx) error {
		cur := tx.NewCursor(idx.ns)
		defer cur.Close()
		if err := cur.First(); err != nil {
			return err
		}
		for cur.Valid() {
			k, kerr := cur.Key()
			if kerr != nil {
				return kerr
			}
			v, verr := cur.Value()
			if verr != nil {
				return verr
			}
			kp := make([]byte, len(k))
			copy(kp, k)
			vp := make([]byte, len(v))
			copy(vp, v)
			out = append(out, rawIndexEntry{Key: kp, Value: vp})
			if err := cur.Next(); err != nil {
				return err
			}
		}
		return nil
	}))
	return out
}

// TestAudit01_ValueByte_Scalar pins the scalar case: a document with a plain
// scalar field on an indexed path produces exactly ONE index entry, and the
// per-entry value byte must be IndexValueScalar (0x00).
//
// This is the baseline assertion for the multi-key bit pipeline. If this byte
// is wrong at write time, the dedup fast path can't trust it at read time.
func TestAudit01_ValueByte_Scalar(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit01_scalar")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_a",
		Fields: []string{"a"},
	}))

	// Single scalar value → exactly one keysBuf entry → IndexValueScalar.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":5}`)))

	entries := readRawIndexEntries(t, fx.DB, "audit01_scalar", "ix_a")
	require.Len(t, entries, 1, "scalar field must produce exactly one index entry")
	assert.Equal(t, qplanner.IndexValueScalar, entries[0].Value,
		"scalar insert must write IndexValueScalar (0x00) per-entry value byte")
	// Tighten the assertion: explicitly confirm bit 0 is cleared.
	require.NotEmpty(t, entries[0].Value)
	assert.Zero(t, entries[0].Value[0]&qplanner.IndexEntryFlagMultiKey,
		"scalar entry must have multi-key flag bit cleared")
}

// TestAudit01_ValueByte_SingleElementArray pins the subtle single-element
// array case. {tags: ["x"]} on an index over `tags` produces TWO index
// entries:
//
//   - one for the element "x" (via the array loop in writeValues)
//   - one for the whole array ["x"] (via the fall-through MarshalTo/writeValues)
//
// Because len(keysBuf) > 1 by the time insertKeys reads it, BOTH entries
// must be tagged IndexValueMultiKey — even though the array contains only a
// single element. This is the corner case future readers most easily get
// wrong, hence pinning it explicitly.
func TestAudit01_ValueByte_SingleElementArray(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit01_single_arr")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":["x"]}`)))

	entries := readRawIndexEntries(t, fx.DB, "audit01_single_arr", "ix_tags")
	require.Len(t, entries, 2,
		"single-element array must produce 2 entries (element + whole-array)")
	for i, e := range entries {
		assert.Equalf(t, qplanner.IndexValueMultiKey, e.Value,
			"entry %d: single-element-array insert must still write IndexValueMultiKey "+
				"because keysBuf already contains 2 keys at insertKeys() time", i)
		require.NotEmptyf(t, e.Value, "entry %d: value must not be empty", i)
		assert.NotZerof(t, e.Value[0]&qplanner.IndexEntryFlagMultiKey,
			"entry %d: multi-key flag bit must be set", i)
	}
}

// TestAudit01_ValueByte_MultiElementArray covers the obvious multi-key case:
// an array with several distinct elements. All entries (including the
// whole-array entry) must be tagged IndexValueMultiKey.
func TestAudit01_ValueByte_MultiElementArray(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit01_multi_arr")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":["a","b","c"]}`)))

	entries := readRawIndexEntries(t, fx.DB, "audit01_multi_arr", "ix_tags")
	// 3 distinct elements + 1 whole-array entry = 4 entries.
	require.Len(t, entries, 4,
		"3-element array must produce 4 entries (3 elements + whole-array)")
	for i, e := range entries {
		assert.Equalf(t, qplanner.IndexValueMultiKey, e.Value,
			"entry %d: multi-element array must write IndexValueMultiKey for every entry", i)
		require.NotEmptyf(t, e.Value, "entry %d: value must not be empty", i)
		assert.NotZerof(t, e.Value[0]&qplanner.IndexEntryFlagMultiKey,
			"entry %d: multi-key flag bit must be set", i)
	}
}

// fieldScratch is laid out widest first so the per-field scratch stays at
// one cache line per field.
func TestFieldScratchLayout(t *testing.T) {
	assert.Equal(t, uintptr(64), unsafe.Sizeof(fieldScratch{}))
}

// BenchmarkIndex_fillKeysBuf measures key generation alone, per document
// shape: the scalar compound field, a leaf array, and a compound index over
// one array of objects (fields iterated together).
func BenchmarkIndex_fillKeysBuf(b *testing.B) {
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(b, err)
	cases := []struct {
		name   string
		fields []string
		doc    string
	}{
		{"scalar_compound", []string{"a", "b", "c"}, `{"id":1,"a":1,"b":"x","c":2.5}`},
		{"leaf_array", []string{"tags"}, `{"id":1,"tags":["a","b","c","d"]}`},
		{"shared_array_compound", []string{"a.b", "a.c"}, `{"id":1,"a":[{"b":1,"c":2},{"b":3,"c":4},{"b":5},{"c":6}]}`},
		{"shared_array_sparse", []string{"a.b", "a.c"}, `{"id":1,"a":[{"b":1,"c":2},{"b":3,"c":4},{"b":5},{"c":6}]}`},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			info := IndexInfo{Name: c.name, Fields: c.fields, Sparse: strings.HasSuffix(c.name, "_sparse")}
			idx := &index{info: info, c: coll.(*collection)}
			require.NoError(b, idx.init())
			it := mustParseItem(b, c.doc)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx.fillKeysBuf(it)
			}
		})
	}
}

// Deleting a document the index has no entry for must leave the entry count
// alone. The missing entry is produced by stripping the raw key behind the
// deleting document's back.
func TestIndex_Sparse_DeleteMissingEntryKeepsEntryCount(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "noentry")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}, Sparse: true}))
	for i := range 50 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
	}
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":100,"a":null}`)))
	assertIndexLen(t, coll.GetIndexes()[0], 51)

	// Strip the null entry so the document has none.
	entries := readRawIndexEntries(t, fx.DB, "noentry", "a")
	var nullKey []byte
	for _, e := range entries {
		if e.Key[0] == byte(anyenc.TypeNull) {
			nullKey = e.Key
		}
	}
	require.NotNil(t, nullKey)
	impl := fx.DB.(*db)
	c := coll.(*collection)
	var ns *btree.Namespace
	c.mu.Lock()
	for _, i := range c.loadIndexes() {
		if i.info.Name == "a" {
			ns = i.ns
		}
	}
	c.mu.Unlock()
	require.NoError(t, impl.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Delete(ns, nullKey)
	}))
	assertIndexLen(t, coll.GetIndexes()[0], 50)

	sketchEntries := func() uint64 {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, i := range c.loadIndexes() {
			if i.info.Name == "a" && i.sketch != nil {
				return i.sketch.EntryCount(0)
			}
		}
		return 0
	}
	before := sketchEntries()

	require.NoError(t, coll.DeleteId(ctx, 100))
	assertIndexLen(t, coll.GetIndexes()[0], 50)
	assert.Equal(t, before, sketchEntries(), "deleting a document with no entry must not move the entry count")

	// The surviving rows are still all reachable through the index.
	got := collectIntField(t, coll.Find(`{"a":{"$exists":true}}`).IndexHint(IndexHint{IndexName: "a", Boost: 1 << 30}), "id")
	assert.Len(t, got, 50)
}

// randPresenceValue builds a random JSON value from the shapes that decide
// presence: nulls, empty and nested arrays, objects with and without the keys
// the indexes below use.
func randPresenceValue(rnd *rand.Rand, depth int) string {
	switch pick := rnd.Intn(10); {
	case depth <= 0 || pick < 3:
		return []string{"null", "null", "0", "1", `"s"`, "true"}[rnd.Intn(6)]
	case pick < 6:
		keys := []string{"b", "c", "0", "d"}
		rnd.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
		var parts []string
		for _, k := range keys[:rnd.Intn(len(keys)+1)] {
			parts = append(parts, fmt.Sprintf("%q:%s", k, randPresenceValue(rnd, depth-1)))
		}
		return "{" + strings.Join(parts, ",") + "}"
	default:
		var parts []string
		for range rnd.Intn(4) {
			parts = append(parts, randPresenceValue(rnd, depth-1))
		}
		return "[" + strings.Join(parts, ",") + "]"
	}
}

// A sparse index holds a document exactly when {$exists:true} matches every
// indexed field, and the keys it builds for one document are distinct
// (insertKeys Puts and counts each one).
func TestIndex_fillKeysBuf_SparseMembership(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	var idxs []*index
	for _, fields := range [][]string{
		{"a"}, {"-a"}, {"a.b"}, {"a.c"}, {"a.0"}, {"a.0.b"}, {"a.b.c"},
		{"a.b", "a.c"}, {"a", "b"}, {"a.b", "b"}, {"b", "a.b"},
		{"a.b", "a.c", "b"}, {"-a.b", "a.c"}, {"a.d", "a.b"},
		{"a.b", "a.c", "a.d"}, {"a.b.c", "a.b.d"}, {"a.b", "a.b.c"}, {"a.0.b", "a.0.c"},
	} {
		info := IndexInfo{Fields: fields, Sparse: true}
		info.Name = info.createName()
		idx := &index{info: info, c: coll.(*collection)}
		require.NoError(t, idx.init())
		idxs = append(idxs, idx)
	}

	rnd := rand.New(rand.NewSource(1))
	var buf syncpool.DocBuffer
	for n := range 5000 {
		doc := fmt.Sprintf(`{"id":%d`, n)
		if rnd.Intn(10) > 0 {
			doc += `,"a":` + randPresenceValue(rnd, 3)
		}
		if rnd.Intn(3) > 0 {
			doc += `,"b":` + randPresenceValue(rnd, 1)
		}
		doc += "}"
		v := anyenc.MustParseJson(doc)
		it := mustParseItem(t, doc)
		for _, idx := range idxs {
			idx.fillKeysBuf(it)
			want := true
			for _, field := range idx.fieldNames {
				exists := query.MustParseCondition(fmt.Sprintf(`{%q:{"$exists":true}}`, field))
				want = want && exists.Ok(v, &buf)
			}
			held := len(idx.keysBuf) > 0
			require.Equal(t, want, held, "%v %s", idx.info.Fields, doc)
			seen := map[string]bool{}
			for _, k := range idx.keysBuf {
				require.False(t, seen[string(k)], "duplicate key: %v %s", idx.info.Fields, doc)
				seen[string(k)] = true
			}
		}
	}
}

// The scalar proof says nothing about a sparse index over a dotted path — a
// document fanning out through an array of objects can keep a single key — so
// the index does not provide the order even when the marker reads scalar.
func TestIndex_Sparse_TraversedSortIgnoresScalarProof(t *testing.T) {
	fx := newFixture(t)
	plain, err := fx.CreateCollection(ctx, "plain")
	require.NoError(t, err)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "s", Fields: []string{"x.y"}, Sparse: true}))
	for _, d := range []string{
		`{"id":1,"x":[{"y":5},{"z":0}]}`,
		`{"id":2,"x":{"y":1}}`,
		`{"id":3,"x":{"y":9}}`,
		`{"id":4,"x":{"y":3}}`,
	} {
		require.NoError(t, plain.Insert(ctx, anyenc.MustParseJson(d)))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
	}
	// Put the marker back to scalar, as an index whose fan-out documents all
	// keep one key reads.
	c := coll.(*collection)
	var idx *index
	c.mu.Lock()
	for _, i := range c.loadIndexes() {
		if i.info.Name == "s" {
			idx = i
		}
	}
	c.mu.Unlock()
	require.NotNil(t, idx)
	// The write path flags the index itself: document 1 fans out.
	require.NoError(t, fx.DB.(*db).doReadTx(ctx, func(tx *btree.ReadTx) error {
		assert.False(t, idx.isScalarProven(tx), "a fan-out keeping one key is not scalar")
		return nil
	}))
	require.NoError(t, fx.DB.(*db).doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Put(c.db.systemNS, multikeyKey(idx.ns.Name()), mkValScalar)
	}))

	filter := `{"x.y":{"$exists":true}}`
	for _, limit := range []uint{0, 2} {
		want := collectIntField(t, plain.Find(filter).Sort("x.y").Limit(limit), "id")
		got := collectIntField(t, coll.Find(filter).Sort("x.y").Limit(limit).IndexHint(IndexHint{IndexName: "s", Boost: 1 << 30}), "id")
		assert.Equal(t, want, got, "limit %d", limit)
	}
}

// TestSketch_DocCountSurvivesReload pins the post-fix invariant that
// in-memory sketch state (DocCount in particular) is preserved across a
// reload triggered by checkStale. The previous implementation replaced
// the sketch outright on every reload; if a reader had triggered reload
// before the writer's persistSketches landed, the in-memory counter would
// silently revert to whatever was last persisted (often zero on a
// freshly-created index).
//
// With the in-place UnmarshalBinary fix, reload either applies persisted
// state (consistent with the read snapshot) or — if no persisted data
// exists — leaves the in-memory counter alone. This test asserts the
// no-data branch: even after a forced reload, GetDocCount reflects every
// inserted doc.
func TestSketch_DocCountSurvivesReload(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "sketch_survives")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	const N = 25
	for i := range N {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
	}

	// Force a stale-detection by running a query that opens a fresh read
	// tx after the writes committed.
	count, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, N, count)

	// Read DocCount via the same path the planner uses; it must reflect
	// every insert, not regress to zero from a reload that dropped state.
	c := coll.(*collection)
	c.mu.Lock()
	got := c.loadIndexes()[0].sketch.GetDocCount()
	c.mu.Unlock()
	assert.Equal(t, uint64(N), got,
		"sketch DocCount must equal inserted-doc count after reload (%d), got %d", N, got)
}

// TestAudit09_EnsureIndexBackfill_ScalarOnly: insert 5 scalar-tagged docs
// before any index exists, then EnsureIndex on tags. Backfill must emit one
// entry per doc, each tagged IndexValueScalar.
func TestAudit09_EnsureIndexBackfill_ScalarOnly(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit09_scalar")
	require.NoError(t, err)

	// Insert 5 scalar-valued docs first; no index yet.
	for i := 0; i < 5; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":"s%d","tags":"v%d"}`, i, i),
		)))
	}
	require.Len(t, coll.GetIndexes(), 0)

	// Backfill: build the index from the existing 5 docs.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))
	require.Len(t, coll.GetIndexes(), 1)
	assertIndexLen(t, coll.GetIndexes()[0], 5)

	entries := readRawIndexEntries(t, fx.DB, "audit09_scalar", "ix_tags")
	require.Len(t, entries, 5,
		"5 scalar docs must produce exactly 5 backfilled index entries")
	for i, e := range entries {
		assert.Equalf(t, qplanner.IndexValueScalar, e.Value,
			"entry %d: scalar-doc backfill must write IndexValueScalar (0x00)", i)
		require.NotEmptyf(t, e.Value, "entry %d: value must not be empty", i)
		assert.Zerof(t, e.Value[0]&qplanner.IndexEntryFlagMultiKey,
			"entry %d: multi-key flag bit must be CLEARED for scalar entry", i)
	}
}

// TestAudit09_EnsureIndexBackfill_MultiKeyOnly: insert 5 array-tagged docs
// (3 elements each) before the index exists, then EnsureIndex on tags.
// Backfill must emit one entry per element + one whole-array entry per doc
// (= 4 entries per doc), each tagged IndexValueMultiKey.
func TestAudit09_EnsureIndexBackfill_MultiKeyOnly(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit09_multi")
	require.NoError(t, err)

	// 5 docs × 3-element arrays; no index yet.
	for i := 0; i < 5; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":"m%d","tags":["a%d","b%d","c%d"]}`, i, i, i, i),
		)))
	}
	require.Len(t, coll.GetIndexes(), 0)

	// Backfill: per-doc, writeValues emits 3 element keys + 1 whole-array
	// key = 4 keys → len(keysBuf)>1 → IndexValueMultiKey for every entry.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))
	require.Len(t, coll.GetIndexes(), 1)

	// 5 docs × 4 keys = 20 backfilled entries.
	const wantEntries = 20
	assertIndexLen(t, coll.GetIndexes()[0], wantEntries)

	entries := readRawIndexEntries(t, fx.DB, "audit09_multi", "ix_tags")
	require.Len(t, entries, wantEntries,
		"5 docs × (3 elements + 1 whole-array) = 20 backfilled entries")
	for i, e := range entries {
		assert.Truef(t, bytes.Equal(e.Value, qplanner.IndexValueMultiKey),
			"entry %d: array-doc backfill must write IndexValueMultiKey, got %v",
			i, e.Value)
		require.NotEmptyf(t, e.Value, "entry %d: value must not be empty", i)
		assert.NotZerof(t, e.Value[0]&qplanner.IndexEntryFlagMultiKey,
			"entry %d: multi-key flag bit must be SET for array-doc entry", i)
	}
}

// TestAudit09_EnsureIndexBackfill_Mixed: 3 scalar-tagged + 3 array-tagged
// docs in the same collection. Backfill must classify per-doc — scalar docs
// get IndexValueScalar, array docs get IndexValueMultiKey. The docId is
// embedded in the trailing bytes of each index key (item.appendId), so we
// distinguish scalar vs array origin by the docId prefix ("sx" vs "mx").
func TestAudit09_EnsureIndexBackfill_Mixed(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit09_mixed")
	require.NoError(t, err)

	// 3 scalar docs (id: "sx0".."sx2", tags: "x0".."x2")
	for i := 0; i < 3; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":"sx%d","tags":"x%d"}`, i, i),
		)))
	}
	// 3 array docs (id: "mx0".."mx2", tags: ["a0","b0"]..)
	for i := 0; i < 3; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":"mx%d","tags":["a%d","b%d"]}`, i, i, i),
		)))
	}
	require.Len(t, coll.GetIndexes(), 0)

	// Backfill in a single EnsureIndex call.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))
	require.Len(t, coll.GetIndexes(), 1)

	// 3 scalar (1 entry each) + 3 array × (2 element keys + 1 whole-array key)
	// = 3 + 9 = 12 backfilled entries.
	const wantEntries = 12
	assertIndexLen(t, coll.GetIndexes()[0], wantEntries)

	entries := readRawIndexEntries(t, fx.DB, "audit09_mixed", "ix_tags")
	require.Len(t, entries, wantEntries,
		"3 scalar (1 each) + 3 array (3 each) = 12 backfilled entries")

	// Tally per-origin classification by inspecting the docId suffix in each key.
	// docIds are short strings: "sx0".."sx2" or "mx0".."mx2". MarshalTo writes
	// TypeString(0x03) + bytes + EOS(0x00), so the literal "sx" or "mx" bytes
	// appear verbatim in the key tail — substring search is sufficient.
	scalarSeen, multiKeySeen := 0, 0
	for i, e := range entries {
		fromScalarDoc := bytes.Contains(e.Key, []byte("sx"))
		fromArrayDoc := bytes.Contains(e.Key, []byte("mx"))
		require.Truef(t, fromScalarDoc != fromArrayDoc,
			"entry %d: key must come from exactly one of scalar/array docs (key=%x)",
			i, e.Key)

		if fromScalarDoc {
			scalarSeen++
			assert.Equalf(t, qplanner.IndexValueScalar, e.Value,
				"entry %d (scalar doc, key=%x): expected IndexValueScalar, got %v",
				i, e.Key, e.Value)
			require.NotEmptyf(t, e.Value, "entry %d: value must not be empty", i)
			assert.Zerof(t, e.Value[0]&qplanner.IndexEntryFlagMultiKey,
				"entry %d (scalar doc): multi-key flag must be CLEARED", i)
		} else {
			multiKeySeen++
			assert.Truef(t, bytes.Equal(e.Value, qplanner.IndexValueMultiKey),
				"entry %d (array doc, key=%x): expected IndexValueMultiKey, got %v",
				i, e.Key, e.Value)
			require.NotEmptyf(t, e.Value, "entry %d: value must not be empty", i)
			assert.NotZerof(t, e.Value[0]&qplanner.IndexEntryFlagMultiKey,
				"entry %d (array doc): multi-key flag must be SET", i)
		}
	}

	// Cross-check totals: 3 scalar entries (1 per scalar doc) and 9 multi-key
	// entries (3 per array doc).
	assert.Equal(t, 3, scalarSeen, "scalar-doc entries must total 3")
	assert.Equal(t, 9, multiKeySeen, "array-doc entries must total 9 (3 docs × 3 keys)")
}

// TestAudit09_EnsureIndexBackfill_DropAndRecreate: same data layout as the
// Mixed test, but drops the freshly built index and re-creates it. Verifies
// the second backfill rewrites every entry from a clean slate (no stale bits
// from the dropped index leak through). DropIndex deletes the index
// namespace, so the recreated index must repopulate every entry through the
// same insertKeys path.
func TestAudit09_EnsureIndexBackfill_DropAndRecreate(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit09_drop_recreate")
	require.NoError(t, err)

	// Same mixed data shape as the Mixed test.
	for i := 0; i < 3; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":"sx%d","tags":"x%d"}`, i, i),
		)))
	}
	for i := 0; i < 3; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":"mx%d","tags":["a%d","b%d"]}`, i, i, i),
		)))
	}

	// First backfill.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))
	require.Len(t, coll.GetIndexes(), 1)
	assertIndexLen(t, coll.GetIndexes()[0], 12)

	// Drop the index (deletes its namespace + sketch).
	require.NoError(t, coll.DropIndex(ctx, "ix_tags"))
	require.Len(t, coll.GetIndexes(), 0)

	// Recreate from scratch — second backfill must replay through insertKeys
	// and produce the exact same per-entry classification.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))
	require.Len(t, coll.GetIndexes(), 1)
	assertIndexLen(t, coll.GetIndexes()[0], 12)

	entries := readRawIndexEntries(t, fx.DB, "audit09_drop_recreate", "ix_tags")
	require.Len(t, entries, 12,
		"recreated index must have the full 12 entries from the second backfill")

	scalarSeen, multiKeySeen := 0, 0
	for i, e := range entries {
		fromScalarDoc := bytes.Contains(e.Key, []byte("sx"))
		fromArrayDoc := bytes.Contains(e.Key, []byte("mx"))
		require.Truef(t, fromScalarDoc != fromArrayDoc,
			"entry %d: key must come from exactly one of scalar/array docs (key=%x)",
			i, e.Key)

		if fromScalarDoc {
			scalarSeen++
			assert.Equalf(t, qplanner.IndexValueScalar, e.Value,
				"entry %d (scalar doc, recreated index, key=%x): expected IndexValueScalar, got %v",
				i, e.Key, e.Value)
			require.NotEmptyf(t, e.Value, "entry %d: value must not be empty", i)
			assert.Zerof(t, e.Value[0]&qplanner.IndexEntryFlagMultiKey,
				"entry %d (scalar doc, recreated index): multi-key flag must be CLEARED — "+
					"recreation must not leak stale bits", i)
		} else {
			multiKeySeen++
			assert.Truef(t, bytes.Equal(e.Value, qplanner.IndexValueMultiKey),
				"entry %d (array doc, recreated index, key=%x): expected IndexValueMultiKey, got %v",
				i, e.Key, e.Value)
			require.NotEmptyf(t, e.Value, "entry %d: value must not be empty", i)
			assert.NotZerof(t, e.Value[0]&qplanner.IndexEntryFlagMultiKey,
				"entry %d (array doc, recreated index): multi-key flag must be SET", i)
		}
	}

	assert.Equal(t, 3, scalarSeen,
		"recreated index: scalar-doc entries must total 3 (no bits carried from old index)")
	assert.Equal(t, 9, multiKeySeen,
		"recreated index: array-doc entries must total 9 (3 docs × 3 keys)")
}

// TestAudit16_EnsureIndexAbort_NoOrphanIndex: after a cancelled EnsureIndex,
// the in-memory coll.indexes slice and the on-disk system-namespace
// metadata must be CONSISTENT — either both reflect the index or neither
// does. The forbidden state is "system NS has the metadata row but
// coll.indexes does not (or vice versa)" — that's the orphan we'd detect
// by reopening the collection and observing a different index list.
func TestAudit16_EnsureIndexAbort_NoOrphanIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit16_no_orphan_idx")
	require.NoError(t, err)

	insertAbortTestDocs(t, coll)
	require.Len(t, coll.GetIndexes(), 0)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	abortErr := coll.EnsureIndex(cancelled, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	})

	// Snapshot in-memory state right after the cancelled call.
	inMemoryCount := len(coll.GetIndexes())

	// Reopen and snapshot on-disk state.
	require.NoError(t, coll.Close())
	coll2, err := fx.OpenCollection(ctx, "audit16_no_orphan_idx")
	require.NoError(t, err)
	onDiskCount := len(coll2.GetIndexes())

	// Pin the consistency invariant: in-memory == on-disk. A mismatch
	// means createIndexes appended to coll.indexes without committing
	// the system-NS row (or vice versa).
	require.Equalf(t, inMemoryCount, onDiskCount,
		"in-memory index count (%d) must equal on-disk index count (%d) — "+
			"mismatch indicates partial commit (orphan metadata row or "+
			"orphan in-memory entry)", inMemoryCount, onDiskCount)

	// Stronger pin if cancellation is ever honored: an erroring
	// EnsureIndex MUST leave both views at zero.
	if abortErr != nil {
		require.Truef(t, errors.Is(abortErr, context.Canceled),
			"errored EnsureIndex must wrap context.Canceled; got: %v", abortErr)
		require.Equal(t, 0, inMemoryCount,
			"on cancellation, in-memory coll.indexes must be empty")
		require.Equal(t, 0, onDiskCount,
			"on cancellation, on-disk system NS must show no index metadata")
	} else {
		// Today: cancelled call commits, both views show 1.
		t.Logf("FINDING: cancelled EnsureIndex committed; in-memory=%d, on-disk=%d "+
			"(expected both=0 if ctx were honored)", inMemoryCount, onDiskCount)
	}
}

// TestAudit16_EnsureIndexAbort_NoOrphanNamespace: after a cancelled
// EnsureIndex, no leftover index namespace must exist on disk. Public
// API has no namespace enumerator, so we verify indirectly: a retry
// EnsureIndex must succeed AND its index must contain EXACTLY the
// expected entry count. A leftover namespace would either:
//
//	(a) cause CreateNamespace in the retry to fail (visible as a non-nil
//	    error from EnsureIndex), or
//	(b) show up as duplicate/extra entries in readRawIndexEntries.
func TestAudit16_EnsureIndexAbort_NoOrphanNamespace(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit16_no_orphan_ns")
	require.NoError(t, err)

	insertAbortTestDocs(t, coll)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	_ = coll.EnsureIndex(cancelled, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	})

	// Retry with a fresh ctx — must succeed. ensure=true swallows
	// ErrIndexExists, so this returns nil whether or not the cancelled
	// call committed.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}), "retry after cancellation must succeed; a failure here means "+
		"either the system-namespace metadata or the index btree namespace "+
		"survived the rollback in a way the retry could not reconcile")

	// Final state must be exactly one index, exactly 3000 entries.
	require.Len(t, coll.GetIndexes(), 1)
	assertIndexLen(t, coll.GetIndexes()[0], 3000)

	entries := readRawIndexEntries(t, fx.DB, "audit16_no_orphan_ns", "ix_tags")
	require.Lenf(t, entries, 3000,
		"final index must have exactly 3000 entries (1000 docs × 3 keys); "+
			"a different count means leftover bytes from the aborted attempt "+
			"contaminated the rebuilt namespace (got %d)", len(entries))
}

// TestAudit16_EnsureIndexAbort_RetryHasCorrectEntries: the ENTRY-LEVEL
// invariant after retry. Even with the (current) silent-commit
// behavior, the final entries must be uniformly correct: every entry
// from an array-valued doc must carry the multi-key flag. This is the
// strongest end-to-end signal of "no stale scalar-tagged bytes leaked
// from a half-built attempt".
func TestAudit16_EnsureIndexAbort_RetryHasCorrectEntries(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit16_retry_entries")
	require.NoError(t, err)

	insertAbortTestDocs(t, coll)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	_ = coll.EnsureIndex(cancelled, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	})

	// Retry. ensure=true swallows ErrIndexExists if the first call
	// committed (current behavior); rebuilds from scratch if it didn't.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))
	require.Len(t, coll.GetIndexes(), 1)
	assertIndexLen(t, coll.GetIndexes()[0], 3000)

	entries := readRawIndexEntries(t, fx.DB, "audit16_retry_entries", "ix_tags")
	require.Len(t, entries, 3000,
		"retry must produce exactly 3000 entries (1000 array docs × 3 keys)")

	// Every single entry must carry the multi-key flag (all docs are
	// arrays). A scalar-tagged entry here would mean stale data from a
	// half-built rollback or duplicate-write contamination.
	for i, e := range entries {
		require.NotEmptyf(t, e.Value, "entry %d: value must not be empty", i)
		assert.Truef(t, bytes.Equal(e.Value, qplanner.IndexValueMultiKey),
			"entry %d: every entry from array-valued docs must be IndexValueMultiKey, "+
				"got %v (suggests stale bytes from aborted attempt)", i, e.Value)
		assert.NotZerof(t, e.Value[0]&qplanner.IndexEntryFlagMultiKey,
			"entry %d: multi-key flag bit must be SET on every retry entry", i)
	}
}

// TestAudit02_Reversibility_ArrayShrinksToSingleElement pins the per-doc
// reversibility claim documented at index.go:148-150:
//
//	"Reversible per-doc: an array shrinking from 3 elements to 1 next time
//	 round will see its single new entry written with IndexValueScalar."
//
// Setup: insert {tags:["a","b","c"]} → 4 entries (3 elements + whole-array),
// all IndexValueMultiKey because len(keysBuf) > 1 at insertKeys time.
//
// Then update to {tags:["x"]} (a single-element array). The update path
// (collection.update at collection.go:407) calls deleteKeys(prev) +
// insertKeys(new). After the update, only entries derived from the new
// value should remain.
//
// IMPORTANT — observed actual behaviour: a single-element array still
// produces TWO keysBuf entries inside writeValues (one for the element "x",
// one for the whole-array ["x"]). See audit_01 single-element-array case.
// So len(keysBuf) == 2 at insertKeys time, which means the "shrunk" entries
// are STILL written as IndexValueMultiKey. The doc claim's wording
// ("array shrinking from 3 elements to 1") therefore only delivers
// IndexValueScalar when the field type changes from array to scalar
// (covered by the next subtest), not when the array merely shrinks to a
// single element. Pinning the actually-observed behaviour here.
func TestAudit02_Reversibility_ArrayShrinksToSingleElement(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit02_shrink")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))

	// Initial insert: 3-element array → 4 entries (elements + whole-array),
	// every entry IndexValueMultiKey.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":["a","b","c"]}`)))

	before := readRawIndexEntries(t, fx.DB, "audit02_shrink", "ix_tags")
	require.Len(t, before, 4, "3-element array baseline must be 4 entries")
	for i, e := range before {
		require.NotEmptyf(t, e.Value, "before entry %d: value must not be empty", i)
		assert.Equalf(t, qplanner.IndexValueMultiKey, e.Value,
			"before entry %d: 3-element array must be multi-key", i)
	}

	// Update: array shrinks to a single element. The update path will
	// deleteKeys(prev) — removing all 4 old entries — then insertKeys(new).
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"tags":["x"]}`)))

	after := readRawIndexEntries(t, fx.DB, "audit02_shrink", "ix_tags")
	// Observed: single-element array still emits 2 keysBuf entries
	// (element "x" + whole-array ["x"]). All old entries for "a","b","c"
	// + the old whole-array must be gone.
	require.Len(t, after, 2,
		"after shrink to single-element array: 2 entries (element + whole-array), "+
			"old multi-element entries must be deleted")
	for i, e := range after {
		require.NotEmptyf(t, e.Value, "after entry %d: value must not be empty", i)
		// Pinning observed behaviour: even though the array "shrunk", a
		// single-element array still produces len(keysBuf) == 2, so each
		// surviving entry is still tagged IndexValueMultiKey. The literal
		// reading of the doc claim ("entry written with IndexValueScalar")
		// therefore does NOT apply when shrinking to a 1-element array —
		// only when shrinking to a scalar field. Surprising but consistent
		// with audit_01_valuebyte_basic_test.go's single-element-array
		// pin.
		assert.Equalf(t, qplanner.IndexValueMultiKey, e.Value,
			"after entry %d: single-element array still tagged multi-key "+
				"(len(keysBuf)==2 at insertKeys time)", i)
		assert.NotZerof(t, e.Value[0]&qplanner.IndexEntryFlagMultiKey,
			"after entry %d: multi-key bit must be set", i)
	}

	// Additionally verify none of the old element keys ("a","b","c") are
	// findable — proving deleteKeys ran cleanly.
	for _, gone := range []string{"a", "b", "c"} {
		count, qerr := coll.Find(`{"tags":"` + gone + `"}`).Count(ctx)
		require.NoError(t, qerr)
		assert.Equalf(t, 0, count, "old tag %q must be gone after update", gone)
	}
	count, qerr := coll.Find(`{"tags":"x"}`).Count(ctx)
	require.NoError(t, qerr)
	assert.Equal(t, 1, count, "new tag \"x\" must be findable")
}

// TestAudit02_Reversibility_ArrayShrinksToScalar exercises the actual
// reversibility path that produces IndexValueScalar: the field type changes
// from array (multi-key) to scalar (single key). This is the case the
// index.go:148-150 doc claim is really about.
//
// Setup: insert {tags:["a","b","c"]} → 4 multi-key entries.
// Update:  set tags to a plain scalar string "single".
// After:   exactly ONE entry, tagged IndexValueScalar (bit 0 cleared).
func TestAudit02_Reversibility_ArrayShrinksToScalar(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit02_to_scalar")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":["a","b","c"]}`)))

	before := readRawIndexEntries(t, fx.DB, "audit02_to_scalar", "ix_tags")
	require.Len(t, before, 4, "3-element array baseline must be 4 entries")
	for i, e := range before {
		require.NotEmptyf(t, e.Value, "before entry %d: value must not be empty", i)
		assert.Equalf(t, qplanner.IndexValueMultiKey, e.Value,
			"before entry %d: 3-element array must be multi-key", i)
	}

	// Update: tags becomes a scalar string, NOT an array. writeValues
	// takes the non-array path → exactly one keysBuf entry → insertKeys
	// records IndexValueScalar.
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"tags":"single"}`)))

	after := readRawIndexEntries(t, fx.DB, "audit02_to_scalar", "ix_tags")
	require.Len(t, after, 1,
		"after shrink to scalar: exactly one entry must remain "+
			"(per-doc reversibility — old 4 multi-key entries deleted, new scalar entry written)")
	require.NotEmpty(t, after[0].Value, "surviving entry value must not be empty")
	assert.Equal(t, qplanner.IndexValueScalar, after[0].Value,
		"shrink-to-scalar must write IndexValueScalar (bit 0 cleared) — "+
			"this is the per-doc reversibility claim at index.go:148-150")
	assert.Zero(t, after[0].Value[0]&qplanner.IndexEntryFlagMultiKey,
		"multi-key flag bit must be cleared on the new scalar entry")

	// Old element keys gone, new scalar findable.
	for _, gone := range []string{"a", "b", "c"} {
		count, qerr := coll.Find(`{"tags":"` + gone + `"}`).Count(ctx)
		require.NoError(t, qerr)
		assert.Equalf(t, 0, count, "old tag %q must be gone after update", gone)
	}
	count, qerr := coll.Find(`{"tags":"single"}`).Count(ctx)
	require.NoError(t, qerr)
	assert.Equal(t, 1, count, "new scalar value \"single\" must be findable")
}

// TestAudit02_Reversibility_ScalarGrowsToArray covers the reverse direction
// of reversibility: a doc that was originally scalar (one entry tagged
// IndexValueScalar) gets updated to a multi-element array. The new entries
// must all be tagged IndexValueMultiKey AND the original scalar entry must
// be deleted (no stale entry left behind with the wrong bit).
func TestAudit02_Reversibility_ScalarGrowsToArray(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit02_to_array")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))

	// Initial: scalar string → exactly one entry, IndexValueScalar.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":"x"}`)))

	before := readRawIndexEntries(t, fx.DB, "audit02_to_array", "ix_tags")
	require.Len(t, before, 1, "scalar baseline must be exactly 1 entry")
	require.NotEmpty(t, before[0].Value)
	assert.Equal(t, qplanner.IndexValueScalar, before[0].Value,
		"scalar baseline entry must be tagged IndexValueScalar")
	originalScalarKey := before[0].Key

	// Update: scalar → 2-element array. New keysBuf will have 2 element
	// entries + 1 whole-array entry = 3 entries, all IndexValueMultiKey.
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"tags":["a","b"]}`)))

	after := readRawIndexEntries(t, fx.DB, "audit02_to_array", "ix_tags")
	require.Len(t, after, 3,
		"after scalar → 2-element array: 3 entries (2 elements + whole-array)")
	for i, e := range after {
		require.NotEmptyf(t, e.Value, "after entry %d: value must not be empty", i)
		assert.Equalf(t, qplanner.IndexValueMultiKey, e.Value,
			"after entry %d: every new array entry must be IndexValueMultiKey", i)
		assert.NotZerof(t, e.Value[0]&qplanner.IndexEntryFlagMultiKey,
			"after entry %d: multi-key bit must be set", i)
	}

	// Assert the original scalar entry's exact key is gone — proving
	// deleteKeys cleaned up the prior shape before insertKeys ran.
	for i, e := range after {
		assert.NotEqualf(t, originalScalarKey, e.Key,
			"after entry %d: original scalar entry key must have been deleted", i)
	}

	// Old scalar value "x" must no longer be findable except as part of
	// the new array — but since we replaced "x" with ["a","b"], a query
	// for tags:"x" should return 0.
	count, qerr := coll.Find(`{"tags":"x"}`).Count(ctx)
	require.NoError(t, qerr)
	assert.Equal(t, 0, count, "old scalar value \"x\" must be gone after update")

	for _, want := range []string{"a", "b"} {
		count, qerr := coll.Find(`{"tags":"` + want + `"}`).Count(ctx)
		require.NoError(t, qerr)
		assert.Equalf(t, 1, count, "new tag %q must be findable", want)
	}
}

// TestAudit10_RangeMultiKey_SingleDocCountAndIter verifies subtests 1 & 2:
//   - Count() on $gte:"a", $lte:"c" against a single doc with tags=[a,b,c]
//     returns 1 (NOT 3) — dedup happens.
//   - Iter() over the same range yields d1 exactly once — planIterator's
//     consumer-side DocDedup gate (or upstream CanonicalKeyDedupIter)
//     collapses the duplicates.
func TestAudit10_RangeMultiKey_SingleDocCountAndIter(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit10_single")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b","c"]}`),
	))

	// Sanity: confirm the raw index actually holds multiple entries for d1
	// in the range — otherwise the test wouldn't be exercising the case
	// the audit is about. Expect 4 entries: one per element ("a","b","c")
	// plus the whole-array entry.
	rawEntries := readRawIndexEntries(t, fx.DB, "audit10_single", "ix_tags")
	require.Len(t, rawEntries, 4,
		"single doc with 3-element array must produce 4 raw index entries "+
			"(one per element + one whole-array). If this changes, the audit assumptions need re-checking.")

	t.Run("Count over $gte/$lte range collapses to one doc", func(t *testing.T) {
		count, err := coll.Find(`{"tags":{"$gte":"a","$lte":"c"}}`).Count(ctx)
		require.NoError(t, err)
		// PINNED behaviour: Count returns 1 (the number of distinct
		// matching docs), NOT 3 (the number of in-range raw index entries
		// for d1) and NOT 4 (3 elements + whole-array entry).
		//
		// If this assertion ever fails with count==3 or count==4, it
		// means a planner change has exposed the raw IndexIter (or its
		// countEntriesBatch fast path) to Count for a range scan,
		// bypassing the CanonicalKeyDedupIter wrap. That would be a
		// correctness regression — Count() must report distinct docs.
		assert.Equal(t, 1, count,
			"$gte/$lte range over [a,c] on tags=[a,b,c] must count d1 exactly once, "+
				"not once per matching array element")
	})

	t.Run("Iter over $gte/$lte range yields the doc once", func(t *testing.T) {
		ids := collectIdsString(t, coll.Find(`{"tags":{"$gte":"a","$lte":"c"}}`))
		// PINNED behaviour: planIterator's dedup (either via
		// CanonicalKeyDedupIter wrap or the consumer-side DocDedup gate
		// in planIterator.Next) collapses the multiple in-range hits for
		// d1 to a single emission.
		assert.Equal(t, []string{"d1"}, ids,
			"Iter over $gte/$lte range must yield d1 exactly once, not once per matching array element")
	})
}

// TestAudit10_RangeMultiKey_RangeAcrossMultipleDocs covers subtest 4:
// the canonical multi-doc range case. Range [a, c]: d1 (tags=[a,b])
// matches via "a" and "b"; d2 (tags=[b,c]) matches via "b" and "c"; d3
// (tags=[x,y]) does not match. Count must be 2 distinct docs (NOT 4
// raw entries: a/d1, b/d1, b/d2, c/d2 + whole-array entries).
func TestAudit10_RangeMultiKey_RangeAcrossMultipleDocs(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit10_range_multi")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b"]}`),
		anyenc.MustParseJson(`{"id":"d2","tags":["b","c"]}`),
		anyenc.MustParseJson(`{"id":"d3","tags":["x","y"]}`),
	))

	// Sanity: verify that a naive count of raw entries in the range
	// would over-report. d1 contributes entries at keys "a" and "b"
	// (plus whole-array entry — but the whole-array entry is ["a","b"]
	// which sorts AFTER "b" in any-enc array encoding, so it may or may
	// not fall in [a,c]). We don't pin the exact raw count here — it's
	// fine for that to evolve — we just verify it's >2 (more than one
	// per doc) so the test is meaningful.
	rawEntries := readRawIndexEntries(t, fx.DB, "audit10_range_multi", "ix_tags")
	// d1: 2 elements + 1 whole = 3 entries
	// d2: 2 elements + 1 whole = 3 entries
	// d3: 2 elements + 1 whole = 3 entries
	// total: 9
	require.Len(t, rawEntries, 9,
		"3 docs with 2-element arrays each must produce 9 raw index entries "+
			"(per doc: 2 elements + 1 whole-array)")

	t.Run("Count over [a,c] returns 2 distinct docs", func(t *testing.T) {
		count, err := coll.Find(`{"tags":{"$gte":"a","$lte":"c"}}`).Count(ctx)
		require.NoError(t, err)
		// PINNED: 2 distinct docs. Both d1 and d2 have multiple matching
		// entries in the range; if dedup failed we'd see ≥4. This is
		// the canonical assertion of the audit.
		assert.Equal(t, 2, count,
			"$gte/$lte range [a,c] must count d1 and d2 exactly once each "+
				"(got %d) — d3 has no matching tags", count)
	})

	t.Run("Iter over [a,c] yields d1 and d2 exactly once each", func(t *testing.T) {
		ids := collectIdsString(t, coll.Find(`{"tags":{"$gte":"a","$lte":"c"}}`))
		assert.ElementsMatch(t, []string{"d1", "d2"}, ids,
			"Iter over [a,c] must yield exactly {d1, d2}")
		assert.Len(t, ids, 2, "Iter must dedup; no duplicates allowed")
	})
}

// TestAudit04_WithinDocDedup_* exercises the per-document key-set built by
// index.go::writeValues for an array-valued field, focusing on what happens
// when the array contains duplicates.
//
// Recap of the relevant control flow (index.go ~L207-L249):
//
//   - For each array element, isUnique() decides whether to emit a key for
//     the per-element pass. Duplicate elements that have already been
//     emitted in the same writeValues frame are skipped.
//   - After the loop completes, writeValues falls through and ALWAYS emits
//     one more key — `idx.keyBuf = v.MarshalTo(k); writeValues(d, i+1)` —
//     keyed on the array as a whole (its full marshaled form).
//
// insertKeys (index.go ~L150) then writes IndexValueScalar when
// len(keysBuf) == 1 and IndexValueMultiKey when len(keysBuf) > 1. So the
// "single-element array" case is still classified as multi-key here,
// because the per-element pass adds 1 key and the post-loop fall-through
// adds 1 more, for 2 keys total.
func TestAudit04_WithinDocDedup_AllDuplicates(t *testing.T) {
	// {tags:["a","a","a"]} → isUnique collapses 3 elements to 1 emission,
	// then the post-loop whole-array marshal adds 1 more = 2 keys total.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "tags", Fields: []string{"tags"}}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":["a","a","a"]}`)))

	entries := readRawIndexEntries(t, fx.DB, "test", "tags")
	assert.Len(t, entries, 2,
		"all-duplicate array: 1 deduped element + 1 whole-array marshal = 2 entries")

	for i, e := range entries {
		assert.True(t, bytes.Equal(e.Value, qplanner.IndexValueMultiKey),
			"entry[%d] value=%v expected IndexValueMultiKey=%v",
			i, e.Value, qplanner.IndexValueMultiKey)
	}
}

func TestAudit04_WithinDocDedup_TwoUnique(t *testing.T) {
	// {tags:["a","a","b"]} → "a","b" emitted by per-element pass + whole-array
	// fall-through = 3 keys total.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "tags", Fields: []string{"tags"}}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":["a","a","b"]}`)))

	entries := readRawIndexEntries(t, fx.DB, "test", "tags")
	assert.Len(t, entries, 3,
		"['a','a','b']: 2 deduped elements + 1 whole-array marshal = 3 entries")

	for i, e := range entries {
		assert.True(t, bytes.Equal(e.Value, qplanner.IndexValueMultiKey),
			"entry[%d] value=%v expected IndexValueMultiKey=%v",
			i, e.Value, qplanner.IndexValueMultiKey)
	}
}

func TestAudit04_WithinDocDedup_DupAtEnd(t *testing.T) {
	// {tags:["a","b","c","a"]} — duplicate at the end. isUnique drops the
	// trailing "a" because it was already emitted in this frame. 3 unique
	// elements + 1 whole-array marshal = 4 keys.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "tags", Fields: []string{"tags"}}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":["a","b","c","a"]}`)))

	entries := readRawIndexEntries(t, fx.DB, "test", "tags")
	assert.Len(t, entries, 4,
		"['a','b','c','a']: 3 deduped elements ('a','b','c') + 1 whole-array marshal = 4 entries")

	for i, e := range entries {
		assert.True(t, bytes.Equal(e.Value, qplanner.IndexValueMultiKey),
			"entry[%d] value=%v expected IndexValueMultiKey=%v",
			i, e.Value, qplanner.IndexValueMultiKey)
	}
}

func TestAudit04_WithinDocDedup_SingleElementArray(t *testing.T) {
	// {tags:["a"]} — surprising case. The per-element pass emits 1 key
	// ("a"), then the post-loop fall-through marshals the whole array as
	// another key ([ "a" ] in encoded form). Total = 2 keys, so
	// len(keysBuf) > 1 and insertKeys writes IndexValueMultiKey for BOTH
	// entries — even though only one logical value exists in the array.
	//
	// This is "multi-key" by the strict len(keysBuf) > 1 definition, not by
	// any user-facing notion of multi-valued field.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "tags", Fields: []string{"tags"}}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":["a"]}`)))

	entries := readRawIndexEntries(t, fx.DB, "test", "tags")
	assert.Len(t, entries, 2,
		"['a']: 1 element key + 1 whole-array marshal key = 2 entries (surprising but per spec)")

	for i, e := range entries {
		assert.True(t, bytes.Equal(e.Value, qplanner.IndexValueMultiKey),
			"entry[%d] value=%v expected IndexValueMultiKey=%v "+
				"(single-element array still classified multi-key because len(keysBuf)>1)",
			i, e.Value, qplanner.IndexValueMultiKey)
	}
}

// TestAudit08_CompoundArrayArray_BasicCartesian pins the worst-case fan-out
// path through index.go::writeValues — a compound index where BOTH dimensions
// are arrays. With doc {tags:["a","b"], cats:["x","y"]} and Fields:["tags","cats"]:
//
// writeValues recursion at i=0 (tags is array): emits one branch per element
// "a","b" plus one fall-through branch for the whole array ["a","b"]. For each
// of those 3 branches, recursion at i=1 (cats is array) again emits per-element
// "x","y" plus one fall-through whole-array ["x","y"]. Total: 3 * 3 = 9 entries.
//
// Because len(idx.keysBuf) > 1 by the time insertKeys reads it, ALL 9 entries
// must be tagged IndexValueMultiKey — including the whole-array entries.
// Pinning both shape (count) and value byte for this entirely untested path.
func TestAudit08_CompoundArrayArray_BasicCartesian(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit08_basic")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags_cats",
		Fields: []string{"tags", "cats"},
	}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
		`{"id":"d1","tags":["a","b"],"cats":["x","y"]}`,
	)))

	entries := readRawIndexEntries(t, fx.DB, "audit08_basic", "ix_tags_cats")

	// Pin the actual count: writeValues recursion produces (N+1) * (M+1) entries
	// where N=len(tags), M=len(cats). N=2, M=2 → 3 * 3 = 9 entries.
	// Composition: 4 element-x-element (Cartesian) + 2 element-x-wholeArr +
	// 2 wholeArr-x-element + 1 wholeArr-x-wholeArr = 9.
	require.Lenf(t, entries, 9,
		"compound array x array: writeValues fan-out is (N+1)*(M+1)=3*3=9 entries "+
			"(4 Cartesian + 2+2 whole-array fall-throughs + 1 whole-x-whole)")

	for i, e := range entries {
		assert.Equalf(t, qplanner.IndexValueMultiKey, e.Value,
			"entry %d: every entry must be IndexValueMultiKey since len(keysBuf)=9 > 1", i)
		require.NotEmptyf(t, e.Value, "entry %d: value byte must not be empty", i)
		assert.NotZerof(t, e.Value[0]&qplanner.IndexEntryFlagMultiKey,
			"entry %d: multi-key flag bit must be set", i)
	}
}

// TestAudit12_SparseEmptyArray_NoFieldZeroEntries: doc with no `tags`
// field at all. Sparse guard short-circuits on v == nil → 0 entries.
// Baseline for what "sparse" is supposed to do.
func TestAudit12_SparseEmptyArray_NoFieldZeroEntries(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit12_no_field")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
		Sparse: true,
	}))

	// Doc with NO `tags` field — sparse guard hits v == nil branch.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))

	entries := readRawIndexEntries(t, fx.DB, "audit12_no_field", "ix_tags")
	require.Empty(t, entries,
		"sparse index over missing field must produce zero entries (v == nil branch)")

	assertIndexLen(t, coll.GetIndexes()[0], 0)
}

// TestAudit12_SparseEmptyArray_NullFieldOneEntry: doc with explicit
// `tags: null`. The field exists, so the sparse index holds it under the
// null key.
func TestAudit12_SparseEmptyArray_NullFieldOneEntry(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit12_null_field")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
		Sparse: true,
	}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":null}`)))

	entries := readRawIndexEntries(t, fx.DB, "audit12_null_field", "ix_tags")
	require.Len(t, entries, 1, "an explicit null is present and gets the null key")
	assert.Equal(t, byte(anyenc.TypeNull), entries[0].Key[0])

	assertIndexLen(t, coll.GetIndexes()[0], 1)
}

// TestAudit12_SparseEmptyArray_EmptyArrayBehaviour: doc with `tags: []`
// — empty array. Pins that an empty array IS indexed (sparse skips only
// a missing field), and confirms the entry uses
// the whole-empty-array marshalled key with IndexValueScalar (because
// len(keysBuf) == 1 at insertKeys time — no per-element entries since
// the array has no elements).
//
// This makes Find({tags:[]}) work via the index; the queryability half is
// TestAudit12_SparseEmptyArray_EmptyArrayQueryable in
// any-store-tests:apitest.
func TestAudit12_SparseEmptyArray_EmptyArrayBehaviour(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit12_empty_arr_sparse")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
		Sparse: true,
	}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":[]}`)))

	entries := readRawIndexEntries(t, fx.DB, "audit12_empty_arr_sparse", "ix_tags")

	// Empty array IS indexed — exactly 1 entry, the whole-empty-array
	// marshal. Matches MongoDB's sparse-index semantics: sparse skips a
	// missing field only, empty arrays are present queryable values.
	require.Len(t, entries, 1,
		"empty array on sparse index produces exactly 1 entry — the "+
			"whole-empty-array marshal. Sparse skips only a missing field.")

	// keysBuf had exactly one entry → IndexValueScalar (0x00).
	require.NotEmpty(t, entries[0].Value)
	assert.Equal(t, qplanner.IndexValueScalar, entries[0].Value,
		"len(keysBuf)==1 at insertKeys time → empty-array entry tagged IndexValueScalar (0x00)")
	assert.Zero(t, entries[0].Value[0]&qplanner.IndexEntryFlagMultiKey,
		"multi-key flag bit must be cleared (single key in keysBuf)")

	assertIndexLen(t, coll.GetIndexes()[0], 1)
}

// TestAudit12_SparseEmptyArray_NonSparse: same empty-array doc, but on
// a NON-sparse index. Behaviour matches the sparse case for this
// input: 1 entry (the whole-array marshal). Confirms the empty-array
// handling is a property of writeValues, not of the sparse flag.
func TestAudit12_SparseEmptyArray_NonSparse(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit12_empty_arr_nonsparse")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
		// Sparse: false (default).
	}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":[]}`)))

	entries := readRawIndexEntries(t, fx.DB, "audit12_empty_arr_nonsparse", "ix_tags")

	require.Len(t, entries, 1,
		"non-sparse index + empty array also produces exactly 1 entry "+
			"(whole-array marshal). Confirms empty-array indexing is "+
			"independent of the sparse flag.")

	require.NotEmpty(t, entries[0].Value)
	assert.Equal(t, qplanner.IndexValueScalar, entries[0].Value,
		"len(keysBuf)==1 → empty-array entry on non-sparse index also tagged IndexValueScalar")
	assert.Zero(t, entries[0].Value[0]&qplanner.IndexEntryFlagMultiKey,
		"multi-key flag bit must be cleared (single key in keysBuf)")

	assertIndexLen(t, coll.GetIndexes()[0], 1)
}

// TestAudit13_UniqueCompoundArray_NoCollision: d1 (x,[a,b]) + d2 (x,[c,d]).
// No shared (category, tag) tuple. Both inserts succeed; verify entry counts
// and that ALL entries carry IndexValueMultiKey (because each doc's keysBuf
// has > 1 entries from the array expansion).
func TestAudit13_UniqueCompoundArray_NoCollision(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit13_no_collision")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_cat_tags",
		Fields: []string{"category", "tags"},
		Unique: true,
	}))

	// d1: tags=["a","b"] → keysBuf entries (x,a), (x,b), (x,["a","b"]) = 3
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","category":"x","tags":["a","b"]}`)))
	// d2: tags=["c","d"] → keysBuf entries (x,c), (x,d), (x,["c","d"]) = 3
	// No shared (x,*) tuple with d1 → no unique conflict.
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d2","category":"x","tags":["c","d"]}`)))

	assertCollCount(t, coll, 2)
	idx := coll.GetIndexes()[0]
	// 3 entries per doc * 2 docs = 6 entries.
	assertIndexLen(t, idx, 6)

	entries := readRawIndexEntries(t, fx.DB, "audit13_no_collision", "ix_cat_tags")
	require.Len(t, entries, 6,
		"compound + array: 2 docs * 3 keys/doc = 6 entries")

	// Every entry must carry IndexValueMultiKey because each doc's keysBuf
	// has 3 entries (>1) — the array dimension forces the multi-key tag on
	// EVERY emitted key for that doc, including the whole-array fall-through.
	for i, e := range entries {
		assert.Equalf(t, qplanner.IndexValueMultiKey, e.Value,
			"entry %d: compound + array insert must write IndexValueMultiKey "+
				"because keysBuf > 1 from array expansion", i)
		require.NotEmptyf(t, e.Value, "entry %d: value must not be empty", i)
		assert.NotZerof(t, e.Value[0]&qplanner.IndexEntryFlagMultiKey,
			"entry %d: multi-key flag bit must be set", i)
	}
}

// TestAudit13_UniqueCompoundArray_CollisionRollsBack: after the failed d2
// insert, the collection must contain only d1; assert via Count and via
// raw-entry inspection (d2's partial entries — including any per-element
// Put() that succeeded BEFORE the colliding one — must NOT linger).
//
// This is the critical rollback-safety assertion: insertKeys returns the
// error to insertItem, which returns it to db.doWriteTx, which calls
// tx.Rollback(). If the namespace ever shows a stray d2 entry post-rollback,
// that's a bug in the transaction layer, not in insertKeys itself.
func TestAudit13_UniqueCompoundArray_CollisionRollsBack(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit13_rollback")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_cat_tags",
		Fields: []string{"category", "tags"},
		Unique: true,
	}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","category":"x","tags":["a","b"]}`)))

	// Snapshot pre-collision: 3 entries for d1.
	idx := coll.GetIndexes()[0]
	assertIndexLen(t, idx, 3)
	preEntries := readRawIndexEntries(t, fx.DB, "audit13_rollback", "ix_cat_tags")
	require.Len(t, preEntries, 3,
		"pre-collision baseline: d1 must have exactly 3 index entries (a, b, [a,b])")

	// Trigger the collision. d2 emits (x,a) [no conflict] then (x,b)
	// [conflict with d1] OR (x,b) [conflict immediately] depending on
	// array iteration order — either way the whole tx must roll back.
	err = coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d2","category":"x","tags":["b","c"]}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	// Doc-level assertion: only d1 in the collection.
	assertCollCount(t, coll, 1)

	// Index-level assertion: still exactly 3 entries (d1's), no orphans.
	assertIndexLen(t, idx, 3)
	postEntries := readRawIndexEntries(t, fx.DB, "audit13_rollback", "ix_cat_tags")
	require.Len(t, postEntries, 3,
		"post-rollback: namespace must hold ONLY d1's 3 entries — "+
			"any additional row would prove a partial d2 write leaked through rollback")

	// Stronger check: the entry bytes must be identical to the pre-collision
	// snapshot — same keys, same values. If d2 partially wrote anything, the
	// post-set wouldn't equal the pre-set.
	require.Equal(t, len(preEntries), len(postEntries),
		"entry count must be unchanged across the rolled-back tx")
	for i := range preEntries {
		assert.Equalf(t, preEntries[i].Key, postEntries[i].Key,
			"entry %d key changed after rolled-back collision", i)
		assert.Equalf(t, preEntries[i].Value, postEntries[i].Value,
			"entry %d value changed after rolled-back collision", i)
	}

	// Sanity: queries still find d1 by every original element of its array.
	count, err := coll.Find(`{"category":"x","tags":"a"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "d1 must still match (category=x, tags=a)")
	count, err = coll.Find(`{"category":"x","tags":"b"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "d1 must still match (category=x, tags=b)")

	// And the colliding tag value "c" (which only d2 had) must NOT match
	// anything — confirming d2 left no trace.
	count, err = coll.Find(`{"category":"x","tags":"c"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count,
		"tag 'c' belonged only to the rolled-back d2 — must match nothing")
}

// TestAudit13_UniqueCompoundArray_IdempotentSelfReinsert exercises the
// `continue` branch in insertKeys (seekBuf == fullKeyBuf path).
//
// Two-pronged coverage:
//
//  1. User-facing UpdateOne(d1) -> d1 with same data. The collection.update
//     fast-path short-circuits via anyencutil.Equal BEFORE insertKeys runs,
//     so the operation is a no-op. We assert it succeeds and changes
//     nothing — entry count unchanged, every entry still IndexValueMultiKey.
//
//  2. Direct double insertKeys() in a single write-tx. This is the only way
//     to actually reach the `continue` branch — second invocation finds the
//     existing entry whose (key, docId) matches fullKeyBuf bytewise and
//     skips the Put without erroring.
func TestAudit13_UniqueCompoundArray_IdempotentSelfReinsert(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit13_idempotent")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_cat_tags",
		Fields: []string{"category", "tags"},
		Unique: true,
	}))

	docJSON := `{"id":"d1","category":"x","tags":["a","b"]}`
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(docJSON)))

	idx := coll.GetIndexes()[0]
	// 3 entries: (x,a), (x,b), (x,["a","b"]).
	assertIndexLen(t, idx, 3)

	preEntries := readRawIndexEntries(t, fx.DB, "audit13_idempotent", "ix_cat_tags")
	require.Len(t, preEntries, 3)
	for i, e := range preEntries {
		require.Equalf(t, qplanner.IndexValueMultiKey, e.Value,
			"pre-update entry %d must already be IndexValueMultiKey "+
				"(array dimension >1 keysBuf entries)", i)
	}

	// === Prong 1: UpdateOne to itself ===
	// collection.update short-circuits on anyencutil.Equal BEFORE insertKeys,
	// so this is effectively a no-op — but the public contract is that it
	// MUST succeed without raising ErrUniqueConstraint.
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(docJSON)),
		"updating d1 to identical data must succeed (short-circuit, no error)")

	// Entry count unchanged.
	assertIndexLen(t, idx, 3)

	postEntries := readRawIndexEntries(t, fx.DB, "audit13_idempotent", "ix_cat_tags")
	require.Len(t, postEntries, 3,
		"self-update must not alter the index entry count")

	// Every post-update entry must still carry IndexValueMultiKey, and the
	// raw bytes must match the pre-update snapshot exactly (no churn).
	for i, e := range postEntries {
		assert.Equalf(t, qplanner.IndexValueMultiKey, e.Value,
			"post-update entry %d must still be IndexValueMultiKey", i)
		assert.Equalf(t, preEntries[i].Key, e.Key,
			"entry %d key changed across self-update", i)
		assert.Equalf(t, preEntries[i].Value, e.Value,
			"entry %d value changed across self-update", i)
	}

	// === Prong 2: Directly invoke insertKeys twice in one tx ===
	// This actually exercises the `continue` branch. We re-fetch the index
	// pointer because GetIndexes() returns the public Index interface; we
	// need the concrete *index for insertKeys.
	idxImpl := idx.(*index)
	it, itErr := newItem(anyenc.MustParseJson(docJSON))
	require.NoError(t, itErr)

	wrTx, err := coll.WriteTx(ctx)
	require.NoError(t, err)
	btWtx := wrTx.btreeWriteTx()

	// First call: would normally fail because the entries already exist
	// — but the unique check sees seekBuf == fullKeyBuf for the SAME docId
	// and takes `continue` instead of returning ErrUniqueConstraint.
	require.NoError(t, idxImpl.insertKeys(btWtx, it),
		"re-inserting the same (key, docId) tuples must hit `continue`, "+
			"not raise ErrUniqueConstraint")

	// Second consecutive call inside the same tx — still idempotent.
	require.NoError(t, idxImpl.insertKeys(btWtx, it),
		"second back-to-back insertKeys must also be a no-op via `continue`")

	// Namespace count must still be exactly 3 — no duplicates were added.
	count, err := btWtx.Count(idxImpl.ns)
	require.NoError(t, err)
	assert.Equal(t, 3, count,
		"idempotent re-insert(s) must not add any duplicate index rows")

	require.NoError(t, wrTx.Rollback())
}

// TestMultikeyFlag_Lifecycle drives the persisted scalar/multikey flag through
// every transition that decides whether tight seek bounds are sound.
func TestMultikeyFlag_Lifecycle(t *testing.T) {
	newColl := func(t *testing.T, name string) Collection {
		coll, err := newFixture(t).CreateCollection(ctx, name)
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1,"a":1}`),
			anyenc.MustParseJson(`{"id":2,"a":3}`),
			anyenc.MustParseJson(`{"id":3,"a":7}`),
		))
		return coll
	}

	t.Run("scalar index serves tight bounds", func(t *testing.T) {
		coll := newColl(t, "c")
		assertTightBounds(t, coll, twoSided)
		assertQueryCount(t, coll.Find(twoSided), 1) // a=3
	})

	t.Run("first array write flips to wide", func(t *testing.T) {
		coll := newColl(t, "c")
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":[6,1]}`)))
		assertWideBounds(t, coll, twoSided)
		// The array doc matches via different elements (6>2, 1<5) — a tight
		// seek would have dropped it.
		assertQueryCount(t, coll.Find(twoSided).IndexHint(IndexHint{IndexName: "a", Boost: 1_000_000}), 2)
	})

	t.Run("backfill over existing arrays flips", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":[6,1]}`)))
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
		assertWideBounds(t, coll, twoSided)
	})

	t.Run("empty arrays do not flip", func(t *testing.T) {
		coll := newColl(t, "c")
		// A doc with an empty array produces exactly one whole-value entry —
		// single-entry docs are always sound under intersection.
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":[]}`)))
		assertTightBounds(t, coll, twoSided)
		assertQueryCount(t, coll.Find(twoSided), 1)
	})

	t.Run("deleting all arrays does not clear", func(t *testing.T) {
		coll := newColl(t, "c")
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":[6,1]}`)))
		require.NoError(t, coll.DeleteId(ctx, 4))
		// One-way: older snapshots may still hold the fanned-out entries.
		assertWideBounds(t, coll, twoSided)
	})

	t.Run("drop and recreate resets", func(t *testing.T) {
		coll := newColl(t, "c")
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":[6,1]}`)))
		require.NoError(t, coll.DeleteId(ctx, 4))
		require.NoError(t, coll.DropIndex(ctx, "a"))
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
		assertTightBounds(t, coll, twoSided)
	})

	t.Run("rename does not resurrect a stale scalar record", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "renA")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":3}`)))
		assertTightBounds(t, coll, twoSided)

		// A -> B, arrays inserted under B, then back to A: the flag record
		// must travel with the rename, or the stale scalar record written
		// under A would unsoundly re-enable tight seeks.
		require.NoError(t, coll.Rename(ctx, "renB"))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":[6,1]}`)))
		assertWideBounds(t, coll, twoSided)
		require.NoError(t, coll.Rename(ctx, "renA"))
		assertWideBounds(t, coll, twoSided)
		assertQueryCount(t, coll.Find(twoSided).IndexHint(IndexHint{IndexName: "a", Boost: 1_000_000}), 2)
	})

	t.Run("absent record means wide", func(t *testing.T) {
		coll := newColl(t, "c")
		// Simulate an index built before the flag existed by deleting the
		// record out from under the collection.
		c := coll.(*collection)
		require.NoError(t, c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
			return tx.Delete(c.db.systemNS, multikeyKey(c.loadIndexes()[0].ns.Name()))
		}))
		assertWideBounds(t, coll, twoSided)
	})

	t.Run("top-level rollback keeps record and entries consistent", func(t *testing.T) {
		coll := newColl(t, "c")
		tx, err := coll.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":4,"a":[6,1]}`)))
		require.NoError(t, tx.Rollback())

		// Entries and flag rolled back together.
		assertTightBounds(t, coll, twoSided)
		assertQueryCount(t, coll.Find(twoSided), 1)

		// The next committed array write still flips.
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":5,"a":[6,1]}`)))
		assertWideBounds(t, coll, twoSided)
	})

	t.Run("savepoint rollback keeps record and entries consistent", func(t *testing.T) {
		coll := newColl(t, "c")
		outer, err := coll.WriteTx(ctx)
		require.NoError(t, err)
		inner, err := coll.WriteTx(outer.Context())
		require.NoError(t, err)
		require.NoError(t, coll.Insert(inner.Context(), anyenc.MustParseJson(`{"id":4,"a":[6,1]}`)))
		require.NoError(t, inner.Rollback())
		require.NoError(t, outer.Commit())

		// The nested rollback reverted the entries AND the flag together:
		// tight bounds stay sound.
		assertTightBounds(t, coll, twoSided)
		assertQueryCount(t, coll.Find(twoSided), 1)

		// And a subsequent real array write still flips — nothing cached a
		// stale "already flagged" state.
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":5,"a":[6,1]}`)))
		assertWideBounds(t, coll, twoSided)
	})

	t.Run("snapshot visibility", func(t *testing.T) {
		coll := newColl(t, "c")
		rt, err := coll.ReadTx(ctx)
		require.NoError(t, err)
		defer rt.Commit()

		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":[6,1]}`)))

		// The old snapshot holds neither the fan-out entries nor the flag:
		// tight bounds are exactly right for it.
		explain, err := coll.Find(twoSided).
			IndexHint(IndexHint{IndexName: "a", Boost: 1_000_000}).Explain(rt.Context())
		require.NoError(t, err)
		assert.NotContains(t, explain.Sql, "inf]", "old snapshot must keep tight bounds: %s", explain.Sql)

		// A fresh snapshot sees both.
		assertWideBounds(t, coll, twoSided)
	})
}

// Helper to collect a specific field value from query results
func collectField(t testing.TB, q Query, field string) []string {
	t.Helper()
	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var results []string
	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err)
		v := doc.Value().Get(field)
		if v != nil {
			results = append(results, v.String())
		}
	}
	require.NoError(t, iter.Err())
	return results
}

// collectIntField collects an integer field from query results as []int.
func collectIntField(t testing.TB, q Query, field string) []int {
	t.Helper()
	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var results []int
	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err)
		v := doc.Value().Get(field)
		if v != nil {
			n, err := strconv.Atoi(v.String())
			require.NoError(t, err)
			results = append(results, n)
		}
	}
	require.NoError(t, iter.Err())
	return results
}

// insertAbortTestDocs populates coll with docCountForAbortTests array-valued
// documents whose `tags` field is a 2-element string array, ensuring that
// buildIndex would emit ~3 keys per document (2 elements + whole-array).
func insertAbortTestDocs(t *testing.T, coll Collection) {
	t.Helper()
	for i := 0; i < docCountForAbortTests; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"tags":["a%d","b%d"]}`, i, i, i),
		)))
	}
}

// assertTightBounds asserts the hinted index scan carries BOTH range ends
// (no ",inf]" upper bound in its bounds string).
func assertTightBounds(t *testing.T, coll Collection, filter string) {
	t.Helper()
	sql := explainSQL(t, coll, filter)
	require.Contains(t, sql, "IndexScan(a)", "plan: %s", sql)
	assert.Contains(t, sql, "'5')", "expected tight (two-sided) bounds, got: %s", sql)
}

// assertWideBounds asserts the hinted index scan kept the sound half-open
// over-approximation (upper bound +inf).
func assertWideBounds(t *testing.T, coll Collection, filter string) {
	t.Helper()
	sql := explainSQL(t, coll, filter)
	require.Contains(t, sql, "IndexScan(a)", "plan: %s", sql)
	assert.Contains(t, sql, "'<string>')", "expected wide (bracket-open) bounds, got: %s", sql)
}

const twoSided = `{"a":{"$gt":2,"$lt":5}}`
const docCountForAbortTests = 1000

// explainSQL returns explain.Sql for a hinted query on the "a" index.
func explainSQL(t *testing.T, coll Collection, filter string) string {
	t.Helper()
	explain, err := coll.Find(filter).
		IndexHint(IndexHint{IndexName: "a", Boost: 1_000_000}).Explain(ctx)
	require.NoError(t, err)
	return explain.Sql
}

/*
Index/Planner tests inspired by SQLite: corruptG.test, corruptI.test, corruptE.test,
corrupt2.test, corrupt9.test, reindex.test
Test scenario:
Index corruption and recovery scenarios: stale index entries pointing to
deleted docs, missing index entries, index-data inconsistency, integrity
check on corrupted index pages, and EnsureIndex (drop+recreate) as recovery.
These tests verify that our system handles index corruption gracefully and
that EnsureIndex can rebuild a corrupted index from scratch.
*/
// --- Category 1: Stale Index Entries ---

// TestIndex_Corruption_StaleIndexEntry creates an index, then directly deletes
// a document from the data namespace (bypassing index cleanup). The index
// still has an entry pointing to the deleted doc. A query via index may
// return fewer results than expected. EnsureIndex rebuild fixes it.
// Inspired by corruptI.test section 7 (missing metadata causes stale references).
func TestIndex_Corruption_StaleIndexEntry(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Insert 10 docs
	for i := 1; i <= 10; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10))))
	}
	assertCollCount(t, coll, 10)

	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	assertIndexLen(t, idx, 10)

	// Directly delete doc id=5 from the data namespace, bypassing index cleanup.
	// This leaves a stale entry in the index for a=50/docId=5.
	idKey := anyenc.Tuple(nil)
	idKey = anyenc.AppendAnyValue(idKey, 5)
	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Delete(c.ns, idKey)
	})
	require.NoError(t, err)

	// Data namespace now has 9 docs
	assertCollCount(t, coll, 9)
	// But index still has 10 entries (stale entry for doc id=5)
	assertIndexLen(t, idx, 10)

	// FindId for deleted doc should fail
	_, err = coll.FindId(ctx, 5)
	require.ErrorIs(t, err, ErrDocNotFound)

	// Recovery: drop and recreate index
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// After rebuild, index should have 9 entries
	newIdx := coll.GetIndexes()[0]
	assertIndexLen(t, newIdx, 9)

	// Queries should be consistent
	count, err := coll.Find(`{"a":50}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "stale entry for deleted doc should be gone after rebuild")

	count, err = coll.Find(`{"a":30}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// TestIndex_Corruption_MissingIndexEntry creates an index, then directly deletes
// an entry from the index namespace. The doc exists but the index doesn't know
// about it. A filtered query via index misses the doc.
// Inspired by corruptE.test (out-of-order/missing entries detected by integrity check).
func TestIndex_Corruption_MissingIndexEntry(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := 1; i <= 10; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10))))
	}

	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	assertIndexLen(t, idx, 10)

	// Directly delete the index entry for doc id=7 (a=70).
	// For non-unique index: key = Tuple(a_value, docId)
	idxKey := anyenc.Tuple(nil)
	idxKey = anyenc.AppendAnyValue(idxKey, 70) // a=70
	idKey := anyenc.Tuple(nil)
	idKey = anyenc.AppendAnyValue(idKey, 7) // docId=7
	fullKey := append(anyenc.Tuple(nil), idxKey...)
	fullKey = append(fullKey, idKey...)

	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Delete(idx.ns, fullKey)
	})
	require.NoError(t, err)

	// Data has 10, index has 9
	assertCollCount(t, coll, 10)
	assertIndexLen(t, idx, 9)

	// Doc id=7 still exists in data
	doc, err := coll.FindId(ctx, 7)
	require.NoError(t, err)
	assert.Equal(t, `{"id":7,"a":70}`, doc.Value().String())

	// Recovery: drop and recreate
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	newIdx := coll.GetIndexes()[0]
	assertIndexLen(t, newIdx, 10)

	// Now the query should find all 10 docs
	count, err := coll.Find(`{"a":70}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// TestIndex_Corruption_UniqueIndexWrongDocId creates a unique index, then
// modifies the value (docId) stored in an index entry to point to a different doc.
// Inspired by corruptG.test (corrupt index cell payload).
func TestIndex_Corruption_UniqueIndexWrongDocId(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":100}`),
		anyenc.MustParseJson(`{"id":2,"a":200}`),
		anyenc.MustParseJson(`{"id":3,"a":300}`),
	))

	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	assertIndexLen(t, idx, 3)

	// For unique index: key = Tuple(a_value), value = docId
	// Overwrite the value for a=200 to point to docId=999 (non-existent doc)
	idxKey := anyenc.Tuple(nil)
	idxKey = anyenc.AppendAnyValue(idxKey, 200)
	wrongDocId := anyenc.Tuple(nil)
	wrongDocId = anyenc.AppendAnyValue(wrongDocId, 999)

	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Put(idx.ns, idxKey, wrongDocId)
	})
	require.NoError(t, err)

	// Recovery: rebuild
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

	newIdx := coll.GetIndexes()[0]
	assertIndexLen(t, newIdx, 3)

	// Verify all docs are properly indexed after rebuild
	for _, a := range []int{100, 200, 300} {
		count, err := coll.Find(fmt.Sprintf(`{"a":%d}`, a)).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "a=%d should have 1 result after rebuild", a)
	}
}

// --- Category 2: Extra/Duplicate Index Entries ---

// TestIndex_Corruption_ExtraIndexEntries inserts extra spurious entries into the
// index namespace that don't correspond to any document. EnsureIndex rebuild
// should clean them up.
// Inspired by corrupt9.test (freelist corruption causes extra/duplicate pages
// during index rebuild).
func TestIndex_Corruption_ExtraIndexEntries(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := 1; i <= 5; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10))))
	}

	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	assertIndexLen(t, idx, 5)

	// Insert 3 extra spurious entries into the index namespace
	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		for _, fakeA := range []int{999, 998, 997} {
			fakeIdxKey := anyenc.Tuple(nil)
			fakeIdxKey = anyenc.AppendAnyValue(fakeIdxKey, fakeA)
			fakeDocId := anyenc.Tuple(nil)
			fakeDocId = anyenc.AppendAnyValue(fakeDocId, fakeA) // non-existent docId
			fullKey := append(anyenc.Tuple(nil), fakeIdxKey...)
			fullKey = append(fullKey, fakeDocId...)
			if err := tx.Put(idx.ns, fullKey, nil); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Index now has 8 entries (5 real + 3 fake)
	assertIndexLen(t, idx, 8)

	// Recovery: rebuild
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	newIdx := coll.GetIndexes()[0]
	assertIndexLen(t, newIdx, 5)
}

// --- Category 3: Index-Data Count Mismatch ---

// TestIndex_Corruption_CountMismatch verifies that after corruption the index
// count diverges from doc count, and rebuild restores consistency.
func TestIndex_Corruption_CountMismatch(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := 1; i <= 20; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5))))
	}

	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	assertCollCount(t, coll, 20)
	assertIndexLen(t, idx, 20)

	// Delete 5 docs from data only (bypass index)
	for i := 1; i <= 5; i++ {
		idKey := anyenc.Tuple(nil)
		idKey = anyenc.AppendAnyValue(idKey, i)
		err := c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
			return tx.Delete(c.ns, idKey)
		})
		require.NoError(t, err)
	}

	// Data has 15, index has 20 — mismatch
	assertCollCount(t, coll, 15)
	assertIndexLen(t, idx, 20)

	// Rebuild fixes it
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	newIdx := coll.GetIndexes()[0]
	assertCollCount(t, coll, 15)
	assertIndexLen(t, newIdx, 15)
}

// --- Category 4: Recovery via EnsureIndex ---

// TestIndex_Corruption_EnsureIndexRecoversMissingEntries deletes multiple index
// entries directly, then rebuilds via drop+ensure. All docs should be re-indexed.
// Inspired by reindex.test (REINDEX rebuilds from scratch).
func TestIndex_Corruption_EnsureIndexRecoversMissingEntries(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"name"}}))

	names := []string{"alice", "bob", "carol", "dave", "eve"}
	for i, name := range names {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"name":"%s"}`, i+1, name))))
	}

	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	assertIndexLen(t, idx, 5)

	// Delete index entries for "bob" and "dave" directly
	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		for _, entry := range []struct {
			name string
			id   int
		}{{"bob", 2}, {"dave", 4}} {
			idxKey := anyenc.Tuple(nil)
			idxKey = anyenc.AppendAnyValue(idxKey, entry.name)
			docId := anyenc.Tuple(nil)
			docId = anyenc.AppendAnyValue(docId, entry.id)
			fullKey := append(anyenc.Tuple(nil), idxKey...)
			fullKey = append(fullKey, docId...)
			if err := tx.Delete(idx.ns, fullKey); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	assertIndexLen(t, idx, 3)

	// Rebuild
	require.NoError(t, coll.DropIndex(ctx, "name"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"name"}}))

	newIdx := coll.GetIndexes()[0]
	assertIndexLen(t, newIdx, 5)

	// All names should be findable
	for _, name := range names {
		count, err := coll.Find(fmt.Sprintf(`{"name":"%s"}`, name)).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "name=%s should be found after rebuild", name)
	}
}

// TestIndex_Corruption_EnsureIndexRecoversStaleEntries adds stale entries
// to the index and rebuilds. The stale entries should be eliminated.
func TestIndex_Corruption_EnsureIndexRecoversStaleEntries(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"x"}}))

	for i := 1; i <= 8; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"x":%d}`, i, i))))
	}

	c := coll.(*collection)
	idx := c.loadIndexes()[0]

	// Delete docs 3 and 6 from data only
	for _, docId := range []int{3, 6} {
		idKey := anyenc.Tuple(nil)
		idKey = anyenc.AppendAnyValue(idKey, docId)
		err := c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
			return tx.Delete(c.ns, idKey)
		})
		require.NoError(t, err)
	}

	// Also add 2 fake entries
	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		for _, fakeX := range []int{100, 200} {
			k := anyenc.Tuple(nil)
			k = anyenc.AppendAnyValue(k, fakeX)
			d := anyenc.Tuple(nil)
			d = anyenc.AppendAnyValue(d, fakeX)
			full := append(anyenc.Tuple(nil), k...)
			full = append(full, d...)
			if err := tx.Put(idx.ns, full, nil); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Data: 6, Index: 10 (8 original + 2 fake, but 2 are stale = 10)
	assertCollCount(t, coll, 6)
	assertIndexLen(t, idx, 10)

	// Rebuild
	require.NoError(t, coll.DropIndex(ctx, "x"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"x"}}))

	newIdx := coll.GetIndexes()[0]
	assertCollCount(t, coll, 6)
	assertIndexLen(t, newIdx, 6)
}

// --- Category 5: Compound Index Corruption ---

// TestIndex_Corruption_CompoundIndexMissingEntry corrupts a compound index by
// removing an entry, then rebuilds.
func TestIndex_Corruption_CompoundIndexMissingEntry(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

	for i := 1; i <= 10; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%3, i%5))))
	}

	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	assertIndexLen(t, idx, 10)

	// Delete compound index entry for doc id=4 (a=1, b=4)
	// Compound non-unique key: Tuple(a_value, b_value, docId)
	idxKey := anyenc.Tuple(nil)
	idxKey = anyenc.AppendAnyValue(idxKey, 1) // a = 4%3 = 1
	idxKey = anyenc.AppendAnyValue(idxKey, 4) // b = 4%5 = 4
	docId := anyenc.Tuple(nil)
	docId = anyenc.AppendAnyValue(docId, 4)
	fullKey := append(anyenc.Tuple(nil), idxKey...)
	fullKey = append(fullKey, docId...)

	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Delete(idx.ns, fullKey)
	})
	require.NoError(t, err)
	assertIndexLen(t, idx, 9)

	// Rebuild
	require.NoError(t, coll.DropIndex(ctx, "a,b"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

	newIdx := coll.GetIndexes()[0]
	assertIndexLen(t, newIdx, 10)

	// Doc id=4 should be findable via compound query
	count, err := coll.Find(`{"a":1,"b":4}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// --- Category 6: Unique Index Corruption and Recovery ---

// TestIndex_Corruption_UniqueIndexDuplicateEntries inserts a duplicate key
// directly into a unique index namespace (bypassing constraint check).
// After rebuild, the correct unique constraint should be restored.
func TestIndex_Corruption_UniqueIndexDuplicateEntries(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"email"}, Unique: true}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"email":"alice@test.com"}`),
		anyenc.MustParseJson(`{"id":2,"email":"bob@test.com"}`),
	))

	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	assertIndexLen(t, idx, 2)

	// Corrupt: overwrite the unique index entry for "bob@test.com" to point to doc id=1
	// This means both alice and bob's emails map to doc id=1 in the index
	idxKey := anyenc.Tuple(nil)
	idxKey = anyenc.AppendAnyValue(idxKey, "bob@test.com")
	wrongDocId := anyenc.Tuple(nil)
	wrongDocId = anyenc.AppendAnyValue(wrongDocId, 1)

	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Put(idx.ns, idxKey, wrongDocId)
	})
	require.NoError(t, err)

	// Rebuild
	require.NoError(t, coll.DropIndex(ctx, "email"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"email"}, Unique: true}))

	newIdx := coll.GetIndexes()[0]
	assertIndexLen(t, newIdx, 2)

	// Unique constraint should work again
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"email":"alice@test.com"}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	// Both original docs should be findable
	count, err := coll.Find(`{"email":"alice@test.com"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"email":"bob@test.com"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// --- Category 7: Multiple Index Corruption ---

// TestIndex_Corruption_MultipleIndexesCorrupted corrupts multiple indexes
// on the same collection, then rebuilds them all.
func TestIndex_Corruption_MultipleIndexesCorrupted(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))

	for i := 1; i <= 15; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i, i*2))))
	}

	c := coll.(*collection)
	idxA := c.loadIndexes()[0]
	idxB := c.loadIndexes()[1]
	assertIndexLen(t, idxA, 15)
	assertIndexLen(t, idxB, 15)

	// Corrupt index A: delete entries for docs 1-5
	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		for i := 1; i <= 5; i++ {
			k := anyenc.Tuple(nil)
			k = anyenc.AppendAnyValue(k, i)
			d := anyenc.Tuple(nil)
			d = anyenc.AppendAnyValue(d, i)
			full := append(anyenc.Tuple(nil), k...)
			full = append(full, d...)
			if err := tx.Delete(idxA.ns, full); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Corrupt index B: add 3 fake entries
	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		for _, fakeB := range []int{900, 901, 902} {
			k := anyenc.Tuple(nil)
			k = anyenc.AppendAnyValue(k, fakeB)
			d := anyenc.Tuple(nil)
			d = anyenc.AppendAnyValue(d, fakeB)
			full := append(anyenc.Tuple(nil), k...)
			full = append(full, d...)
			if err := tx.Put(idxB.ns, full, nil); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	assertIndexLen(t, idxA, 10)
	assertIndexLen(t, idxB, 18)

	// Rebuild both
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.DropIndex(ctx, "b"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))

	indexes := coll.GetIndexes()
	require.Len(t, indexes, 2)
	for _, idx := range indexes {
		assertIndexLen(t, idx, 15)
	}
}

// --- Category 8: Sparse Index Corruption ---

// TestIndex_Corruption_SparseIndexExtraEntries adds entries to a sparse index
// for docs that don't have the indexed field. Rebuild should remove them.
func TestIndex_Corruption_SparseIndexExtraEntries(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true}))

	// Insert mix: some with "a", some without
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":10}`),
		anyenc.MustParseJson(`{"id":2,"b":20}`),
		anyenc.MustParseJson(`{"id":3,"a":30}`),
		anyenc.MustParseJson(`{"id":4,"c":40}`),
	))

	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	assertIndexLen(t, idx, 2) // only docs 1 and 3

	// Corrupt: add fake entries for docs 2 and 4 (which don't have "a")
	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		for _, entry := range []struct {
			val int
			id  int
		}{{99, 2}, {88, 4}} {
			k := anyenc.Tuple(nil)
			k = anyenc.AppendAnyValue(k, entry.val)
			d := anyenc.Tuple(nil)
			d = anyenc.AppendAnyValue(d, entry.id)
			full := append(anyenc.Tuple(nil), k...)
			full = append(full, d...)
			if err := tx.Put(idx.ns, full, nil); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	assertIndexLen(t, idx, 4)

	// Rebuild
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true}))

	newIdx := coll.GetIndexes()[0]
	assertIndexLen(t, newIdx, 2) // back to 2
}

// --- Category 9: Corruption After Mutations ---

// TestIndex_Corruption_InsertAfterStaleCorruption inserts a new doc after
// creating stale index entries. The insert should succeed normally through
// the collection API.
func TestIndex_Corruption_InsertAfterStaleCorruption(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := 1; i <= 5; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10))))
	}

	c := coll.(*collection)

	// Delete doc 3 from data only (stale index entry remains)
	idKey := anyenc.Tuple(nil)
	idKey = anyenc.AppendAnyValue(idKey, 3)
	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Delete(c.ns, idKey)
	})
	require.NoError(t, err)

	// Insert new docs — should work fine despite stale index entries
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":6,"a":60}`),
		anyenc.MustParseJson(`{"id":7,"a":70}`),
	))

	assertCollCount(t, coll, 6) // 5-1+2 = 6

	// New docs should be findable
	count, err := coll.Find(`{"a":60}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"a":70}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// TestIndex_Corruption_DeleteAfterExtraEntries deletes a doc normally after
// extra entries were injected into the index.
func TestIndex_Corruption_DeleteAfterExtraEntries(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := 1; i <= 5; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10))))
	}

	c := coll.(*collection)
	idx := c.loadIndexes()[0]

	// Add fake entries
	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		k := anyenc.Tuple(nil)
		k = anyenc.AppendAnyValue(k, 555)
		d := anyenc.Tuple(nil)
		d = anyenc.AppendAnyValue(d, 555)
		full := append(anyenc.Tuple(nil), k...)
		full = append(full, d...)
		return tx.Put(idx.ns, full, nil)
	})
	require.NoError(t, err)
	assertIndexLen(t, idx, 6) // 5 + 1 fake

	// Normal delete should still work
	require.NoError(t, coll.DeleteId(ctx, 2))
	assertCollCount(t, coll, 4)
	assertIndexLen(t, idx, 5) // 6 - 1 (deleted doc 2's entry)

	// Rebuild to clean up fake entry
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	newIdx := coll.GetIndexes()[0]
	assertIndexLen(t, newIdx, 4)
}

// --- Category 10: Large Scale Corruption Recovery ---

// TestIndex_Corruption_LargeScaleRecovery corrupts a large index and verifies
// complete recovery via rebuild.
func TestIndex_Corruption_LargeScaleRecovery(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"v"}}))

	// Insert 200 docs
	for i := 1; i <= 200; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"v":%d}`, i, i%20))))
	}

	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	assertIndexLen(t, idx, 200)

	// Delete 50 docs from data only (bypass index)
	for i := 1; i <= 50; i++ {
		idKey := anyenc.Tuple(nil)
		idKey = anyenc.AppendAnyValue(idKey, i)
		err := c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
			return tx.Delete(c.ns, idKey)
		})
		require.NoError(t, err)
	}

	assertCollCount(t, coll, 150)
	assertIndexLen(t, idx, 200) // stale

	// Rebuild
	require.NoError(t, coll.DropIndex(ctx, "v"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"v"}}))

	newIdx := coll.GetIndexes()[0]
	assertCollCount(t, coll, 150)
	assertIndexLen(t, newIdx, 150)

	// Verify query correctness for each value bucket
	for v := 0; v < 20; v++ {
		count, err := coll.Find(fmt.Sprintf(`{"v":%d}`, v)).Count(ctx)
		require.NoError(t, err)
		// docs 51-200 that have v=i%20: count how many i in [51,200] where i%20==v
		expected := 0
		for i := 51; i <= 200; i++ {
			if i%20 == v {
				expected++
			}
		}
		assert.Equal(t, expected, count, "v=%d", v)
	}
}

// --- Category 11: Array Index Corruption ---

// TestIndex_Corruption_ArrayIndexMissingEntries corrupts an array-based index
// by removing some multi-key entries. Rebuild should restore them.
func TestIndex_Corruption_ArrayIndexMissingEntries(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"tags":["go","rust"]}`),
		anyenc.MustParseJson(`{"id":2,"tags":["python","java"]}`),
	))

	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	// Each doc: 2 elements + 1 array-as-value = 3 entries, total 6
	assertIndexLen(t, idx, 6)

	// Delete the "go" index entry for doc 1
	idxKey := anyenc.Tuple(nil)
	idxKey = anyenc.AppendAnyValue(idxKey, "go")
	docId := anyenc.Tuple(nil)
	docId = anyenc.AppendAnyValue(docId, 1)
	fullKey := append(anyenc.Tuple(nil), idxKey...)
	fullKey = append(fullKey, docId...)

	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Delete(idx.ns, fullKey)
	})
	require.NoError(t, err)
	assertIndexLen(t, idx, 5)

	// Rebuild
	require.NoError(t, coll.DropIndex(ctx, "tags"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))

	newIdx := coll.GetIndexes()[0]
	assertIndexLen(t, newIdx, 6)

	// "go" should be findable again
	count, err := coll.Find(`{"tags":"go"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// --- Category 12: Index Namespace Deleted Entirely ---

// TestIndex_Corruption_IndexNamespaceCleared empties the index namespace entirely,
// then rebuilds via drop+ensure.
func TestIndex_Corruption_IndexNamespaceCleared(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := 1; i <= 10; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
	}

	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	assertIndexLen(t, idx, 10)

	// Clear all entries from the index namespace
	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		cursor := tx.NewCursor(idx.ns)
		if err := cursor.First(); err != nil {
			return err
		}
		var keysToDelete [][]byte
		for cursor.Valid() {
			key, err := cursor.Key()
			if err != nil {
				return err
			}
			keysToDelete = append(keysToDelete, append([]byte(nil), key...))
			if err := cursor.Next(); err != nil {
				return err
			}
		}
		for _, key := range keysToDelete {
			if err := tx.Delete(idx.ns, key); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	assertIndexLen(t, idx, 0)

	// Data still has 10 docs
	assertCollCount(t, coll, 10)

	// Rebuild
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	newIdx := coll.GetIndexes()[0]
	assertIndexLen(t, newIdx, 10)

	// All docs findable via index
	for i := 1; i <= 10; i++ {
		count, err := coll.Find(fmt.Sprintf(`{"a":%d}`, i)).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "a=%d should be found", i)
	}
}

// --- Category 13: Update on Corrupted Index ---

// TestIndex_Corruption_UpdateWithMissingIndexEntry updates a doc whose index
// entry was previously deleted. The update should still work through the
// collection API (which deletes old + inserts new index entries).
func TestIndex_Corruption_UpdateWithMissingIndexEntry(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":10}`),
		anyenc.MustParseJson(`{"id":2,"a":20}`),
	))

	c := coll.(*collection)
	idx := c.loadIndexes()[0]

	// Delete index entry for doc 1 (a=10)
	idxKey := anyenc.Tuple(nil)
	idxKey = anyenc.AppendAnyValue(idxKey, 10)
	docId := anyenc.Tuple(nil)
	docId = anyenc.AppendAnyValue(docId, 1)
	fullKey := append(anyenc.Tuple(nil), idxKey...)
	fullKey = append(fullKey, docId...)

	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Delete(idx.ns, fullKey)
	})
	require.NoError(t, err)
	assertIndexLen(t, idx, 1)

	// Update doc 1: a=10 -> a=99.
	// The old index entry delete will silently fail (already deleted).
	// The new index entry for a=99 should be created.
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"a":99}`)))

	// Index should now have 2 entries (a=99/1 and a=20/2)
	assertIndexLen(t, idx, 2)

	count, err := coll.Find(`{"a":99}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

// --- Category 14: High-Level Recovery via Drop/Recreate ---

// TestIndex_Corruption_RecoveryViaDropRecreate verifies that dropping and
// recreating an index via EnsureIndex rebuilds it correctly from existing data,
// producing the same query results as before.
func TestIndex_Corruption_RecoveryViaDropRecreate(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_drop_recreate")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 50 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i*3),
		)))
	}

	// Capture results before drop
	countBefore, err := coll.Find(`{"a":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, countBefore)

	resultsBefore := collectField(t, coll.Find(`{"a":{"$gte":3,"$lte":7}}`).Sort("a"), "id")
	require.True(t, len(resultsBefore) > 0)

	assertIndexLen(t, coll.GetIndexes()[0], 50)

	// Drop and recreate
	require.NoError(t, coll.DropIndex(ctx, "a"))
	assert.Len(t, coll.GetIndexes(), 0)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	assert.Len(t, coll.GetIndexes(), 1)

	// Verify same results after rebuild
	countAfter, err := coll.Find(`{"a":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countBefore, countAfter)

	resultsAfter := collectField(t, coll.Find(`{"a":{"$gte":3,"$lte":7}}`).Sort("a"), "id")
	assert.Equal(t, resultsBefore, resultsAfter)

	assertIndexLen(t, coll.GetIndexes()[0], 50)

	// Verify index is used
	explain, err := coll.Find(`{"a":5}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")
}

// TestIndex_Corruption_EnsureIndexRebuildsFromData verifies that EnsureIndex
// builds an index retroactively from existing documents, and that drop+recreate
// produces the same results.
func TestIndex_Corruption_EnsureIndexRebuildsFromData(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_rebuild")
	require.NoError(t, err)

	// Insert docs WITHOUT index
	for i := range 40 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%8),
		)))
	}

	// Capture non-indexed results
	countNoIdx, err := coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, countNoIdx) // 40/8 = 5

	resultsNoIdx := collectField(t, coll.Find(`{"a":{"$gte":2,"$lte":5}}`).Sort("a"), "id")

	// Now create index retroactively
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	assertIndexLen(t, coll.GetIndexes()[0], 40)

	// Verify same results with index
	countIdx, err := coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countNoIdx, countIdx)

	resultsIdx := collectField(t, coll.Find(`{"a":{"$gte":2,"$lte":5}}`).Sort("a"), "id")
	assert.Equal(t, resultsNoIdx, resultsIdx)

	// Drop, recreate again — still works
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	countAgain, err := coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countNoIdx, countAgain)

	resultsAgain := collectField(t, coll.Find(`{"a":{"$gte":2,"$lte":5}}`).Sort("a"), "id")
	assert.Equal(t, resultsNoIdx, resultsAgain)
	assertIndexLen(t, coll.GetIndexes()[0], 40)
}

// --- Category 15: Crash/Unclean Close Recovery ---

// TestIndex_Corruption_CrashRecoveryWithIndex verifies that after closing the
// database (simulating shutdown), reopening recovers the index and queries work.
func TestIndex_Corruption_CrashRecoveryWithIndex(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "idx-crash-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Phase 1: create, insert, index — then close (no explicit checkpoint)
	func() {
		db, err := Open(ctx, dbPath, nil)
		require.NoError(t, err)

		coll, err := db.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

		for i := range 30 {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%6),
			)))
		}

		count, err := coll.Find(`{"a":2}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 5, count)

		require.NoError(t, db.Close())
	}()

	// Phase 2: reopen and verify
	db2, err := Open(ctx, dbPath, nil)
	require.NoError(t, err)
	defer db2.Close()

	coll2, err := db2.OpenCollection(ctx, "test")
	require.NoError(t, err)

	indexes := coll2.GetIndexes()
	require.Len(t, indexes, 1)
	assert.Equal(t, "a", indexes[0].Info().Name)

	count, err := coll2.Find(`{"a":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, count)

	assertCollCount(t, coll2, 30)
	assertIndexLen(t, indexes[0], 30)

	explain, err := coll2.Find(`{"a":2}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")
}

// TestIndex_Corruption_PersistenceAfterUncleanClose verifies that indexes
// survive an unclean close (default CommitSync=false mode) and WAL recovery restores them.
func TestIndex_Corruption_PersistenceAfterUncleanClose(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "idx-unclean-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Phase 1: insert data with index using default CommitSync=false
	func() {
		conf := &Config{}
		db, err := Open(ctx, dbPath, conf)
		require.NoError(t, err)

		coll, err := db.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

		for i := range 50 {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"a":%d}`, i, i),
			)))
		}

		assertIndexLen(t, coll.GetIndexes()[0], 50)
		require.NoError(t, db.Close())
	}()

	// Phase 2: reopen and verify
	db2, err := Open(ctx, dbPath, nil)
	require.NoError(t, err)
	defer db2.Close()

	coll2, err := db2.OpenCollection(ctx, "test")
	require.NoError(t, err)

	indexes := coll2.GetIndexes()
	require.Len(t, indexes, 1)
	assert.Equal(t, "a", indexes[0].Info().Name)
	assert.True(t, indexes[0].Info().Unique)

	assertCollCount(t, coll2, 50)
	assertIndexLen(t, indexes[0], 50)

	count, err := coll2.Find(`{"a":25}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Unique constraint still holds
	err = coll2.Insert(ctx, anyenc.MustParseJson(`{"id":999,"a":0}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	count, err = coll2.Find(`{"a":{"$gte":10,"$lt":20}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, count)
}

// --- Category 16: Drop/Recreate After Deletions ---

// TestIndex_Corruption_DropAndRecreateFixesStaleData verifies that after
// deleting some docs, dropping and recreating the index produces an index
// that matches the current data state exactly.
func TestIndex_Corruption_DropAndRecreateFixesStaleData(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_stale")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 60 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%10),
		)))
	}
	assertIndexLen(t, coll.GetIndexes()[0], 60)

	// Delete all docs where a=3 (6 docs)
	res, err := coll.Find(`{"a":3}`).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 6, res.Modified)
	assertCollCount(t, coll, 54)
	assertIndexLen(t, coll.GetIndexes()[0], 54)

	count, err := coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Delete a=7 (6 docs)
	res, err = coll.Find(`{"a":7}`).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 6, res.Modified)
	assertCollCount(t, coll, 48)

	// Drop and recreate index
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	assertIndexLen(t, coll.GetIndexes()[0], 48)

	count, err = coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	count, err = coll.Find(`{"a":7}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	count, err = coll.Find(`{"a":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 6, count)

	total := 0
	for v := range 10 {
		if v == 3 || v == 7 {
			continue
		}
		c, err := coll.Find(fmt.Sprintf(`{"a":%d}`, v)).Count(ctx)
		require.NoError(t, err)
		total += c
	}
	assert.Equal(t, 48, total)
}

// --- Category 17: Multiple Drop/Recreate Cycles ---

// TestIndex_Corruption_MultipleDropRecreate verifies that multiple cycles of
// drop/recreate with interleaved data modifications produce a consistent index.
func TestIndex_Corruption_MultipleDropRecreate(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_multi_cycle")
	require.NoError(t, err)

	// Cycle 1: create index, insert data
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	for i := range 20 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5),
		)))
	}
	assertIndexLen(t, coll.GetIndexes()[0], 20)
	count, err := coll.Find(`{"a":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 4, count)

	// Cycle 2: drop, insert more, recreate
	require.NoError(t, coll.DropIndex(ctx, "a"))
	for i := 20; i < 40; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5),
		)))
	}
	assertCollCount(t, coll, 40)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	assertIndexLen(t, coll.GetIndexes()[0], 40)
	count, err = coll.Find(`{"a":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 8, count) // 40/5

	// Cycle 3: drop, delete some, insert more, recreate
	require.NoError(t, coll.DropIndex(ctx, "a"))
	_, err = coll.Find(`{"a":0}`).Delete(ctx) // delete 8 docs with a=0
	require.NoError(t, err)
	for i := 40; i < 50; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5),
		)))
	}
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	assertCollCount(t, coll, 42) // 40 - 8 + 10
	assertIndexLen(t, coll.GetIndexes()[0], 42)

	// a=0: 8 deleted, 2 added (40,45) = 2
	count, err = coll.Find(`{"a":0}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	// a=2: originally 8, none deleted, 2 more (42,47) = 10
	count, err = coll.Find(`{"a":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, count)

	explain, err := coll.Find(`{"a":2}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")
}

// --- Category 18: EnsureIndex Idempotency ---

// TestIndex_Corruption_EnsureIndexIdempotentAfterCorruption verifies that
// EnsureIndex is a no-op when the index already exists, and works correctly
// after drop to rebuild.
func TestIndex_Corruption_EnsureIndexIdempotentAfterCorruption(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_idempotent")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	for i := range 25 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5),
		)))
	}

	assertIndexLen(t, coll.GetIndexes()[0], 25)

	// EnsureIndex again — should be a no-op
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	assert.Len(t, coll.GetIndexes(), 1)
	assertIndexLen(t, coll.GetIndexes()[0], 25)

	count, err := coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, count)

	// Drop and then EnsureIndex — should rebuild
	require.NoError(t, coll.DropIndex(ctx, "a"))
	assert.Len(t, coll.GetIndexes(), 0)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	assert.Len(t, coll.GetIndexes(), 1)
	assertIndexLen(t, coll.GetIndexes()[0], 25)

	count, err = coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, count)

	// EnsureIndex after rebuild — still idempotent
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	assert.Len(t, coll.GetIndexes(), 1)
	assertIndexLen(t, coll.GetIndexes()[0], 25)
}

// --- Category 19: Crash Recovery With Multiple Indexes ---

// TestIndex_Corruption_CrashRecoveryMultipleIndexes verifies WAL recovery
// handles multiple indexes on the same collection.
func TestIndex_Corruption_CrashRecoveryMultipleIndexes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "idx-crash-multi-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Phase 1
	func() {
		db, err := Open(ctx, dbPath, &Config{})
		require.NoError(t, err)

		coll, err := db.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}, Unique: true}))
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

		for i := range 25 {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%5, i),
			)))
		}

		require.NoError(t, db.Close())
	}()

	// Phase 2: verify all indexes survived
	db2, err := Open(ctx, dbPath, nil)
	require.NoError(t, err)
	defer db2.Close()

	coll2, err := db2.OpenCollection(ctx, "test")
	require.NoError(t, err)

	indexes := coll2.GetIndexes()
	require.Len(t, indexes, 3)

	assertCollCount(t, coll2, 25)

	count, err := coll2.Find(`{"a":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, count)

	count, err = coll2.Find(`{"b":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Unique constraint on b
	err = coll2.Insert(ctx, anyenc.MustParseJson(`{"id":99,"a":0,"b":0}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)
}

// --- Category 20: Compound and Unique Index Recovery ---

// TestIndex_Corruption_RecoveryWithCompoundIndex verifies drop/recreate
// recovery works with compound indexes.
func TestIndex_Corruption_RecoveryWithCompoundIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_compound_recovery")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

	for i := range 60 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%6, i%4),
		)))
	}

	countBefore, err := coll.Find(`{"a":2,"b":1}`).Count(ctx)
	require.NoError(t, err)

	resultsBefore := collectField(t, coll.Find(`{"a":3}`).Sort("b"), "id")

	require.NoError(t, coll.DropIndex(ctx, "a,b"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

	countAfter, err := coll.Find(`{"a":2,"b":1}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countBefore, countAfter)

	resultsAfter := collectField(t, coll.Find(`{"a":3}`).Sort("b"), "id")
	assert.Equal(t, resultsBefore, resultsAfter)

	assertIndexLen(t, coll.GetIndexes()[0], 60)
}

// TestIndex_Corruption_RecoveryWithUniqueIndex verifies drop/recreate
// recovery preserves unique constraint semantics.
func TestIndex_Corruption_RecoveryWithUniqueIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_unique_recovery")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"email"}, Unique: true}))

	for i := range 30 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"email":"user%d@test.com"}`, i, i),
		)))
	}

	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":99,"email":"user0@test.com"}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	require.NoError(t, coll.DropIndex(ctx, "email"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"email"}, Unique: true}))

	// Unique constraint still holds after rebuild
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":99,"email":"user0@test.com"}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	// But a new unique value works
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":99,"email":"new@test.com"}`)))
	assertIndexLen(t, coll.GetIndexes()[0], 31)

	count, err := coll.Find(`{"email":"user15@test.com"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// --- Category 21: Reopen After Multiple Write Batches ---

// TestIndex_Corruption_ReopenAfterMultipleWrites verifies that index
// consistency is maintained after multiple write batches and a reopen.
func TestIndex_Corruption_ReopenAfterMultipleWrites(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "idx-multi-write-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Phase 1: create, insert in multiple batches
	func() {
		db, err := Open(ctx, dbPath, nil)
		require.NoError(t, err)

		coll, err := db.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

		// Batch 1: inserts
		for i := range 20 {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%4),
			)))
		}

		// Batch 2: updates
		for i := range 10 {
			require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"a":%d}`, i, 100+i),
			)))
		}

		// Batch 3: deletes
		for i := 15; i < 20; i++ {
			require.NoError(t, coll.DeleteId(ctx, i))
		}

		require.NoError(t, db.Close())
	}()

	// Phase 2: reopen and verify consistency
	db2, err := Open(ctx, dbPath, nil)
	require.NoError(t, err)
	defer db2.Close()

	coll2, err := db2.OpenCollection(ctx, "test")
	require.NoError(t, err)

	assertCollCount(t, coll2, 15) // 20 - 5 deleted

	indexes := coll2.GetIndexes()
	require.Len(t, indexes, 1)
	assertIndexLen(t, indexes[0], 15)

	// Updated docs findable via new values
	count, err := coll2.Find(`{"a":100}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count) // id=0 was updated to a=100

	// Deleted docs should not be found
	_, err = coll2.FindId(ctx, 17)
	require.ErrorIs(t, err, ErrDocNotFound)
}

// --- Category 22: Empty Collection Edge Case ---

// TestIndex_Corruption_EmptyCollectionDropRecreate verifies that
// drop/recreate works on empty collections.
func TestIndex_Corruption_EmptyCollectionDropRecreate(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_empty")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	assertIndexLen(t, coll.GetIndexes()[0], 0)

	// Drop and recreate on empty collection
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	assertIndexLen(t, coll.GetIndexes()[0], 0)

	// Now insert data, drop, recreate
	for i := range 5 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i),
		)))
	}
	assertIndexLen(t, coll.GetIndexes()[0], 5)

	// Delete all docs, drop, recreate
	_, err = coll.Find(nil).Delete(ctx)
	require.NoError(t, err)
	assertCollCount(t, coll, 0)

	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	assertIndexLen(t, coll.GetIndexes()[0], 0)
}

// --- Category 23: Sparse Index Recovery ---

// TestIndex_Corruption_RecoveryWithSparseIndex verifies drop/recreate
// recovery works correctly with sparse indexes.
func TestIndex_Corruption_RecoveryWithSparseIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_sparse_recovery")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"score"}, Sparse: true}))

	for i := range 20 {
		if i%3 == 0 {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"name":"no_score_%d"}`, i, i),
			)))
		} else {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"score":%d}`, i, i*10),
			)))
		}
	}

	// i=0,3,6,9,12,15,18 → 7 docs without score, 13 with score
	idxLenBefore, err := coll.GetIndexes()[0].Len(ctx)
	require.NoError(t, err)
	assert.Equal(t, 13, idxLenBefore)

	require.NoError(t, coll.DropIndex(ctx, "score"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"score"}, Sparse: true}))

	idxLenAfter, err := coll.GetIndexes()[0].Len(ctx)
	require.NoError(t, err)
	assert.Equal(t, idxLenBefore, idxLenAfter)

	assertCollCount(t, coll, 20)
}

// --- Category 24: Drop/Recreate With Updated Docs ---

// TestIndex_Corruption_DropRecreateWithUpdatedDocs verifies that after
// updating indexed fields, drop+recreate builds the index with the
// current (updated) values.
func TestIndex_Corruption_DropRecreateWithUpdatedDocs(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_updated")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"status"}}))

	for i := range 20 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"status":"active"}`, i),
		)))
	}

	count, err := coll.Find(`{"status":"active"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 20, count)

	// Update half to "inactive"
	for i := range 10 {
		require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"status":"inactive"}`, i),
		)))
	}

	count, err = coll.Find(`{"status":"active"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, count)

	count, err = coll.Find(`{"status":"inactive"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, count)

	require.NoError(t, coll.DropIndex(ctx, "status"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"status"}}))

	count, err = coll.Find(`{"status":"active"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, count)

	count, err = coll.Find(`{"status":"inactive"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, count)

	assertIndexLen(t, coll.GetIndexes()[0], 20)
}

// --- Category 25: Multi-Index Drop One, Recreate ---

// TestIndex_Corruption_MultiIndexDropOneRecreate verifies that dropping and
// recreating one index doesn't affect another index on the same collection.
func TestIndex_Corruption_MultiIndexDropOneRecreate(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_multi_drop")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))

	for i := range 40 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%8, i%5),
		)))
	}

	require.Len(t, coll.GetIndexes(), 2)

	countA, err := coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, countA)

	countB, err := coll.Find(`{"b":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 8, countB)

	// Drop only "a" index
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.Len(t, coll.GetIndexes(), 1)
	assert.Equal(t, "b", coll.GetIndexes()[0].Info().Name)

	// "b" index should still work
	countB2, err := coll.Find(`{"b":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countB, countB2)

	// Recreate "a"
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.Len(t, coll.GetIndexes(), 2)

	countA2, err := coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countA, countA2)

	countB3, err := coll.Find(`{"b":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countB, countB3)
}

// injectRawIndexEntry writes (key, value) directly into the index
// namespace, bypassing insertKeys. Used to simulate legacy nil-value
// entries (or any other raw entry shape) for backward-compat tests.
//
// Mirrors the corruption tests above (search for "tx.Put(idx.ns") — same
// internals access via collection.indexes + db.doWriteTx.
func injectRawIndexEntry(t *testing.T, c *collection, idx *index, fullKey, value []byte) {
	t.Helper()
	require.NoError(t, c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Put(idx.ns, fullKey, value)
	}))
}

// buildIndexFullKey returns a Tuple-encoded (idxFieldVal..., docId) key
// suitable for direct injection into a non-unique index namespace. This
// matches what insertKeys builds (key + idKey concatenation).
func buildIndexFullKey(idxFieldValues []any, docId any) []byte {
	idxKey := anyenc.Tuple(nil)
	for _, v := range idxFieldValues {
		idxKey = anyenc.AppendAnyValue(idxKey, v)
	}
	idKey := anyenc.Tuple(nil)
	idKey = anyenc.AppendAnyValue(idKey, docId)
	full := append(anyenc.Tuple(nil), idxKey...)
	full = append(full, idKey...)
	return full
}

// findIndex returns the named index pointer from a collection. Test-only
// helper that mirrors the access pattern in readRawIndexEntries.
func findIndex(t *testing.T, coll Collection, indexName string) (*collection, *index) {
	t.Helper()
	c := coll.(*collection)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, i := range c.loadIndexes() {
		if i.info.Name == indexName {
			return c, i
		}
	}
	t.Fatalf("index %q not found", indexName)
	return nil, nil
}

// --- Subtest 1: pure decoder pin ---

// TestAudit14_LegacyNilValue_DecodeMultiKey pins the decoder behavior
// of qplanner.EntryValueIsMultiKey for all four shapes of input:
//
//   - nil           → true  (legacy, conservative)
//   - []byte{}      → true  (legacy, conservative)
//   - []byte{0x00}  → false (new format, scalar)
//   - []byte{0x01}  → true  (new format, multi-key)
//
// If this contract changes, the entire backward-compat story breaks:
// queries on partially-migrated indexes would either over-count (miss
// dedup on legacy entries) or skip valid scalar entries.
func TestAudit14_LegacyNilValue_DecodeMultiKey(t *testing.T) {
	assert.True(t, qplanner.EntryValueIsMultiKey(nil),
		"nil value must be reported as multi-key (legacy/safe)")
	assert.True(t, qplanner.EntryValueIsMultiKey([]byte{}),
		"empty value must be reported as multi-key (legacy/safe)")
	assert.False(t, qplanner.EntryValueIsMultiKey([]byte{0x00}),
		"0x00 (IndexValueScalar) must be reported as scalar (not multi-key)")
	assert.True(t, qplanner.EntryValueIsMultiKey([]byte{0x01}),
		"0x01 (IndexValueMultiKey) must be reported as multi-key")

	// Round-trip via the public sentinels too — guards against drift in
	// the sentinel byte values themselves.
	assert.False(t, qplanner.EntryValueIsMultiKey(qplanner.IndexValueScalar),
		"qplanner.IndexValueScalar sentinel must decode as scalar")
	assert.True(t, qplanner.EntryValueIsMultiKey(qplanner.IndexValueMultiKey),
		"qplanner.IndexValueMultiKey sentinel must decode as multi-key")
}

// --- Subtest 2: real doc + injected legacy entry, query stays correct ---

// TestAudit14_LegacyNilValue_QueryWithLegacyMix simulates a
// partially-migrated index. We insert a real array doc via the public
// API (gets new bit-set entries), then directly inject one nil-value
// entry simulating a leftover legacy row pointing at the SAME doc.
//
// A query Find({tags:"a"}).Count must still return 1: the legacy entry
// (decoded as multi-key) triggers conservative dedup, so the doc is not
// over-counted across the matching bound.
func TestAudit14_LegacyNilValue_QueryWithLegacyMix(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit14_mix")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "tags",
		Fields: []string{"tags"},
	}))

	// Insert one doc with a 2-element array. Per AUDIT01 this produces
	// 3 raw entries (2 element entries + 1 whole-array entry), each
	// tagged IndexValueMultiKey (0x01).
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b"]}`),
	))

	c, idx := findIndex(t, coll, "tags")

	// Inject a nil-value entry simulating a leftover legacy row for the
	// same doc on a different key value ("c"). This is what a pre-bit
	// version of the writer would have produced for a single insert.
	legacyKey := buildIndexFullKey([]any{"c"}, "d1")
	injectRawIndexEntry(t, c, idx, legacyKey, nil)

	// Index now has 4 entries: 3 bit-set (0x01) for "a"/"b" + whole-array
	// + 1 nil-value (legacy) for "c".
	entries := readRawIndexEntries(t, fx.DB, "audit14_mix", "tags")
	require.Len(t, entries, 4,
		"expected 3 new + 1 injected legacy entry, got %d", len(entries))

	// Sanity: confirm at least one legacy (len==0) and one bit-set entry
	// are present, exercising EntryValueIsMultiKey across both shapes.
	var legacyCount, bitSetCount int
	for _, e := range entries {
		if len(e.Value) == 0 {
			legacyCount++
		} else {
			bitSetCount++
		}
	}
	assert.Equal(t, 1, legacyCount, "exactly one nil-value (legacy) entry expected")
	assert.Equal(t, 3, bitSetCount, "three bit-set entries expected from the public-API insert")

	// Query must still return 1 — the legacy entry on "c" doesn't match
	// {"tags":"a"}, so this verifies the matching-bound case stays
	// correct in the presence of legacy data on other keys.
	n, err := coll.Find(`{"tags":"a"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n,
		"Find({tags:a}).Count must be 1 (legacy entries elsewhere don't affect this match)")

	// And cross-bound: Find({tags:{$in:["a","b"]}}) hits BOTH "a" and "b"
	// bounds for d1. Without dedup the count would be 2; with dedup
	// (which all entries route through because at least one is decoded
	// as multi-key), it must be 1.
	n, err = coll.Find(`{"tags":{"$in":["a","b"]}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n,
		"$in over [a,b] on a single doc whose tags overlap both bounds must "+
			"dedup to 1 (got %d) — legacy entries must not break dedup", n)
}

// --- Subtest 3: update via public API rewrites legacy → bit-set ---

// TestAudit14_LegacyNilValue_OverwriteOnUpdate verifies that an update
// via the public API REPLACES a legacy nil-value entry with a new
// bit-set value. The mechanism: collection.update calls deleteKeys
// (removes by key — value-byte irrelevant) followed by insertKeys
// (writes the new bit-set value).
//
// We must change the doc value (not just rewrite identical content) —
// collection.update has an early-out via anyencutil.Equal that skips
// the deleteKeys/insertKeys cycle when the old and new values are
// identical. So we insert {a:10}, inject a legacy entry at the same
// key, then update to {a:20}: deleteKeys removes the old key (10,d1)
// regardless of its value byte, and insertKeys writes (20,d1) with the
// new bit-set value.
//
// This proves there's a natural migration path: any doc that gets
// touched (with a real value change) will have its index entries
// normalized to the new format.
func TestAudit14_LegacyNilValue_OverwriteOnUpdate(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit14_update")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_a",
		Fields: []string{"a"},
	}))

	// Insert a scalar doc — produces one entry tagged IndexValueScalar.
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","a":10}`),
	))

	c, idx := findIndex(t, coll, "ix_a")

	// Replace that bit-set entry with a nil-value one to simulate a
	// pre-upgrade write for the same (key, docId) pair. Direct overwrite
	// at the same key is safe — it just changes the value.
	legacyKey := buildIndexFullKey([]any{10}, "d1")
	injectRawIndexEntry(t, c, idx, legacyKey, nil)

	// Confirm the entry is now legacy (nil value).
	entries := readRawIndexEntries(t, fx.DB, "audit14_update", "ix_a")
	require.Len(t, entries, 1, "exactly one entry expected")
	assert.Empty(t, entries[0].Value,
		"after injection the entry value must be empty (legacy)")

	// Update the doc via the public API. We MUST change the value (10→20)
	// so collection.update doesn't early-out on the equality check. The
	// deleteKeys call removes the old (10,d1) entry by key (value byte
	// irrelevant), and insertKeys writes (20,d1) with the new bit-set
	// value.
	require.NoError(t, coll.UpdateOne(ctx,
		anyenc.MustParseJson(`{"id":"d1","a":20}`)))

	entries = readRawIndexEntries(t, fx.DB, "audit14_update", "ix_a")
	require.Len(t, entries, 1, "still exactly one entry after update")
	assert.Equal(t, qplanner.IndexValueScalar, entries[0].Value,
		"after public-API update the legacy nil entry must be replaced "+
			"with IndexValueScalar (0x00) — this is the migration path")

	// Query still works for the new value.
	n, err := coll.Find(`{"a":20}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	// And the old key is gone (no orphaned entry).
	n, err = coll.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, n,
		"after update the old key (a:10) must produce 0 hits — the legacy "+
			"entry there must have been deleted by deleteKeys")
}

// --- Subtest 4: fully-pre-upgrade index, multi-bound $in still correct ---

// TestAudit14_LegacyNilValue_AllNilSimulatesPreUpgrade simulates an
// index where EVERY entry was written by old code (nil values). We
// inject several nil-value entries directly (no public-API inserts on
// this index), then run a multi-bound $in query.
//
// Because every entry decodes as multi-key (len==0), the consumer-side
// DocDedup must collapse cross-bound duplicates. Without that, an $in
// query with overlapping bounds would over-count.
//
// We use the trick of inserting docs into the data namespace via a
// SECOND collection that doesn't share the indexed field, then we hand-
// build the index entries. Simpler approach: insert minimal docs (with
// no array), then inject ONLY legacy index entries for them.
func TestAudit14_LegacyNilValue_AllNilSimulatesPreUpgrade(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit14_allnil")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "tags",
		Fields: []string{"tags"},
	}))

	// Insert docs with NO "tags" field. The public-API insert path will
	// not write any index entries for them (sparse-like behavior on a
	// missing field path). Then we'll inject legacy entries by hand.
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1"}`),
		anyenc.MustParseJson(`{"id":"d2"}`),
	))

	c, idx := findIndex(t, coll, "tags")

	// Sanity: no entries from the public-API path (the docs don't have
	// "tags", and the index is non-sparse but the field is missing —
	// writeValues calls Get which returns a typeNull-ish value; the
	// actual behavior depends on writeValues, but to be robust we verify
	// the count rather than asserting zero).
	pre := readRawIndexEntries(t, fx.DB, "audit14_allnil", "tags")
	preLen := len(pre)

	// Inject legacy entries: d1.tags = ["a","b"] and d2.tags = ["b","c"]
	// — i.e. d1 matches {a,b}, d2 matches {b,c}, both overlap on "b".
	// This reproduces the layout a pre-value-byte writer produced: per-element
	// keys PLUS the canonical whole-array key, all with nil value bytes.
	// writeValues has emitted the canonical (0x06-prefixed) whole-array key
	// since before the value byte existed, so real legacy multi-key data
	// always carries it.
	for _, kv := range []struct {
		key string
		doc string
	}{
		{"a", "d1"},
		{"b", "d1"},
		{"b", "d2"},
		{"c", "d2"},
	} {
		full := buildIndexFullKey([]any{kv.key}, kv.doc)
		injectRawIndexEntry(t, c, idx, full, nil)
	}
	// Canonical whole-array keys (0x06 prefix), nil value — built via the
	// array Value's MarshalTo since AppendAnyValue handles scalars only.
	for _, kv := range []struct {
		arr string
		doc string
	}{
		{`["a","b"]`, "d1"},
		{`["b","c"]`, "d2"},
	} {
		canonical := append(anyenc.MustParseJson(kv.arr).MarshalTo(nil),
			anyenc.AppendAnyValue(nil, kv.doc)...)
		injectRawIndexEntry(t, c, idx, canonical, nil)
	}

	// A pre-flag writer never wrote the scalar-proven marker either: drop
	// the one EnsureIndex wrote so the index reads as legacy (unproven).
	require.NoError(t, c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Delete(c.db.systemNS, multikeyKey(idx.ns.Name()))
	}))

	post := readRawIndexEntries(t, fx.DB, "audit14_allnil", "tags")
	require.Equal(t, preLen+6, len(post),
		"expected %d (pre) + 4 per-element + 2 canonical entries, got %d", preLen, len(post))
	for _, e := range post[preLen:] {
		assert.Empty(t, e.Value,
			"all injected entries must have empty (legacy) value")
	}

	// Multi-bound $in over [a,b,c]: d1 matches "a" and "b" (2 hits), d2
	// matches "b" and "c" (2 hits). Without dedup, count = 4. The missing
	// scalar-proven marker routes the count path to the dedup walk, so the
	// distinct-doc count is 2.
	n, err := coll.Find(`{"tags":{"$in":["a","b","c"]}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, n,
		"distinct doc count over legacy index must be 2 (d1+d2). "+
			"4 would mean dedup is broken on the legacy path; got %d", n)
}

// --- Subtest 5: rebuild via DropIndex + EnsureIndex normalizes everything ---

// TestAudit14_LegacyNilValue_RebuildNormalizes injects a mix of legacy
// (nil) and new (bit-set) entries, then drops and recreates the index.
// The rebuild must walk the data namespace and rewrite EVERY entry with
// the new bit-set format — no nil values must remain.
//
// This is the explicit migration knob: DropIndex + EnsureIndex has
// always been the documented index-rebuild path; we pin that it also
// upgrades the entry-value-byte format.
func TestAudit14_LegacyNilValue_RebuildNormalizes(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit14_rebuild")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "tags",
		Fields: []string{"tags"},
	}))

	// Insert two real docs via the public API → bit-set entries.
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b"]}`),
		anyenc.MustParseJson(`{"id":"d2","tags":["c"]}`),
	))

	c, idx := findIndex(t, coll, "tags")

	// Now overwrite a couple of those entries with nil values to
	// simulate leftover legacy data co-existing with new data.
	overwrite := buildIndexFullKey([]any{"a"}, "d1")
	injectRawIndexEntry(t, c, idx, overwrite, nil)
	overwrite2 := buildIndexFullKey([]any{"c"}, "d2")
	injectRawIndexEntry(t, c, idx, overwrite2, nil)

	pre := readRawIndexEntries(t, fx.DB, "audit14_rebuild", "tags")
	var preLegacy, preBitSet int
	for _, e := range pre {
		if len(e.Value) == 0 {
			preLegacy++
		} else {
			preBitSet++
		}
	}
	require.GreaterOrEqual(t, preLegacy, 2,
		"setup expected at least 2 legacy entries before rebuild")
	require.GreaterOrEqual(t, preBitSet, 1,
		"setup expected at least 1 bit-set entry before rebuild")

	// Drop + EnsureIndex — the documented rebuild path.
	require.NoError(t, coll.DropIndex(ctx, "tags"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "tags",
		Fields: []string{"tags"},
	}))

	// Every entry must now be bit-set. No nil values may remain.
	post := readRawIndexEntries(t, fx.DB, "audit14_rebuild", "tags")
	require.NotEmpty(t, post, "rebuild must produce some entries")
	for i, e := range post {
		require.NotEmptyf(t, e.Value,
			"entry %d after rebuild must NOT have empty value (got %d entries total)",
			i, len(post))
	}

	// Sanity: queries still work.
	n, err := coll.Find(`{"tags":"a"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

// --- Subtest 6: planner routes via SeenSet/DocDedup on legacy index ---

// TestAudit14_LegacyNilValue_PlannerRoutesViaSeenSet pins the end-to-end
// behavior: when the index is fully legacy (nil values), a multi-bound
// $in query with OVERLAPPING bounds against the same doc must still
// produce the correct distinct-doc count via Count AND yield each doc
// exactly once via Iter.
//
// The mechanism: every legacy entry decodes as multi-key (true), so
// DocDedup at the consumer side collapses repeated docIds across
// bounds. Without that — if EntryValueIsMultiKey reported false on
// nil — both Count and Iter would over-count.
//
// Setup: insert d1 WITH tags = [a,b,c] (so the data namespace has a
// real, filter-matching doc). Then overwrite EVERY bit-set entry in
// the index with a nil value, simulating an index that was written by
// the old code but whose data was somehow up to date — i.e. a fully
// pre-upgrade index that hasn't yet been touched (no inserts/updates
// migrate it; no EnsureIndex rebuild).
func TestAudit14_LegacyNilValue_PlannerRoutesViaSeenSet(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit14_seenset")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "tags",
		Fields: []string{"tags"},
	}))

	// Insert a real doc with the array — the public-API path writes
	// bit-set entries for each key (3 element entries + 1 whole-array).
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b","c"]}`),
	))

	c, idx := findIndex(t, coll, "tags")

	// Now overwrite EVERY entry's value with nil to simulate a fully
	// pre-upgrade index. The keys stay the same (so they still point at
	// d1 correctly); only the value byte changes from 0x01 → empty.
	pre := readRawIndexEntries(t, fx.DB, "audit14_seenset", "tags")
	require.NotEmpty(t, pre, "public-API insert must produce some entries")
	for _, e := range pre {
		injectRawIndexEntry(t, c, idx, e.Key, nil)
	}

	// Verify all entries are now legacy (nil values).
	post := readRawIndexEntries(t, fx.DB, "audit14_seenset", "tags")
	require.Equal(t, len(pre), len(post),
		"overwrite must keep entry count constant (same keys, different values)")
	for i, e := range post {
		require.Emptyf(t, e.Value,
			"entry %d after overwrite must have empty (legacy) value, got %v",
			i, e.Value)
	}

	// Count must dedup → 1. Without dedup (if nil decoded as scalar
	// false) the count loop would tally each matching bound separately
	// and report >1.
	n, err := coll.Find(`{"tags":{"$in":["a","b"]}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n,
		"distinct-doc Count over fully-legacy index with overlapping $in "+
			"bounds must be 1 (got %d) — legacy path must route via DocDedup", n)

	// Iter must yield d1 exactly once (in any order). Same dedup logic,
	// different code path (planIterator.Next + DocDedup vs. Count's loop).
	ids := collectField(t, coll.Find(`{"tags":{"$in":["a","b"]}}`), "id")
	sort.Strings(ids)
	assert.Equal(t, []string{`"d1"`}, ids,
		"Iter over fully-legacy index with overlapping $in bounds must yield "+
			"d1 exactly once (got %v)", ids)
}

// ── Corruption-recovery audit (act-38..42) ──
/*
Audit tests for the "corruption-recovery" domain (any-store-tests:docs/any-store/qplanner/audit/actionable_by_domain.json).
These pin documented limitations (index/data trust boundaries) and prove that
DropIndex + EnsureIndex rebuild recovers. Direct-namespace corruption uses the
same internal pattern as the corruption tests above.

  act-38  A stale unique entry (data row deleted directly) falsely blocks reinsert; rebuild fixes it.
  act-39  API delete only removes keys recomputed from the doc's current values; a spurious
          same-docId entry is orphaned, not self-healed; rebuild fixes it.
  act-40  DeleteId on a doc whose data row is already gone fails fast (ErrDocNotFound), index untouched.
  act-41  A missing trailing compound field == explicit null padding; rebuild restores a deleted padding entry.
  act-42  EnsureIndex on a same-named index with a changed Unique flag is a swallowed no-op.
*/

// act-38
func TestIndex_Corruption_UniqueStaleEntryBlocksReinsert(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":100}`)))

	// Delete the data row directly, leaving the phantom unique index entry.
	c := coll.(*collection)
	idKey := anyenc.AppendAnyValue(anyenc.Tuple(nil), 1)
	require.NoError(t, c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Delete(c.ns, idKey)
	}))

	// Unique insertKeys trusts the index entry (no data-liveness check) → false reject.
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":100}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)
	assertCollCount(t, coll, 0)

	// Recovery: rebuild from (now-empty) data.
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":100}`)))
	assertIndexLen(t, coll.GetIndexes()[0], 1)
	assertQueryCount(t, coll.Find(`{"a":100}`), 1)
}

// act-39
func TestIndex_Corruption_DeleteLeavesOrphanedSpuriousEntry(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":100}`)))

	c, idx := findIndex(t, coll, "a")
	// Inject a spurious entry: same docId=1, different value 777.
	injectRawIndexEntry(t, c, idx, buildIndexFullKey([]any{777}, 1), []byte{0x00})
	assertIndexLen(t, idx, 2)

	// API delete recomputes keys from the doc's CURRENT value (a=100); the 777
	// entry is never visited → orphaned, not self-healed.
	require.NoError(t, coll.DeleteId(ctx, 1))
	assertCollCount(t, coll, 0)
	assertIndexLen(t, idx, 1)

	// Recovery: rebuild from (empty) data drops the orphan.
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	assertIndexLen(t, coll.GetIndexes()[0], 0)
}

// act-40
func TestIndex_Corruption_DeleteIdAfterDataRowGone(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	for i := 1; i <= 3; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			`{"id":`+strconv.Itoa(i)+`,"a":`+strconv.Itoa(i*10)+`}`)))
	}

	c := coll.(*collection)
	idKey := anyenc.AppendAnyValue(anyenc.Tuple(nil), 2)
	require.NoError(t, c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Delete(c.ns, idKey)
	}))
	assertIndexLen(t, coll.GetIndexes()[0], 3)

	// loadById maps ErrKeyNotFound → ErrDocNotFound and returns before deleteItem.
	err = coll.DeleteId(ctx, 2)
	require.ErrorIs(t, err, ErrDocNotFound)
	assertIndexLen(t, coll.GetIndexes()[0], 3) // stale entry untouched
	assertCollCount(t, coll, 2)

	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	assertIndexLen(t, coll.GetIndexes()[0], 2)
	assertCollCount(t, coll, 2)
}

// act-41
func TestIndex_Corruption_CompoundRebuildRestoresNullPadding(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":1,"b":2}`),
		anyenc.MustParseJson(`{"id":2,"a":1}`), // missing b → null padding
		anyenc.MustParseJson(`{"id":3,"a":1,"b":null}`),
	))
	c, idx := findIndex(t, coll, "a,b")
	assertIndexLen(t, idx, 3)

	ids := func(filter string) []string {
		return collectField(t, coll.Find(filter).Sort("id"), "id")
	}
	assert.ElementsMatch(t, []string{"2", "3"}, ids(`{"a":1,"b":null}`))
	assertQueryCount(t, coll.Find(`{"a":1,"b":2}`), 1)

	// Directly delete id=2's (1,null) compound entry.
	full := buildIndexFullKey([]any{1, nil}, 2)
	require.NoError(t, c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Delete(idx.ns, full)
	}))
	assertIndexLen(t, idx, 2)
	assert.ElementsMatch(t, []string{"3"}, ids(`{"a":1,"b":null}`))

	// Recovery: rebuild restores the deleted null-padding entry.
	require.NoError(t, coll.DropIndex(ctx, "a,b"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))
	assert.ElementsMatch(t, []string{"2", "3"}, ids(`{"a":1,"b":null}`))
	assertQueryCount(t, coll.Find(`{"a":1,"b":2}`), 1)
	assertIndexLen(t, coll.GetIndexes()[0], 3)
}

// act-42
//
// Updated for the index-staleness fix: a same-name EnsureIndex with a CHANGED
// definition is no longer a silent no-op. registerIndex now compares the
// persisted definition against the request and returns ErrIndexMismatch when
// they differ (fields / unique / sparse) — surfaced even under ensure=true — so
// a redefinition is never silently dropped. The upgrade path remains explicit:
// drop, then create with the new definition.
func TestIndex_EnsureIndex_SameNameChangedDefinitionErrors(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":5}`),
		anyenc.MustParseJson(`{"id":2,"a":5}`),
	))

	// Index name == join(Fields), so a changed Unique flag does NOT change the
	// name. The request collides with the existing "a" index but carries a
	// different definition, so EnsureIndex returns ErrIndexMismatch (rather than
	// silently keeping the old non-unique definition) and the index is unchanged.
	err = coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true})
	require.ErrorIs(t, err, ErrIndexMismatch)
	assert.False(t, coll.GetIndexes()[0].Info().Unique)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"a":5}`))) // still non-unique

	// An identical EnsureIndex (same definition) remains an idempotent no-op.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	assert.False(t, coll.GetIndexes()[0].Info().Unique)

	// Upgrade path: drop then ensure-unique — backfill fails on the existing dups.
	require.NoError(t, coll.DropIndex(ctx, "a"))
	err = coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true})
	require.ErrorIs(t, err, ErrUniqueConstraint)
	assert.Empty(t, coll.GetIndexes())

	// Remove the duplicates, then the unique index builds and enforces.
	require.NoError(t, coll.DeleteId(ctx, 2))
	require.NoError(t, coll.DeleteId(ctx, 3))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))
	assert.True(t, coll.GetIndexes()[0].Info().Unique)
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":5}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)
}
