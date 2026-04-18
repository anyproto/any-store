package anystore

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

func mustParseItem(t testing.TB, s string) item {
	it, err := newItem(anyenc.MustParseJson(s))
	require.NoError(t, err)
	return it
}

func assertIdxKeyBuf(t *testing.T, idx *index, keyCase fillKeysCase) {
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
			{`{"id":1,"a":null}`, []string{}},
		},
	},
	{
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
		sketchKeyCountBefore = idx.sketch.Estimate(keyCopy)
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
		assert.Equal(t, sketchKeyCountBefore, idx.sketch.Estimate(keyCopy),
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
