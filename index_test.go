package anystore

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/btree"
	"github.com/anyproto/any-store/internal/qplanner"
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
	for _, i := range c.indexes {
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
