/*
Index/Planner tests inspired by SQLite: index.test, index3.test

Test scenario:
Tests that indexes correctly handle edge-case data types: large numbers,
empty strings, and unicode strings. These require imperative logic
(mid-test inserts, assertIndexLen).

These tests verify our custom index and query planner implementation.
While inspired by SQLite test patterns, our system has a different
architecture (document-oriented with weight-based planner vs SQL VDBE).
*/
package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

func TestIndex_DataTypes_LargeNumbers(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"big"}}))

	// Insert large numbers
	largeVals := []int64{100001, 500002, 999999, 42, 750003}
	for i, v := range largeVals {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"big":%d}`, i, v),
		)))
	}

	// Equality on large number
	count, err := coll.Find(`{"big":999999}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Range on large numbers
	count, err = coll.Find(`{"big":{"$gte":100001,"$lte":999999}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 4, count) // 100001, 500002, 750003, 999999

	// Sort ascending — verify correct numeric order
	vals := collectField(t, coll.Find(nil).Sort("big"), "big")
	require.Len(t, vals, 5)
	assert.Equal(t, "42", vals[0])
	assert.Equal(t, "100001", vals[1])
	assert.Equal(t, "500002", vals[2])
	assert.Equal(t, "750003", vals[3])
	assert.Equal(t, "999999", vals[4])

	// Sort descending
	valsDesc := collectField(t, coll.Find(nil).Sort("-big"), "big")
	require.Len(t, valsDesc, 5)
	assert.Equal(t, "999999", valsDesc[0])
	assert.Equal(t, "42", valsDesc[4])

	// Also verify truly large numbers work for equality and range (even if formatting differs)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":10,"big":9876543210}`)))
	count, err = coll.Find(`{"big":9876543210}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"big":{"$gt":999999}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_DataTypes_EmptyString(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"s"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"s":""}`),
		anyenc.MustParseJson(`{"id":2,"s":"hello"}`),
		anyenc.MustParseJson(`{"id":3,"s":""}`),
		anyenc.MustParseJson(`{"id":4,"s":"world"}`),
	))

	// Query for empty string
	count, err := coll.Find(`{"s":""}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	// Non-empty
	count, err = coll.Find(`{"s":"hello"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	assertIndexLen(t, coll.GetIndexes()[0], 4)
}

func TestIndex_DataTypes_UnicodeStrings(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"text"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"text":"hello"}`),
		anyenc.MustParseJson(`{"id":2,"text":"\u4f60\u597d"}`),
		anyenc.MustParseJson(`{"id":3,"text":"\u00e9l\u00e8ve"}`),
		anyenc.MustParseJson(`{"id":4,"text":"\u0410\u043b\u0438\u0441\u0430"}`),
		anyenc.MustParseJson(`{"id":5,"text":"\u4f60\u597d"}`),
	))

	// Equality on unicode
	count, err := coll.Find(`{"text":"\u4f60\u597d"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	count, err = coll.Find(`{"text":"\u00e9l\u00e8ve"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"text":"\u0410\u043b\u0438\u0441\u0430"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	assertIndexLen(t, coll.GetIndexes()[0], 5)
}

// --- Coverage tests from anyenc_nul_coverage_test.go ---

// TestAnyenc_Coverage_EmbeddedNulPreservesSeparation verifies that inserting
// two documents whose name field differs only by an embedded NUL byte
// ({id:"d1", name:"a\x00b"} and {id:"d2", name:"a"}) remain independently
// queryable. Each of the two equality queries must match exactly one doc.
//
// Gap item 47: String with embedded NUL byte ("a\x00b").
func TestAnyenc_Coverage_EmbeddedNulPreservesSeparation(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	// JSON \u0000 produces a literal NUL byte in the resulting string.
	d1 := anyenc.MustParseJson(`{"id":"d1","name":"a\u0000b"}`)
	d2 := anyenc.MustParseJson(`{"id":"d2","name":"a"}`)
	require.NoError(t, coll.Insert(ctx, d1, d2))

	// Query by name = "a\x00b" — must return exactly d1.
	got1 := collectField(t, coll.Find(`{"name":"a\u0000b"}`), "id")
	assert.Equal(t, []string{`"d1"`}, got1,
		"query by name=\"a\\x00b\" must match only d1")

	// Query by name = "a" — must return exactly d2.
	got2 := collectField(t, coll.Find(`{"name":"a"}`), "id")
	assert.Equal(t, []string{`"d2"`}, got2,
		"query by name=\"a\" must match only d2")
}
