/*
Index/Planner tests inspired by SQLite: index.test, index3.test

Test scenario:
Tests that indexes correctly handle various data types: strings, integers,
floats, booleans, nulls, mixed types, negative numbers, large numbers,
empty strings, and unicode strings. Verifies equality, range, and sort
operations produce correct results for each type.

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

func TestIndex_DataTypes_StringValues(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"name"}}))

	names := []string{"charlie", "alice", "bob", "dave", "eve"}
	for i, name := range names {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"name":"%s"}`, i, name),
		)))
	}

	// Equality
	count, err := coll.Find(`{"name":"bob"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Range on strings
	count, err = coll.Find(`{"name":{"$gte":"bob","$lte":"dave"}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, count) // bob, charlie, dave

	// Sort ascending
	vals := collectField(t, coll.Find(nil).Sort("name"), "name")
	require.Len(t, vals, 5)
	assert.Equal(t, `"alice"`, vals[0])
	assert.Equal(t, `"bob"`, vals[1])
	assert.Equal(t, `"charlie"`, vals[2])
	assert.Equal(t, `"dave"`, vals[3])
	assert.Equal(t, `"eve"`, vals[4])

	// Sort descending
	vals = collectField(t, coll.Find(nil).Sort("-name"), "name")
	require.Len(t, vals, 5)
	assert.Equal(t, `"eve"`, vals[0])
	assert.Equal(t, `"alice"`, vals[4])
}

func TestIndex_DataTypes_IntegerValues(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"count"}}))

	for i := range 20 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"count":%d}`, i, i*5),
		)))
	}

	// Equality
	count, err := coll.Find(`{"count":25}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count) // i=5 → count=25

	// Range
	count, err = coll.Find(`{"count":{"$gte":20,"$lt":50}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 6, count) // 20,25,30,35,40,45

	// Sort ascending
	vals := collectField(t, coll.Find(`{"count":{"$lte":20}}`).Sort("count"), "count")
	require.Len(t, vals, 5) // 0,5,10,15,20
	assert.Equal(t, "0", vals[0])
	assert.Equal(t, "20", vals[4])
}

func TestIndex_DataTypes_FloatValues(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"score"}}))

	scores := []float64{1.5, 2.7, 0.1, 3.14, 2.0, 9.99, 0.5}
	for i, s := range scores {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"score":%g}`, i, s),
		)))
	}

	// Range query on floats
	count, err := coll.Find(`{"score":{"$gte":1.0,"$lte":3.0}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, count) // 1.5, 2.7, 2.0

	// Sort ascending
	vals := collectField(t, coll.Find(nil).Sort("score"), "score")
	require.Len(t, vals, 7)
	assert.Equal(t, "0.1", vals[0])
	assert.Equal(t, "9.99", vals[6])
}

func TestIndex_DataTypes_BooleanValues(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"active"}}))

	for i := range 10 {
		active := "true"
		if i%3 == 0 {
			active = "false"
		}
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"active":%s}`, i, active),
		)))
	}

	// Equality on true
	count, err := coll.Find(`{"active":true}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 6, count) // i=1,2,4,5,7,8 (not i%3==0)

	// Equality on false
	count, err = coll.Find(`{"active":false}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 4, count) // i=0,3,6,9

	explain, err := coll.Find(`{"active":true}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")
}

func TestIndex_DataTypes_NullValues(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":null}`),
		anyenc.MustParseJson(`{"id":2,"a":10}`),
		anyenc.MustParseJson(`{"id":3}`),
		anyenc.MustParseJson(`{"id":4,"a":20}`),
		anyenc.MustParseJson(`{"id":5,"a":null}`),
	))

	// All docs indexed (non-sparse)
	assertIndexLen(t, coll.GetIndexes()[0], 5)

	// Query for null values
	count, err := coll.Find(`{"a":null}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, count) // id:1 (a=null), id:3 (missing=null), id:5 (a=null)

	// Query for non-null
	count, err = coll.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_DataTypes_MixedTypes(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"val"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"val":null}`),
		anyenc.MustParseJson(`{"id":2,"val":42}`),
		anyenc.MustParseJson(`{"id":3,"val":"hello"}`),
		anyenc.MustParseJson(`{"id":4,"val":false}`),
		anyenc.MustParseJson(`{"id":5,"val":true}`),
		anyenc.MustParseJson(`{"id":6,"val":0}`),
		anyenc.MustParseJson(`{"id":7,"val":"abc"}`),
	))

	assertIndexLen(t, coll.GetIndexes()[0], 7)

	// Each type is queryable
	count, err := coll.Find(`{"val":null}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"val":42}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"val":"hello"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"val":false}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"val":true}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Sort produces consistent ordering — type ordering: null < number < string < false < true
	vals := collectField(t, coll.Find(nil).Sort("val"), "val")
	require.Len(t, vals, 7)
	// Verify null is first
	assert.Equal(t, "null", vals[0])
	// Verify numbers come before strings
	// Numbers: 0, 42; Strings: "abc", "hello"; Bools: false, true
	assert.Equal(t, "0", vals[1])
	assert.Equal(t, "42", vals[2])
}

func TestIndex_DataTypes_NegativeNumbers(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"n"}}))

	values := []int{-10, -5, -1, 0, 1, 5, 10}
	for i, v := range values {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"n":%d}`, i, v),
		)))
	}

	// Range including negatives
	count, err := coll.Find(`{"n":{"$gte":-5,"$lte":5}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, count) // -5,-1,0,1,5

	// Only negatives
	count, err = coll.Find(`{"n":{"$lt":0}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, count) // -10,-5,-1

	// Sort ascending should order negatives correctly
	vals := collectField(t, coll.Find(nil).Sort("n"), "n")
	require.Len(t, vals, 7)
	assert.Equal(t, "-10", vals[0])
	assert.Equal(t, "-5", vals[1])
	assert.Equal(t, "-1", vals[2])
	assert.Equal(t, "0", vals[3])
	assert.Equal(t, "1", vals[4])
	assert.Equal(t, "5", vals[5])
	assert.Equal(t, "10", vals[6])
}

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
