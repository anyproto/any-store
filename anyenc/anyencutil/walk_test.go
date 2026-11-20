package anyencutil

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

func TestWalk(t *testing.T) {
	var a = &anyenc.Arena{}
	t.Run("set single key ", func(t *testing.T) {
		v := anyenc.MustParseJson(`{}`)
		require.NoError(t, Walk(a, v, []string{"a"}, true, func(prevValue *anyenc.Value, value *anyenc.Value) (res *anyenc.Value, err error) {
			assert.Equal(t, `{}`, prevValue.String())
			assert.Nil(t, value)
			return a.NewTrue(), nil
		}))
		assert.Equal(t, `{"a":true}`, v.String())
	})
	t.Run("set path key", func(t *testing.T) {
		v := anyenc.MustParseJson(`{}`)
		require.NoError(t, Walk(a, v, []string{"a", "b"}, true, func(prevValue *anyenc.Value, value *anyenc.Value) (res *anyenc.Value, err error) {
			assert.Equal(t, `{}`, prevValue.String())
			assert.Nil(t, value)
			return a.NewTrue(), nil
		}))
		assert.Equal(t, `{"a":{"b":true}}`, v.String())
	})
	t.Run("rewrite key", func(t *testing.T) {
		v := anyenc.MustParseJson(`{"a":1}`)
		require.NoError(t, Walk(a, v, []string{"a"}, true, func(prevValue *anyenc.Value, value *anyenc.Value) (res *anyenc.Value, err error) {
			assert.Equal(t, `{"a":1}`, prevValue.String())
			assert.Equal(t, `1`, value.String())
			return a.NewTrue(), nil
		}))
		assert.Equal(t, `{"a":true}`, v.String())
	})
	t.Run("delete key", func(t *testing.T) {
		v := anyenc.MustParseJson(`{"a":1}`)
		require.NoError(t, Walk(a, v, []string{"a"}, true, func(prevValue *anyenc.Value, value *anyenc.Value) (res *anyenc.Value, err error) {
			assert.Equal(t, `{"a":1}`, prevValue.String())
			assert.Equal(t, `1`, value.String())
			return nil, nil
		}))
		assert.Equal(t, `{}`, v.String())
	})
	t.Run("set to array", func(t *testing.T) {
		v := anyenc.MustParseJson(`{"a":{"b":[1,2,3]}}`)
		require.NoError(t, Walk(a, v, []string{"a", "b", "1"}, true, func(prevValue *anyenc.Value, value *anyenc.Value) (res *anyenc.Value, err error) {
			assert.Equal(t, `[1,2,3]`, prevValue.String())
			assert.Equal(t, `2`, value.String())
			return a.NewNumberFloat64(22), nil
		}))
		assert.Equal(t, `{"a":{"b":[1,22,3]}}`, v.String())
	})
	t.Run("set to object inside array", func(t *testing.T) {
		v := anyenc.MustParseJson(`{"a":[{"b":2}]}`)
		require.NoError(t, Walk(a, v, []string{"a", "0", "b"}, true, func(prevValue *anyenc.Value, value *anyenc.Value) (res *anyenc.Value, err error) {
			assert.Equal(t, `{"b":2}`, prevValue.String())
			assert.Equal(t, `2`, value.String())
			return a.NewNumberFloat64(22), nil
		}))
		assert.Equal(t, `{"a":[{"b":22}]}`, v.String())
	})
	t.Run("error create", func(t *testing.T) {
		var errorCases = [][]string{
			{
				`{"a":1}`,
				"a", "b",
			},
			{
				`{"a":[1,2]}`,
				"a", "b",
			},
			{
				`{"a":[1,2]}`,
				"a", "b", "c",
			},
		}

		for _, ec := range errorCases {
			v := anyenc.MustParseJson(ec[0])
			if !assert.Error(t, Walk(a, v, ec[1:], true, func(prevValue *anyenc.Value, value *anyenc.Value) (res *anyenc.Value, err error) {
				t.Log(prevValue.String())
				return a.NewTrue(), nil
			})) {
				t.Log(v.String())
			}

		}
	})
}

// A numeric segment past the end of an array pads it with nulls. The padding
// is bounded: one path segment must not ask for an allocation proportional to
// a number the caller wrote. A refused segment leaves the array as it was, its
// backing storage too; with the largest int as the index, an implementation
// that allocated toward it would not return at all.
func TestWalk_ArrayPaddingIsBounded(t *testing.T) {
	a := &anyenc.Arena{}
	set := func(v *anyenc.Value, path ...string) error {
		return Walk(a, v, path, true, func(_, _ *anyenc.Value) (*anyenc.Value, error) {
			return a.NewTrue(), nil
		})
	}
	const arrLen = 2
	atCap := strconv.Itoa(arrLen + MaxArrayPadding)
	overCap := strconv.Itoa(arrLen + MaxArrayPadding + 1)

	// long is {"arr":[1,2,null…,true]}: the array the at-limit set builds.
	var long *anyenc.Value
	t.Run("padding of exactly the limit succeeds", func(t *testing.T) {
		long = anyenc.MustParseJson(`{"arr":[1,2]}`)
		require.NoError(t, set(long, "arr", atCap))
		arr := long.GetArray("arr")
		require.Len(t, arr, arrLen+MaxArrayPadding+1)
		assert.Equal(t, anyenc.TypeNull, arr[arrLen].Type())
		assert.Equal(t, anyenc.TypeTrue, arr[len(arr)-1].Type())
	})
	t.Run("intermediate segment at the limit succeeds", func(t *testing.T) {
		v := anyenc.MustParseJson(`{"arr":[1,2]}`)
		require.NoError(t, set(v, "arr", atCap, "x"))
		arr := v.GetArray("arr")
		require.Len(t, arr, arrLen+MaxArrayPadding+1)
		assert.Equal(t, `{"x":true}`, arr[len(arr)-1].String())
	})
	for _, tc := range []struct {
		name string
		path []string
	}{
		{"one over the limit", []string{"arr", overCap}},
		{"intermediate segment one over the limit", []string{"arr", overCap, "x"}},
		{"far over the limit", []string{"arr", strconv.Itoa(math.MaxInt32)}},
		{"intermediate segment far over the limit", []string{"arr", strconv.Itoa(math.MaxInt32), "x"}},
		{"largest int", []string{"arr", strconv.Itoa(math.MaxInt)}},
		{"intermediate segment at the largest int", []string{"arr", strconv.Itoa(math.MaxInt), "x"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := anyenc.MustParseJson(`{"arr":[1,2]}`)
			before, _ := v.Get("arr").Array()
			err := set(v, tc.path...)
			require.ErrorContains(t, err, "can't backfill more than 1500000 elements")
			assert.Equal(t, `{"arr":[1,2]}`, v.String(), "a refused path leaves the document as it was")
			after, _ := v.Get("arr").Array()
			assert.Equal(t, cap(before), cap(after), "a refused path does not grow the array")
		})
	}
	t.Run("an index that is not a non-negative int is refused", func(t *testing.T) {
		// The decimal overflows int on every platform.
		for _, seg := range []string{"-1", "99999999999999999999999999", "x"} {
			v := anyenc.MustParseJson(`{"arr":[1,2]}`)
			require.ErrorContains(t, set(v, "arr", seg), "cannot create field", seg)
			require.ErrorContains(t, set(v, "arr", seg, "x"), "cannot create field", seg)
			assert.Equal(t, `{"arr":[1,2]}`, v.String())
		}
	})
	t.Run("an existing element far into a long array is not padding", func(t *testing.T) {
		require.NotNil(t, long, "needs the at-limit subtest")
		n := len(long.GetArray("arr"))
		require.Greater(t, n-1, MaxArrayPadding)
		v := anyenc.MustParseJson(`false`)
		require.NoError(t, Walk(a, long, []string{"arr", strconv.Itoa(n - 1)}, true,
			func(_, _ *anyenc.Value) (*anyenc.Value, error) { return v, nil }))
		arr := long.GetArray("arr")
		require.Len(t, arr, n, "overwriting an element keeps the length")
		assert.Equal(t, anyenc.TypeFalse, arr[n-1].Type())
		// Appending right after the end pads nothing.
		require.NoError(t, set(long, "arr", strconv.Itoa(n)))
		assert.Len(t, long.GetArray("arr"), n+1)
	})
}

// The padding a segment needs is its index minus the array's length; the
// check is exact at the limit and cannot overflow for any index.
func TestPaddingExceeds(t *testing.T) {
	for _, tc := range []struct {
		idx, n int
		want   bool
	}{
		{0, 0, false},
		{5, 10, false},
		{MaxArrayPadding, 0, false},
		{MaxArrayPadding + 1, 0, true},
		{MaxArrayPadding + 7, 7, false},
		{MaxArrayPadding + 8, 7, true},
		{math.MaxInt, 0, true},
		{math.MaxInt, math.MaxInt - MaxArrayPadding, false},
		{math.MaxInt, math.MaxInt - MaxArrayPadding - 1, true},
	} {
		assert.Equal(t, tc.want, paddingExceeds(tc.idx, tc.n), "idx %d, len %d", tc.idx, tc.n)
	}
}
