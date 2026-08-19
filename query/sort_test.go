package query

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

func TestParseSort(t *testing.T) {
	t.Run("asc string", func(t *testing.T) {
		s, err := ParseSort("a.c", "-b")
		require.NoError(t, err)
		require.NotNil(t, s)
		ss := s.(Sorts)
		require.Len(t, ss, 2)
		assert.False(t, ss[0].(*SortField).Reverse)
		assert.Equal(t, []string{"a", "c"}, ss[0].(*SortField).Path)
		assert.True(t, ss[1].(*SortField).Reverse)
		assert.Equal(t, []string{"b"}, ss[1].(*SortField).Path)
	})
	t.Run("custom", func(t *testing.T) {
		s, err := ParseSort(&SortField{Path: []string{"a"}})
		require.NoError(t, err)
		require.NotNil(t, s)
	})
}

func TestSortField_AppendKey(t *testing.T) {
	t.Run("asc", func(t *testing.T) {
		s := MustParseSort("a")
		k := s.AppendKey(nil, anyenc.MustParseJson(`{"a":123}`))
		k = s.AppendKey(k, anyenc.MustParseJson(`{"a":321}`))
		assert.Equal(t, "123/321", anyenc.Tuple(k).String())
	})
	t.Run("desc", func(t *testing.T) {
		s := MustParseSort("-a")
		k1 := s.AppendKey(nil, anyenc.MustParseJson(`{"a":123}`))
		k2 := s.AppendKey(nil, anyenc.MustParseJson(`{"a":321}`))
		assert.Equal(t, 1, bytes.Compare(k1, k2))
	})
}

// Array-valued sort fields use Mongo semantics: the sort key is the MINIMUM
// element ascending / MAXIMUM element descending, independent of any query
// predicate. Empty arrays keep their whole-array encoding; missing fields
// keep TypeNull.
func TestSortField_AppendKey_ArrayElements(t *testing.T) {
	key := func(spec string, doc string) []byte {
		return MustParseSort(spec).AppendKey(nil, anyenc.MustParseJson(doc))
	}

	t.Run("asc uses min element", func(t *testing.T) {
		assert.Equal(t, key("a", `{"a":1}`), key("a", `{"a":[5,1,3]}`))
	})
	t.Run("desc uses max element", func(t *testing.T) {
		assert.Equal(t, key("-a", `{"a":5}`), key("-a", `{"a":[5,1,3]}`))
	})
	t.Run("asc orders arrays by their minima", func(t *testing.T) {
		lo := key("a", `{"a":[9,0]}`)
		hi := key("a", `{"a":[1,2]}`)
		assert.Equal(t, -1, bytes.Compare(lo, hi), "min 0 sorts before min 1")
	})
	t.Run("desc orders arrays by their maxima", func(t *testing.T) {
		first := key("-a", `{"a":[1,9]}`)
		second := key("-a", `{"a":[8,2]}`)
		assert.Equal(t, -1, bytes.Compare(first, second), "max 9 sorts before max 8 descending")
	})
	t.Run("single-element array equals its element", func(t *testing.T) {
		assert.Equal(t, key("a", `{"a":7}`), key("a", `{"a":[7]}`))
	})
	t.Run("nested array elements compare by encoding", func(t *testing.T) {
		// min of [[9],3] is the number 3 (number tag < array tag).
		assert.Equal(t, key("a", `{"a":3}`), key("a", `{"a":[[9],3]}`))
		// max of [[9],3] is the nested array [9].
		assert.Equal(t, key("-a", `{"a":[[9]]}`), key("-a", `{"a":[[9],3]}`))
	})
	t.Run("empty array keeps the whole-array encoding", func(t *testing.T) {
		a := &anyenc.Arena{}
		want := anyenc.Tuple(nil).Append(a.NewArray())
		assert.Equal(t, []byte(want), key("a", `{"a":[]}`))
	})
	t.Run("missing field stays null", func(t *testing.T) {
		assert.Equal(t, key("a", `{"a":null}`), key("a", `{"b":1}`))
	})
	t.Run("multi-field key picks per-field extrema independently", func(t *testing.T) {
		s := MustParseSort("a", "-b")
		got := s.AppendKey(nil, anyenc.MustParseJson(`{"a":[5,1],"b":[5,1]}`))
		want := s.AppendKey(nil, anyenc.MustParseJson(`{"a":1,"b":5}`))
		require.NotEmpty(t, got)
		assert.Equal(t, want, got)
	})
}
