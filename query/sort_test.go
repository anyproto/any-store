package query

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
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
