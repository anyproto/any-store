package qplanner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
)

// TestCoverIter_Next covers all branches of CoverIter.Next:
// - empty-start bound → skip (continue)
// - AppendSeekKey error → skip (continue)
// - seek result doesn't have prefix → skip
// - happy path → return key + extracted docId
func TestCoverIter_Next(t *testing.T) {
	db, ns := coverageBtree(t, "cover_iter", []string{"alpha", "beta"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	cs := &CursorSource{Tx: rtx, Ns: ns}
	info := &IndexInfo{Name: "ci", FieldNames: []string{"id"}}

	t.Run("empty_start_skip", func(t *testing.T) {
		// First bound has empty Start → loop takes continue at line 26.
		// Second bound is a real hit.
		it := &CoverIter{
			Source:  cs,
			IdxInfo: info,
			Bounds: query.Bounds{
				{Start: nil, End: nil},
				{Start: anyenc.AppendAnyValue(nil, "alpha")},
			},
		}
		_, docId, _, err := it.Next()
		require.NoError(t, err)
		assert.NotNil(t, docId, "second bound must yield")
	})
	t.Run("prefix_mismatch_skip", func(t *testing.T) {
		// Start prefix doesn't match any key → HasPrefix false → skip.
		it := &CoverIter{
			Source:  cs,
			IdxInfo: info,
			Bounds: query.Bounds{
				{Start: anyenc.AppendAnyValue(nil, "zzz_nonexistent")},
			},
		}
		_, docId, _, err := it.Next()
		require.NoError(t, err)
		assert.Nil(t, docId, "missing prefix must not yield")
	})
}

// TestCoverIter_Next_PrefixMismatch covers cover_iter.go:35 — when
// AppendSeekKey succeeds but the returned key does NOT have the requested
// prefix (Seek lands on the next-greater key). Construction: db has "a"
// and "z"; Start="b". Seek("b") returns "z" which lacks prefix "b".
func TestCoverIter_Next_PrefixMismatch(t *testing.T) {
	db, ns := coverageBtree(t, "cover_prefix_miss", []string{"a", "z"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	startB := anyenc.AppendAnyValue(nil, "b")
	it := &CoverIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "ci", FieldNames: []string{"id"}},
		Bounds: query.Bounds{
			{Start: startB},
		},
	}
	_, docId, _, err := it.Next()
	require.NoError(t, err)
	assert.Nil(t, docId, "seek finds 'z' which lacks prefix 'b' → skip")
}

// TestCoverIter_MultiBound_MultiKey_SetsMultiKeyFlag pins the I-06 fix: a
// multi-bound CoverIter over an index that holds an array (0x06) entry yields
// multiKey=true (so DocDedup collapses cross-bound repeats); single-bound and
// pure-scalar multi-bound keep the zero-cost multiKey=false.
func TestCoverIter_MultiBound_MultiKey_SetsMultiKeyFlag(t *testing.T) {
	elemKey := func(s, doc string) []byte {
		return append(anyenc.AppendAnyValue(nil, s), []byte(doc)...)
	}
	arrayKey := func(json, doc string) []byte {
		return append(anyenc.MustParseJson(json).MarshalTo(nil), []byte(doc)...)
	}
	info := &IndexInfo{Name: "ci", FieldNames: []string{"x"}}
	twoBounds := query.Bounds{
		{Start: anyenc.AppendAnyValue(nil, "a")},
		{Start: anyenc.AppendAnyValue(nil, "b")},
	}

	t.Run("multi-bound + array data → multiKey true", func(t *testing.T) {
		db, ns := rawKeyBtree(t, []rawEntry{
			{elemKey("a", "d1"), IndexValueMultiKey},
			{elemKey("b", "d1"), IndexValueMultiKey},
			{arrayKey(`["a","b"]`, "d1"), IndexValueMultiKey}, // canonical 0x06 key
		})
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		defer func() { _ = rtx.Rollback() }()
		it := &CoverIter{Source: &CursorSource{Tx: rtx, Ns: ns}, IdxInfo: info, Bounds: twoBounds}
		_, docID, multiKey, err := it.Next()
		require.NoError(t, err)
		require.NotNil(t, docID)
		assert.True(t, multiKey, "array (0x06) entry present → probe true → multiKey=true")
	})

	t.Run("single-bound → multiKey false (no probe)", func(t *testing.T) {
		db, ns := rawKeyBtree(t, []rawEntry{
			{elemKey("a", "d1"), IndexValueMultiKey},
			{arrayKey(`["a","b"]`, "d1"), IndexValueMultiKey},
		})
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		defer func() { _ = rtx.Rollback() }()
		it := &CoverIter{
			Source:  &CursorSource{Tx: rtx, Ns: ns},
			IdxInfo: info,
			Bounds:  query.Bounds{{Start: anyenc.AppendAnyValue(nil, "a")}},
		}
		_, docID, multiKey, err := it.Next()
		require.NoError(t, err)
		require.NotNil(t, docID)
		assert.False(t, multiKey, "single bound can't straddle → probe skipped → multiKey=false")
	})

	t.Run("multi-bound pure-scalar → multiKey false", func(t *testing.T) {
		db, ns := rawKeyBtree(t, []rawEntry{
			{elemKey("a", "d1"), IndexValueScalar},
			{elemKey("b", "d2"), IndexValueScalar},
		})
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		defer func() { _ = rtx.Rollback() }()
		it := &CoverIter{Source: &CursorSource{Tx: rtx, Ns: ns}, IdxInfo: info, Bounds: twoBounds}
		_, docID, multiKey, err := it.Next()
		require.NoError(t, err)
		require.NotNil(t, docID)
		assert.False(t, multiKey, "pure-scalar index → probe false → multiKey=false")
	})
}
