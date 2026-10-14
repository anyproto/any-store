package anystore

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/fts"
)

func TestFtsDocTable(t *testing.T) {
	tab := ftsGetDocTable()
	defer ftsPutDocTable(tab)

	// Insertion order defines the slots; a repeat returns the existing slot.
	for i := uint64(0); i < 5000; i++ {
		slot, inserted := tab.insert(i * 7)
		assert.True(t, inserted)
		assert.Equal(t, int(i), slot)
	}
	slot, inserted := tab.insert(21)
	assert.False(t, inserted)
	assert.Equal(t, 3, slot)
	assert.Equal(t, 5000, tab.len())
	for i := uint64(0); i < 5000; i++ {
		s, ok := tab.lookup(i * 7)
		require.True(t, ok, i)
		assert.Equal(t, int(i), s)
		assert.Equal(t, i*7, tab.ids[s])
	}
	_, ok := tab.lookup(1)
	assert.False(t, ok)

	// reset forgets everything in O(1) and keeps the (grown) arrays.
	tab.reset()
	assert.Equal(t, 0, tab.len())
	_, ok = tab.lookup(21)
	assert.False(t, ok)
	slot, inserted = tab.insert(21)
	assert.True(t, inserted)
	assert.Equal(t, 0, slot)
}

func TestFtsDocTable_GenerationWrap(t *testing.T) {
	tab := ftsGetDocTable()
	defer ftsPutDocTable(tab)
	tab.insert(5)
	tab.gen = ^uint32(0) // next reset wraps
	tab.reset()
	_, ok := tab.lookup(5)
	assert.False(t, ok, "a stale tag must not survive the wrap")
}

func TestCountCommonAndSearch(t *testing.T) {
	assert.Equal(t, uint32(0), countCommon(nil, []uint32{1}))
	assert.Equal(t, uint32(2), countCommon([]uint32{1, 3, 5, 9}, []uint32{3, 4, 9}))
	assert.Equal(t, uint32(1), countCommon([]uint32{7}, []uint32{1, 7}))
	i, ok := searchU32([]uint32{2, 4, 8}, 4)
	assert.True(t, ok)
	assert.Equal(t, 1, i)
	i, ok = searchU32([]uint32{2, 4, 8}, 5)
	assert.False(t, ok)
	assert.Equal(t, 2, i)
	_, ok = searchU32(nil, 0)
	assert.False(t, ok)
}

// TestFtsPositionsOf_FieldPartition pins the field attribution of positions:
// a document's positions are laid out field by field and the per-field TFs
// come in the same order, so the scope filter cuts the right runs without
// any stored field offsets.
func TestFtsPositionsOf_FieldPartition(t *testing.T) {
	fx, c := ftsTestColl(t, "f0", "f1", "f2")
	defer fx.finish()
	insertJSON(t, c, `{"id":"d","f0":"x a","f1":"a a b","f2":"b a"}`)
	// Positions: f0 → 0,1; f1 starts at 1+1+gap = 102 → 102,103,104; f2 starts
	// at 104+1+gap = 205 → 205,206. Term "a" sits at 1 | 102,103 | 206.
	fxi := c.loadFtsIndexes()[0]
	want := map[string][]uint32{
		"":      {1, 102, 103, 206},
		"f1":    {102, 103},
		"f0,f2": {1, 206},
		"f2":    {206},
	}
	require.NoError(t, c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		for scope, exp := range want {
			var fields []string
			if scope != "" {
				fields = splitComma(scope)
			}
			mask, scoped, err := fxi.resolveScope(fields)
			require.NoError(t, err)
			e := ftsExec{tx: tx, fx: fxi, nFields: fxi.nFields, scope: mask, scoped: scoped}
			var got []uint32
			var tf float64
			require.NoError(t, e.walkTerm("a", func(it fts.ChunkIterator, _ uint64) error {
				got = e.positionsOf(it, got[:0])
				tf, _ = e.postingTF(it)
				return nil
			}))
			assert.Equal(t, exp, got, "scope %q", scope)
			assert.Equal(t, float64(len(exp)), tf, "scope %q", scope)
		}
		return nil
	}))
	_, _, err := fxi.resolveScope([]string{"nope"})
	assert.ErrorIs(t, err, ErrFulltextUnknownField)
}

func splitComma(s string) (out []string) {
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return out
}

// TestFtsPrefix_CorruptSeqIsAnError pins that an absurd fts:meta seq (which
// would size the bit set in gigabytes) fails the query instead of allocating.
func TestFtsPrefix_CorruptSeqIsAnError(t *testing.T) {
	fx, c := ftsTestColl(t, "name", "body")
	defer fx.finish()
	insertJSON(t, c, `{"id":"a","name":"zebra","body":"x"}`)
	fxi := c.loadFtsIndexes()[0]
	require.NoError(t, c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return fxi.putMetaUint(tx, ftsMetaSeq, 1<<58)
	}))
	for _, q := range []string{`{"$text":{"$search":"zebra"}}`, `{"$text":{"$search":"zeb*"}}`} {
		ids, _ := collectIter(t, c.Find(q))
		assert.Equal(t, []string{"a"}, ids, "%s never reads the sequence", q)
	}
	// A scoped prefix and a phrase-prefix count documents in the bit set.
	for _, q := range []string{`{"$text":{"$search":"zeb*","$fields":"name"}}`, `{"$text":{"$search":"^zeb*"}}`, `{"$text":{"$search":"\"x zeb\"*"}}`} {
		_, err := c.Find(q).Count(ctx)
		assert.ErrorIs(t, err, errFtsCorruptMeta, q)
		it, err := c.Find(q).Iter(ctx)
		if err == nil {
			assert.False(t, it.Next())
			assert.ErrorIs(t, errors.Join(it.Err(), it.Close()), errFtsCorruptMeta, q)
		}
	}
}

func TestFtsDocBits(t *testing.T) {
	b := ftsGetDocBits(100)
	defer ftsPutDocBits(b)
	b.set(3)
	b.set(3)
	b.set(100)
	b.set(5000) // beyond the sizing bound: the index grows
	b.set(ftsMaxDocID + 1)
	assert.Equal(t, uint64(3), b.count())
	assert.Len(t, b.arena, 2*ftsBitsWords, "two blocks touched")
	b.reset(10)
	assert.Equal(t, uint64(0), b.count())
	assert.Empty(t, b.arena)
	b.set(3)
	assert.Equal(t, uint64(1), b.count())
	// Cost follows the touched blocks, not the sequence: a narrow set over a
	// huge id range lays out one block.
	b.reset(1 << 30)
	b.set(1 << 29)
	b.set(1<<29 + 1)
	assert.Equal(t, uint64(2), b.count())
	assert.Len(t, b.arena, ftsBitsWords)
	assert.Len(t, b.index, 1<<30/ftsBitsBlock+1)
}
