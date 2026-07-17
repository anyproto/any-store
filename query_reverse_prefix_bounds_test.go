package anystore

import (
	"context"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// End-to-end coverage for index bounds whose endpoints are mid-value PREFIXES
// of the anyenc encoding — $type's bare type tag, $regex's tag + string
// prefix — on DESCENDING indexes. Bitwise key inversion preserves the
// prefix-extension relation instead of reversing it, so such a bound survives
// only via the prefix-successor transform (qplanner.transformReverseBounds).
// Without it, every row whose first inverted continuation byte is 0xFF — a
// leading 0x00 payload byte: a 1970s ObjectID timestamp, a float <= -~9e307,
// the immediate EOS of an empty string, or a stored string equal to a $regex
// prefix (its EOS inverts to 0xFF) — silently fell outside the 0xFF-padded
// End on Count, Iter, Delete and Update, with err == nil.
//
// The ascending runs pin the forward behavior (exclusive next-tag End, see
// commit 192c239) that the reverse transform must not regress.

func trpbInsert(t *testing.T, coll Collection, id int, set func(a *anyenc.Arena, d *anyenc.Value)) {
	t.Helper()
	a := &anyenc.Arena{}
	doc := a.NewObject()
	doc.Set("id", a.NewNumberInt(id))
	set(a, doc)
	require.NoError(t, coll.Insert(ctx, doc))
}

func trpbFindIds(t *testing.T, ctx context.Context, coll Collection, hint IndexHint, cond string) []int {
	t.Helper()
	it, err := coll.Find(cond).IndexHint(hint).Iter(ctx)
	require.NoError(t, err)
	var got []int
	for it.Next() {
		d, derr := it.Doc()
		require.NoError(t, derr)
		got = append(got, d.Value().GetInt("id"))
	}
	require.NoError(t, it.Close())
	slices.Sort(got)

	cnt, err := coll.Find(cond).IndexHint(hint).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, len(got), cnt, "Count vs Iter divergence for %s", cond)
	return got
}

func TestReversePrefixBounds(t *testing.T) {
	zeroOid, err := anyenc.ObjectIDFromHex("000000000000000000000000")
	require.NoError(t, err)
	liveOid := anyenc.NewObjectID()

	for _, field := range []string{"v", "-v"} {
		for _, unique := range []bool{false, true} {
			name := field
			if unique {
				name += " unique"
			}
			newColl := func(t *testing.T) (Collection, IndexHint) {
				fx := newFixture(t)
				coll, err := fx.CreateCollection(ctx, "test")
				require.NoError(t, err)
				require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{field}, Unique: unique}))
				return coll, IndexHint{IndexName: strings.TrimPrefix(field, "-"), Boost: 100}
			}

			t.Run("type objectId "+name, func(t *testing.T) {
				coll, hint := newColl(t)
				trpbInsert(t, coll, 1, func(a *anyenc.Arena, d *anyenc.Value) { d.Set("v", a.NewObjectID(zeroOid)) })
				trpbInsert(t, coll, 2, func(a *anyenc.Arena, d *anyenc.Value) { d.Set("v", a.NewObjectID(liveOid)) })
				trpbInsert(t, coll, 3, func(a *anyenc.Arena, d *anyenc.Value) { d.Set("v", a.NewNumberInt(7)) })

				got := trpbFindIds(t, ctx, coll, hint, `{"v":{"$type":"objectId"}}`)
				assert.Equal(t, []int{1, 2}, got)

				res, err := coll.Find(`{"v":{"$type":"objectId"}}`).IndexHint(hint).Delete(ctx)
				require.NoError(t, err)
				assert.Equal(t, 2, res.Modified)
				cnt, err := coll.Find(nil).Count(ctx)
				require.NoError(t, err)
				assert.Equal(t, 1, cnt)
			})

			t.Run("type number "+name, func(t *testing.T) {
				coll, hint := newColl(t)
				nums := []float64{-math.MaxFloat64, -1e308, -1, 0, 1, math.MaxFloat64}
				for i, n := range nums {
					n := n
					trpbInsert(t, coll, i+1, func(a *anyenc.Arena, d *anyenc.Value) { d.Set("v", a.NewNumberFloat64(n)) })
				}
				trpbInsert(t, coll, 100, func(a *anyenc.Arena, d *anyenc.Value) { d.Set("v", a.NewString("s")) })

				got := trpbFindIds(t, ctx, coll, hint, `{"v":{"$type":"number"}}`)
				assert.Equal(t, []int{1, 2, 3, 4, 5, 6}, got)
			})

			t.Run("type string empty "+name, func(t *testing.T) {
				coll, hint := newColl(t)
				trpbInsert(t, coll, 1, func(a *anyenc.Arena, d *anyenc.Value) { d.Set("v", a.NewString("")) })
				trpbInsert(t, coll, 2, func(a *anyenc.Arena, d *anyenc.Value) { d.Set("v", a.NewString("x")) })
				trpbInsert(t, coll, 3, func(a *anyenc.Arena, d *anyenc.Value) { d.Set("v", a.NewNumberInt(7)) })

				got := trpbFindIds(t, ctx, coll, hint, `{"v":{"$type":"string"}}`)
				assert.Equal(t, []int{1, 2}, got)
			})

			t.Run("regex prefix "+name, func(t *testing.T) {
				coll, hint := newColl(t)
				// "foo" is one boundary row: its key continues with the
				// inverted EOS (0xFF) right after the prefix bytes on a
				// descending index. "foo\xff\xffz" is the other: a raw 0xFF
				// payload run right after the prefix (legal — only NUL is
				// escaped), which the old inclusive prefix+0xFF ascending
				// End also dropped.
				words := []string{"foo", "foobar", "fop", "fo", "other", "foo\xff\xffz"}
				for i, w := range words {
					w := w
					trpbInsert(t, coll, i+1, func(a *anyenc.Arena, d *anyenc.Value) { d.Set("v", a.NewString(w)) })
				}

				got := trpbFindIds(t, ctx, coll, hint, `{"v":{"$regex":"^foo"}}`)
				assert.Equal(t, []int{1, 2, 6}, got)
			})
		}
	}
}
