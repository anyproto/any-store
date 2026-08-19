package anystore

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
)

// End-to-end coverage for strings containing NUL bytes (newly representable —
// the old encoder silently stripped them): document round-trip, primary-key
// lookups, and exact index bounds on ascending and descending indexes where
// the encoding of "a" is a byte-prefix of the encoding of "a\x00b".
func TestNulStrings(t *testing.T) {
	t.Run("doc round trip and FindId", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		a := &anyenc.Arena{}
		doc := a.NewObject()
		doc.Set("id", a.NewString("id\x00x"))
		doc.Set("va\x00l", a.NewString("data\x00data"))
		require.NoError(t, coll.Insert(ctx, doc))

		got, err := coll.FindId(ctx, "id\x00x")
		require.NoError(t, err)
		assert.Equal(t, "data\x00data", got.Value().GetString("va\x00l"))

		_, err = coll.FindId(ctx, "idx")
		assert.ErrorIs(t, err, ErrDocNotFound)
	})

	for _, field := range []string{"name", "-name"} {
		t.Run("comparison ops "+field, func(t *testing.T) {
			fx := newFixture(t)
			coll, err := fx.CreateCollection(ctx, "test")
			require.NoError(t, err)
			require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{field}}))
			names := []string{"a", "a\x00b", "a\x00", "ab", "b", "\x00"}
			for i, name := range names {
				a := &anyenc.Arena{}
				doc := a.NewObject()
				doc.Set("id", a.NewNumberInt(i))
				doc.Set("name", a.NewString(name))
				require.NoError(t, coll.Insert(ctx, doc))
			}
			hint := IndexHint{IndexName: strings.TrimPrefix(field, "-"), Boost: 100}
			expected := map[query.CompOp][]string{
				query.CompOpEq:  {"a"},
				query.CompOpGt:  {"a\x00", "a\x00b", "ab", "b"},
				query.CompOpGte: {"a", "a\x00", "a\x00b", "ab", "b"},
				query.CompOpLt:  {"\x00"},
				query.CompOpLte: {"\x00", "a"},
			}
			for op, want := range expected {
				q := query.Key{Path: []string{"name"}, Filter: query.NewComp(op, "a")}
				it, err := coll.Find(q).IndexHint(hint).Iter(ctx)
				require.NoError(t, err)
				var got []string
				for it.Next() {
					d, derr := it.Doc()
					require.NoError(t, derr)
					got = append(got, d.Value().GetString("name"))
				}
				require.NoError(t, it.Close())
				slices.Sort(got)
				assert.Equal(t, want, got, "find op=%d (%s)", op, field)

				cnt, err := coll.Find(q).IndexHint(hint).Count(ctx)
				require.NoError(t, err)
				assert.Equal(t, len(want), cnt, "count op=%d (%s)", op, field)
			}
		})

		t.Run("unique index "+field, func(t *testing.T) {
			fx := newFixture(t)
			coll, err := fx.CreateCollection(ctx, "test")
			require.NoError(t, err)
			require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{field}, Unique: true}))

			a := &anyenc.Arena{}
			doc := a.NewObject()
			doc.Set("id", a.NewNumberInt(1))
			doc.Set("name", a.NewString("a\x00b"))
			require.NoError(t, coll.Insert(ctx, doc))

			// unique point lookup for the absent prefix value must not match
			// the "a\x00b" entry (CoverIter exact-prefix check)
			hint := IndexHint{IndexName: strings.TrimPrefix(field, "-"), Boost: 100}
			q := query.Key{Path: []string{"name"}, Filter: query.NewComp(query.CompOpEq, "a")}
			cnt, err := coll.Find(q).IndexHint(hint).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, 0, cnt)

			// inserting "a" must NOT trip the unique constraint against "a\x00b"
			doc2 := a.NewObject()
			doc2.Set("id", a.NewNumberInt(2))
			doc2.Set("name", a.NewString("a"))
			require.NoError(t, coll.Insert(ctx, doc2))

			// but a real duplicate must — both the NUL value and the plain
			// value whose duplicate probe could be shadowed by the NUL entry
			doc3 := a.NewObject()
			doc3.Set("id", a.NewNumberInt(3))
			doc3.Set("name", a.NewString("a\x00b"))
			assert.ErrorIs(t, coll.Insert(ctx, doc3), ErrUniqueConstraint)
			doc4 := a.NewObject()
			doc4.Set("id", a.NewNumberInt(4))
			doc4.Set("name", a.NewString("a"))
			assert.ErrorIs(t, coll.Insert(ctx, doc4), ErrUniqueConstraint)

			cnt, err = coll.Find(q).IndexHint(hint).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, 1, cnt)
		})

		t.Run("index bounds "+field, func(t *testing.T) {
			fx := newFixture(t)
			coll, err := fx.CreateCollection(ctx, "test")
			require.NoError(t, err)
			require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{field}}))

			names := []string{"a", "a\x00b", "a\x00", "ab", "b", "\x00"}
			for i, name := range names {
				a := &anyenc.Arena{}
				doc := a.NewObject()
				doc.Set("id", a.NewNumberInt(i))
				doc.Set("name", a.NewString(name))
				require.NoError(t, coll.Insert(ctx, doc))
			}

			for _, name := range names {
				q := query.Key{Path: []string{"name"}, Filter: query.NewComp(query.CompOpEq, name)}

				count, err := coll.Find(q).Count(ctx)
				require.NoError(t, err, "count eq %q", name)
				assert.Equal(t, 1, count, "count eq %q (%s)", name, field)

				iter, err := coll.Find(q).Iter(ctx)
				require.NoError(t, err)
				var got []string
				for iter.Next() {
					d, derr := iter.Doc()
					require.NoError(t, derr)
					got = append(got, d.Value().GetString("name"))
				}
				require.NoError(t, iter.Close())
				assert.Equal(t, []string{name}, got, "find eq %q (%s)", name, field)
			}

			// range scan must see every doc exactly once, in field order
			it, err := coll.Find(nil).Sort(field).Iter(ctx)
			require.NoError(t, err)
			var got []string
			for it.Next() {
				d, derr := it.Doc()
				require.NoError(t, derr)
				got = append(got, d.Value().GetString("name"))
			}
			require.NoError(t, it.Close())
			assert.Len(t, got, len(names), field)
			sorted := slices.Clone(names)
			slices.Sort(sorted)
			if field == "-name" {
				slices.Reverse(sorted)
			}
			assert.Equal(t, sorted, got, field)
		})
	}
}
