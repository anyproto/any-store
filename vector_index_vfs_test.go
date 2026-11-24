//go:build vfs

package anystore

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

func faultVec(i, dim int) string {
	parts := make([]string, dim)
	for j := range parts {
		parts[j] = fmt.Sprintf("%g", float32((i*(j+3)+j*17)%251)/251)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

// A vector search, or an insert into a vector index, that fails to read a
// stored vector returns the read error: it neither ranks the candidate by
// another vector nor reports the record as malformed. For HNSW (int8 and
// float32) and IVF-SQ, a search returns an error that errors.Is matches to
// the injected read error, or the ranking of the stored vectors. An insert
// returns that error and leaves the collection and its index as they were,
// in this process and after a reopen, or succeeds with the new vector among
// the nearest to itself.
func TestReadFault_VectorIndex(t *testing.T) {
	const docs, dim = 400, 16
	knn := fmt.Sprintf(`{"v":{"$knn":{"$query":%s,"$k":10}}}`, faultVec(1234, dim))
	search := func(coll Collection) (ids []int, err error) {
		iter, err := coll.Find(knn).Iter(ctx)
		if err != nil {
			return nil, err
		}
		for iter.Next() {
			doc, err := iter.Doc()
			if err != nil {
				_ = iter.Close()
				return nil, err
			}
			ids = append(ids, doc.Value().GetInt("id"))
		}
		return ids, errors.Join(iter.Err(), iter.Close())
	}
	for _, tc := range []struct {
		name   string
		params VectorParams
	}{
		{"hnsw int8", VectorParams{Field: "v", Dim: dim, Metric: VectorL2, Quantization: VectorQuantInt8, EfSearch: 32}},
		{"hnsw float32", VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 32}},
		{"ivf", VectorParams{Field: "v", Dim: dim, Metric: VectorL2, Mode: VectorModeIVFSQ, NProbe: 8}},
	} {
		var want []int
		build := func(t *testing.T, db DB) {
			coll, err := db.CreateCollection(ctx, "vecs")
			require.NoError(t, err)
			// The documents first: the IVF mode trains on them.
			for i := 0; i < docs; i++ {
				require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"v":%s}`, i, faultVec(i, dim)))))
			}
			params := tc.params
			require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "v", Kind: IndexKindVector, Vector: &params}))
			want, err = search(coll)
			require.NoError(t, err)
			require.Len(t, want, 10)
		}
		t.Run(tc.name+" search", func(t *testing.T) {
			eachReadFault(t, build, func(t *testing.T, db DB, rf *readFaults) {
				coll, err := db.OpenCollection(ctx, "vecs")
				require.NoError(t, err)
				var got []int
				err = rf.during(func() (err error) {
					got, err = search(coll)
					return err
				})
				if err != nil {
					require.ErrorIs(t, err, errInjectedRead)
					return
				}
				assert.Equal(t, want, got, "the ranking is the one of the stored vectors (fault fired: %v)", rf.fired.Load())
			})
		})
		t.Run(tc.name+" insert", func(t *testing.T) {
			eachReadFault(t, build, func(t *testing.T, db DB, rf *readFaults) {
				coll, err := db.OpenCollection(ctx, "vecs")
				require.NoError(t, err)
				doc := fmt.Sprintf(`{"id":%d,"v":%s}`, docs, faultVec(1234, dim))
				err = rf.during(func() error { return coll.Insert(ctx, anyenc.MustParseJson(doc)) })
				if err != nil {
					require.ErrorIs(t, err, errInjectedRead)
					check := func(db DB) {
						coll, err := db.OpenCollection(ctx, "vecs")
						require.NoError(t, err)
						assertCollCount(t, coll, docs)
						got, err := search(coll)
						require.NoError(t, err)
						assert.Equal(t, want, got, "a failed insert left the index as it was")
					}
					check(db)
					check(reopened(t, db, rf))
					return
				}
				assertCollCount(t, coll, docs+1)
				got, err := search(coll)
				require.NoError(t, err)
				assert.Contains(t, got, docs, "the inserted vector is among the nearest to itself")
			})
		})
	}
}
