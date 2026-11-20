package qplanner

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// scoreProber scores a docId by the table it holds; an id it does not hold
// does not match.
type scoreProber map[string]float64

func (p scoreProber) Probe(docId []byte) (float64, uint64, bool, error) {
	s, ok := p[string(docId)]
	return s, uint64(len(docId)), ok, nil
}

func (p scoreProber) Close() {}

// rankIds runs the rank form of FtsProbeIter over ids and returns the emitted
// ids and the first error.
func rankIds(t *testing.T, topK int, scores scoreProber, ids ...string) ([]string, error) {
	t.Helper()
	hits := make([]fakeHit, len(ids))
	for i, id := range ids {
		hits[i] = fakeHit{key: []byte(id), docId: []byte(id)}
	}
	it := &FtsProbeIter{
		Spec:   &FtsQuerySpec{Probe: func(*btree.ReadTx) (FtsProber, error) { return scores, nil }},
		Source: &fakeIter{hits: hits},
		Rank:   true,
		TopK:   topK,
	}
	defer it.Close()
	var got []string
	for {
		_, docId, _, err := it.Next()
		if err != nil {
			return got, err
		}
		if docId == nil {
			return got, nil
		}
		got = append(got, string(docId))
	}
}

// The rank form keeps the matched ids in an arena its entries address with
// 32-bit offsets. Kept ids past that are an error, never a wrapped offset; a
// bounded heap packs its kept ids, the evicted one's bytes dead, before it
// decides.
func TestFtsProbeIter_RankedArenaLimit(t *testing.T) {
	scores := scoreProber{}
	var ids []string
	for i := 0; i < 6; i++ {
		id := fmt.Sprintf("id-%d", i)
		ids = append(ids, id)
		scores[id] = float64(i) // later ids rank higher
	}
	const idLen = 4

	t.Run("unbounded: kept ids at and past the limit", func(t *testing.T) {
		setSortArenaLimit(t, 6*idLen)
		got, err := rankIds(t, 0, scores, ids...)
		require.NoError(t, err)
		assert.Len(t, got, 6)
		setSortArenaLimit(t, 6*idLen-1)
		_, err = rankIds(t, 0, scores, ids...)
		require.ErrorIs(t, err, errRankedTooLarge)
	})
	t.Run("bounded: evictions pack the arena at the limit", func(t *testing.T) {
		// Every id after the third evicts the root: the arena would grow
		// to 6 ids without packing, and holds 3 after it.
		setSortArenaLimit(t, 3*idLen)
		got, err := rankIds(t, 3, scores, ids...)
		require.NoError(t, err)
		assert.Equal(t, []string{"id-5", "id-4", "id-3"}, got)
	})
	t.Run("bounded: kept ids past the limit", func(t *testing.T) {
		setSortArenaLimit(t, 3*idLen-1)
		_, err := rankIds(t, 3, scores, ids...)
		require.ErrorIs(t, err, errRankedTooLarge)
	})
}
