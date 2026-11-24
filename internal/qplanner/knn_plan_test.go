package qplanner

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// VectorScoreIter keeps its K nearest ids under the same 32-bit offsets.
func TestVectorScoreIter_RankedArenaLimit(t *testing.T) {
	a := &anyenc.Arena{}
	var hits []fakeHit
	for i := 0; i < 6; i++ {
		o := a.NewObject()
		o.Set("d", a.NewNumberInt(10-i)) // later ids are nearer
		id := fmt.Sprintf("id-%d", i)
		hits = append(hits, fakeHit{key: []byte(id), docId: []byte(id), doc: o})
	}
	const idLen = 4
	run := func(t *testing.T, k int) ([]string, error) {
		plan := &Plan{}
		it := &VectorScoreIter{
			Spec: &VectorQuerySpec{K: k, DistFromDoc: func(doc *anyenc.Value) (float32, bool) {
				return float32(doc.GetFloat64("d")), true
			}},
			Source: &fakeIter{plan: plan, hits: hits},
			Plan:   plan,
		}
		defer it.Close()
		if err := it.init(); err != nil {
			return nil, err
		}
		var got []string
		for _, c := range it.kept {
			got = append(got, string(it.arena[c.off:c.off+c.ln]))
		}
		return got, nil
	}

	t.Run("bounded: evictions pack the arena at the limit", func(t *testing.T) {
		setSortArenaLimit(t, 3*idLen)
		got, err := run(t, 3)
		require.NoError(t, err)
		assert.Equal(t, []string{"id-5", "id-4", "id-3"}, got)
	})
	t.Run("bounded: kept ids past the limit", func(t *testing.T) {
		setSortArenaLimit(t, 3*idLen-1)
		_, err := run(t, 3)
		require.ErrorIs(t, err, errRankedTooLarge)
	})
	t.Run("unbounded: kept ids past the limit", func(t *testing.T) {
		setSortArenaLimit(t, 6*idLen-1)
		_, err := run(t, 0)
		require.ErrorIs(t, err, errRankedTooLarge)
	})
}
