package vindex

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// A vector read that fails returns the read error, whatever the record
// format: a quantized record that could not be read is not a record of
// length zero.
func TestReadVec_ReadError(t *testing.T) {
	for _, quant := range []Quantization{QuantNone, QuantInt8} {
		t.Run(quant.String(), func(t *testing.T) {
			const dim = 8
			db, err := btree.Open(":memory:", btree.Options{InMemory: true})
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			wtx, err := db.BeginWrite()
			require.NoError(t, err)
			ix, err := Create(wtx, "vix", Params{Dim: dim, Metric: L2, EfSearch: 64, Quantization: quant}, 1)
			require.NoError(t, err)
			require.NoError(t, wtx.Commit())
			vecs := randVecs(20, dim, 1)
			insertAll(t, db, ix, vecs)

			rtx, err := db.BeginRead()
			require.NoError(t, err)
			s := ix.getSearcher(rtx, vecs[0])
			defer ix.putSearcher(s)
			_, err = s.readVec(0)
			require.NoError(t, err)
			_, err = s.readVec2(0)
			require.NoError(t, err)

			// The transaction ends under the searcher: every later read fails.
			require.NoError(t, rtx.Rollback())
			_, err = s.readVec(0)
			require.ErrorIs(t, err, btree.ErrTxClosed)
			_, err = s.readVec2(0)
			require.ErrorIs(t, err, btree.ErrTxClosed)
		})
	}
}
