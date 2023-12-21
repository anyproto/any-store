package iterator

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/internal/key"
	"github.com/anyproto/any-store/internal/testdb"
)

func TestInIterator_Next(t *testing.T) {
	fx := testdb.NewFixture(t)
	defer fx.Finish(t)

	ns := key.NewNS("/ns/test")
	ti := &testIterator{
		ns: ns,
		keys: []key.Key{
			ns.GetKey().AppendAny(1).AppendAny(1),
			ns.GetKey().AppendAny(2).AppendAny(2),
			ns.GetKey().AppendAny(3).AppendAny(3),
			ns.GetKey().AppendAny(4).AppendAny(4),
			ns.GetKey().AppendAny(5).AppendAny(5),
		},
	}

	inNs := key.NewNS("/ns/in")

	fillNs(t, fx.DB,
		inNs.GetKey().AppendAny(3).AppendAny(1),
		inNs.GetKey().AppendAny(4).AppendAny(4),
		inNs.GetKey().AppendAny(2).AppendAny(2),
	)

	require.NoError(t, fx.DB.View(func(txn *badger.Txn) error {
		in := &InIterator{
			Txn: txn,
			Keys: []key.Key{
				inNs.GetKey().AppendAny(3),
				inNs.GetKey().AppendAny(4),
				inNs.GetKey().AppendAny(2),
			},
			IdIterator: ti,
		}

		var expected = [][]any{
			{float64(1), float64(1)},
			{float64(2), float64(2)},
			{float64(4), float64(4)},
		}

		var result [][]any

		for in.Next() {
			result = append(result, toAnyVals(t, in.Values()))
		}

		assert.Equal(t, expected, result)

		return in.Close()
	}))

}
