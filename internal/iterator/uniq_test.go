package iterator

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/anyproto/any-store/internal/key"
)

func TestUniqIterator_Next(t *testing.T) {
	ns := key.NewNS("/ns")
	ti := &testIterator{
		ns: ns,
		keys: []key.Key{
			ns.GetKey().AppendAny(1).AppendAny(1),
			ns.GetKey().AppendAny(2).AppendAny(1),
			ns.GetKey().AppendAny(2).AppendAny(2),
			ns.GetKey().AppendAny(3).AppendAny(1),
		},
	}

	uniq := &UniqIterator{
		uniq:       make(map[string]struct{}),
		IdIterator: ti,
	}

	var expected = [][]any{
		{float64(1), float64(1)},
		{float64(2), float64(2)},
	}
	var result [][]any
	for uniq.Next() {
		result = append(result, toAnyVals(t, uniq.Values()))
	}
	assert.Equal(t, expected, result)
}

type testIterator struct {
	ns   *key.NS
	keys []key.Key
	pos  int
	err  error
}

func (t *testIterator) Next() bool {
	ok := t.pos < len(t.keys)
	t.pos++
	return ok
}

func (t *testIterator) Valid() bool {
	return t.pos <= len(t.keys)
}

func (t *testIterator) Values() [][]byte {
	if !t.Valid() {
		return nil
	}
	k := t.keys[t.pos-1]
	var values [][]byte
	t.err = k.ReadByteValues(t.ns, func(b []byte) error {
		values = append(values, bytes.Clone(b))
		return nil
	})
	return values
}

func (t *testIterator) Close() error {
	return t.err
}
