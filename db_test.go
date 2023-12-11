package anystore

import (
	"os"

	"github.com/stretchr/testify/require"
)

func newFixture(t require.TestingT) *fixture {
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	db, err := Open(dir)
	require.NoError(t, err)
	return &fixture{dir: dir, DB: db, t: t}
}

type fixture struct {
	*DB
	t   require.TestingT
	dir string
}

func (fx *fixture) finish() {
	require.NoError(fx.t, fx.Close())
	_ = os.RemoveAll(fx.dir)
}
