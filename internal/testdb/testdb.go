package testdb

import (
	"os"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func NewFixture(t *testing.T) *Fixture {
	tmpDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	db, err := badger.Open(badger.DefaultOptions(tmpDir).WithLoggingLevel(badger.WARNING))
	require.NoError(t, err)
	fx := &Fixture{
		tmpDir: tmpDir,
		DB:     db,
	}
	return fx
}

type Fixture struct {
	tmpDir string
	*badger.DB
}

func (fx *Fixture) Finish(t *testing.T) {
	require.NoError(t, fx.Close())
	_ = os.RemoveAll(fx.tmpDir)
}
