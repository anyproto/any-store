package test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	anystore "github.com/anyproto/any-store"
)

var ctx = context.Background()

func newFixture(t testing.TB, c ...*anystore.Config) *fixture {
	tmpDir, err := os.MkdirTemp("", "any-store-*")
	require.NoError(t, err)

	var conf *anystore.Config
	if len(c) != 0 {
		conf = c[0]
	}

	db, err := anystore.Open(ctx, filepath.Join(tmpDir, "any-store-test.db"), conf)
	require.NoError(t, err)

	fx := &fixture{
		DB:     db,
		tmpDir: tmpDir,
		t:      t,
	}
	t.Cleanup(fx.finish)
	return fx
}

type fixture struct {
	anystore.DB
	tmpDir string
	t      testing.TB
}

func (fx *fixture) finish() {
	require.NoError(fx.t, fx.Close())
	if fx.tmpDir != "" {
		if true {
			_ = os.RemoveAll(fx.tmpDir)
		} else {
			fx.t.Log(fx.tmpDir)
		}
	}
}
