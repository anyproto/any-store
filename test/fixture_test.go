package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	anystore "github.com/anyproto/any-store"
)

var ctx = context.Background()

func newFixture(t testing.TB, c ...*anystore.Config) *fixture {
	var conf *anystore.Config
	if len(c) != 0 {
		conf = c[0]
	}
	if conf == nil {
		conf = &anystore.Config{}
	}
	conf.InMemory = true

	db, err := anystore.Open(ctx, ":memory:", conf)
	require.NoError(t, err)

	fx := &fixture{
		DB: db,
		t:  t,
	}
	t.Cleanup(fx.finish)
	return fx
}

type fixture struct {
	anystore.DB
	t testing.TB
}

func (fx *fixture) finish() {
	require.NoError(fx.t, fx.Close())
}
