package anystore

import (
	"context"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	go http.ListenAndServe(":6066", nil)
}

var ctx = context.Background()

func TestDb_CreateCollection(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	assert.NotNil(t, coll)

	cNames, err := fx.GetCollectionNames(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"test"}, cNames)

	_, err = fx.CreateCollection(ctx, "test")
	assert.ErrorIs(t, err, ErrCollectionExists)

	require.NoError(t, coll.Close())

	_, err = fx.CreateCollection(ctx, "test")
	assert.ErrorIs(t, err, ErrCollectionExists)
}

func TestDb_OpenCollection(t *testing.T) {
	t.Run("err not found", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.OpenCollection(ctx, "test")
		require.Nil(t, coll)
		require.ErrorIs(t, err, ErrCollectionNotFound)
	})
	t.Run("success", func(t *testing.T) {
		fx := newFixture(t)
		_, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		coll, err := fx.OpenCollection(ctx, "test")
		require.NoError(t, err)
		assert.NotNil(t, coll)
	})
	t.Run("with indexes", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		indexInfo := IndexInfo{Fields: []string{"a", "-b"}, Sparse: true, Unique: true}
		require.NoError(t, coll.EnsureIndex(ctx, indexInfo))
		require.NoError(t, coll.Close())

		coll, err = fx.OpenCollection(ctx, "test")
		require.NoError(t, err)
		assert.NotNil(t, coll)
		indexes := coll.GetIndexes()
		assert.Len(t, indexes, 1)
	})
}

func TestDb_GetCollectionNames(t *testing.T) {
	fx := newFixture(t)
	var collNames = []string{"c1", "c2", "c3"}
	for _, collName := range collNames {
		coll, err := fx.CreateCollection(ctx, collName)
		require.NoError(t, err)
		require.NoError(t, coll.Close())
	}
	names, err := fx.GetCollectionNames(ctx)
	require.NoError(t, err)
	assert.Equal(t, collNames, names)
}

func TestDb_Stats(t *testing.T) {
	fx := newFixture(t)
	stats, err := fx.Stats(ctx)
	require.NoError(t, err)
	assert.Empty(t, 0, stats.IndexesCount)
	assert.Empty(t, 0, stats.CollectionsCount)
	assert.NotEmpty(t, stats.TotalSizeBytes)
	assert.NotEmpty(t, stats.DataSizeBytes)
}

func newFixture(t testing.TB, c ...*Config) *fixture {
	tmpDir, err := os.MkdirTemp("", "any-store-*")
	require.NoError(t, err)

	var conf *Config
	if len(c) != 0 {
		conf = c[0]
	}

	db, err := Open(ctx, filepath.Join(tmpDir, "any-store-test.db"), conf)
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
	DB
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
