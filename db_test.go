package anystore

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/driver"
	"github.com/anyproto/any-store/internal/objectid"
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

func TestDb_QuickCheck(t *testing.T) {
	fx := newFixture(t)
	assert.NoError(t, fx.QuickCheck(ctx))
}

func TestDb_Checkpoint(t *testing.T) {
	fx := newFixture(t)
	assert.NoError(t, fx.Checkpoint(ctx, false))
	assert.NoError(t, fx.Checkpoint(ctx, true))
}

func TestDb_Backup(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.Collection(ctx, "coll")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"doc"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "doc":"a"}`), anyenc.MustParseJson(`{"id":2, "doc":"b"}`)))

	tmpDir, err := os.MkdirTemp("", "any-store-backup-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	require.NoError(t, fx.Backup(ctx, filepath.Join(tmpDir, "any-store-test.db")))

	fx2 := newFixturePath(t, tmpDir)
	coll2, err := fx2.Collection(ctx, "coll")
	require.NoError(t, err)
	assert.Len(t, coll2.GetIndexes(), 1)
	assertCollCount(t, coll2, 2)
}

func TestDb_Close(t *testing.T) {
	t.Run("race", func(t *testing.T) {
		fx := newFixture(t, &Config{ReadConnections: 2})

		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		var docs []*anyenc.Value
		for i := range 1000 {
			docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(`{"id": %d, "value": %d}`, i, rand.Int())))
		}
		require.NoError(t, coll.Insert(ctx, docs...))
		var results = make(chan error, 3)
		go func() {
			// writing
			for {
				if pErr := coll.UpsertOne(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id": %d, "value": %d}`, rand.Int(), rand.Int()))); pErr != nil {
					results <- errors.Join(pErr, errors.New("upsertOne"))
					return
				}
			}
		}()

		go func() {
			// iterating
			for {
				iter, pErr := coll.Find(nil).Iter(ctx)
				if pErr != nil {
					results <- errors.Join(pErr, errors.New("find"))
					return
				}
				for iter.Next() {
					if _, pErr = iter.Doc(); pErr != nil {
						results <- errors.Join(pErr, errors.New("doc"))
						return
					}
				}
				if pErr = iter.Close(); pErr != nil {
					results <- errors.Join(pErr, errors.New("close"))
					return
				}
			}
		}()

		go func() {
			// tx insert
			tx, tErr := coll.WriteTx(ctx)
			if tErr != nil {
				results <- errors.Join(tErr, errors.New("writeTx"))
				return
			}
			tErr = coll.Insert(tx.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id": "%s", "value": %d}`, objectid.NewObjectID().Hex(), rand.Int())))
			if tErr != nil {
				results <- errors.Join(tErr, errors.New("insert tx"))
				return
			}
			if tErr = tx.Commit(); tErr != nil {
				results <- errors.Join(tErr, errors.New("insert tx commit"))
				return
			}
		}()

		time.Sleep(time.Second / 2)

		require.NoError(t, fx.Close())

		for range len(results) {
			rErr := <-results
			assert.True(t, errors.Is(rErr, driver.ErrDBIsClosed) || errors.Is(rErr, driver.ErrStmtIsClosed), rErr.Error())
		}
		dirEntries, err := os.ReadDir(fx.tmpDir)
		require.NoError(t, err)
		for _, dirEntry := range dirEntries {
			if strings.HasSuffix(dirEntry.Name(), "-wal") {
				t.Errorf("wal file is not removed after close")
			}
		}
	})

}

func newFixture(t testing.TB, c ...*Config) *fixture {
	tmpDir, err := os.MkdirTemp("", "any-store-*")
	require.NoError(t, err)
	return newFixturePath(t, tmpDir, c...)
}

func newFixturePath(t testing.TB, tmpDir string, c ...*Config) *fixture {
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
	closeErr := fx.Close()
	if !errors.Is(closeErr, ErrDBIsClosed) {
		require.NoError(fx.t, closeErr)
	}
	if fx.tmpDir != "" {
		if true {
			_ = os.RemoveAll(fx.tmpDir)
		} else {
			fx.t.Log(fx.tmpDir)
		}
	}
}
