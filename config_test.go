//go:build (linux || darwin) && (amd64 || arm64)

package anystore

// The global page buffer is configured once per process, so each
// configuration under test runs in its own process (re-exec).

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

const (
	// pageBufferChildEnv carries "pageSize,nPages,id" to the child.
	pageBufferChildEnv = "ANYSTORE_TEST_PAGE_BUFFER_CHILD"
	// pageBufferPathEnv carries the database path, which may hold a comma.
	pageBufferPathEnv = "ANYSTORE_TEST_PAGE_BUFFER_PATH"
)

// A page buffer pool of the wrong page size, or with no pages, must never
// hand a database a wrong-sized page: a healthy database opened with
// UseGlobalPageBuffer reads, writes and passes its checks whatever
// InitPageBuffer was given, as SQLite's page cache does with a
// SQLITE_CONFIG_PAGECACHE buffer whose slots do not fit.
func TestInitPageBuffer_MisSizedPoolIsNotUsed(t *testing.T) {
	ctx := context.Background()
	if spec := os.Getenv(pageBufferChildEnv); spec != "" {
		pageBufferChild(t, ctx, spec, os.Getenv(pageBufferPathEnv))
		return
	}

	// A comma in the path must reach the child intact.
	dir := filepath.Join(t.TempDir(), "a,b")
	require.NoError(t, os.Mkdir(dir, 0o755))
	path := filepath.Join(dir, "h.db")
	db, err := Open(ctx, path, nil)
	require.NoError(t, err)
	coll, err := db.CreateCollection(ctx, "c")
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":"%s"}`, i, strings.Repeat("x", 100)))))
	}
	require.NoError(t, db.Close())

	configs := [][2]int{
		{4096, 64}, // the one configuration that uses the pool
		{1024, 64},
		{2048, 64},
		{8192, 64},
		{65536, 4},
		{1000, 64}, // not a page size at all
		{256, 64},  // below SQLite's 512
		{0, 64},
		{-4096, 64},
		{4096, 0},
		{4096, -5},
		{0, 0},
		{math.MaxInt, 2}, // the pool's size overflows an int
		{math.MaxInt, 1}, // a slot past the largest page size
	}
	for i, args := range configs {
		t.Run(fmt.Sprintf("%d_%d", args[0], args[1]), func(t *testing.T) {
			spec := fmt.Sprintf("%d,%d,%d", args[0], args[1], 1000+i)
			cmd := exec.Command(os.Args[0], "-test.run=^TestInitPageBuffer_MisSizedPoolIsNotUsed$", "-test.timeout=60s")
			cmd.Env = append(os.Environ(), pageBufferChildEnv+"="+spec, pageBufferPathEnv+"="+path)
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, "child failed:\n%s", out)
		})
	}

	// Every child added one document in each of its two passes and left the
	// file healthy.
	db, err = Open(ctx, path, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	require.NoError(t, db.QuickCheck(ctx))
	coll, err = db.Collection(ctx, "c")
	require.NoError(t, err)
	n, err := coll.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 200+2*len(configs), n)
}

// pageBufferChild configures the pool as spec says ("pageSize,nPages,id")
// and uses the database at path, and a new one, through it. The databases
// are opened right after that first call, so a first call the wrapper
// ignored fails the open (ErrPageSlabNotInitialized) instead of being made
// good by a later call; the later calls, with other arguments, come after
// and must change nothing.
func pageBufferChild(t *testing.T, ctx context.Context, spec, path string) {
	parts := strings.Split(spec, ",")
	require.Len(t, parts, 3, spec)
	pageSize, err := strconv.Atoi(parts[0])
	require.NoError(t, err)
	nPages, err := strconv.Atoi(parts[1])
	require.NoError(t, err)
	id, err := strconv.Atoi(parts[2])
	require.NoError(t, err)
	require.NotEmpty(t, path)

	InitPageBuffer(pageSize, nPages)
	pageBufferChildUse(t, ctx, path, id, 0)

	// The first call wins: a second one with other arguments changes nothing
	// and does not fail.
	InitPageBuffer(8192, 3)
	InitPageBuffer(4096, 128)
	pageBufferChildUse(t, ctx, path, id, 1)
}

// pageBufferChildUse adds one document to the database at path and fills a
// new one, both with UseGlobalPageBuffer; round tells the two passes apart.
func pageBufferChildUse(t *testing.T, ctx context.Context, path string, id, round int) {
	conf := &Config{UseGlobalPageBuffer: true}
	db, err := Open(ctx, path, conf)
	require.NoError(t, err)
	coll, err := db.Collection(ctx, "c")
	require.NoError(t, err)
	n, err := coll.Find(`{"id":{"$lt":200}}`).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 200, n)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, id*10+round))))
	require.NoError(t, db.QuickCheck(ctx))
	require.NoError(t, db.Close())

	// A database created under the same configuration.
	fresh, err := Open(ctx, filepath.Join(filepath.Dir(path), fmt.Sprintf("fresh-%d-%d.db", id, round)), conf)
	require.NoError(t, err)
	fc, err := fresh.CreateCollection(ctx, "c")
	require.NoError(t, err)
	for i := 0; i < 300; i++ {
		require.NoError(t, fc.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":"%s"}`, i, strings.Repeat("y", 200)))))
	}
	n, err = fc.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 300, n)
	require.NoError(t, fresh.QuickCheck(ctx))
	require.NoError(t, fresh.Close())
}

// An unknown Durability.FlushMode fails Open with ErrInvalidConfig, AutoFlush
// on or off, before anything is created or registered: it used to panic in
// Open and leave the path registered as open, refusing every later Open of it
// in the process.
func TestOpen_InvalidFlushMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	for _, autoFlush := range []bool{true, false} {
		d, err := Open(ctx, path, &Config{Durability: DurabilityConfig{AutoFlush: autoFlush, FlushMode: "CHECKPOINT_SOMETIMES"}})
		require.ErrorIs(t, err, ErrInvalidConfig)
		require.ErrorContains(t, err, "Durability.FlushMode")
		require.Nil(t, d)
	}
	_, statErr := os.Stat(path)
	require.ErrorIs(t, statErr, os.ErrNotExist, "a refused Open creates no file")

	for _, mode := range []FlushMode{"", FlushModeCheckpointPassive, FlushModeCheckpointFull, FlushModeCheckpointRestart, FlushModeCheckpointTruncate} {
		d, err := Open(ctx, path, &Config{Durability: DurabilityConfig{AutoFlush: true, FlushMode: mode}})
		require.NoError(t, err, "flush mode %q", mode)
		require.NoError(t, d.Close())
	}
}

// A CacheSize or ReadConcurrency past what Open can allocate is taken as its
// bound; zero and negative values take the defaults.
func TestConfig_CacheSizes(t *testing.T) {
	cs, rc := (&Config{}).cacheSizes()
	require.Equal(t, 5000, cs)
	require.Equal(t, max(runtime.NumCPU()-1, 4), rc)

	cs, rc = (&Config{CacheSize: -1, ReadConcurrency: -1}).cacheSizes()
	require.Equal(t, 5000, cs)
	require.Equal(t, max(runtime.NumCPU()-1, 4), rc)

	cs, rc = (&Config{CacheSize: 7, ReadConcurrency: 3}).cacheSizes()
	require.Equal(t, 7, cs)
	require.Equal(t, 3, rc)

	cs, rc = (&Config{CacheSize: maxCacheSize, ReadConcurrency: maxReadConcurrency}).cacheSizes()
	require.Equal(t, maxCacheSize, cs)
	require.Equal(t, maxReadConcurrency, rc)

	cs, rc = (&Config{CacheSize: math.MaxInt, ReadConcurrency: math.MaxInt}).cacheSizes()
	require.Equal(t, maxCacheSize, cs)
	require.Equal(t, maxReadConcurrency, rc)
}

// Open with the largest CacheSize and ReadConcurrency returns promptly and the
// database works: the cache size used to hang Open, and the reader count to
// panic in it, leaving the path registered as open.
func TestOpen_HugeCacheSizeAndReadConcurrency(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	type opened struct {
		d   DB
		err error
	}
	done := make(chan opened, 1)
	go func() {
		d, err := Open(ctx, path, &Config{CacheSize: math.MaxInt, ReadConcurrency: math.MaxInt})
		done <- opened{d, err}
	}()
	var o opened
	select {
	case o = <-done:
	case <-time.After(time.Minute):
		t.Fatal("Open with the largest CacheSize and ReadConcurrency did not return")
	}
	require.NoError(t, o.err)
	coll, err := o.d.Collection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))
	n, err := coll.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoError(t, o.d.Close())

	d, err := Open(ctx, path, nil)
	require.NoError(t, err)
	require.NoError(t, d.Close())
}
