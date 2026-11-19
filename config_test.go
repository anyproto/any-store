//go:build (linux || darwin) && (amd64 || arm64)

package anystore

// The global page buffer is configured once per process, so each
// configuration under test runs in its own process (re-exec).

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

const pageBufferChildEnv = "ANYSTORE_TEST_PAGE_BUFFER_CHILD"

// A page buffer pool of the wrong page size, or with no pages, must never
// hand a database a wrong-sized page: a healthy database opened with
// UseGlobalPageBuffer reads, writes and passes its checks whatever
// InitPageBuffer was given, as SQLite's page cache does with a
// SQLITE_CONFIG_PAGECACHE buffer whose slots do not fit.
func TestInitPageBuffer_MisSizedPoolIsNotUsed(t *testing.T) {
	ctx := context.Background()
	if spec := os.Getenv(pageBufferChildEnv); spec != "" {
		pageBufferChild(t, ctx, spec)
		return
	}

	path := filepath.Join(t.TempDir(), "h.db")
	db, err := Open(ctx, path, nil)
	require.NoError(t, err)
	coll, err := db.CreateCollection(ctx, "c")
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":"%s"}`, i, strings.Repeat("x", 100)))))
	}
	require.NoError(t, db.Close())

	for i, args := range [][2]int{
		{4096, 64}, // the one configuration that uses the pool
		{1024, 64},
		{2048, 64},
		{8192, 64},
		{65536, 4},
		{1000, 64}, // not a page size at all
		{0, 64},
		{-4096, 64},
		{4096, 0},
		{4096, -5},
		{0, 0},
	} {
		t.Run(fmt.Sprintf("%d_%d", args[0], args[1]), func(t *testing.T) {
			spec := fmt.Sprintf("%d,%d,%s,%d", args[0], args[1], path, 1000+i)
			cmd := exec.Command(os.Args[0], "-test.run=^TestInitPageBuffer_MisSizedPoolIsNotUsed$", "-test.timeout=60s")
			cmd.Env = append(os.Environ(), pageBufferChildEnv+"="+spec)
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, "child failed:\n%s", out)
		})
	}

	// Every child added one document and left the file healthy.
	db, err = Open(ctx, path, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	require.NoError(t, db.QuickCheck(ctx))
	coll, err = db.Collection(ctx, "c")
	require.NoError(t, err)
	n, err := coll.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 200+11, n)
}

// pageBufferChild configures the pool as spec says ("pageSize,nPages,path,id")
// and uses the database at path, and a new one, through it.
func pageBufferChild(t *testing.T, ctx context.Context, spec string) {
	parts := strings.SplitN(spec, ",", 4)
	pageSize, _ := strconv.Atoi(parts[0])
	nPages, _ := strconv.Atoi(parts[1])
	path := parts[2]
	id, _ := strconv.Atoi(parts[3])

	InitPageBuffer(pageSize, nPages)
	// The first call wins: a second one with other arguments changes nothing
	// and does not fail.
	InitPageBuffer(8192, 3)
	InitPageBuffer(4096, 128)

	conf := &Config{UseGlobalPageBuffer: true}
	db, err := Open(ctx, path, conf)
	require.NoError(t, err)
	coll, err := db.Collection(ctx, "c")
	require.NoError(t, err)
	n, err := coll.Find(`{"id":{"$lt":200}}`).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 200, n)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, id))))
	require.NoError(t, db.QuickCheck(ctx))
	require.NoError(t, db.Close())

	// A database created under the same configuration.
	fresh, err := Open(ctx, filepath.Join(filepath.Dir(path), fmt.Sprintf("fresh-%d.db", id)), conf)
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
