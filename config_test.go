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
	switch {
	case runtime.GOOS != "linux" && runtime.GOOS != "android" && runtime.GOOS != "darwin" && runtime.GOOS != "ios",
		runtime.GOARCH != "amd64" && runtime.GOARCH != "arm64":
		t.Skip("runs its children, copies of the test binary, on the targets with multi-process access, as the other subprocess tests do")
	}
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

// A CacheSize or ReadConcurrency past what Open can allocate is taken as its
// bound; zero and negative values take the defaults. The bounds are SQLite's
// cache_size clamp, or on a 32-bit target 2 GiB of 4 KiB pages, and 65,536
// readers.
func TestConfig_CacheSizes(t *testing.T) {
	if math.MaxInt == math.MaxInt32 {
		require.Equal(t, 524_288, maxCacheSize)
	} else {
		require.Equal(t, 2_147_418_112, maxCacheSize)
	}
	require.Equal(t, 65_536, maxReadConcurrency)

	cs, rc := (&Config{}).cacheSizes()
	require.Equal(t, 5000, cs)
	require.Equal(t, max(runtime.NumCPU()-1, 4), rc)

	cs, rc = (&Config{CacheSize: -1, ReadConcurrency: -1}).cacheSizes()
	require.Equal(t, 5000, cs)
	require.Equal(t, max(runtime.NumCPU()-1, 4), rc)

	cs, rc = (&Config{CacheSize: 7, ReadConcurrency: 3}).cacheSizes()
	require.Equal(t, 7, cs)
	require.Equal(t, 3, rc)

	cs, rc = (&Config{CacheSize: 524_288, ReadConcurrency: 65_536}).cacheSizes()
	require.Equal(t, 524_288, cs)
	require.Equal(t, 65_536, rc)

	cs, rc = (&Config{CacheSize: maxCacheSize + 1, ReadConcurrency: 65_537}).cacheSizes()
	require.Equal(t, maxCacheSize, cs)
	require.Equal(t, 65_536, rc)

	cs, rc = (&Config{CacheSize: math.MaxInt, ReadConcurrency: math.MaxInt}).cacheSizes()
	require.Equal(t, maxCacheSize, cs)
	require.Equal(t, 65_536, rc)
}

// KDFIterations is refused past 2^31-1, SQLCipher's kdf_iter range, or on a
// 32-bit target past 2^31-2, as Go's PBKDF2 loop never ends at the largest
// int, when a passphrase goes through the KDF, and only then: a Key is the
// cipher key itself, and no passphrase or key is no encryption. A
// passphrase of 32 bytes is stretched as any other. A count an earlier
// release created a database with, above 100,000,000 included, is taken.
func TestConfig_KDFIterationsBound(t *testing.T) {
	if math.MaxInt == math.MaxInt32 {
		require.Equal(t, math.MaxInt32-1, maxKDFIterations)
	} else {
		require.Equal(t, math.MaxInt32, maxKDFIterations)
	}
	for _, pass := range [][]byte{[]byte("correct horse"), make([]byte, 32)} {
		for _, n := range []int{100_000_001, maxKDFIterations} {
			require.NoError(t, (&Config{Encryption: EncryptionConfig{Passphrase: pass, KDFIterations: n}}).validate())
		}
		for _, n := range []int{maxKDFIterations + 1, math.MaxInt} {
			err := (&Config{Encryption: EncryptionConfig{Passphrase: pass, KDFIterations: n}}).validate()
			require.ErrorIs(t, err, ErrInvalidConfig)
			require.ErrorContains(t, err, "Encryption.KDFIterations")
		}
	}
	require.NoError(t, (&Config{Encryption: EncryptionConfig{Key: make([]byte, 32), KDFIterations: math.MaxInt}}).validate())
	require.NoError(t, (&Config{Encryption: EncryptionConfig{KDFIterations: math.MaxInt}}).validate())
}

// An unknown CipherType is refused with a passphrase or a key, and ignored
// without either, as the rest of EncryptionConfig is.
func TestConfig_CipherTypeKnown(t *testing.T) {
	for _, enc := range []EncryptionConfig{{Passphrase: []byte("correct horse")}, {Passphrase: make([]byte, 32)}, {Key: make([]byte, 32)}} {
		for _, ct := range []CipherType{CipherAES256GCM, CipherChaCha20Poly1305, CipherXChaCha20Poly1305} {
			enc.CipherType = ct
			require.NoError(t, (&Config{Encryption: enc}).validate())
		}
		enc.CipherType = "rot13"
		err := (&Config{Encryption: enc}).validate()
		require.ErrorIs(t, err, ErrInvalidConfig)
		require.ErrorContains(t, err, "Encryption.CipherType")
	}
	require.NoError(t, (&Config{Encryption: EncryptionConfig{CipherType: "rot13"}}).validate())
}

// A Key is 32 bytes: one of another length, an empty one included, is
// refused, and so is a Key set together with a Passphrase. A nil Key is no
// key. An empty Passphrase that is not nil is refused as well.
func TestConfig_Key(t *testing.T) {
	require.NoError(t, (&Config{Encryption: EncryptionConfig{Key: make([]byte, 32)}}).validate())
	require.NoError(t, (&Config{Encryption: EncryptionConfig{Key: nil, Passphrase: []byte("pw")}}).validate())
	err := (&Config{Encryption: EncryptionConfig{Passphrase: []byte{}}}).validate()
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.ErrorContains(t, err, "Encryption.Passphrase")
	for _, n := range []int{0, 1, 16, 31, 33, 64} {
		err := (&Config{Encryption: EncryptionConfig{Key: make([]byte, n)}}).validate()
		require.ErrorIs(t, err, ErrInvalidConfig, "a Key of %d bytes", n)
		require.ErrorContains(t, err, "Encryption.Key")
	}
	for _, pass := range [][]byte{[]byte("pw"), make([]byte, 32), {}} {
		err := (&Config{Encryption: EncryptionConfig{Key: make([]byte, 32), Passphrase: pass}}).validate()
		require.ErrorIs(t, err, ErrInvalidConfig)
		require.ErrorContains(t, err, "Encryption.Key and Encryption.Passphrase")
	}
}
