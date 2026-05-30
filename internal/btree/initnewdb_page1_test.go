//go:build (linux || darwin) && (amd64 || arm64)

package btree

// Regression tests pinning the by-design behavior of initNewDB writing page 1
// directly to the main DB file (bypassing the WAL).
// See docs/btree/NOTES.md#drift-63-new-db-page-1-written-directly-to-file-bypassing-wal
//
// SQLite defers page-1 creation to the first write transaction so the new
// page 1 reaches the main file only through a checksum-protected WAL frame.
// Go's initNewDB instead builds the empty page-1 image and writes it straight
// to the main file via WriteAt+fdatasync inside Open(), before opening the WAL.
// That is intentional and kept; these tests pin the two properties that make it
// safe:
//
//  1. A crash during initial page-1 creation puts no pre-existing user data at
//     risk: a zero-length leftover re-initializes cleanly, and the only image
//     the direct write can produce is the fully-deterministic empty page 1,
//     which is trivially re-derivable and opens as a valid empty DB even with
//     no WAL present.
//
//  2. Cross-process creation is harmless even though Go takes only a shared
//     flock (not C's exclusive write lock) while deciding "new DB": the empty
//     page-1 image is byte-for-byte deterministic for an unencrypted DB, so
//     concurrent creators write identical bytes and the racing-creator window
//     cannot corrupt the file.

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// referenceEmptyPage1 returns the exact bytes initNewDB writes to the main file
// for a fresh unencrypted DB created with the given options. It opens a fresh DB
// on a throwaway path, closes it, and reads back the raw main-file image.
func referenceEmptyPage1(t *testing.T, opts Options) []byte {
	t.Helper()
	resetPageBufferPool()
	p := filepath.Join(t.TempDir(), "ref.db")
	db, err := Open(p, opts)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	img, err := os.ReadFile(p)
	require.NoError(t, err)
	require.Len(t, img, int(opts.PageSize), "fresh DB main file must be exactly one page")
	return img
}

// assertValidEmptyDB reopens path and confirms it behaves as a valid, empty DB:
// a write transaction can create a namespace, store a key, and read it back.
func assertValidEmptyDB(t *testing.T, path string, opts Options) {
	t.Helper()
	resetPageBufferPool()
	db, err := Open(path, opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("ns")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("k"), []byte("v")))
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Rollback()) }()
	ns2, err := db.getNamespaceLocked("ns")
	require.NoError(t, err)
	got, err := rtx.Get(ns2, []byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), got)
}

// TestInitNewDB_Page1_CrashRecoverable pins that a crash during the initial
// direct-to-file page-1 creation leaves a recoverable / trivially re-derivable
// empty page 1 with no pre-existing user data at risk.
//
// Because initNewDB writes the new page 1 straight to the main file (not through
// the WAL), the only crash states reachable mid-creation are:
//   - a zero-length leftover file (WriteAt never landed / fdatasync skipped),
//     which Open re-initializes cleanly on the next open, identical to C; and
//   - the complete deterministic page-1 image (WriteAt+fdatasync completed),
//     which is a valid empty DB on its own with no WAL frame required.
//
// A torn intermediate (header written, body still zero) is not produced by the
// happy path; the recoverable invariant tested here is that the only persisted
// new-DB image is one Open accepts as a clean empty DB, and that no earlier
// user data ever existed to be lost.
func TestInitNewDB_Page1_CrashRecoverable(t *testing.T) {
	opts := DefaultOptions()

	t.Run("zero-length leftover re-initializes", func(t *testing.T) {
		resetPageBufferPool()
		path := filepath.Join(t.TempDir(), "test.db")

		// Create then truncate to 0 to model "crash before page-1 WriteAt landed":
		// Open() opened the file with O_CREATE but the direct write never reached
		// disk, so the next opener still sees info.Size()==0.
		db, err := Open(path, opts)
		require.NoError(t, err)
		require.NoError(t, db.Close())
		require.NoError(t, os.Truncate(path, 0))
		_ = os.Remove(path + "-wal")
		_ = os.Remove(path + "-shm")

		// Reopen: Open must re-run initNewDB (no pre-existing user data) and the
		// result must be a valid empty DB.
		assertValidEmptyDB(t, path, opts)
	})

	t.Run("completed direct write opens with no WAL", func(t *testing.T) {
		// Model "crash right after page-1 WriteAt+fdatasync, before any write
		// transaction / WAL activity": only the bare deterministic page-1 image
		// is on disk, with no WAL or shm. It must open as a valid empty DB,
		// proving the direct-to-file image is self-consistent and re-derivable
		// without a covering WAL frame.
		img := referenceEmptyPage1(t, opts)

		path := filepath.Join(t.TempDir(), "bare.db")
		require.NoError(t, os.WriteFile(path, img, 0o644))
		require.NoFileExists(t, path+"-wal")
		require.NoFileExists(t, path+"-shm")

		assertValidEmptyDB(t, path, opts)
	})
}

// childCreateEnvVar selects the cross-process page-1 creator child mode and
// carries the target DB path for TestInitNewDB_Page1_CrossProcessSerialization.
const childCreateEnvVar = "TEST_INITNEWDB_PAGE1_CREATE_PATH"

// TestInitNewDB_Page1_CrossProcessSerialization pins that cross-process
// creation of the initial page 1 behaves as intended even though Go only holds
// a shared flock (not C's exclusive write lock) while deciding "new DB".
//
// Several OS processes race to Open the same brand-new path with
// InProcess:false. Each independently sees info.Size()==0 and writes the empty
// page-1 image directly to the main file. Because that image is byte-for-byte
// deterministic for an unencrypted DB, every concurrent creator writes the
// identical 4096 bytes, so the missing exclusive lock cannot corrupt the file:
// the final on-disk image equals the single-creator reference image, and the DB
// opens as a valid empty DB.
func TestInitNewDB_Page1_CrossProcessSerialization(t *testing.T) {
	opts := Options{PageSize: 4096, CacheSize: 100, InProcess: false}

	if path := os.Getenv(childCreateEnvVar); path != "" {
		// === CHILD PROCESS ===
		// Race the other children to create page 1, then close immediately.
		resetPageBufferPool()
		db, err := Open(path, opts)
		require.NoError(t, err)
		require.NoError(t, db.Close())
		return
	}

	// === PARENT PROCESS ===
	// Reference image a lone single-process creator would produce.
	reference := referenceEmptyPage1(t, opts)

	path := filepath.Join(t.TempDir(), "race.db")

	const nChildren = 4
	var wg sync.WaitGroup
	errs := make([]error, nChildren)
	for i := range nChildren {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cmd := exec.Command(os.Args[0],
				"-test.run=^TestInitNewDB_Page1_CrossProcessSerialization$",
				"-test.timeout=60s",
			)
			cmd.Env = append(os.Environ(), childCreateEnvVar+"="+path)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			errs[i] = cmd.Run()
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "child %d failed to create page 1", i)
	}

	// The racing creators must have produced exactly the deterministic
	// single-creator image — no torn or divergent bytes from the missing
	// exclusive lock.
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.True(t, bytes.Equal(reference, got),
		"cross-process page-1 image diverged from the single-creator reference")

	// And the raced-into-existence DB must open as a valid empty DB.
	assertValidEmptyDB(t, path, opts)
}
