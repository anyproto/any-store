//go:build vfs && (linux || darwin) && (amd64 || arm64)

package btree

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// powerLossFS is a VFS over the real file system that keeps what a power
// loss would leave of the files opened through it, on a file system that
// makes nothing durable before it is synced:
//
//   - a file's content is durable as of its last sync (Fdatasync);
//   - a name is durable as of the last sync of its directory (syncDir): a
//     name created since then is lost, and a name removed since then comes
//     back, naming the file it named, with that file's last synced content.
//
// image writes that state out. Files that were in a directory before the
// test adopted it are durable as they were.
type powerLossFS struct {
	mu       sync.Mutex
	nextIno  int
	names    map[string]int // the names that exist now, and the file each names
	synced   map[int][]byte // each file's content at its last sync
	durable  map[string]int // the names as the last directory sync left them
	failSync func(name string) error
	trace    func(event string)
}

type powerLossFile struct {
	*os.File
	ino int
}

// newPowerLossFS installs a powerLossFS over dir for the rest of the test.
func newPowerLossFS(t *testing.T, dir string) *powerLossFS {
	t.Helper()
	fs := &powerLossFS{names: map[string]int{}, synced: map[int][]byte{}, durable: map[string]int{}}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		name := filepath.Join(dir, e.Name())
		content, err := os.ReadFile(name)
		require.NoError(t, err)
		fs.nextIno++
		fs.names[name], fs.durable[name], fs.synced[fs.nextIno] = fs.nextIno, fs.nextIno, content
	}
	SetVFS(fs.vfs())
	testSyncDirHook = fs.dirSynced
	t.Cleanup(func() {
		testSyncDirHook = nil
		ResetVFS()
	})
	return fs
}

func (fs *powerLossFS) record(event, name string) {
	if fs.trace != nil && !strings.HasSuffix(name, "-shm") {
		fs.trace(event + " " + filepath.Base(name))
	}
}

func (fs *powerLossFS) vfs() VFS {
	return VFS{
		OpenFile: func(name string, flag int, perm os.FileMode) (File, error) {
			_, statErr := os.Lstat(name)
			f, err := os.OpenFile(name, flag, perm)
			if err != nil {
				return nil, err
			}
			fs.mu.Lock()
			defer fs.mu.Unlock()
			ino, ok := fs.names[name]
			if !ok {
				fs.nextIno++
				ino = fs.nextIno
				fs.names[name], fs.synced[ino] = ino, nil
				if statErr == nil {
					// Made outside this file system: durable as it is.
					content, err := os.ReadFile(name)
					if err != nil {
						_ = f.Close()
						return nil, err
					}
					fs.durable[name], fs.synced[ino] = ino, content
				} else {
					fs.record("create", name)
				}
			}
			return powerLossFile{File: f, ino: ino}, nil
		},
		Remove: func(name string) error {
			err := os.Remove(name)
			if err == nil {
				fs.mu.Lock()
				delete(fs.names, name)
				fs.record("remove", name)
				fs.mu.Unlock()
			}
			return err
		},
		Fdatasync: func(f File) error {
			pf := f.(powerLossFile)
			if fs.failSync != nil {
				if err := fs.failSync(pf.Name()); err != nil {
					return err
				}
			}
			info, err := pf.Stat()
			if err != nil {
				return err
			}
			content := make([]byte, info.Size())
			if n, err := pf.ReadAt(content, 0); err != nil && !errors.Is(err, io.EOF) {
				return err
			} else {
				content = content[:n]
			}
			fs.mu.Lock()
			fs.synced[pf.ino] = content
			fs.record("sync", pf.Name())
			fs.mu.Unlock()
			return nil
		},
	}
}

// dirSynced is testSyncDirHook: the names in path's directory are durable as
// they are now.
func (fs *powerLossFS) dirSynced(path string) {
	dir := filepath.Dir(path)
	fs.mu.Lock()
	defer fs.mu.Unlock()
	for name := range fs.durable {
		if filepath.Dir(name) == dir {
			delete(fs.durable, name)
		}
	}
	for name, ino := range fs.names {
		if filepath.Dir(name) == dir {
			fs.durable[name] = ino
		}
	}
	fs.record("sync-dir", path)
}

// image writes what a power loss now would leave of the files in fromDir
// into toDir. The wal-index is not among them: it does not outlive a reboot.
func (fs *powerLossFS) image(t *testing.T, fromDir, toDir string) {
	t.Helper()
	fs.mu.Lock()
	defer fs.mu.Unlock()
	for name, ino := range fs.durable {
		if filepath.Dir(name) != fromDir || strings.HasSuffix(name, "-shm") {
			continue
		}
		require.NoError(t, os.WriteFile(filepath.Join(toDir, filepath.Base(name)), fs.synced[ino], 0o644))
	}
}

// powerLossOpts are the options of the power-loss tests: every commit synced,
// no checkpoint unless the test asks for one.
func powerLossOpts(inProcess bool) Options {
	opts := DefaultOptions()
	opts.InProcess = inProcess
	opts.DisableAutoCheckpoint = true
	opts.NoCommitSync = false
	return opts
}

func forEachLockMode(t *testing.T, f func(t *testing.T, inProcess bool)) {
	for _, inProcess := range []bool{false, true} {
		t.Run(map[bool]string{false: "multiprocess", true: "inprocess"}[inProcess], func(t *testing.T) {
			f(t, inProcess)
		})
	}
}

// reopenAfterPowerLoss takes the image of dir while live is open, closes
// live, and opens the database of the image.
func reopenAfterPowerLoss(t *testing.T, fs *powerLossFS, dir string, opts Options, live *DB) *DB {
	t.Helper()
	after := t.TempDir()
	fs.image(t, dir, after)
	require.NoError(t, live.Close())
	ResetVFS()
	testSyncDirHook = nil
	db, err := testOpen(t, filepath.Join(after, "test.db"), opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

var errCreatorDied = errors.New("the creator dies before its page 1 is synced")

// TestCreation_CommitOverADeadCreatorsPage1SurvivesPowerLoss: a creator
// writes page 1 and dies before the page is synced. Another process opens the
// path, commits a row with a synced commit, and the power goes. The row was
// acknowledged as durable: the open after the power loss must find it.
//
// The second process must therefore not build on the dead creator's page 1,
// which the power loss takes away: the database file comes back empty, and a
// WAL beside an empty database file is deleted, the acknowledged commit with
// it.
func TestCreation_CommitOverADeadCreatorsPage1SurvivesPowerLoss(t *testing.T) {
	forEachLockMode(t, func(t *testing.T, inProcess bool) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.db")
		opts := powerLossOpts(inProcess)
		fs := newPowerLossFS(t, dir)

		// The creator: its sync of the database file never happens.
		fs.failSync = func(name string) error {
			if name == path {
				return errCreatorDied
			}
			return nil
		}
		_, err := testOpen(t, path, opts)
		require.ErrorIs(t, err, errCreatorDied)
		fs.failSync = nil
		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, int64(opts.PageSize), info.Size(), "setup: the dead creator's page 1 is in the file")

		// The second process, and its acknowledged commit.
		db, err := testOpen(t, path, opts)
		require.NoError(t, err)
		createPutRow(t, db, "acknowledged")

		db = reopenAfterPowerLoss(t, fs, dir, opts, db)
		createRequireRow(t, db, "acknowledged")
		require.NoError(t, db.IntegrityCheck())
	})
}

// TestCreation_FirstCommitToANewWALSurvivesPowerLoss: a new database, one
// synced commit, and the power goes. The commit is in a WAL whose name was
// created after the last directory sync; the WAL's first sync syncs the
// directory too, so the name, and the commit with it, is still there.
func TestCreation_FirstCommitToANewWALSurvivesPowerLoss(t *testing.T) {
	forEachLockMode(t, func(t *testing.T, inProcess bool) {
		dir := t.TempDir()
		opts := powerLossOpts(inProcess)
		fs := newPowerLossFS(t, dir)

		db, err := testOpen(t, filepath.Join(dir, "test.db"), opts)
		require.NoError(t, err)
		createPutRow(t, db, "acknowledged")

		db = reopenAfterPowerLoss(t, fs, dir, opts, db)
		createRequireRow(t, db, "acknowledged")
		require.NoError(t, db.IntegrityCheck())
	})
}

// TestCreation_CommitToAWALAnotherHandleCreatedSurvivesPowerLoss: one handle
// creates the database and its WAL and closes without syncing the WAL; the
// next handle opens the WAL that is already there and commits. The WAL's name
// was never made durable by its creator, so the second handle's first sync
// of it syncs the directory too, as every SQLite WAL descriptor opened with
// the create flag does, whether or not its open created the file.
func TestCreation_CommitToAWALAnotherHandleCreatedSurvivesPowerLoss(t *testing.T) {
	forEachLockMode(t, func(t *testing.T, inProcess bool) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.db")
		opts := powerLossOpts(inProcess)
		fs := newPowerLossFS(t, dir)

		db, err := testOpen(t, path, opts)
		require.NoError(t, err)
		require.NoError(t, db.Close())
		require.FileExists(t, path+"-wal", "setup: the first handle's WAL")

		db, err = testOpen(t, path, opts)
		require.NoError(t, err)
		createPutRow(t, db, "acknowledged")

		db = reopenAfterPowerLoss(t, fs, dir, opts, db)
		createRequireRow(t, db, "acknowledged")
		require.NoError(t, db.IntegrityCheck())
	})
}

// TestCreation_CommitAfterTheCreationsLastJournalStepSurvivesPowerLoss: the
// creator's last step on its journal is not made durable (a sync of the
// journal after its first fails, or the delete is not synced), another open
// commits a row with a synced commit, and the power goes. Whatever the
// journal's name and content come back as, it must not be hot beside the
// database the row was committed to: a hot journal there truncates the
// database, and the WAL beside the empty file is deleted with the row.
func TestCreation_CommitAfterTheCreationsLastJournalStepSurvivesPowerLoss(t *testing.T) {
	forEachLockMode(t, func(t *testing.T, inProcess bool) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.db")
		opts := powerLossOpts(inProcess)
		fs := newPowerLossFS(t, dir)

		journalSyncs := 0
		fs.failSync = func(name string) error {
			if name == path+creationJournalSuffix {
				if journalSyncs++; journalSyncs > 1 {
					return errCreatorDied
				}
			}
			return nil
		}
		if db, err := testOpen(t, path, opts); err == nil {
			require.NoError(t, db.Close())
		}
		fs.failSync = nil

		db, err := testOpen(t, path, opts)
		require.NoError(t, err)
		createPutRow(t, db, "acknowledged")

		db = reopenAfterPowerLoss(t, fs, dir, opts, db)
		createRequireRow(t, db, "acknowledged")
		require.NoError(t, db.IntegrityCheck())
	})
}

// TestCreation_PowerLossBeforeAnyCommitCreatesTheDatabaseAgain: a creation
// is committed by its journal's delete, which syncs no directory, as SQLite's
// delete of a journal in journal_mode=DELETE syncs none (pager.c:2109). With
// nothing committed to the database since, a power loss can bring the
// journal back, hot, beside the committed page 1. The next open rolls the
// creation back and creates the database again: nothing is lost.
func TestCreation_PowerLossBeforeAnyCommitCreatesTheDatabaseAgain(t *testing.T) {
	forEachLockMode(t, func(t *testing.T, inProcess bool) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.db")
		opts := powerLossOpts(inProcess)
		fs := newPowerLossFS(t, dir)

		db, err := testOpen(t, path, opts)
		require.NoError(t, err)
		require.NoError(t, db.Close())
		require.NoFileExists(t, path+creationJournalSuffix)

		after := t.TempDir()
		fs.image(t, dir, after)
		ResetVFS()
		testSyncDirHook = nil
		require.FileExists(t, filepath.Join(after, "test.db"+creationJournalSuffix), "setup: the journal came back")

		seen, stop := creationStagesSeen(t)
		db, err = testOpen(t, filepath.Join(after, "test.db"), opts)
		stop()
		require.NoError(t, err)
		defer func() { _ = db.Close() }()
		require.Contains(t, *seen, "journal-playback")
		require.Contains(t, *seen, "journal-deleted")
		createPutRow(t, db, "after")
		require.NoError(t, db.IntegrityCheck())
	})
}

// TestCreationJournal_OrderOfTheSyncs records the syncs a creation and the
// commits after it make, as the file system sees them. The journal and its
// directory are synced before page 1 is written, the stale WAL's removal
// before that directory sync; page 1 is synced before the journal's delete.
// The directory is synced once at the first sync of each descriptor opened
// with the create flag (the journal's, the WAL's of each open) and at no
// other sync.
func TestCreationJournal_OrderOfTheSyncs(t *testing.T) {
	forEachLockMode(t, func(t *testing.T, inProcess bool) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.db")
		require.NoError(t, os.WriteFile(path+"-wal", []byte("the log of a database that is gone"), 0o644))
		opts := powerLossOpts(inProcess)
		fs := newPowerLossFS(t, dir)
		var events []string
		fs.trace = func(event string) { events = append(events, event) }
		testOpenStageHook = func(_ *pager, stage string) { events = append(events, "stage "+stage) }
		t.Cleanup(func() { testOpenStageHook = nil })
		take := func() []string {
			got := events
			events = nil
			return got
		}

		db, err := testOpen(t, path, opts)
		require.NoError(t, err)
		require.Equal(t, []string{
			"stage before-lock",
			"create test.db",
			"remove test.db-wal",
			"create test.db-journal",
			"stage journal-opened",
			"stage before-journal-sync",
			"sync test.db-journal",
			"sync-dir test.db-journal",
			"stage before-page1-write",
			"stage before-page1-sync",
			"sync test.db",
			"stage before-journal-delete",
			"remove test.db-journal",
			"stage journal-deleted",
			"create test.db-wal",
		}, take())

		createPutRow(t, db, "one")
		require.Equal(t, []string{"sync test.db-wal", "sync-dir test.db-wal", "sync test.db-wal"}, take(),
			"the WAL's header sync is its first, and syncs the directory")
		createPutRow(t, db, "two")
		require.Equal(t, []string{"sync test.db-wal"}, take())
		require.NoError(t, db.Checkpoint(CheckpointTruncate))
		require.Equal(t, []string{"sync test.db-wal", "sync test.db"}, take())
		createPutRow(t, db, "three")
		require.NotContains(t, take(), "sync-dir test.db-wal")
		require.NoError(t, db.Close())
		take()

		// A second open of the same files: its WAL descriptor is opened
		// with the create flag too, and its first sync syncs the directory.
		db, err = testOpen(t, path, opts)
		require.NoError(t, err)
		defer func() { _ = db.Close() }()
		require.NotContains(t, take(), "sync-dir test.db-wal")
		createPutRow(t, db, "four")
		got := take()
		require.Equal(t, []string{"sync test.db-wal", "sync-dir test.db-wal"}, got[:2])
		require.NotContains(t, got[2:], "sync-dir test.db-wal")
	})
}

// raceProbeFile is the file of Open's first, unlocked look at the header,
// with another process's work done around its read.
type raceProbeFile struct {
	*os.File
	beforeRead, afterRead func()
}

func (f *raceProbeFile) ReadAt(b []byte, off int64) (int, error) {
	if f.beforeRead != nil {
		f.beforeRead()
		f.beforeRead = nil
	}
	n, err := f.File.ReadAt(b, off)
	if f.afterRead != nil {
		f.afterRead()
		f.afterRead = nil
	}
	return n, err
}

// TestOpen_FirstLookRacingARecoveryDoesNotFail: Open's first look at a file
// stats a dead creator's page 1, another process plays the creation back
// (truncates the file) before the look reads it, and then creates the
// database and deletes the journal. The look read a file shorter than a
// header; the database is healthy by the time this open holds the lock, and
// the open must not fail with what the look saw.
func TestOpen_FirstLookRacingARecoveryDoesNotFail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	opts := DefaultOptions()
	page1 := referenceEmptyPage1(t, opts)
	require.NoError(t, os.WriteFile(path, page1, 0o644))
	require.NoError(t, os.WriteFile(path+creationJournalSuffix, creationJournalHdr(opts.PageSize), 0o644))

	probed := false
	SetVFS(VFS{
		OpenFile: func(name string, flag int, perm os.FileMode) (File, error) {
			f, err := os.OpenFile(name, flag, perm)
			if err != nil || probed || name != path || flag != os.O_RDONLY {
				if err != nil {
					return nil, err
				}
				return f, nil
			}
			probed = true
			return &raceProbeFile{
				File: f,
				// The other process's playback.
				beforeRead: func() { require.NoError(t, os.Truncate(path, 0)) },
				// Its creation, committed.
				afterRead: func() {
					require.NoError(t, os.WriteFile(path, page1, 0o644))
					require.NoError(t, os.Remove(path+creationJournalSuffix))
				},
			}, nil
		},
	})
	t.Cleanup(ResetVFS)

	db, err := testOpen(t, path, opts)
	require.True(t, probed, "setup: the first look was not made")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	createPutRow(t, db, "opener")
	require.NoError(t, db.IntegrityCheck())
}
