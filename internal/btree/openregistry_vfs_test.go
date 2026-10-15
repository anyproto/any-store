//go:build vfs && unix

package btree

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// emptyFileFakeIno reports one fixed, wrong inode for every zero-size file,
// the way macOS msdos volumes do (SQLite ticket #3260). Tests that use it
// also call modelUnstableEmptyInode, as the real detection is fstatfs.
type emptyFileFakeIno struct{ *os.File }

const fakeEmptyIno = 999999999

type fakeInoInfo struct {
	os.FileInfo
	sys *syscall.Stat_t
}

func (i fakeInoInfo) Sys() any { return i.sys }

func (f emptyFileFakeIno) Stat() (os.FileInfo, error) {
	info, err := f.File.Stat()
	if err != nil || info.Size() != 0 {
		return info, err
	}
	st := *info.Sys().(*syscall.Stat_t)
	st.Ino = fakeEmptyIno
	return fakeInoInfo{FileInfo: info, sys: &st}, nil
}

func modelUnstableEmptyInode(t *testing.T) {
	prev := emptyFileInodeUnstable
	emptyFileInodeUnstable = func(fileHandle) bool { return true }
	t.Cleanup(func() { emptyFileInodeUnstable = prev })
}

// stallingFile runs onStat after each Stat returns (the caller then holds
// that FileInfo; on an unstable volume the first stat of an empty file is
// unlocked and the second runs under unstableOpenMu) and onFirstWrite
// before its first WriteAt (a creator, holding the mutex, about to write
// page 1).
type stallingFile struct {
	emptyFileFakeIno
	onStat       func(n int32)
	onFirstWrite func()
	statN        atomic.Int32
	writeN       atomic.Int32
}

func (s *stallingFile) Stat() (os.FileInfo, error) {
	info, err := s.emptyFileFakeIno.Stat()
	if n := s.statN.Add(1); s.onStat != nil {
		s.onStat(n)
	}
	return info, err
}

func (s *stallingFile) WriteAt(b []byte, off int64) (int, error) {
	if s.writeN.Add(1) == 1 && s.onFirstWrite != nil {
		s.onFirstWrite()
	}
	return s.emptyFileFakeIno.WriteAt(b, off)
}

// fakeVolume installs a VFS where every .db file reports the shared
// empty-file inode; the first read-write handle of the file named stall
// is wrapped for the test's hooks, and fsync, when set, replaces fdatasync.
func fakeVolume(t *testing.T, stall string, wrap func(*stallingFile), fsync ...func(File) error) {
	var stalled atomic.Int32
	var fsyncHook func(File) error
	if len(fsync) > 0 {
		fsyncHook = fsync[0]
	}
	SetVFS(VFS{Fdatasync: fsyncHook, OpenFile: func(name string, flag int, perm os.FileMode) (File, error) {
		f, err := os.OpenFile(name, flag, perm)
		if err != nil {
			return nil, err
		}
		if !strings.HasSuffix(name, ".db") {
			return f, nil
		}
		if wrap != nil && strings.HasSuffix(name, stall) && flag&os.O_RDWR != 0 && stalled.Add(1) == 1 {
			s := &stallingFile{emptyFileFakeIno: emptyFileFakeIno{f}}
			wrap(s)
			return s, nil
		}
		return emptyFileFakeIno{f}, nil
	}})
	t.Cleanup(ResetVFS)
	modelUnstableEmptyInode(t)
	resetPageBufferPool()
}

func closeIfOpen(t *testing.T, db *DB) {
	if db != nil {
		t.Cleanup(func() { _ = db.Close() })
	}
}

// A zero-size file may report an inode that changes once its first page is
// written. The registry entry follows the file: a second open of the
// database just created is refused, and another new database on the same
// volume is not mistaken for it.
func TestOpen_RegistryFollowsInodeOfNewFile(t *testing.T) {
	dir := t.TempDir()
	fakeVolume(t, "", nil)

	a, err := Open(filepath.Join(dir, "a.db"), Options{})
	require.NoError(t, err)
	closeIfOpen(t, a)
	keyA := a.pager.registryKey
	require.NotContains(t, keyA, "999999999", "the entry still carries the empty-file inode")

	_, err = Open(filepath.Join(dir, "a.db"), Options{})
	require.ErrorIs(t, err, ErrDatabaseOpen)

	b, err := Open(filepath.Join(dir, "b.db"), Options{})
	require.NoError(t, err, "a second new database collided with the first one's empty-file inode")
	require.NoError(t, b.Close())

	require.NoError(t, a.Close())
	_, held := openDBs.Load(keyA)
	require.False(t, held, "the entry outlived its handle")
}

// Opens on such a volume serialize on the creator: while it holds the mutex
// before writing page 1, a second open of its path, an open through a
// symlink to it and an open of a different new database, each on its own
// goroutine, wait; then the first two are refused by inode and the third
// succeeds. Without the mutex the symlink spelling would create the file
// too and the loser's page 1 would land in the winner's file.
func TestOpen_OpensOnUnstableVolumeSerializeOnCreate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.db")
	link := filepath.Join(dir, "s.db")
	require.NoError(t, os.Symlink(path, link)) // dangling until created
	var same, viaLink, other *DB
	var sameErr, linkErr, otherErr error
	var wg sync.WaitGroup
	fakeVolume(t, "a.db", func(s *stallingFile) {
		s.onFirstWrite = func() {
			wg.Add(3)
			go func() { defer wg.Done(); same, sameErr = Open(path, Options{}) }()
			go func() { defer wg.Done(); viaLink, linkErr = Open(link, Options{}) }()
			go func() { defer wg.Done(); other, otherErr = Open(filepath.Join(dir, "b.db"), Options{}) }()
			time.Sleep(20 * time.Millisecond) // the opens above reach the mutex
		}
	})

	a, err := Open(path, Options{})
	closeIfOpen(t, a)
	wg.Wait()
	closeIfOpen(t, same)
	closeIfOpen(t, viaLink)
	closeIfOpen(t, other)
	require.NoError(t, err, "the creator lost its own file")
	require.ErrorIs(t, sameErr, ErrDatabaseOpen)
	require.ErrorIs(t, linkErr, ErrDatabaseOpen, "another spelling of the file being created got through")
	require.NoError(t, otherErr, "a different new database was refused while another was being created")
	createNS(t, a, "ns")
	putN(t, a, "ns", 10, 16)
}

// A handle keyed by path (its stats saw the file empty) whose file has
// content by its create/existing decision — another process created it —
// joins the file by inode there: a later same-path open is refused.
func TestOpen_PathKeyedOpenJoinsContentByInode(t *testing.T) {
	dir := t.TempDir()
	resetPageBufferPool()
	tmpl, err := Open(filepath.Join(dir, "tmpl.db"), Options{})
	require.NoError(t, err)
	require.NoError(t, tmpl.Close())
	page1, err := os.ReadFile(filepath.Join(dir, "tmpl.db"))
	require.NoError(t, err)

	path := filepath.Join(dir, "a.db")
	fakeVolume(t, "a.db", func(s *stallingFile) {
		s.onStat = func(n int32) {
			if n != 1 { // after the key stat, before the create/existing decision
				return
			}
			w, err := os.OpenFile(path, os.O_WRONLY, 0)
			require.NoError(t, err)
			_, err = w.WriteAt(page1, 0)
			require.NoError(t, err)
			require.NoError(t, w.Close())
		}
	})

	x, err := Open(path, Options{})
	require.NoError(t, err)
	closeIfOpen(t, x)
	abs, err := filepath.Abs(path)
	require.NoError(t, err)
	require.NotEqual(t, abs, x.pager.registryKey, "the handle kept its path key on a file with content")

	d, err := Open(path, Options{})
	closeIfOpen(t, d)
	require.ErrorIs(t, err, ErrDatabaseOpen, "a same-path second open got past the registry")
}

// Two spellings of one crash-stale empty file: the first opener holds the
// mutex before writing page 1; the second, through a symlink, waits, then
// finds the inode held and is refused before it opens a WAL: one handle,
// one WAL, next to the surviving spelling.
func TestOpen_TwoSpellingsSettleBeforeWAL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.db")
	link := filepath.Join(dir, "s.db")
	require.NoError(t, os.WriteFile(path, nil, 0o644))
	require.NoError(t, os.Symlink(path, link))

	var c *DB
	var cErr error
	done := make(chan struct{})
	fakeVolume(t, "a.db", func(s *stallingFile) {
		s.onFirstWrite = func() {
			go func() {
				defer close(done)
				c, cErr = Open(link, Options{}) // the other spelling, waiting on the mutex
			}()
			time.Sleep(20 * time.Millisecond)
		}
	})

	x, xErr := Open(path, Options{})
	closeIfOpen(t, x)
	<-done
	closeIfOpen(t, c)
	require.NoError(t, xErr)
	require.ErrorIs(t, cErr, ErrDatabaseOpen, "both spellings of one file ended up open")
	createNS(t, x, "ns")
	_, err := os.Stat(path + "-wal")
	require.NoError(t, err, "the surviving handle's WAL sits next to its spelling")
	_, err = os.Stat(link + "-wal")
	require.True(t, os.IsNotExist(err), "the refused spelling opened a WAL")
}
