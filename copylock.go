//go:build unix || windows

package anystore

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
)

// copyLockAttempts bounds how often acquireCopyLock opens the lock file
// again after it locked a file the path no longer names.
const copyLockAttempts = 16

// Test seams of acquireCopyLock: testHookCopyLockOpened runs once the lock
// file is open, before it is locked; copyLockSameFile compares the locked
// file with the one the path names, and is a variable so that a test can
// model a volume that gives every empty file one identity. And of
// stagingOpenAsDatabase: testHookStagingLstat runs between the Lstat of the
// staging file and its open.
var (
	testHookCopyLockOpened func()
	copyLockSameFile       = os.SameFile
	testHookStagingLstat   func(path string)
)

// acquireCopyLock takes the lock of a copy (CopyToLatestFormat): an
// exclusive, non-blocking lock of the operating system on the file at path,
// which it creates if needed. The lock belongs to the open file, so the
// system drops it when the process dies, however it dies: a killed copy
// leaves no lock behind, only, at most, an unlocked file. The primitives are
// those of the database file lock (internal/btree/dbfile_lock_unix.go,
// dbfile_lock_windows.go): flock(LOCK_EX|LOCK_NB) on Unix, LockFileEx with
// LOCKFILE_EXCLUSIVE_LOCK|LOCKFILE_FAIL_IMMEDIATELY on Windows. A lock
// another holder has, in this process or another, returns ErrBusy at once.
//
// release removes the lock file and drops the lock, so that a finished copy
// leaves nothing beside its destination. Removing a lock file is safe only
// with the check made here after locking: a caller that opened the file
// before its holder removed it would otherwise hold a lock on a file that
// has no name any more, while the next caller creates and locks a new one.
// So once the lock is taken, the path must still name the locked file
// (os.SameFile of the open file and the path); if it does not, the file is
// closed and the path opened again. A lock file that cannot be removed is
// left: it locks nothing by existing, and the next copy to the destination
// takes it over.
//
// The check compares identities, which an empty file may not have: macOS
// reports a wrong inode number for an empty file on an msdos volume (SQLite
// ticket #3260): its kernel msdosfs numbers a file by its first cluster,
// and gives every empty file, which has none, the one number FILENO_EMPTY
// (msdosfs_defileid). A file removed by its last holder and the new one at
// the path, both empty, would pass for one. The locked file is therefore
// given content before the check (withContentStat).
func acquireCopyLock(path string) (release func(), err error) {
	for range copyLockAttempts {
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o666)
		if err != nil {
			return nil, err
		}
		if testHookCopyLockOpened != nil {
			testHookCopyLockOpened()
		}
		if err = lockFileExclusive(f); err != nil {
			_ = f.Close()
			return nil, err
		}
		held, err := withContentStat(f)
		if err != nil {
			// Not removed: the path may name another holder's file by now.
			_ = f.Close()
			return nil, err
		}
		named, nerr := os.Stat(path)
		if nerr == nil && copyLockSameFile(held, named) {
			return func() { releaseCopyLock(f, path) }, nil
		}
		// The holder before us removed the file between our open and our
		// lock: this lock excludes nobody.
		_ = f.Close()
	}
	// Each attempt met a holder that was just leaving.
	return nil, ErrBusy
}

// withContentStat returns the FileInfo of the locked lock file f, after
// writing a byte to f if it is empty, syncing it and stat'ing it again, so
// that the identity in the FileInfo is the file's own.
//
// ~ os_unix.c:1552-1579 at version-3.52.0 (findInodeInfo): "On OS X on an
// msdos filesystem, the inode number is reported incorrectly for zero-size
// files", so SQLite writes one byte to a zero-size file there, fsyncs it
// and fstats again before it uses the inode. SQLite does it only on the
// volumes unixOpen finds to be msdos or exfat on Apple platforms
// (os_unix.c:6758-6770), and the byte it writes is the 'S' that opens every
// database header. Here it is done for every empty lock file on every
// platform: the lock file's content is never read, so any byte will do and
// costs a write and a sync once per lock file, and the check then holds on
// any volume that leaves an empty file without an identity of its own,
// with no list of such volumes to keep (the FSKit msdos of recent macOS
// numbers an empty file afresh when it is removed, and again when it gets
// its first byte). Only a holder writes to its lock file, and only before
// it compares, so the identity compared does not change under the check.
func withContentStat(f *os.File) (os.FileInfo, error) {
	fi, err := f.Stat()
	if err != nil || fi.Size() != 0 {
		return fi, err
	}
	if _, err = f.WriteAt([]byte{0}, 0); err != nil {
		return nil, err
	}
	if err = syncLockFile(f); err != nil {
		return nil, err
	}
	return f.Stat()
}

// stagingOpenAsDatabase reports whether a database has the file at path
// open. An open database holds a lock of the operating system on its file
// from before it reads the file until it closes it (the database file lock,
// internal/btree/pager.go and dbfile_lock_unix.go, dbfile_lock_windows.go):
// on Unix a flock, shared when the database is shared between processes and
// exclusive when it is kept in the process; on Windows, where it is always
// kept in the process, LockFileEx over the whole file. The exclusive lock
// without waiting that acquireCopyLock takes, tried here on a descriptor of
// its own, conflicts with either, in this process as in another. It is
// released at once, by the close; a file whose lock it gets is reported
// free. A path that names nothing is reported free; no file is created. The
// file is opened for writing, as the database opens it: flock emulated over
// NFS takes an exclusive lock only on a descriptor open for writing.
//
// The file is opened only if it is what a copy leaves at the staging name, a
// regular file with one link, which cannot be another name of a file that
// something else has open. A symbolic link, a file with more than one link,
// and anything but a regular file fail the call (errStagingAlias), and are
// not removed. The reason is the POSIX record lock: closing a descriptor
// drops every record lock the process holds on the file, whichever
// descriptor took it (os_unix.c:1246-1254 at version-3.52.0), and a
// database's -wal-shm carries its record locks. The probe's close of another
// name of the -wal-shm of a database open in this process would drop that
// database's locks, and a peer process would then be granted what the
// database holds. So the name is Lstat'ed first; the open does not follow a
// symbolic link where the platform can refuse one (stagingOpenFlags), so
// that one put there after the Lstat fails the open; and the open file is
// fstat'ed, and must be the file the Lstat saw, with one link, before its
// lock is tried. A window is left and accepted: a link made to the file, or
// the name moved onto another file, between the Lstat and the close. Only
// something that races the copy on a name kept for it does that.
//
// On Windows the link count is not checked: os.FileInfo does not carry it
// there, and the hazard it guards against does not exist, as a LockFileEx
// lock belongs to the handle that took it and no other handle's close
// drops it.
func stagingOpenAsDatabase(path string) (bool, error) {
	fi, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err = stagingOwnFile(path, fi); err != nil {
		return false, err
	}
	if testHookStagingLstat != nil {
		testHookStagingLstat(path)
	}
	f, err := os.OpenFile(path, os.O_RDWR|stagingOpenFlags, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()
	opened, err := f.Stat()
	if err != nil {
		return false, err
	}
	if !os.SameFile(fi, opened) {
		return false, fmt.Errorf("the staging file %s was replaced while the copy looked at it: %w", path, errStagingAlias)
	}
	if err = stagingOwnFile(path, opened); err != nil {
		return false, err
	}
	if err = lockFileExclusive(f); err != nil {
		if errors.Is(err, ErrBusy) {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

// stagingOwnFile refuses a file a copy could not have made at the staging
// name: a symbolic link, anything but a regular file, a file with more than
// one link (where the platform reports the count, linkCount).
func stagingOwnFile(path string, fi os.FileInfo) error {
	switch n, ok := linkCount(fi); {
	case fi.Mode()&fs.ModeSymlink != 0:
		return fmt.Errorf("the staging file %s is a symbolic link: %w", path, errStagingAlias)
	case !fi.Mode().IsRegular():
		return fmt.Errorf("the staging file %s is not a regular file (%v): %w", path, fi.Mode().Type(), errStagingAlias)
	case ok && n != 1:
		return fmt.Errorf("the staging file %s has %d links: %w", path, n, errStagingAlias)
	}
	return nil
}

// copyLockPerName: each name has a copy lock of its own, its lock file, so
// a copy can hold the locks of several names (copyLockNames).
const copyLockPerName = true
