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
// openReserved: testHookReservedLstat runs between the Lstat of a lock file
// or of the staging file and its open.
var (
	testHookCopyLockOpened func()
	copyLockSameFile       = os.SameFile
	testHookReservedLstat  func(path string)
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
// (os.SameFile of the open file and what the path names, Lstat'ed, so that
// a symbolic link to the file is not taken for it); if it does not, the
// file is closed and the path opened again. A lock file that cannot be
// removed is left: it locks nothing by existing, and the next copy to the
// destination takes it over.
//
// The lock file is opened only if it is what a copy leaves at its name, a
// regular file with one link, as the staging file is (lstatReserved,
// openReserved): a symbolic link, a file with other links (where the
// platform reports the count) and anything but a regular file fail the
// call with errReservedNameAlias, and are neither opened, written nor
// removed. A path that names nothing is created exclusively (O_EXCL), so
// that the file opened there is a new one, this call's, whatever is put at
// the name after the Lstat. A lock file that another copy created, removed
// or replaced between the Lstat and the open, as a copy coming or leaving
// does, is looked at again.
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
		fi, err := lstatReserved("lock file", path)
		if err != nil {
			return nil, err
		}
		flag := os.O_RDWR
		if fi == nil {
			flag |= os.O_CREATE | os.O_EXCL
		}
		f, opened, err := openReserved("lock file", path, flag, 0o666)
		switch {
		case fi == nil && errors.Is(err, fs.ErrExist), fi != nil && errors.Is(err, fs.ErrNotExist):
			// Another copy created or removed the lock file since the Lstat.
			continue
		case err != nil:
			return nil, err
		case fi != nil && !os.SameFile(fi, opened):
			// Another copy replaced it since the Lstat. What was opened is a
			// regular file with no other link (openReserved), so its close
			// drops no lock of another file.
			_ = f.Close()
			continue
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
		named, nerr := os.Lstat(path)
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
// regular file with one link (lstatReserved, openReserved): a symbolic link,
// a file with more than one link and anything but a regular file fail the
// call (errReservedNameAlias), and are not removed. The open file must also
// be the one the Lstat saw: under the copy lock nothing but this call puts a
// file at the staging name, so one replaced between the Lstat and the open
// fails the call the same way.
func stagingOpenAsDatabase(path string) (bool, error) {
	fi, err := lstatReserved("staging file", path)
	if fi == nil || err != nil {
		return false, err
	}
	f, opened, err := openReserved("staging file", path, os.O_RDWR, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()
	if !os.SameFile(fi, opened) {
		return false, fmt.Errorf("the staging file %s was replaced while the copy looked at it: %w", path, errReservedNameAlias)
	}
	if err = lockFileExclusive(f); err != nil {
		if errors.Is(err, ErrBusy) {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

// lstatReserved and openReserved open a file under one of the names a copy
// keeps for itself, a lock file (acquireCopyLock) or the staging file
// (stagingOpenAsDatabase), only if it is what a copy leaves there: a regular
// file with one link, which cannot be another name of a file that something
// else has open. A symbolic link, a file with more than one link and
// anything but a regular file fail the call (errReservedNameAlias), and are
// neither opened, written nor removed. The reason is the POSIX record lock:
// closing a descriptor drops every record lock the process holds on the
// file, whichever descriptor took it (os_unix.c:1246-1254 at
// version-3.52.0), and a database's -wal-shm carries its record locks. The
// copy's close of a descriptor it opened through another name of the
// -wal-shm of a database open in this process would drop that database's
// locks, and a peer process would then be granted what the database holds.
// And a lock file that is empty gets a byte (withContentStat), which would
// go to the file the other name is of.
//
// So the name is Lstat'ed first (lstatReserved); the open does not follow a
// symbolic link where the platform can refuse one (reservedOpenFlags), so
// that one put there after the Lstat fails the open; and the open file is
// fstat'ed, and must be a regular file with one link (openReserved) and the
// file the Lstat saw (each caller), before it is locked or written. A
// window is left and accepted: a link made to the file, or the name moved
// onto another file, between the Lstat and the close. Only something that
// races the copy on a name kept for it does that.
//
// On Windows the link count is not checked: os.FileInfo does not carry it
// there. The close hazard does not exist there, as a LockFileEx lock belongs
// to the handle that took it and no other handle's close drops it; a hard
// link to an empty file, at a lock file's name, would get the lock file's
// byte.
//
// lstatReserved returns what Lstat finds at path, or nil if the path names
// nothing; something a copy does not leave there fails the call
// (reservedOwnFile). what names the file in the error: "lock file" or
// "staging file".
func lstatReserved(what, path string) (os.FileInfo, error) {
	fi, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err = reservedOwnFile(what, path, fi); err != nil {
		return nil, err
	}
	return fi, nil
}

// openReserved opens path, one of the names a copy keeps for itself, after
// its Lstat (lstatReserved), with flag and, where the platform has them,
// the flags that do not follow a symbolic link (reservedOpenFlags). It
// returns the open file and what fstat finds of it, which must be what a
// copy leaves there too (reservedOwnFile); otherwise the file is closed and
// the call fails. The caller compares the file with what the Lstat saw.
func openReserved(what, path string, flag int, perm os.FileMode) (*os.File, os.FileInfo, error) {
	if testHookReservedLstat != nil {
		testHookReservedLstat(path)
	}
	f, err := os.OpenFile(path, flag|reservedOpenFlags, perm)
	if err != nil {
		return nil, nil, err
	}
	opened, err := f.Stat()
	if err == nil {
		err = reservedOwnFile(what, path, opened)
	}
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	return f, opened, nil
}

// reservedOwnFile refuses a file a copy could not have made under one of
// the names it keeps for itself: a symbolic link, anything but a regular
// file, a file with more than one link (where the platform reports the
// count, linkCount). An open file with no link left was removed after its
// open, as a lock file is by the holder that leaves, and is no other name
// of anything.
func reservedOwnFile(what, path string, fi os.FileInfo) error {
	switch n, ok := linkCount(fi); {
	case fi.Mode()&fs.ModeSymlink != 0:
		return fmt.Errorf("the %s %s is a symbolic link: %w", what, path, errReservedNameAlias)
	case !fi.Mode().IsRegular():
		return fmt.Errorf("the %s %s is not a regular file (%v): %w", what, path, fi.Mode().Type(), errReservedNameAlias)
	case ok && n > 1:
		return fmt.Errorf("the %s %s has %d links: %w", what, path, n, errReservedNameAlias)
	}
	return nil
}

// copyLockPerName: each name has a copy lock of its own, its lock file, so
// a copy can hold the locks of several names (copyLockNames).
const copyLockPerName = true
