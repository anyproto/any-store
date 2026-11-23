//go:build unix

package btree

import (
	"fmt"
	"os"
	"strings"
)

// FullPathname returns the full name of the database file at path. A
// database is opened by it, and its -wal, -shm and every other file beside
// it are named from it, so a working directory that changes later moves none
// of them. SQLite resolves the name once (sqlite3OsFullPathname,
// pager.c:4792), builds the -journal and -wal names from the result
// (pager.c:4915, pager.c:4929) and opens the database by it (pager.c:4948).
//
// It is unixFullPathname's rule for a path that is not absolute: the name
// starts from the working directory ("if( zPath[0]!='/' )", then osGetcwd,
// os_unix.c:7021-7028). An absolute path is returned as given, so a full
// name resolves to itself. A working directory that cannot be read fails
// the call, as unixFullPathname returns SQLITE_CANTOPEN (os_unix.c:7024).
// Nothing is measured here: a full name too long for the OS fails when the
// file is opened by it, where unixFullPathname fails for a name that does
// not fit its buffer (os_unix.c:6940-6942, 7030).
// DRIFT: no symbolic link is followed and no "." or ".." element is removed, where unixFullPathname follows each link and drops those elements as it appends (os_unix.c:6930-6939, 6964). See docs/btree/NOTES.md#drift-2026-11-23-1-full-pathname-follows-no-symbolic-link
func FullPathname(path string) (string, error) {
	if strings.HasPrefix(path, "/") {
		return path, nil
	}
	wd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("btree: cannot resolve path: %w", err)
	}
	return wd + "/" + path, nil
}
