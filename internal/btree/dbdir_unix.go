//go:build unix

package btree

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// databaseDir returns the directory dirSync syncs for the files of the
// database at path, resolved once when the database is opened, as SQLite
// resolves the name (sqlite3OsFullPathname, pager.c:4792): a relative path is
// joined to the working directory, as unixFullPathname starts from getcwd
// (os_unix.c:7021-7028), and the directory is the full pathname up to its
// last separator, as openDirectory takes it (os_unix.c:3880-3886). A working
// directory that changes later does not change it. A full pathname too long
// for the OS fails the open before any file is created, as SQLite fails the
// open with SQLITE_CANTOPEN when it cannot resolve the name (os_unix.c:7024,
// 7030).
// DRIFT: only the directory comes from the full pathname; the files are opened by the path as given, with no symbolic link resolved, where SQLite opens them by the full pathname it resolves (pager.c:4948). See docs/btree/NOTES.md#drift-2026-11-21-7-creation-journal
func databaseDir(path string) (string, error) {
	full := path
	if !filepath.IsAbs(path) {
		wd, err := os.Getwd()
		if err != nil {
			return "", fmt.Errorf("btree: cannot resolve path: %w", err)
		}
		full = wd + "/" + path
		if _, err := os.Lstat(full); errors.Is(err, syscall.ENAMETOOLONG) {
			return "", fmt.Errorf("btree: cannot resolve path: %w", err)
		}
	}
	i := strings.LastIndexByte(full, '/')
	if i == 0 {
		return "/", nil
	}
	return full[:i], nil
}
