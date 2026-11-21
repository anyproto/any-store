package anystore

import (
	"os"
	"path/filepath"
	"runtime"
)

// fullPathname returns the name Open gives a file-backed database: the
// database file, its WAL and its sentinel file are all named from it, so a
// later change of the working directory moves none of them. SQLite resolves
// the name once, opens the database by it and builds the -wal name from it
// (pager.c:4792, pager.c:4928-4929, pager.c:4948 at version-3.52.0).
//
// Outside Windows a relative path is joined to the working directory as
// written, as SQLite's unixFullPathname starts from getcwd
// (os_unix.c:7021-7028), and an absolute path is kept as given. Nothing is
// cleaned lexically: each ".." is left for the OS to follow. On Windows it is
// filepath.Abs, which is GetFullPathNameW, as in SQLite's winFullPathname
// (os_win.c:6022-6034).
func fullPathname(path string) (string, error) {
	if runtime.GOOS == "windows" {
		return filepath.Abs(path)
	}
	if filepath.IsAbs(path) {
		return path, nil
	}
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return wd + string(filepath.Separator) + path, nil
}
