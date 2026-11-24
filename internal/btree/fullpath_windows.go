package btree

import (
	"fmt"
	"syscall"
)

// FullPathname returns the full name of the database file at path. A
// database is opened by it, and its -wal and every other file beside it are
// named from it, as SQLite resolves the name once (sqlite3OsFullPathname,
// pager.c:4792) and builds the other names from the result (pager.c:4915,
// pager.c:4929, pager.c:4948).
//
// It is GetFullPathNameW, as in winFullPathname
// ("osGetFullPathNameW((LPCWSTR)zConverted, nByte, zTemp, 0)",
// os_win.c:6034), and its failure fails the call (os_win.c:6035-6040).
func FullPathname(path string) (string, error) {
	full, err := syscall.FullPath(path)
	if err != nil {
		return "", fmt.Errorf("btree: cannot resolve path: %w", err)
	}
	return full, nil
}
