//go:build !darwin

package btree

// platformEmptyFileInodeUnstable: every other platform reports a stable
// inode for a zero-size file.
func platformEmptyFileInodeUnstable(_ fileHandle) bool { return false }
