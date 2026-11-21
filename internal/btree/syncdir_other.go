//go:build !unix

package btree

// dirSync does nothing outside Unix: a file's sync is its fdatasync alone.
// SQLite's Windows VFS syncs no directory: UNIXFILE_DIRSYNC is the unix VFS's
// (os_unix.c:6777), and winSync syncs the file only. The other platforms that
// are not Unix (js/wasm, wasip1) do as Windows does.
type dirSync struct{}

// set does nothing.
func (*dirSync) set() {}

// sync syncs f.
func (*dirSync) sync(f fileHandle, _ string) error { return fdatasync(f) }
