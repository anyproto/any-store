//go:build !unix && !wasip1

package btree

// dirSync does nothing on Windows and js/wasm: a file's sync is its fdatasync
// alone. SQLite's Windows VFS syncs no directory: UNIXFILE_DIRSYNC is the
// unix VFS's (os_unix.c:6777), and winSync syncs the file only. js/wasm does
// as Windows does.
type dirSync struct{}

// set does nothing.
func (*dirSync) set(string) {}

// sync syncs f.
func (*dirSync) sync(f fileHandle) error { return fdatasync(f) }
