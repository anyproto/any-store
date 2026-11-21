//go:build unix

package btree

import (
	"os"
	"path/filepath"
	"syscall"
)

// syncDir fsyncs the directory that holds path, which makes the creation and
// the removal of names in it durable. It is what unixSync does the first time
// it syncs a file opened as a new journal (UNIXFILE_DIRSYNC, set at
// os_unix.c:6777, used at os_unix.c:3940-3952). As there, a directory that
// does not open is no error (osOpenDirectory's failure becomes SQLITE_OK)
// while a failed fsync is, and the sync is a plain fsync (full_fsync(dirfd,
// 0, 0)).
func syncDir(path string) error {
	if h := testSyncDirHook; h != nil {
		h(path)
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return nil
	}
	defer dir.Close()
	for {
		err = syscall.Fsync(int(dir.Fd()))
		if err != syscall.EINTR {
			break
		}
	}
	if err != nil {
		return &os.PathError{Op: "fsync", Path: dir.Name(), Err: err}
	}
	return nil
}
