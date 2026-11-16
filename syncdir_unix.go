//go:build unix

package anystore

import (
	"os"
	"path/filepath"
	"syscall"
)

// syncDir makes the creation or removal of the file at path durable: it
// fsyncs the directory that holds path, as SQLite's unix VFS does after it
// deletes a file with dirSync set (unixDelete, os_unix.c:6853-6866 at
// version-3.52.0). As there, a directory that does not open is no error
// (openDirectory's SQLITE_CANTOPEN becomes SQLITE_OK) while a failed fsync
// is, and the sync is a plain fsync, F_FULLFSYNC not even on Darwin
// (full_fsync(fd, 0, 0), os_unix.c:3778).
func syncDir(path string) error {
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
