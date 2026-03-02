package btree

import "os"

// File abstracts *os.File for testability. All methods map 1:1 to *os.File.
type File interface {
	ReadAt(b []byte, off int64) (int, error)
	WriteAt(b []byte, off int64) (int, error)
	Stat() (os.FileInfo, error)
	Truncate(size int64) error
	Sync() error
	Fd() uintptr
	Close() error
}

// VFS allows replacing OS-level operations for fault injection in tests.
// Nil fields keep their default (real OS) implementations.
type VFS struct {
	OpenFile  func(name string, flag int, perm os.FileMode) (File, error)
	Remove    func(name string) error
	Fdatasync func(f File) error
}
