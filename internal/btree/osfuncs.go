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

// OSFuncs allows replacing OS-level operations for fault injection in tests.
// Nil fields keep their default (real OS) implementations.
type OSFuncs struct {
	OpenFile  func(name string, flag int, perm os.FileMode) (File, error)
	Remove    func(name string) error
	Fdatasync func(f File) error
}

var (
	defaultOpenFile = func(name string, flag int, perm os.FileMode) (File, error) {
		return os.OpenFile(name, flag, perm)
	}
	defaultRemove    = os.Remove
	defaultFdatasync = func(f File) error { return f.Sync() }

	osOpenFile = defaultOpenFile
	osRemove   = defaultRemove
	fdatasync  = defaultFdatasync
)

// SetOSFuncs replaces OS-level operations for testing. Nil fields keep defaults.
func SetOSFuncs(funcs OSFuncs) {
	if funcs.OpenFile != nil {
		osOpenFile = funcs.OpenFile
	}
	if funcs.Remove != nil {
		osRemove = funcs.Remove
	}
	if funcs.Fdatasync != nil {
		fdatasync = funcs.Fdatasync
	}
}

// ResetOSFuncs restores all OS-level operations to their defaults.
func ResetOSFuncs() {
	osOpenFile = defaultOpenFile
	osRemove = defaultRemove
	fdatasync = defaultFdatasync
}
