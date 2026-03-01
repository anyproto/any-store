//go:build !vfs

package btree

import "os"

// fileHandle is *os.File in production — zero interface overhead.
type fileHandle = *os.File

func osOpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}

var osRemove = os.Remove

func fdatasync(f *os.File) error { return f.Sync() }

// SetVFS replaces OS-level operations. Panics unless built with -tags vfs.
func SetVFS(_ VFS) {
	panic("btree: SetVFS requires building with -tags vfs")
}

// ResetVFS restores defaults. Panics unless built with -tags vfs.
func ResetVFS() {
	panic("btree: ResetVFS requires building with -tags vfs")
}
