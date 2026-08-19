//go:build vfs || (js && wasm)

package btree

import "os"

type fileHandle = File

var (
	defaultOpenFile = func(name string, flag int, perm os.FileMode) (File, error) {
		return os.OpenFile(name, flag, perm)
	}
	defaultRemove = os.Remove

	osOpenFile = defaultOpenFile
	osRemove   = defaultRemove
	fdatasync  = defaultFdatasync
)

// SetVFS replaces OS-level operations for testing. Nil fields keep defaults.
func SetVFS(vfs VFS) {
	if vfs.OpenFile != nil {
		osOpenFile = vfs.OpenFile
	}
	if vfs.Remove != nil {
		osRemove = vfs.Remove
	}
	if vfs.Fdatasync != nil {
		fdatasync = vfs.Fdatasync
	}
}

// ResetVFS restores all OS-level operations to their defaults.
func ResetVFS() {
	osOpenFile = defaultOpenFile
	osRemove = defaultRemove
	fdatasync = defaultFdatasync
}

// ResetOpenRegistry clears the process-global registry of open databases.
// Tests that simulate process crashes (where Close is intentionally skipped)
// use this to allow a subsequent Open of the same file to succeed.
func ResetOpenRegistry() {
	openDBs.Range(func(k, _ any) bool {
		openDBs.Delete(k)
		return true
	})
}
