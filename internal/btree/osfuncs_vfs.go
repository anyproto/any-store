//go:build vfs || (js && wasm)

package btree

import "os"

type fileHandle = File

var (
	defaultOpenFile = func(name string, flag int, perm os.FileMode) (File, error) {
		return os.OpenFile(name, flag, perm)
	}
	defaultRemove = os.Remove
	defaultStat   = os.Stat

	osOpenFile = defaultOpenFile
	osRemove   = defaultRemove
	fdatasync  = defaultFdatasync
	// osStat looks at a file's metadata by name: os.Stat, or
	// statThroughOpen on js/wasm and once a VFS replaces OpenFile.
	osStat = defaultStat
)

// statThroughOpen is osStat under a VFS, which has no stat of its own: the
// file is opened read-only through the VFS and its descriptor stat'ed.
func statThroughOpen(name string) (os.FileInfo, error) {
	f, err := osOpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.Stat()
}

// SetVFS replaces OS-level operations for testing. Nil fields keep defaults.
func SetVFS(vfs VFS) {
	if vfs.OpenFile != nil {
		osOpenFile = vfs.OpenFile
		osStat = statThroughOpen
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
	osStat = defaultStat
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
