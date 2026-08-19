//go:build js && wasm

package btree

import "os"

// Under wasm, the default OS funcs installed in osfuncs_vfs.go point at
// os.OpenFile / os.Remove / f.Sync — which on GOOS=js return ENOSYS
// rather than panicking, producing a silent-wrong code path if the user
// forgets to call anystore.SetVFS.
//
// This init runs after osfuncs_vfs.go's var block (Go initializes package
// vars before running init functions; init order within a package follows
// filename order, and "osfuncs_vfs.go" < "osfuncs_vfs_js.go"). It replaces
// the defaults with panic stubs so a missing SetVFS call fails loudly at
// the first OS operation.
//
// anystore.SetVFS(vfs) installs the caller's implementation over these
// stubs. anystore.ResetVFS reinstalls the stubs — the correct "unset"
// state under wasm.
func init() {
	defaultOpenFile = func(name string, _ int, _ os.FileMode) (File, error) {
		panic("btree: SetVFS not called — anystore on wasm requires a VFS backend (path=" + name + ")")
	}
	defaultRemove = func(name string) error {
		panic("btree: SetVFS not called — anystore on wasm requires a VFS backend (path=" + name + ")")
	}
	defaultFdatasync = func(File) error {
		panic("btree: SetVFS not called — anystore on wasm requires a VFS backend")
	}
	osOpenFile = defaultOpenFile
	osRemove = defaultRemove
	fdatasync = defaultFdatasync
}
