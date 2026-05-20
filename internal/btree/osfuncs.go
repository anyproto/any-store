//go:build !vfs && !(js && wasm)

package btree

import "os"

// fileHandle is *os.File in production — zero interface overhead.
type fileHandle = *os.File

func osOpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}

var osRemove = os.Remove

// SetVFS replaces OS-level operations. Panics unless built with -tags vfs
// or GOOS=js GOARCH=wasm.
func SetVFS(_ VFS) {
	panic("btree: SetVFS requires building with -tags vfs or GOOS=js GOARCH=wasm")
}

// ResetVFS restores defaults. Panics unless built with -tags vfs
// or GOOS=js GOARCH=wasm.
func ResetVFS() {
	panic("btree: ResetVFS requires building with -tags vfs or GOOS=js GOARCH=wasm")
}

// ResetOpenRegistry clears the process-global registry of open databases.
// Panics unless built with -tags vfs or GOOS=js GOARCH=wasm.
func ResetOpenRegistry() {
	panic("btree: ResetOpenRegistry requires building with -tags vfs or GOOS=js GOARCH=wasm")
}
