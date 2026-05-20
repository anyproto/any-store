package anystore

import (
	"os"

	"github.com/anyproto/any-store/v2/internal/btree"
)

var (
	osStat      = os.Stat
	osReadFile  = os.ReadFile
	osWriteFile = os.WriteFile
	osRemove    = os.Remove
)

// File and VFS are always available for type-checking.
type File = btree.File
type VFS = btree.VFS

// SetVFS replaces OS-level operations. Panics unless built with -tags vfs
// or GOOS=js GOARCH=wasm.
func SetVFS(vfs VFS) { btree.SetVFS(vfs) }

// ResetVFS restores defaults. Panics unless built with -tags vfs
// or GOOS=js GOARCH=wasm.
func ResetVFS() { btree.ResetVFS() }

// ResetOpenRegistry clears the process-global registry of open databases.
// Tests that simulate process crashes (where Close is intentionally skipped)
// call this to allow a subsequent Open of the same file to succeed.
// Panics unless built with -tags vfs or GOOS=js GOARCH=wasm.
func ResetOpenRegistry() { btree.ResetOpenRegistry() }
