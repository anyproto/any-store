package anystore

import (
	"os"

	"github.com/anyproto/any-store/internal/btree"
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

// SetVFS replaces OS-level operations. Panics unless built with -tags vfs.
func SetVFS(vfs VFS) { btree.SetVFS(vfs) }

// ResetVFS restores defaults. Panics unless built with -tags vfs.
func ResetVFS() { btree.ResetVFS() }
