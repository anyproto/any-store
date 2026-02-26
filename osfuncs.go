package anystore

import (
	"os"

	"github.com/anyproto/any-store/internal/btree"
)

var (
	osStat      = os.Stat
	osReadFile  = os.ReadFile
	osWriteFile = os.WriteFile
)

// File abstracts *os.File for testability. Re-exported from internal/btree.
type File = btree.File

// OSFuncs allows replacing OS-level operations for fault injection in tests.
// Re-exported from internal/btree.
type OSFuncs = btree.OSFuncs

// SetOSFuncs replaces OS-level operations for testing. Nil fields keep defaults.
func SetOSFuncs(funcs OSFuncs) {
	btree.SetOSFuncs(funcs)
}

// ResetOSFuncs restores all OS-level operations to their defaults.
func ResetOSFuncs() {
	btree.ResetOSFuncs()
}
