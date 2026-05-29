//go:build !vfs && linux

package btree

import (
	"os"
	"syscall"
)

// DRIFT: fdatasync splits Linux Fdatasync vs other OS full Sync/fsync; undocumented See docs/btree/NOTES.md#drift-121-fdatasync-durability-primitive-platform-split-undocumented
func fdatasync(f *os.File) error { return syscall.Fdatasync(int(f.Fd())) }
