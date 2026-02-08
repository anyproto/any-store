//go:build linux

package btree

import (
	"os"
	"syscall"
)

// fdatasync flushes file data to disk without flushing metadata (faster than fsync).
func fdatasync(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}
