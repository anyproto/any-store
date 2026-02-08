//go:build !linux

package btree

import "os"

// fdatasync falls back to Sync on non-Linux platforms.
func fdatasync(f *os.File) error {
	return f.Sync()
}
