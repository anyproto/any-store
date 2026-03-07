//go:build !vfs && !linux

package btree

import "os"

func fdatasync(f *os.File) error { return f.Sync() }
