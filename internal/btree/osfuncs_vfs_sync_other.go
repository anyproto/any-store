//go:build vfs && !linux

package btree

var defaultFdatasync = func(f File) error { return f.Sync() }
