//go:build (vfs || (js && wasm)) && !linux

package btree

var defaultFdatasync = func(f File) error { return f.Sync() }
