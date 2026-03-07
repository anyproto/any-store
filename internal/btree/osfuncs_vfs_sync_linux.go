//go:build vfs && linux

package btree

import "syscall"

var defaultFdatasync = func(f File) error { return syscall.Fdatasync(int(f.Fd())) }
