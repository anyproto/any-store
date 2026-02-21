//go:build linux

package btree

import "syscall"

const sysPWRITEV = syscall.SYS_PWRITEV
