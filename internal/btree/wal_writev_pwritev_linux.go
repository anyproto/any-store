//go:build linux && !nopwritev

package btree

import "syscall"

const sysPWRITEV = syscall.SYS_PWRITEV
