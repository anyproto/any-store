//go:build darwin && !nopwritev

package btree

// SYS_PWRITEV on darwin (macOS). Defined locally to avoid depending on
// golang.org/x/sys/unix. Value from <sys/syscall.h>.
const sysPWRITEV = 267
