//go:build unix

package btree

import (
	"os"
	"strconv"
	"syscall"
)

// fileIdentityKey derives a process-stable identity key for a file from its
// os.FileInfo, using the (device, inode) pair — the same identity os.SameFile
// compares. Two paths that resolve to the same physical file (symlink,
// hardlink, bind-mount, distinct mount paths, /proc/self/fd aliases, …) yield
// the same key even though their lexical paths differ. This is what SQLite's
// process-global unixInodeInfo registry keys on (os_unix.c:1282-1349).
//
// Returns ok=false when no real inode is available (e.g. a VFS-injected
// FileInfo whose Sys() is not a *syscall.Stat_t); callers then fall back to
// lexical-path keying.
func fileIdentityKey(fi os.FileInfo) (string, bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || st == nil {
		return "", false
	}
	return "inode:" + strconv.FormatUint(uint64(st.Dev), 10) + ":" +
		strconv.FormatUint(uint64(st.Ino), 10), true
}
