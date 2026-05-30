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

// statIdentity extracts the (device, inode) identity from an os.FileInfo. ok is
// false when no real inode is available (a VFS-injected FileInfo whose Sys() is
// not a *syscall.Stat_t), in which case callers treat the file as having an
// unavailable HAS_MOVED check (analog of C's SQLITE_NOTFOUND fallback in
// databaseIsUnmoved, pager.c:4150-4154). Reuses the exact Sys().(*syscall.Stat_t)
// idiom as fileIdentityKey above.
func statIdentity(fi os.FileInfo) (dev, ino uint64, ok bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || st == nil {
		return 0, 0, false
	}
	return uint64(st.Dev), uint64(st.Ino), true
}

// dbFileUnmoved reports whether the file currently at path still has the
// (device, inode) identity captured at open (openDev/openIno). Models C's
// fileHasMoved (os_unix.c:1623-1632): a stat error (file renamed/unlinked) or
// an inode mismatch (file relinked — a different file now sits at path) means
// the original DB file has moved, so this returns false. Caller (pager.close)
// uses the result to skip the close-time checkpoint, matching SQLite.
func dbFileUnmoved(path string, openDev, openIno uint64) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	dev, ino, ok := statIdentity(info)
	if !ok {
		// Identity unavailable now though it was available at open: be
		// conservative and treat as unmoved (mirrors C's assume-unmoved
		// fallback rather than discarding the checkpoint on an ambiguous
		// stat result).
		return true
	}
	return dev == openDev && ino == openIno
}
