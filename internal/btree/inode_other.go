//go:build !unix

package btree

import "os"

// fileIdentityKey has no portable (device, inode) source on non-unix targets
// (Windows, wasm), so it always reports ok=false and callers fall back to
// lexical-path keying. On those targets any-store forces InProcess=true and
// single-process operation anyway (hasMmapShm=false), so the symlink/hardlink
// aliasing this guards against is not the same concern as on unix.
func fileIdentityKey(_ os.FileInfo) (string, bool) {
	return "", false
}

// statIdentity has no portable (device, inode) source on non-unix targets, so
// it always reports ok=false. databaseIsUnmoved then short-circuits to "unmoved"
// (the analog of C's SQLITE_NOTFOUND "HAS_MOVED unimplemented → assume unmoved"
// fallback, pager.c:4150-4154), preserving today's close-time-checkpoint
// behavior on wasm / Windows / VFS where no real inode is available.
func statIdentity(_ os.FileInfo) (dev, ino uint64, ok bool) {
	return 0, 0, false
}

// dbFileUnmoved is unreachable in practice on non-unix targets because
// statIdentity reports ok=false at open (openIdentOK==false) so databaseIsUnmoved
// returns early without calling this. It exists so the unconditional reference
// in pager.databaseIsUnmoved compiles; it conservatively reports unmoved.
// Mirrors the single-process stubs in dbfile_lock_other.go.
func dbFileUnmoved(_ string, _, _ uint64) bool {
	return true
}
