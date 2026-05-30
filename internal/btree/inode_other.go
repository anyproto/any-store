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
