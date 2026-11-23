//go:build !unix && !windows

package btree

// FullPathname returns path as given: on js/wasm and wasip1 the name a
// database is opened by is the name the caller wrote, as SQLite's key-value
// VFS copies the name unchanged ("memcpy(zOut, zPath, nPath)",
// kvvfsFullPathname, os_kv.c:983-998). The name reaches Go's file
// functions unchanged, so a key of a browser store stays the caller's.
func FullPathname(path string) (string, error) {
	return path, nil
}
