//go:build !unix

package btree

// syncDir does nothing outside Unix. SQLite's Windows VFS syncs no
// directory: the SQLITE_OPEN_* flags that make the unix VFS do it are not
// used there (winOpen, winSync). The other platforms that are not Unix
// (js/wasm, wasip1) do as Windows does.
func syncDir(path string) error {
	if h := testSyncDirHook; h != nil {
		h(path)
	}
	return nil
}
