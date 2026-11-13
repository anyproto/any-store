//go:build !unix

package anystore

// syncDir does nothing outside Unix. SQLite's Windows VFS syncs no directory
// after it deletes a file (winDelete: syncDir is "Not used on win32",
// os_win.c:5573-5584 at version-3.52.0): NTFS keeps the names in its
// metadata journal. The other platforms that are not Unix (js/wasm, wasip1)
// do as Windows does.
func syncDir(string) error { return nil }
