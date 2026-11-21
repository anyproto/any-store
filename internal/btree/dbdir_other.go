//go:build !unix

package btree

import "path/filepath"

// databaseDir returns the directory of path as given. Windows and js/wasm
// sync no directory (syncdir_other.go); on wasip1 the working directory is
// not resolved.
func databaseDir(path string) (string, error) {
	return filepath.Dir(path), nil
}
