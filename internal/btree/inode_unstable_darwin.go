//go:build darwin

package btree

import "syscall"

// platformEmptyFileInodeUnstable reports whether the volume holding f
// reports a wrong inode for a zero-size file: macOS msdos and exfat volumes
// do (SQLite ticket #3260, the case findInodeInfo writes a byte for,
// os_unix.c:1553-1576).
func platformEmptyFileInodeUnstable(f fileHandle) bool {
	var st syscall.Statfs_t
	if err := syscall.Fstatfs(int(f.Fd()), &st); err != nil {
		return false
	}
	name := make([]byte, 0, len(st.Fstypename))
	for _, c := range st.Fstypename {
		if c == 0 {
			break
		}
		name = append(name, byte(c))
	}
	switch string(name) {
	case "msdos", "exfat":
		return true
	}
	return false
}
