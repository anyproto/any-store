//go:build unix || wasip1

package btree

import "strings"

// databaseDir returns the directory dirSync syncs for the file opened as
// name. It is openDirectory's rule (os_unix.c:3874-3893): the name up to its
// last "/", "/" for a file in the root directory
// ("for(ii=(int)strlen(zDirname); ii>0 && zDirname[ii]!='/'; ii--);", then
// "zDirname[ii] = '\0';", os_unix.c:3880-3882), and "." for a name with no
// "/" in it ("if( zDirname[0]!='/' ) zDirname[0] = '.';", os_unix.c:3884).
// Nothing is cleaned. unixSync gives openDirectory the name the file was
// opened by (pFile->zPath, os_unix.c:3945), which is the full name the pager
// resolved (FullPathname): on Unix the directory is therefore the one the
// file is in, whatever the working directory is by then; on wasip1 the name
// is the path as given, and "." or a relative directory is opened against
// the working directory of the sync.
func databaseDir(name string) string {
	i := strings.LastIndexByte(name, '/')
	switch {
	case i > 0:
		return name[:i]
	case i == 0:
		return "/"
	}
	return "."
}
