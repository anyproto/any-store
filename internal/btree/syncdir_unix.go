//go:build unix || wasip1

package btree

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
)

// dirSync is SQLite's UNIXFILE_DIRSYNC flag for one open file: unixOpen sets
// it on a descriptor it opens with the create flag as a main journal, a super
// journal or a WAL (isNewJrnl, os_unix.c:6569-6573; os_unix.c:6777), and the
// descriptor's first unixSync consumes it by syncing the directory after the
// file (os_unix.c:3936-3954). The flag is the descriptor's, not the file's:
// every descriptor so opened syncs the directory once, whether or not its open
// created the file, so a name that some other descriptor created and never
// synced is durable before anything synced through this one is relied on.
type dirSync struct {
	pending atomic.Bool
	mu      sync.Mutex
	// dir is the directory to sync, resolved when the file was opened.
	dir string
}

// set marks the file, opened as path, as opened with the create flag. The
// directory is taken from the path as it resolves now, as SQLite builds a
// database's file names from the full pathname it resolves at open
// (sqlite3OsFullPathname, pager.c:4792) and unixSync derives the directory
// from that name (os_unix.c:3944): a working directory that changes later
// does not change which directory is synced.
func (d *dirSync) set(path string) {
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}
	d.dir = filepath.Dir(path)
	d.pending.Store(true)
}

// sync is unixSync for f: f is synced, and then, on the first sync since
// set, its directory. A failed sync of f returns before the directory and
// leaves the flag set, as unixSync returns early (os_unix.c:3930-3935).
// Callers that race to the first sync all wait for the directory sync: none
// returns before it is done.
func (d *dirSync) sync(f fileHandle) error {
	if err := fdatasync(f); err != nil {
		return err
	}
	if d.pending.Load() {
		d.mu.Lock()
		if d.pending.Load() {
			syncDir(d.dir)
			d.pending.Store(false)
		}
		d.mu.Unlock()
	}
	return nil
}

// syncDir fsyncs the directory dir, which makes the names created and removed
// in it so far durable. It is unixSync's directory sync, and like it, it
// reports nothing: a directory that does not open is skipped
// (osOpenDirectory's SQLITE_CANTOPEN becomes SQLITE_OK, os_unix.c:3949-3951),
// and the result of the fsync is not looked at (full_fsync(dirfd, 0, 0),
// os_unix.c:3947, "Many systems (examples: AIX) are unable to fsync a
// directory, so ignore errors on the fsync", os_unix.c:3937-3939). The sync
// is a plain fsync, F_FULLFSYNC not even on Darwin, as full_fsync with
// fullSync 0 is. SQLite's unix VFS serves its WASI builds too and keeps the
// directory sync there (os_unix.c:326-330); whether a WASI host makes a
// directory's fd_sync durable is the host's.
func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	defer d.Close()
	for {
		err = syscall.Fsync(int(d.Fd()))
		if err != syscall.EINTR {
			break
		}
	}
	if h := testSyncDirHook; h != nil && err == nil {
		h(d)
	}
}
