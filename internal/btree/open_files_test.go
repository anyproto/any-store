package btree

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// OpenFiles names the files by the database's full name (FullPathname) and
// identifies them by their descriptors: the database file, the log, and
// the log's index when that is a file. An in-memory database has none; a
// closed one, in memory or not, returns ErrClosed.
func TestOpenFiles(t *testing.T) {
	dir := t.TempDir()
	for _, inProcess := range []bool{false, true} {
		path := filepath.Join(dir, "sub") + string(filepath.Separator) + ".." + string(filepath.Separator) + "x.db"
		if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
			t.Fatal(err)
		}
		db, err := Open(path, Options{InProcess: inProcess})
		if err != nil {
			t.Fatal(err)
		}
		files, err := db.OpenFiles()
		if err != nil {
			t.Fatal(err)
		}
		want, err := FullPathname(path)
		if err != nil {
			t.Fatal(err)
		}
		if files.Path != want {
			t.Errorf("InProcess=%v: Path %q, want the full name %q", inProcess, files.Path, want)
		}
		for _, f := range []struct {
			name string
			fi   os.FileInfo
		}{{"x.db", files.DB}, {"x.db-wal", files.WAL}} {
			named, err := os.Stat(filepath.Join(dir, f.name))
			if err != nil {
				t.Fatal(err)
			}
			if f.fi == nil || !os.SameFile(f.fi, named) {
				t.Errorf("InProcess=%v: %s is not the file open", inProcess, f.name)
			}
		}
		shm, err := os.Stat(filepath.Join(dir, "x.db-wal-shm"))
		switch {
		case inProcess || !hasMmapShm:
			if files.SHM != nil {
				t.Errorf("InProcess=%v: the log's index is in memory, SHM is %v", inProcess, files.SHM.Name())
			}
		case err != nil:
			t.Fatal(err)
		case files.SHM == nil || !os.SameFile(files.SHM, shm):
			t.Errorf("the -wal-shm is not the file open")
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := db.OpenFiles(); !errors.Is(err, ErrClosed) {
			t.Errorf("InProcess=%v: closed: %v, want ErrClosed", inProcess, err)
		}
	}

	mem, err := Open("", Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	files, err := mem.OpenFiles()
	if err != nil || files != (OpenFiles{}) {
		t.Errorf("in memory: %+v, %v", files, err)
	}
	if err := mem.Close(); err != nil {
		t.Fatal(err)
	}
	if files, err := mem.OpenFiles(); !errors.Is(err, ErrClosed) || files != (OpenFiles{}) {
		t.Errorf("in memory, closed: %+v, %v, want ErrClosed", files, err)
	}
}
