package test

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
)

// A second Open of a file this process has open is refused with
// ErrDBAlreadyOpen, by whatever path it names the file, and the open handle
// goes on working. Backup into the open file is refused the same way.
func TestOpen_SecondOpenInProcess(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "a.db")
	db, err := anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	defer db.Close()

	// The "./" spelling is built by hand: filepath.Join would clean it away.
	dotted := dir + string(filepath.Separator) + "." + string(filepath.Separator) + "a.db"
	require.NotEqual(t, path, dotted)
	paths := []string{path, dotted}
	if runtime.GOOS != "windows" && runtime.GOOS != "js" && runtime.GOOS != "wasip1" {
		sym := filepath.Join(dir, "sym.db")
		if err := os.Symlink(path, sym); err == nil {
			paths = append(paths, sym)
		} else {
			t.Logf("no symbolic link on this filesystem: %v", err)
		}
		hard := filepath.Join(dir, "hard.db")
		if err := os.Link(path, hard); err == nil {
			paths = append(paths, hard)
		} else {
			t.Logf("no hard link on this filesystem: %v", err)
		}
	}
	for _, p := range paths {
		_, err := anystore.Open(ctx, p, nil)
		require.ErrorIs(t, err, anystore.ErrDBAlreadyOpen, p)
		require.NotErrorIs(t, err, anystore.ErrCorrupt, p)
		require.NotErrorIs(t, err, anystore.ErrBusy, p)
	}

	other, err := anystore.Open(ctx, filepath.Join(dir, "b.db"), nil)
	require.NoError(t, err)
	defer other.Close()
	require.ErrorIs(t, other.Backup(ctx, path), anystore.ErrDBAlreadyOpen)

	_, err = db.CreateCollection(ctx, "still-open")
	require.NoError(t, err)

	// Closed, the file opens again.
	require.NoError(t, db.Close())
	db2, err := anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	require.NoError(t, db2.Close())
}

// An OnIntegrityError that panics inside Open, after the file is open (in
// the quick check of a dirty open, which reads the damaged catalog page),
// leaves nothing of the open behind: a caller that recovers opens the file
// again, and is not refused as a second open of it.
func TestOpen_PanickingIntegrityCallbackReleasesTheFile(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "a.db")
	cfg := func(cb func(anystore.IntegrityError)) *anystore.Config {
		return &anystore.Config{Durability: anystore.DurabilityConfig{Sentinel: true}, OnIntegrityError: cb}
	}
	db, err := anystore.Open(ctx, path, cfg(nil))
	require.NoError(t, err)
	require.NoError(t, db.Close())
	// Page 2 is the catalog's root, which the quick check reads; the open
	// before it reads page 1 only. The marker is what a process that died
	// after a write leaves.
	flipByte(t, path, 4096+1000)
	require.NoError(t, os.WriteFile(path+".lock", nil, 0o644))

	func() {
		defer func() { require.Equal(t, "integrity callback", recover()) }()
		db, err := anystore.Open(ctx, path, cfg(func(anystore.IntegrityError) { panic("integrity callback") }))
		if err == nil {
			_ = db.Close()
		}
		t.Fatalf("Open returned (%v) without calling the callback", err)
	}()

	_, err = anystore.Open(ctx, path, cfg(nil))
	require.ErrorIs(t, err, anystore.ErrQuickCheckFailed)
	require.NotErrorIs(t, err, anystore.ErrDBAlreadyOpen)
}

// A write-ahead log written by a newer release is ErrIncompatibleVersion,
// as a newer database file is, and the log is left as it was: it holds
// commits this release cannot read.
func TestOpen_NewerWAL(t *testing.T) {
	// given: a database and its log, copied while it is open
	dir := t.TempDir()
	path := filepath.Join(dir, "live.db")
	db, err := anystore.Open(ctx, path, &anystore.Config{DisableAutoCheckpoint: true})
	require.NoError(t, err)
	coll, err := db.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"1"}`)))
	cp := filepath.Join(dir, "copy.db")
	for _, suffix := range []string{"", "-wal"} {
		data, err := os.ReadFile(path + suffix)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(cp+suffix, data, 0o644))
	}
	require.NoError(t, db.Close())

	// the log's version one above this release's, under a valid header checksum
	wal, err := os.ReadFile(cp + "-wal")
	require.NoError(t, err)
	require.Greater(t, len(wal), 32)
	binary.BigEndian.PutUint32(wal[4:8], binary.BigEndian.Uint32(wal[4:8])+1)
	var s1, s2 uint32
	for i := 0; i < 24; i += 8 {
		s1 += binary.BigEndian.Uint32(wal[i:]) + s2
		s2 += binary.BigEndian.Uint32(wal[i+4:]) + s1
	}
	binary.BigEndian.PutUint32(wal[24:28], s1)
	binary.BigEndian.PutUint32(wal[28:32], s2)
	require.NoError(t, os.WriteFile(cp+"-wal", wal, 0o644))

	// when
	_, err = anystore.Open(ctx, cp, nil)

	// then
	require.ErrorIs(t, err, anystore.ErrIncompatibleVersion)
	require.NotErrorIs(t, err, anystore.ErrCorrupt)
	after, err := os.ReadFile(cp + "-wal")
	require.NoError(t, err)
	require.Equal(t, wal, after, "a refused open must leave the log as it was")
}
