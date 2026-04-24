//go:build unix

package btree

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDBFileLock_SharedAllowsShared(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	f1, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f1.Close()
	f2, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()

	if err := acquireSharedDBLock(f1); err != nil {
		t.Fatalf("f1 shared: %v", err)
	}
	if err := acquireSharedDBLock(f2); err != nil {
		t.Fatalf("f2 shared (peer has shared): %v", err)
	}
}

func TestDBFileLock_ExclusiveBlocksShared(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	f1, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f1.Close()

	if err := acquireSharedDBLock(f1); err != nil {
		t.Fatalf("f1 shared: %v", err)
	}
	got, err := tryUpgradeDBLockExclusive(f1)
	if err != nil {
		t.Fatalf("f1 upgrade: %v", err)
	}
	if !got {
		t.Fatalf("f1 upgrade should succeed when only holder")
	}

	f2, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()
	if err := acquireSharedDBLock(f2); err != ErrBusy {
		t.Fatalf("f2 shared should ErrBusy while f1 exclusive; got %v", err)
	}

	if err := downgradeDBLockToShared(f1); err != nil {
		t.Fatalf("f1 downgrade: %v", err)
	}
	if err := acquireSharedDBLock(f2); err != nil {
		t.Fatalf("f2 shared after f1 downgrade: %v", err)
	}
}

func TestDBFileLock_UpgradeFailsWhenPeerShared(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	f1, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f1.Close()
	f2, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()

	if err := acquireSharedDBLock(f1); err != nil {
		t.Fatal(err)
	}
	if err := acquireSharedDBLock(f2); err != nil {
		t.Fatal(err)
	}

	got, err := tryUpgradeDBLockExclusive(f1)
	if err != nil {
		t.Fatalf("f1 upgrade: %v", err)
	}
	if got {
		t.Fatalf("f1 upgrade should FAIL while f2 holds shared")
	}
}
