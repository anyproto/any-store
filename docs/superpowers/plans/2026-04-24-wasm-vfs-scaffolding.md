# WASM build + VFS scaffolding — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make any-store compile cleanly under `GOOS=js GOARCH=wasm` with VFS auto-enabled (no `-tags vfs` required), and add a separate `wasm/` Go submodule with OPFS / IndexedDB / MemFS backends plus a smoke-test `main.go`.

**Architecture:** Fold `(js && wasm)` into the existing `vfs` tag expression across four `internal/btree/osfuncs*.go` files, split `dbfile_lock.go` on the `unix` / `!unix` axis using the existing `fileHandle` type alias, add a wasm-only `init()` that replaces the default OS funcs with panic stubs (so `SetVFS` becomes mandatory on wasm), then port the VFS backends from `../any-crdt-sdk/wasm/` into a new `wasm/` submodule.

**Tech Stack:** Go 1.23+ with `syscall/js`, Go build tags, Make, plain JavaScript (ES modules). No bundler, no TypeScript.

**Spec:** `docs/superpowers/specs/2026-04-24-wasm-vfs-scaffolding-design.md`

**Reference to port from:** `/home/dev/work/any-crdt-sdk/wasm/`

---

## Task 1: Reproduce the two compile breaks

Before any code changes, capture the current failure modes so we have a red baseline.

**Files:** none (verification only)

- [ ] **Step 1.1: Confirm `-tags vfs` build is broken**

Run: `go build -tags vfs ./...`

Expected output includes:
```
internal/btree/pager.go:350:35: cannot use f (variable of interface type File) as *os.File value in argument to acquireSharedDBLock: need type assertion
internal/btree/pager.go:2442:43: cannot use p.file (variable of interface type fileHandle) as *os.File value in argument to tryUpgradeDBLockExclusive: need type assertion
```

- [ ] **Step 1.2: Confirm `GOOS=js GOARCH=wasm` build is broken**

Run: `GOOS=js GOARCH=wasm go build ./...`

Expected output includes:
```
internal/btree/dbfile_lock.go:34:29: undefined: syscall.LOCK_SH
internal/btree/dbfile_lock.go:43:29: undefined: syscall.LOCK_EX
internal/btree/dbfile_lock.go:56:29: undefined: syscall.LOCK_SH
internal/btree/dbfile_lock.go:65:20: undefined: syscall.Flock
internal/btree/dbfile_lock.go:65:52: undefined: syscall.LOCK_NB
```

- [ ] **Step 1.3: Confirm existing non-tag build is green**

Run: `go build ./...`

Expected: exit 0, no output.

- [ ] **Step 1.4: Confirm existing non-tag tests pass**

Run: `go test ./internal/btree/... -run TestDBFileLock -count=1`

Expected: `PASS`.

No commit for this task — verification only.

---

## Task 2: Split `dbfile_lock.go` into `_unix.go` / `_other.go` using `fileHandle`

Fixes the pre-existing `-tags vfs` break AND removes the wasm-breaking `syscall.Flock` reference.

**Files:**
- Delete: `internal/btree/dbfile_lock.go`
- Create: `internal/btree/dbfile_lock_unix.go`
- Create: `internal/btree/dbfile_lock_other.go`
- Modify: `internal/btree/dbfile_lock_test.go` (add build tag)

- [ ] **Step 2.1: Create `internal/btree/dbfile_lock_unix.go`**

Write this exact content:

```go
//go:build unix

package btree

import (
	"errors"
	"fmt"
	"syscall"
)

// DB-file lock protocol
// =====================
//
// SQLite's WAL-mode close protocol (wal.c:2487-2551 sqlite3WalClose) acquires
// an EXCLUSIVE fcntl lock on the database file before unlinking the shm.
// New openers take SHARED on the DB file first, which blocks against the
// closer's EXCLUSIVE — serializing "last-client-unlink" vs. "new-opener-
// attach" at the DB-file lock level.
//
// any-store used to handle this via SHM dead-man-switch (DMS) alone, which
// left a narrow orphan-inode window (see NOTES.md §SHM open/close protocol
// drift). This file adopts SQLite's approach — adapted to use BSD flock
// (whole-file, per-file-description) instead of byte-range fcntl. flock
// semantics dodge POSIX fcntl's "close any fd releases all locks on inode"
// gotcha when two goroutines in the same process open the same DB path.
//
// Simplified state machine (vs. SQLite's 5-state NO/SHARED/RESERVED/PENDING/
// EXCLUSIVE): WAL mode only needs NO / SHARED / EXCLUSIVE. RESERVED and
// PENDING exist in SQLite to support rollback-journal mode and don't apply
// here.

// acquireSharedDBLock takes a non-blocking shared flock on fd. Returns
// ErrBusy if another process holds exclusive on the same file.
func acquireSharedDBLock(fd fileHandle) error {
	return flockNB(fd, syscall.LOCK_SH)
}

// tryUpgradeDBLockExclusive attempts to upgrade the caller's shared flock
// to exclusive. Returns (true, nil) if the upgrade succeeded (caller is
// the only holder, safe to unlink shm / truncate WAL). Returns (false, nil)
// if another holder prevents the upgrade. Returns (false, err) on OS
// errors other than EWOULDBLOCK.
func tryUpgradeDBLockExclusive(fd fileHandle) (bool, error) {
	err := flockNB(fd, syscall.LOCK_EX)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, ErrBusy):
		return false, nil
	default:
		return false, err
	}
}

// downgradeDBLockToShared converts an exclusive lock back to shared.
func downgradeDBLockToShared(fd fileHandle) error {
	return flockNB(fd, syscall.LOCK_SH)
}

// flockNB is a thin wrapper that maps EWOULDBLOCK/EAGAIN to ErrBusy so
// callers share the same BUSY handling they use for shm fcntl locks.
func flockNB(fd fileHandle, how int) error {
	if fd == nil {
		return fmt.Errorf("btree: dbfile lock: nil fd")
	}
	if err := syscall.Flock(int(fd.Fd()), how|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return ErrBusy
		}
		return fmt.Errorf("btree: flock(%d): %w", how, err)
	}
	return nil
}
```

- [ ] **Step 2.2: Create `internal/btree/dbfile_lock_other.go`**

Write this exact content:

```go
//go:build !unix

package btree

// Single-process stubs. On non-unix targets (wasm, Windows) any-store runs
// with InProcess=true forced (db.go:201-204 via hasMmapShm=false), so these
// lock primitives are dead code at runtime — but the pager open/close paths
// reference them unconditionally, so they must still compile.

func acquireSharedDBLock(_ fileHandle) error              { return nil }
func tryUpgradeDBLockExclusive(_ fileHandle) (bool, error) { return true, nil }
func downgradeDBLockToShared(_ fileHandle) error           { return nil }
```

- [ ] **Step 2.3: Delete the old `internal/btree/dbfile_lock.go`**

Run: `rm internal/btree/dbfile_lock.go`

- [ ] **Step 2.4: Add build tag to the test file**

Modify `internal/btree/dbfile_lock_test.go`. Replace the first line:

From:
```go
package btree
```

To:
```go
//go:build unix

package btree
```

- [ ] **Step 2.5: Verify `go build ./...` still passes**

Run: `go build ./...`
Expected: exit 0, no output.

- [ ] **Step 2.6: Verify `-tags vfs` now compiles (pre-existing break fixed)**

Run: `go build -tags vfs ./...`
Expected: exit 0, no output.

- [ ] **Step 2.7: Verify tests still pass**

Run: `go test ./internal/btree/... -run TestDBFileLock -count=1`
Expected: `PASS`.

Run: `go test -tags vfs ./internal/btree/... -run TestDBFileLock -count=1`
Expected: `PASS`.

- [ ] **Step 2.8: Commit**

```bash
git add internal/btree/dbfile_lock_unix.go internal/btree/dbfile_lock_other.go internal/btree/dbfile_lock_test.go
git rm internal/btree/dbfile_lock.go
git -c commit.gpgsign=false commit -m "btree: split dbfile_lock into unix/other, use fileHandle alias

Fixes -tags vfs compile break introduced by ce57450 (pager.go:350,2442
passed fileHandle to functions typed *os.File). Swapping to the existing
fileHandle alias resolves to *os.File under !vfs and File under vfs.
Split by unix/!unix so syscall.Flock does not leak to wasm; non-unix
callers are already InProcess=true (db.go:201-204) so the no-op stubs
are never exercised at runtime."
```

---

## Task 3: Flip build tags — fold `(js && wasm)` into the `vfs` expression

**Files:**
- Modify: `internal/btree/osfuncs_vfs.go`
- Modify: `internal/btree/osfuncs.go`
- Modify: `internal/btree/osfuncs_vfs_sync_other.go`
- Modify: `internal/btree/osfuncs_sync_other.go`
- Modify: `vfs.go` (doc comments only)

- [ ] **Step 3.1: Update `internal/btree/osfuncs_vfs.go` build tag**

Change line 1 from:
```go
//go:build vfs
```
to:
```go
//go:build vfs || (js && wasm)
```

- [ ] **Step 3.2: Update `internal/btree/osfuncs.go` build tag and panic messages**

Change line 1 from:
```go
//go:build !vfs
```
to:
```go
//go:build !vfs && !(js && wasm)
```

Then update the panic messages. Change lines 16-24 from:
```go
// SetVFS replaces OS-level operations. Panics unless built with -tags vfs.
func SetVFS(_ VFS) {
	panic("btree: SetVFS requires building with -tags vfs")
}

// ResetVFS restores defaults. Panics unless built with -tags vfs.
func ResetVFS() {
	panic("btree: ResetVFS requires building with -tags vfs")
}
```
to:
```go
// SetVFS replaces OS-level operations. Panics unless built with -tags vfs
// or GOOS=js GOARCH=wasm.
func SetVFS(_ VFS) {
	panic("btree: SetVFS requires building with -tags vfs or GOOS=js GOARCH=wasm")
}

// ResetVFS restores defaults. Panics unless built with -tags vfs
// or GOOS=js GOARCH=wasm.
func ResetVFS() {
	panic("btree: ResetVFS requires building with -tags vfs or GOOS=js GOARCH=wasm")
}
```

- [ ] **Step 3.3: Update `internal/btree/osfuncs_vfs_sync_other.go` build tag**

Change line 1 from:
```go
//go:build vfs && !linux
```
to:
```go
//go:build (vfs || (js && wasm)) && !linux
```

- [ ] **Step 3.4: Update `internal/btree/osfuncs_sync_other.go` build tag**

Change line 1 from:
```go
//go:build !vfs && !linux
```
to:
```go
//go:build !vfs && !(js && wasm) && !linux
```

- [ ] **Step 3.5: Update `vfs.go` doc comments**

Modify `vfs.go:20-24`. Change:
```go
// SetVFS replaces OS-level operations. Panics unless built with -tags vfs.
func SetVFS(vfs VFS) { btree.SetVFS(vfs) }

// ResetVFS restores defaults. Panics unless built with -tags vfs.
func ResetVFS() { btree.ResetVFS() }
```
to:
```go
// SetVFS replaces OS-level operations. Panics unless built with -tags vfs
// or GOOS=js GOARCH=wasm.
func SetVFS(vfs VFS) { btree.SetVFS(vfs) }

// ResetVFS restores defaults. Panics unless built with -tags vfs
// or GOOS=js GOARCH=wasm.
func ResetVFS() { btree.ResetVFS() }
```

- [ ] **Step 3.6: Verify baseline builds unchanged**

Run: `go build ./...`
Expected: exit 0, no output.

Run: `go build -tags vfs ./...`
Expected: exit 0, no output.

- [ ] **Step 3.7: Verify `GOOS=js GOARCH=wasm` now compiles the core package**

Run: `GOOS=js GOARCH=wasm go build ./internal/btree/...`
Expected: exit 0, no output. (This confirms all four osfuncs files have correct tags and the dbfile_lock split works on js.)

- [ ] **Step 3.8: Verify existing test suite still passes**

Run: `go test ./internal/btree/... -count=1 -short -timeout=120s`
Expected: `ok` for all packages.

- [ ] **Step 3.9: Commit**

```bash
git add internal/btree/osfuncs_vfs.go internal/btree/osfuncs.go internal/btree/osfuncs_vfs_sync_other.go internal/btree/osfuncs_sync_other.go vfs.go
git -c commit.gpgsign=false commit -m "btree: auto-enable VFS on GOOS=js GOARCH=wasm

Fold (js && wasm) into the vfs build tag expression across osfuncs.go,
osfuncs_vfs.go, osfuncs_sync_other.go, osfuncs_vfs_sync_other.go. Under
wasm there is no real OS filesystem, so VFS is mandatory — making it
implicit avoids requiring downstream wasm builds to pass -tags vfs.

Update SetVFS / ResetVFS panic messages and anystore doc comments to
mention the GOOS=js trigger alongside -tags vfs."
```

---

## Task 4: Add wasm-only panic defaults in `osfuncs_vfs_js.go`

Without this, forgetting to call `SetVFS` under wasm produces silent ENOSYS errors from `os.OpenFile` instead of a clear panic.

**Files:**
- Create: `internal/btree/osfuncs_vfs_js.go`

- [ ] **Step 4.1: Create `internal/btree/osfuncs_vfs_js.go`**

Write this exact content:

```go
//go:build js && wasm

package btree

import "os"

// Under wasm, the default OS funcs installed in osfuncs_vfs.go point at
// os.OpenFile / os.Remove / f.Sync — which on GOOS=js return ENOSYS
// rather than panicking, producing a silent-wrong code path if the user
// forgets to call anystore.SetVFS.
//
// This init runs after osfuncs_vfs.go's var block (Go initializes package
// vars before running init functions; init order within a package follows
// filename order, and "osfuncs_vfs.go" < "osfuncs_vfs_js.go"). It replaces
// the defaults with panic stubs so a missing SetVFS call fails loudly at
// the first OS operation.
//
// anystore.SetVFS(vfs) installs the caller's implementation over these
// stubs. anystore.ResetVFS reinstalls the stubs — the correct "unset"
// state under wasm.
func init() {
	defaultOpenFile = func(name string, _ int, _ os.FileMode) (File, error) {
		panic("btree: SetVFS not called — anystore on wasm requires a VFS backend (path=" + name + ")")
	}
	defaultRemove = func(name string) error {
		panic("btree: SetVFS not called — anystore on wasm requires a VFS backend (path=" + name + ")")
	}
	defaultFdatasync = func(File) error {
		panic("btree: SetVFS not called — anystore on wasm requires a VFS backend")
	}
	osOpenFile = defaultOpenFile
	osRemove = defaultRemove
	fdatasync = defaultFdatasync
}
```

- [ ] **Step 4.2: Verify wasm build still succeeds**

Run: `GOOS=js GOARCH=wasm go build ./internal/btree/...`
Expected: exit 0, no output.

- [ ] **Step 4.3: Verify non-wasm builds are unaffected**

Run: `go build ./...`
Expected: exit 0, no output.

Run: `go build -tags vfs ./...`
Expected: exit 0, no output.

- [ ] **Step 4.4: Commit**

```bash
git add internal/btree/osfuncs_vfs_js.go
git -c commit.gpgsign=false commit -m "btree: panic stubs for default OS funcs under js/wasm

Wasm's default os.OpenFile / os.Remove / f.Sync return ENOSYS rather
than panicking, which would silently bypass a missing SetVFS call.
Override the defaults via init() so the first OS operation panics with
a clear message when the caller forgets to install a VFS backend."
```

---

## Task 5: Verify full-module wasm build passes

End-to-end build gate for the core changes. If this fails, something in `db.go` / `sentinel` / elsewhere is referencing an unavailable symbol and needs a separate fix.

**Files:** none (verification only)

- [ ] **Step 5.1: Run full-module wasm build**

Run: `GOOS=js GOARCH=wasm go build ./...`
Expected: exit 0, no output.

If this fails with new symbol-not-found errors (not the ones from Step 1.2), stop and report — additional `!unix` / `!js` splits are needed in the offending files. This was flagged as a known risk in §8 of the spec (sentinel/top-level os.Stat).

- [ ] **Step 5.2: Run full test suite at baseline to confirm no regressions**

Run: `go test ./... -count=1 -short -timeout=180s`
Expected: `ok` for all packages.

No commit — verification only.

---

## Task 6: Create `wasm/` submodule skeleton

**Files:**
- Create: `wasm/go.mod`
- Create: `wasm/.gitignore`
- Create: `wasm/Makefile`
- Create: `wasm/README.md`

- [ ] **Step 6.1: Create `wasm/` directory**

Run: `mkdir -p wasm/js`

- [ ] **Step 6.2: Create `wasm/go.mod`**

Write this exact content:

```
module github.com/anyproto/any-store/wasm

go 1.23.0

toolchain go1.24.1

require github.com/anyproto/any-store v0.0.0

replace github.com/anyproto/any-store => ../
```

- [ ] **Step 6.3: Create `wasm/.gitignore`**

Write this exact content:

```
/dist/
```

- [ ] **Step 6.4: Create `wasm/Makefile`**

Write this exact content:

```make
.PHONY: build clean

DIST := dist

build:
	mkdir -p $(DIST)
	GOOS=js GOARCH=wasm go build -o $(DIST)/anystore.wasm .
	cp "$$(go env GOROOT)/lib/wasm/wasm_exec.js" $(DIST)/
	cp js/loader.js $(DIST)/
	gzip -9 -k -f $(DIST)/anystore.wasm

clean:
	rm -rf $(DIST)
```

- [ ] **Step 6.5: Create `wasm/README.md`**

Write this exact content:

````markdown
# any-store wasm

Minimal wasm build of any-store with auto-detected browser VFS backends
(OPFS → IndexedDB → in-memory). No JS API is exported here; this is a
smoke-test binary. Downstream SDKs import this module and expose their
own JS bridge.

## Build

```
make build
```

Produces `dist/anystore.wasm`, `dist/wasm_exec.js`, `dist/loader.js`
and a gzipped `dist/anystore.wasm.gz`.

## Manual smoke test in a browser

1. `make build`
2. Create `dist/index.html` (not committed):

   ```html
   <!doctype html>
   <html><head><meta charset=utf-8></head><body>
   <script src="wasm_exec.js"></script>
   <script type="module">
     import { loadAnyStoreWasm } from './loader.js';
     loadAnyStoreWasm().then(() => console.log("[test] wasm loaded"));
   </script>
   </body></html>
   ```

3. Serve the `dist/` directory: `python3 -m http.server 8000 --directory dist`
4. Open `http://localhost:8000/` in Chrome / Firefox.
5. Open DevTools console. Expected output:

   ```
   [test] wasm loaded
   anystore wasm smoke ok, backend=indexeddb
   ```

   Backend name will be `opfs` inside a dedicated Worker, `indexeddb` in
   the main page context, or `memory` if neither API is available.

## Submodule layout

- `main.go` — smoke-test entry point. Opens a file-backed DB, inserts and
  reads one document, logs the backend name.
- `vfs_detect.go` — `DetectVFS()` picks OPFS / IDB / MemFS and wires it
  via `anystore.SetVFS`.
- `vfs_opfs.go` / `vfs_indexeddb.go` / `memfs.go` — VFS backends.
- `vfs_js.go` — shared JS interop helpers.
- `js/loader.js` — minimal JS wrapper around `wasm_exec.js`.

All Go files carry `//go:build js && wasm`. The submodule is a separate
Go module (see `go.mod`) so `syscall/js` does not leak into the root
module's transitive dependency graph.
````

- [ ] **Step 6.6: Verify `go mod tidy` in the submodule**

Run: `cd wasm && go mod tidy && cd ..`
Expected: creates `wasm/go.sum`, no errors.

- [ ] **Step 6.7: Verify non-wasm `go build ./...` in the submodule is a no-op**

Run: `cd wasm && go build ./... && cd ..`
Expected: exit 0, no output. (No Go source files yet carrying the wasm build tag means nothing to build.)

- [ ] **Step 6.8: Commit**

```bash
git add wasm/go.mod wasm/go.sum wasm/.gitignore wasm/Makefile wasm/README.md
git -c commit.gpgsign=false commit -m "wasm: scaffolding for separate submodule

Empty Go module at wasm/ with a replace directive pointing at the root
module. Makefile targets GOOS=js GOARCH=wasm build, copies wasm_exec.js
from the toolchain, emits a gzipped artifact for deployment."
```

---

## Task 7: Port `memfs.go`

**Files:**
- Create: `wasm/memfs.go`

- [ ] **Step 7.1: Create `wasm/memfs.go`**

Write this exact content (ported verbatim from `/home/dev/work/any-crdt-sdk/wasm/memfs.go`):

```go
//go:build js && wasm

package main

import (
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	anystore "github.com/anyproto/any-store"
)

// MemFS implements a purely in-memory filesystem.
// Safe for concurrent use.
type MemFS struct {
	mu    sync.RWMutex
	files map[string]*fileData
}

type fileData struct {
	mu      sync.RWMutex
	data    []byte
	mode    fs.FileMode
	modTime time.Time
	isDir   bool
}

func (m *MemFS) ensureInit() {
	if m.files == nil {
		m.files = make(map[string]*fileData)
	}
}

func (m *MemFS) OpenFile(name string, flag int, perm fs.FileMode) (anystore.File, error) {
	name = filepath.Clean(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureInit()

	fd, exists := m.files[name]
	if !exists {
		if flag&os.O_CREATE == 0 {
			return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrNotExist}
		}
		// Auto-create parent directories for O_CREATE.
		dir := filepath.Dir(name)
		if dir != "." && dir != "/" {
			for _, seg := range mkdirSegments(dir) {
				if _, ok := m.files[seg]; !ok {
					m.files[seg] = &fileData{isDir: true, mode: 0755, modTime: time.Now()}
				}
			}
		}
		fd = &fileData{mode: perm, modTime: time.Now()}
		m.files[name] = fd
	} else if flag&os.O_EXCL != 0 && flag&os.O_CREATE != 0 {
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrExist}
	}

	if flag&os.O_TRUNC != 0 {
		fd.mu.Lock()
		fd.data = fd.data[:0]
		fd.mu.Unlock()
	}

	return &memFile{name: name, fd: fd}, nil
}

func (m *MemFS) MkdirAll(path string, perm fs.FileMode) error {
	path = filepath.Clean(path)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureInit()

	for _, seg := range mkdirSegments(path) {
		if fd, ok := m.files[seg]; ok {
			if !fd.isDir {
				return &os.PathError{Op: "mkdir", Path: seg, Err: os.ErrExist}
			}
			continue
		}
		m.files[seg] = &fileData{isDir: true, mode: perm, modTime: time.Now()}
	}
	return nil
}

func (m *MemFS) Remove(name string) error {
	name = filepath.Clean(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureInit()

	if _, ok := m.files[name]; !ok {
		return &os.PathError{Op: "remove", Path: name, Err: os.ErrNotExist}
	}
	delete(m.files, name)
	return nil
}

func (m *MemFS) Stat(name string) (fs.FileInfo, error) {
	name = filepath.Clean(name)
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.ensureInit()

	fd, ok := m.files[name]
	if !ok {
		return nil, &os.PathError{Op: "stat", Path: name, Err: os.ErrNotExist}
	}
	fd.mu.RLock()
	defer fd.mu.RUnlock()
	return &memFileInfo{
		name:    filepath.Base(name),
		size:    int64(len(fd.data)),
		mode:    fd.mode,
		modTime: fd.modTime,
		isDir:   fd.isDir,
	}, nil
}

func (m *MemFS) ReadFile(name string) ([]byte, error) {
	name = filepath.Clean(name)
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.ensureInit()

	fd, ok := m.files[name]
	if !ok {
		return nil, &os.PathError{Op: "read", Path: name, Err: os.ErrNotExist}
	}
	fd.mu.RLock()
	defer fd.mu.RUnlock()
	cp := make([]byte, len(fd.data))
	copy(cp, fd.data)
	return cp, nil
}

func (m *MemFS) WriteFile(name string, data []byte, perm fs.FileMode) error {
	name = filepath.Clean(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureInit()

	fd, ok := m.files[name]
	if !ok {
		fd = &fileData{mode: perm}
		m.files[name] = fd
	}
	fd.mu.Lock()
	defer fd.mu.Unlock()
	fd.data = make([]byte, len(data))
	copy(fd.data, data)
	fd.modTime = time.Now()
	return nil
}

// --- memFile implements anystore.File ---

type memFile struct {
	name string
	fd   *fileData
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	f.fd.mu.RLock()
	defer f.fd.mu.RUnlock()
	if int(off) >= len(f.fd.data) {
		return 0, &os.PathError{Op: "read", Path: f.name, Err: os.ErrClosed}
	}
	n := copy(p, f.fd.data[off:])
	return n, nil
}

func (f *memFile) WriteAt(p []byte, off int64) (int, error) {
	f.fd.mu.Lock()
	defer f.fd.mu.Unlock()
	end := int(off) + len(p)
	if end > len(f.fd.data) {
		grown := make([]byte, end)
		copy(grown, f.fd.data)
		f.fd.data = grown
	}
	copy(f.fd.data[off:], p)
	f.fd.modTime = time.Now()
	return len(p), nil
}

func (f *memFile) Stat() (fs.FileInfo, error) {
	f.fd.mu.RLock()
	defer f.fd.mu.RUnlock()
	return &memFileInfo{
		name:    filepath.Base(f.name),
		size:    int64(len(f.fd.data)),
		mode:    f.fd.mode,
		modTime: f.fd.modTime,
		isDir:   f.fd.isDir,
	}, nil
}

func (f *memFile) Truncate(size int64) error {
	f.fd.mu.Lock()
	defer f.fd.mu.Unlock()
	if int(size) < len(f.fd.data) {
		f.fd.data = f.fd.data[:size]
	} else if int(size) > len(f.fd.data) {
		grown := make([]byte, size)
		copy(grown, f.fd.data)
		f.fd.data = grown
	}
	f.fd.modTime = time.Now()
	return nil
}

func (f *memFile) Sync() error  { return nil }
func (f *memFile) Fd() uintptr  { return 0 }
func (f *memFile) Close() error { return nil }

// --- memFileInfo implements fs.FileInfo ---

type memFileInfo struct {
	name    string
	size    int64
	mode    fs.FileMode
	modTime time.Time
	isDir   bool
}

func (fi *memFileInfo) Name() string       { return fi.name }
func (fi *memFileInfo) Size() int64        { return fi.size }
func (fi *memFileInfo) Mode() fs.FileMode  { return fi.mode }
func (fi *memFileInfo) ModTime() time.Time { return fi.modTime }
func (fi *memFileInfo) IsDir() bool        { return fi.isDir }
func (fi *memFileInfo) Sys() interface{}   { return nil }

// mkdirSegments returns cumulative path segments for MkdirAll-style creation.
// e.g. "/tmp/any-store-wasm" -> ["/tmp", "/tmp/any-store-wasm"]
func mkdirSegments(path string) []string {
	parts := splitPath(filepath.Clean(path))
	segs := make([]string, len(parts))
	for i, p := range parts {
		if i == 0 {
			segs[i] = p
		} else {
			segs[i] = segs[i-1] + "/" + p
		}
	}
	return segs
}

// splitPath breaks a cleaned path into its components.
func splitPath(path string) []string {
	if path == "/" || path == "." {
		return []string{path}
	}
	var parts []string
	for {
		dir, base := filepath.Split(path)
		if base != "" {
			parts = append([]string{base}, parts...)
		}
		path = filepath.Clean(dir)
		if path == "." || path == "/" {
			if path == "/" {
				parts = append([]string{"/"}, parts...)
			}
			break
		}
	}
	return parts
}
```

- [ ] **Step 7.2: Verify compile (wasm mode only — file is no-op on non-wasm)**

Run: `cd wasm && GOOS=js GOARCH=wasm go build ./... && cd ..`
Expected: the build will fail with `undefined: goSliceToJS` / `undefined: jsToGoSlice` — those symbols live in `vfs_js.go` which we port next. For now, confirm the error is ONLY about those two symbols (not memfs.go syntax issues).

- [ ] **Step 7.3: Commit**

```bash
git add wasm/memfs.go
git -c commit.gpgsign=false commit -m "wasm: port MemFS backend from any-crdt-sdk/wasm

In-memory fs.FS implementation satisfying anystore.File and fs.FileInfo.
Used as the always-available fallback when OPFS and IndexedDB are
unavailable (e.g. non-secure origins, older browsers)."
```

---

## Task 8: Port `vfs_js.go` (shared JS interop helpers)

**Files:**
- Create: `wasm/vfs_js.go`

- [ ] **Step 8.1: Create `wasm/vfs_js.go`**

Write this exact content (ported verbatim from `/home/dev/work/any-crdt-sdk/wasm/vfs_js.go`):

```go
//go:build js && wasm

package main

import (
	"fmt"
	"syscall/js"
)

// awaitPromise blocks the calling goroutine until a JS Promise resolves or
// rejects, returning either the resolved value or an error.
func awaitPromise(promise js.Value) (js.Value, error) {
	ch := make(chan struct{})
	var result js.Value
	var jsErr error
	then := js.FuncOf(func(this js.Value, args []js.Value) interface{} {
		result = args[0]
		close(ch)
		return nil
	})
	catch := js.FuncOf(func(this js.Value, args []js.Value) interface{} {
		jsErr = fmt.Errorf("%s", args[0].Call("toString").String())
		close(ch)
		return nil
	})
	promise.Call("then", then).Call("catch", catch)
	<-ch
	then.Release()
	catch.Release()
	return result, jsErr
}

// awaitIDBRequest waits for an IDBRequest's onsuccess/onerror and returns the
// result. The callbacks are one-shot so their js.Func values are released
// immediately after firing.
func awaitIDBRequest(req js.Value) (js.Value, error) {
	ch := make(chan struct{})
	var result js.Value
	var reqErr error

	onSuccess := js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		result = args[0].Get("target").Get("result")
		close(ch)
		return nil
	})
	onError := js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		errVal := req.Get("error")
		if !errVal.IsNull() && !errVal.IsUndefined() {
			reqErr = fmt.Errorf("indexeddb: %s", errVal.Call("toString").String())
		} else {
			reqErr = fmt.Errorf("indexeddb: unknown error")
		}
		close(ch)
		return nil
	})

	req.Set("onsuccess", onSuccess)
	req.Set("onerror", onError)
	<-ch
	onSuccess.Release()
	onError.Release()
	return result, reqErr
}

// goSliceToJS copies a Go []byte into a new JS Uint8Array.
func goSliceToJS(data []byte) js.Value {
	arr := js.Global().Get("Uint8Array").New(len(data))
	js.CopyBytesToJS(arr, data)
	return arr
}

// jsToGoSlice copies a JS Uint8Array or ArrayBuffer into a Go []byte.
func jsToGoSlice(arr js.Value, length int) []byte {
	buf := make([]byte, length)
	if arr.Get("constructor").Get("name").String() == "ArrayBuffer" {
		arr = js.Global().Get("Uint8Array").New(arr)
	}
	js.CopyBytesToGo(buf, arr)
	return buf
}
```

- [ ] **Step 8.2: Verify memfs.go + vfs_js.go compile together**

Run: `cd wasm && GOOS=js GOARCH=wasm go build ./... && cd ..`
Expected: exit 0, no output. (Enough files in place to form a valid `package main` for wasm — actually, `main` still needs `func main()`, so this may still fail with "function main is undeclared" — that's fine and expected.)

If the error is `function main is undeclared in the main package`, proceed. Any other error is a bug.

- [ ] **Step 8.3: Commit**

```bash
git add wasm/vfs_js.go
git -c commit.gpgsign=false commit -m "wasm: port JS interop helpers (awaitPromise, slice converters)"
```

---

## Task 9: Port `vfs_indexeddb.go`

**Files:**
- Create: `wasm/vfs_indexeddb.go`

- [ ] **Step 9.1: Create `wasm/vfs_indexeddb.go`**

Write this exact content (ported verbatim from `/home/dev/work/any-crdt-sdk/wasm/vfs_indexeddb.go`):

```go
//go:build js && wasm

package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall/js"
	"time"

	anystore "github.com/anyproto/any-store"
)

// ---------------------------------------------------------------------------
// IDBFS – filesystem backed by IndexedDB
// ---------------------------------------------------------------------------

// IDBFS implements a filesystem using the browser IndexedDB API. Files are loaded
// into memory on OpenFile; reads and writes operate on the in-memory buffer.
// Sync and Close flush dirty data back to IndexedDB.
type IDBFS struct {
	mu sync.RWMutex
	db js.Value // IDBDatabase reference
}

// InitIDBFS opens (or creates) the "anystore-vfs" IndexedDB database and
// returns a ready-to-use *IDBFS.
func InitIDBFS() (*IDBFS, error) {
	indexedDB := js.Global().Get("indexedDB")
	if indexedDB.IsUndefined() || indexedDB.IsNull() {
		self := js.Global().Get("self")
		if !self.IsUndefined() && !self.IsNull() {
			indexedDB = self.Get("indexedDB")
		}
	}
	if indexedDB.IsUndefined() || indexedDB.IsNull() {
		return nil, fmt.Errorf("indexeddb: API not available")
	}

	request := indexedDB.Call("open", "anystore-vfs", 2)

	ch := make(chan struct{})
	var database js.Value
	var openErr error

	onUpgrade := js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		db := args[0].Get("target").Get("result")
		names := db.Get("objectStoreNames")
		if names.IsUndefined() || names.IsNull() || !names.Call("contains", "files").Bool() {
			opts := js.Global().Get("Object").New()
			opts.Set("keyPath", "path")
			db.Call("createObjectStore", "files", opts)
		}
		return nil
	})
	onSuccess := js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
		database = args[0].Get("target").Get("result")
		close(ch)
		return nil
	})
	onError := js.FuncOf(func(_ js.Value, _ []js.Value) interface{} {
		openErr = fmt.Errorf("indexeddb: open failed: %s", request.Get("error").Call("toString").String())
		close(ch)
		return nil
	})

	request.Set("onupgradeneeded", onUpgrade)
	request.Set("onsuccess", onSuccess)
	request.Set("onerror", onError)

	<-ch
	onUpgrade.Release()
	onSuccess.Release()
	onError.Release()

	if openErr != nil {
		return nil, openErr
	}
	return &IDBFS{db: database}, nil
}

// ---------------------------------------------------------------------------
// IDB transaction helpers
// ---------------------------------------------------------------------------

func (f *IDBFS) idbGet(path string) (js.Value, error) {
	tx := f.db.Call("transaction", "files", "readonly")
	store := tx.Call("objectStore", "files")
	req := store.Call("get", path)
	return awaitIDBRequest(req)
}

func (f *IDBFS) idbPut(record js.Value) error {
	tx := f.db.Call("transaction", "files", "readwrite")
	store := tx.Call("objectStore", "files")
	req := store.Call("put", record)
	_, err := awaitIDBRequest(req)
	return err
}

func (f *IDBFS) idbDelete(path string) error {
	tx := f.db.Call("transaction", "files", "readwrite")
	store := tx.Call("objectStore", "files")
	req := store.Call("delete", path)
	_, err := awaitIDBRequest(req)
	return err
}

// makeRecord builds a JS object suitable for storing in the "files" object store.
func makeRecord(path string, data []byte, isDir bool, mode fs.FileMode, modTime time.Time) js.Value {
	rec := js.Global().Get("Object").New()
	rec.Set("path", path)
	if data != nil {
		rec.Set("data", goSliceToJS(data).Get("buffer"))
	} else {
		rec.Set("data", js.Null())
	}
	rec.Set("isDir", isDir)
	rec.Set("mode", int(mode))
	rec.Set("modTime", float64(modTime.UnixMilli()))
	return rec
}

// ---------------------------------------------------------------------------
// FS implementation
// ---------------------------------------------------------------------------

func (f *IDBFS) OpenFile(name string, flag int, perm fs.FileMode) (anystore.File, error) {
	name = filepath.Clean(name)

	f.mu.Lock()
	defer f.mu.Unlock()

	result, err := f.idbGet(name)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}

	exists := !result.IsUndefined() && !result.IsNull()

	if !exists {
		if flag&os.O_CREATE == 0 {
			return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrNotExist}
		}
		// Auto-create parent directories.
		dir := filepath.Dir(name)
		if dir != "." && dir != "/" {
			for _, seg := range mkdirSegments(dir) {
				segResult, segErr := f.idbGet(seg)
				if segErr != nil {
					return nil, &os.PathError{Op: "open", Path: name, Err: segErr}
				}
				if segResult.IsUndefined() || segResult.IsNull() {
					rec := makeRecord(seg, nil, true, 0755, time.Now())
					if putErr := f.idbPut(rec); putErr != nil {
						return nil, &os.PathError{Op: "open", Path: name, Err: putErr}
					}
				}
			}
		}
		// Create new empty file in IDB.
		now := time.Now()
		rec := makeRecord(name, []byte{}, false, perm, now)
		if putErr := f.idbPut(rec); putErr != nil {
			return nil, &os.PathError{Op: "open", Path: name, Err: putErr}
		}
		return &IDBFile{
			fs:      f,
			name:    name,
			data:    []byte{},
			mode:    perm,
			modTime: now,
		}, nil
	}

	// Entry exists.
	if flag&os.O_EXCL != 0 && flag&os.O_CREATE != 0 {
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrExist}
	}

	// Copy data from IDB into memory.
	var fileData []byte
	dataVal := result.Get("data")
	if !dataVal.IsNull() && !dataVal.IsUndefined() {
		length := dataVal.Get("byteLength").Int()
		fileData = jsToGoSlice(dataVal, length)
	}

	mode := fs.FileMode(result.Get("mode").Int())
	modTime := time.UnixMilli(int64(result.Get("modTime").Float()))

	idbFile := &IDBFile{
		fs:      f,
		name:    name,
		data:    fileData,
		mode:    mode,
		modTime: modTime,
	}

	if flag&os.O_TRUNC != 0 {
		idbFile.data = idbFile.data[:0]
		idbFile.dirty = true
	}

	return idbFile, nil
}

func (f *IDBFS) MkdirAll(path string, perm fs.FileMode) error {
	path = filepath.Clean(path)

	f.mu.Lock()
	defer f.mu.Unlock()

	for _, seg := range mkdirSegments(path) {
		result, err := f.idbGet(seg)
		if err != nil {
			return &os.PathError{Op: "mkdir", Path: seg, Err: err}
		}
		if !result.IsUndefined() && !result.IsNull() {
			if !result.Get("isDir").Bool() {
				return &os.PathError{Op: "mkdir", Path: seg, Err: os.ErrExist}
			}
			continue
		}
		rec := makeRecord(seg, nil, true, perm, time.Now())
		if putErr := f.idbPut(rec); putErr != nil {
			return &os.PathError{Op: "mkdir", Path: seg, Err: putErr}
		}
	}
	return nil
}

func (f *IDBFS) Remove(name string) error {
	name = filepath.Clean(name)

	f.mu.Lock()
	defer f.mu.Unlock()

	result, err := f.idbGet(name)
	if err != nil {
		return &os.PathError{Op: "remove", Path: name, Err: err}
	}
	if result.IsUndefined() || result.IsNull() {
		return &os.PathError{Op: "remove", Path: name, Err: os.ErrNotExist}
	}
	if delErr := f.idbDelete(name); delErr != nil {
		return &os.PathError{Op: "remove", Path: name, Err: delErr}
	}
	return nil
}

func (f *IDBFS) Stat(name string) (fs.FileInfo, error) {
	name = filepath.Clean(name)

	f.mu.RLock()
	defer f.mu.RUnlock()

	result, err := f.idbGet(name)
	if err != nil {
		return nil, &os.PathError{Op: "stat", Path: name, Err: err}
	}
	if result.IsUndefined() || result.IsNull() {
		return nil, &os.PathError{Op: "stat", Path: name, Err: os.ErrNotExist}
	}

	var size int64
	dataVal := result.Get("data")
	if !dataVal.IsNull() && !dataVal.IsUndefined() {
		size = int64(dataVal.Get("byteLength").Int())
	}

	return &memFileInfo{
		name:    filepath.Base(name),
		size:    size,
		mode:    fs.FileMode(result.Get("mode").Int()),
		modTime: time.UnixMilli(int64(result.Get("modTime").Float())),
		isDir:   result.Get("isDir").Bool(),
	}, nil
}

func (f *IDBFS) ReadFile(name string) ([]byte, error) {
	name = filepath.Clean(name)

	f.mu.RLock()
	defer f.mu.RUnlock()

	result, err := f.idbGet(name)
	if err != nil {
		return nil, &os.PathError{Op: "read", Path: name, Err: err}
	}
	if result.IsUndefined() || result.IsNull() {
		return nil, &os.PathError{Op: "read", Path: name, Err: os.ErrNotExist}
	}

	dataVal := result.Get("data")
	if dataVal.IsNull() || dataVal.IsUndefined() {
		return []byte{}, nil
	}
	length := dataVal.Get("byteLength").Int()
	return jsToGoSlice(dataVal, length), nil
}

func (f *IDBFS) WriteFile(name string, data []byte, perm fs.FileMode) error {
	name = filepath.Clean(name)

	f.mu.Lock()
	defer f.mu.Unlock()

	rec := makeRecord(name, data, false, perm, time.Now())
	if putErr := f.idbPut(rec); putErr != nil {
		return &os.PathError{Op: "write", Path: name, Err: putErr}
	}
	return nil
}

// ---------------------------------------------------------------------------
// IDBFile – anystore.File backed by an in-memory buffer + IndexedDB persistence
// ---------------------------------------------------------------------------

// IDBFile holds a file's contents in memory. Reads and writes operate on the
// in-memory buffer. Sync and Close flush dirty data back to IndexedDB.
type IDBFile struct {
	fs      *IDBFS
	name    string
	mu      sync.RWMutex
	data    []byte
	mode    fs.FileMode
	modTime time.Time
	dirty   bool
}

func (f *IDBFile) ReadAt(p []byte, off int64) (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if int(off) >= len(f.data) {
		return 0, &os.PathError{Op: "read", Path: f.name, Err: os.ErrClosed}
	}
	n := copy(p, f.data[off:])
	return n, nil
}

func (f *IDBFile) WriteAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	end := int(off) + len(p)
	if end > len(f.data) {
		grown := make([]byte, end)
		copy(grown, f.data)
		f.data = grown
	}
	copy(f.data[off:], p)
	f.modTime = time.Now()
	f.dirty = true
	return len(p), nil
}

func (f *IDBFile) Stat() (fs.FileInfo, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return &memFileInfo{
		name:    filepath.Base(f.name),
		size:    int64(len(f.data)),
		mode:    f.mode,
		modTime: f.modTime,
		isDir:   false,
	}, nil
}

func (f *IDBFile) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if int(size) < len(f.data) {
		f.data = f.data[:size]
	} else if int(size) > len(f.data) {
		grown := make([]byte, size)
		copy(grown, f.data)
		f.data = grown
	}
	f.modTime = time.Now()
	f.dirty = true
	return nil
}

func (f *IDBFile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.dirty {
		return nil
	}
	rec := makeRecord(f.name, f.data, false, f.mode, f.modTime)
	f.fs.mu.Lock()
	err := f.fs.idbPut(rec)
	f.fs.mu.Unlock()
	if err != nil {
		return &os.PathError{Op: "sync", Path: f.name, Err: err}
	}
	f.dirty = false
	return nil
}

// Fd returns 0 since IndexedDB has no real file descriptor.
func (f *IDBFile) Fd() uintptr { return 0 }

func (f *IDBFile) Close() error {
	return f.Sync()
}
```

- [ ] **Step 9.2: Verify compile**

Run: `cd wasm && GOOS=js GOARCH=wasm go build ./... && cd ..`
Expected: still fails only with `function main is undeclared`. No other errors.

- [ ] **Step 9.3: Commit**

```bash
git add wasm/vfs_indexeddb.go
git -c commit.gpgsign=false commit -m "wasm: port IndexedDB backend (IDBFS + IDBFile)

In-memory buffer with Sync()/Close() flushing dirty pages back to an
IDBDatabase objectStore. Universal persistent fallback when OPFS is
unavailable (main page context, all modern browsers)."
```

---

## Task 10: Port `vfs_opfs.go`

**Files:**
- Create: `wasm/vfs_opfs.go`

- [ ] **Step 10.1: Create `wasm/vfs_opfs.go`**

Write this exact content (ported verbatim from `/home/dev/work/any-crdt-sdk/wasm/vfs_opfs.go`):

```go
//go:build js && wasm

package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall/js"
	"time"

	anystore "github.com/anyproto/any-store"
)

// OPFSFS implements a filesystem using the Origin Private File System (OPFS) browser API.
// It must be used in a Web Worker context where synchronous access handles are
// available.
type OPFSFS struct {
	root js.Value // FileSystemDirectoryHandle for the OPFS root
}

// InitOPFS creates an OPFSFS by obtaining the OPFS root directory via
// navigator.storage.getDirectory().
func InitOPFS() (*OPFSFS, error) {
	storage := js.Global().Get("navigator").Get("storage")
	promise := storage.Call("getDirectory")
	root, err := awaitPromise(promise)
	if err != nil {
		return nil, fmt.Errorf("opfs: get root directory: %w", err)
	}
	return &OPFSFS{root: root}, nil
}

// getDirectoryHandle traverses directory segments starting from root,
// optionally creating them. Returns the final directory handle.
func (o *OPFSFS) getDirectoryHandle(path string, create bool) (js.Value, error) {
	segments := splitPathSegments(path)
	dir := o.root
	for _, seg := range segments {
		opts := js.Global().Get("Object").New()
		opts.Set("create", create)
		promise := dir.Call("getDirectoryHandle", seg, opts)
		next, err := awaitPromise(promise)
		if err != nil {
			if !create && isJSNotFound(err) {
				return js.Value{}, os.ErrNotExist
			}
			return js.Value{}, fmt.Errorf("opfs: getDirectoryHandle(%q): %w", seg, err)
		}
		dir = next
	}
	return dir, nil
}

// getParentAndBase splits a path into the parent directory handle and the
// base filename. The parent directories are optionally created.
func (o *OPFSFS) getParentAndBase(name string, createParent bool) (js.Value, string, error) {
	name = filepath.Clean(name)
	dir := filepath.Dir(name)
	base := filepath.Base(name)
	parent, err := o.getDirectoryHandle(dir, createParent)
	if err != nil {
		return js.Value{}, "", err
	}
	return parent, base, nil
}

// OpenFile opens (or creates) a file in OPFS and returns a sync access handle
// wrapped as an anystore.File.
func (o *OPFSFS) OpenFile(name string, flag int, perm fs.FileMode) (anystore.File, error) {
	name = filepath.Clean(name)
	createFile := flag&os.O_CREATE != 0

	parent, base, err := o.getParentAndBase(name, createFile)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}

	if flag&os.O_EXCL != 0 && flag&os.O_CREATE != 0 {
		// Check if file already exists -- getFileHandle without create throws
		// if not found, so we try without create first.
		checkOpts := js.Global().Get("Object").New()
		checkOpts.Set("create", false)
		checkPromise := parent.Call("getFileHandle", base, checkOpts)
		_, checkErr := awaitPromise(checkPromise)
		if checkErr == nil {
			return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrExist}
		}
	}

	opts := js.Global().Get("Object").New()
	opts.Set("create", createFile)
	fhPromise := parent.Call("getFileHandle", base, opts)
	fileHandle, err := awaitPromise(fhPromise)
	if err != nil {
		if !createFile && isJSNotFound(err) {
			return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrNotExist}
		}
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}

	sahPromise := fileHandle.Call("createSyncAccessHandle")
	syncHandle, err := awaitPromise(sahPromise)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: fmt.Errorf("createSyncAccessHandle: %w", err)}
	}

	if flag&os.O_TRUNC != 0 {
		syncHandle.Call("truncate", 0)
	}

	return &OPFSFile{
		handle: syncHandle,
		name:   base,
	}, nil
}

// MkdirAll creates a directory path and all parents that do not yet exist.
func (o *OPFSFS) MkdirAll(path string, perm fs.FileMode) error {
	_, err := o.getDirectoryHandle(path, true)
	return err
}

// Remove deletes a file or empty directory by name.
func (o *OPFSFS) Remove(name string) error {
	name = filepath.Clean(name)
	parent, base, err := o.getParentAndBase(name, false)
	if err != nil {
		return &os.PathError{Op: "remove", Path: name, Err: err}
	}
	promise := parent.Call("removeEntry", base)
	if _, err := awaitPromise(promise); err != nil {
		return &os.PathError{Op: "remove", Path: name, Err: err}
	}
	return nil
}

// Stat returns file info for the named file. It opens a temporary sync access
// handle to obtain the file size.
func (o *OPFSFS) Stat(name string) (fs.FileInfo, error) {
	name = filepath.Clean(name)
	parent, base, err := o.getParentAndBase(name, false)
	if err != nil {
		return nil, &os.PathError{Op: "stat", Path: name, Err: err}
	}

	// Try as file first.
	opts := js.Global().Get("Object").New()
	opts.Set("create", false)
	fhPromise := parent.Call("getFileHandle", base, opts)
	fileHandle, fhErr := awaitPromise(fhPromise)
	if fhErr == nil {
		sahPromise := fileHandle.Call("createSyncAccessHandle")
		syncHandle, sahErr := awaitPromise(sahPromise)
		if sahErr != nil {
			return nil, &os.PathError{Op: "stat", Path: name, Err: fmt.Errorf("createSyncAccessHandle: %w", sahErr)}
		}
		size := syncHandle.Call("getSize").Int()
		syncHandle.Call("close")
		return &opfsFileInfo{
			name:  base,
			size:  int64(size),
			mode:  0644,
			isDir: false,
		}, nil
	}

	// Try as directory.
	dirOpts := js.Global().Get("Object").New()
	dirOpts.Set("create", false)
	dhPromise := parent.Call("getDirectoryHandle", base, dirOpts)
	_, dhErr := awaitPromise(dhPromise)
	if dhErr == nil {
		return &opfsFileInfo{
			name:  base,
			size:  0,
			mode:  fs.ModeDir | 0755,
			isDir: true,
		}, nil
	}

	return nil, &os.PathError{Op: "stat", Path: name, Err: os.ErrNotExist}
}

// ReadFile reads the entire contents of the named file.
func (o *OPFSFS) ReadFile(name string) ([]byte, error) {
	f, err := o.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if size == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, size)
	n, err := f.ReadAt(buf, 0)
	if err != nil && n == 0 {
		return nil, err
	}
	return buf[:n], nil
}

// WriteFile writes data to the named file, creating it if necessary and
// truncating it if it already exists.
func (o *OPFSFS) WriteFile(name string, data []byte, perm fs.FileMode) error {
	f, err := o.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	_, err = f.WriteAt(data, 0)
	if err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// isJSNotFound returns true when the error originates from a JavaScript
// DOMException with name "NotFoundError" — the standard OPFS error when
// getFileHandle or getDirectoryHandle is called with {create: false} on a
// non-existent entry.
func isJSNotFound(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "NotFoundError") || strings.Contains(s, "not found")
}

// splitPathSegments splits a cleaned path into directory segments, stripping
// leading slashes and dots.
func splitPathSegments(path string) []string {
	path = filepath.Clean(path)
	path = strings.TrimPrefix(path, "/")
	path = strings.TrimPrefix(path, ".")
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return nil
	}
	return strings.Split(path, "/")
}

// =========================================================================
// OPFSFile implements anystore.File using a FileSystemSyncAccessHandle.
// =========================================================================

// OPFSFile wraps a OPFS FileSystemSyncAccessHandle. The read/write/truncate/
// flush/close/getSize methods on the sync access handle are synchronous (they
// return values directly, not Promises) when used in a Worker context.
type OPFSFile struct {
	handle js.Value // FileSystemSyncAccessHandle
	name   string
}

// ReadAt reads len(p) bytes from the file starting at byte offset off.
func (f *OPFSFile) ReadAt(p []byte, off int64) (int, error) {
	buf := js.Global().Get("Uint8Array").New(len(p))
	opts := js.Global().Get("Object").New()
	opts.Set("at", off)
	n := f.handle.Call("read", buf, opts).Int()
	js.CopyBytesToGo(p, buf)
	return n, nil
}

// WriteAt writes len(p) bytes to the file starting at byte offset off.
func (f *OPFSFile) WriteAt(p []byte, off int64) (int, error) {
	buf := goSliceToJS(p)
	opts := js.Global().Get("Object").New()
	opts.Set("at", off)
	n := f.handle.Call("write", buf, opts).Int()
	return n, nil
}

// Stat returns file information including the current size via getSize().
func (f *OPFSFile) Stat() (fs.FileInfo, error) {
	size := f.handle.Call("getSize").Int()
	return &opfsFileInfo{
		name: f.name,
		size: int64(size),
		mode: 0644,
	}, nil
}

// Truncate changes the size of the file.
func (f *OPFSFile) Truncate(size int64) error {
	f.handle.Call("truncate", size)
	return nil
}

// Sync flushes pending writes to stable storage.
func (f *OPFSFile) Sync() error {
	f.handle.Call("flush")
	return nil
}

// Fd returns 0 since OPFS has no real file descriptor.
func (f *OPFSFile) Fd() uintptr { return 0 }

// Close releases the sync access handle.
func (f *OPFSFile) Close() error {
	f.handle.Call("close")
	return nil
}

// =========================================================================
// opfsFileInfo implements fs.FileInfo.
// =========================================================================

type opfsFileInfo struct {
	name  string
	size  int64
	mode  fs.FileMode
	isDir bool
}

func (fi *opfsFileInfo) Name() string       { return fi.name }
func (fi *opfsFileInfo) Size() int64        { return fi.size }
func (fi *opfsFileInfo) Mode() fs.FileMode  { return fi.mode }
func (fi *opfsFileInfo) ModTime() time.Time { return time.Time{} }
func (fi *opfsFileInfo) IsDir() bool        { return fi.isDir }
func (fi *opfsFileInfo) Sys() interface{}   { return nil }
```

- [ ] **Step 10.2: Verify compile**

Run: `cd wasm && GOOS=js GOARCH=wasm go build ./... && cd ..`
Expected: still fails only with `function main is undeclared`.

- [ ] **Step 10.3: Commit**

```bash
git add wasm/vfs_opfs.go
git -c commit.gpgsign=false commit -m "wasm: port OPFS backend (OPFSFS + OPFSFile)

High-performance filesystem backed by FileSystemSyncAccessHandle.
Requires a dedicated Web Worker context (sync access handles are not
available on the main thread). When unavailable, callers fall through
to IndexedDB."
```

---

## Task 11: Create `vfs_detect.go`

This is the one file that differs from the any-crdt-sdk reference — we omit the `osfuncs.SetOSFuncs` call since that is an `any-sync` concern, not any-store's.

**Files:**
- Create: `wasm/vfs_detect.go`

- [ ] **Step 11.1: Create `wasm/vfs_detect.go`**

Write this exact content:

```go
//go:build js && wasm

package main

import (
	"io/fs"
	"syscall/js"

	anystore "github.com/anyproto/any-store"
)

// VFS backend name constants.
const (
	VFSBackendOPFS      = "opfs"
	VFSBackendIndexedDB = "indexeddb"
	VFSBackendMemory    = "memory"
)

// DetectVFS detects the best available storage backend for the current
// browser environment, configures anystore's VFS hook, and returns the
// backend name. Detection priority:
//
//  1. OPFS (Origin Private File System) — requires Web Worker context
//  2. IndexedDB — universal persistent fallback
//  3. MemFS — in-memory, no persistence (always available)
//
// Must be called before anystore.Open. Subsequent OS operations panic
// under wasm if this is not called first.
func DetectVFS() string {
	console := js.Global().Get("console")
	log := func(msg string) {
		if !console.IsUndefined() && !console.IsNull() {
			console.Call("log", "[anystore-vfs] "+msg)
		}
	}

	if hasOPFS() {
		opfs, err := InitOPFS()
		if err == nil {
			log("using OPFS backend (persistent, high-performance)")
			setBackendFuncs(opfs)
			return VFSBackendOPFS
		}
		log("OPFS detected but init failed: " + err.Error())
	}

	if hasIndexedDB() {
		idb, err := InitIDBFS()
		if err == nil {
			log("using IndexedDB backend (persistent, universal)")
			setBackendFuncs(idb)
			return VFSBackendIndexedDB
		}
		log("IndexedDB detected but init failed: " + err.Error())
	}

	log("using in-memory backend (no persistence)")
	mem := &MemFS{}
	setBackendFuncs(mem)
	return VFSBackendMemory
}

// backend is the interface all three VFS implementations satisfy.
// anystore only consumes OpenFile + Remove; the other methods live on
// the concrete types for downstream app-level use and are not wired here.
type backend interface {
	OpenFile(name string, flag int, perm fs.FileMode) (anystore.File, error)
	Remove(name string) error
}

func setBackendFuncs(b backend) {
	anystore.SetVFS(anystore.VFS{
		OpenFile: b.OpenFile,
		Remove:   b.Remove,
		Fdatasync: func(f anystore.File) error {
			return f.Sync()
		},
	})
}

// hasOPFS checks whether the Origin Private File System API is available.
// OPFS sync access handles require a dedicated Worker context.
func hasOPFS() bool {
	navigator := js.Global().Get("navigator")
	if navigator.IsUndefined() || navigator.IsNull() {
		return false
	}
	storage := navigator.Get("storage")
	if storage.IsUndefined() || storage.IsNull() {
		return false
	}
	getDir := storage.Get("getDirectory")
	if getDir.IsUndefined() || getDir.IsNull() {
		return false
	}
	// createSyncAccessHandle is only available in dedicated Workers.
	wgs := js.Global().Get("WorkerGlobalScope")
	return !wgs.IsUndefined()
}

// hasIndexedDB checks whether the IndexedDB API is available.
func hasIndexedDB() bool {
	idb := js.Global().Get("indexedDB")
	if !idb.IsUndefined() && !idb.IsNull() {
		return true
	}
	self := js.Global().Get("self")
	if !self.IsUndefined() && !self.IsNull() {
		idb = self.Get("indexedDB")
		return !idb.IsUndefined() && !idb.IsNull()
	}
	return false
}
```

- [ ] **Step 11.2: Verify compile**

Run: `cd wasm && GOOS=js GOARCH=wasm go build ./... && cd ..`
Expected: still fails only with `function main is undeclared`.

- [ ] **Step 11.3: Commit**

```bash
git add wasm/vfs_detect.go
git -c commit.gpgsign=false commit -m "wasm: add DetectVFS() backend picker

Probes for OPFS (Worker-only) → IndexedDB → MemFS and wires the chosen
backend into anystore.SetVFS. Returns a short backend name for logging."
```

---

## Task 12: Write `main.go` smoke test

**Files:**
- Create: `wasm/main.go`

- [ ] **Step 12.1: Create `wasm/main.go`**

Write this exact content. API references verified against
`any-store/db.go:104` (`anystore.Open`), `collection.go:200,257,426`
(`FindId`, `Insert`, `Collection`), `document.go:8` (`Doc.Value()`),
and `anyenc/parser.go:36` (`MustParseJson`).

```go
//go:build js && wasm

package main

import (
	"context"
	"fmt"
	"syscall/js"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-store/anyenc"
)

const smokeDBPath = "/anystore-smoke/smoke.db"

func main() {
	console := js.Global().Get("console")
	logf := func(msg string) {
		if !console.IsUndefined() && !console.IsNull() {
			console.Call("log", msg)
		}
	}
	errf := func(msg string) {
		if !console.IsUndefined() && !console.IsNull() {
			console.Call("error", msg)
		}
	}

	backend := DetectVFS()

	if err := runSmoke(); err != nil {
		errf(fmt.Sprintf("anystore wasm smoke FAILED (backend=%s): %v", backend, err))
	} else {
		logf(fmt.Sprintf("anystore wasm smoke ok, backend=%s", backend))
	}

	// Keep the wasm runtime alive so JS callers can inspect state.
	<-make(chan struct{})
}

func runSmoke() error {
	ctx := context.Background()

	db, err := anystore.Open(ctx, smokeDBPath, nil)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer db.Close()

	coll, err := db.Collection(ctx, "smoke")
	if err != nil {
		return fmt.Errorf("open collection: %w", err)
	}

	doc := anyenc.MustParseJson(`{"id":"smoke-1","v":"hello wasm"}`)
	if err := coll.Insert(ctx, doc); err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	got, err := coll.FindId(ctx, "smoke-1")
	if err != nil {
		return fmt.Errorf("find: %w", err)
	}
	if v := got.Value().GetString("v"); v != "hello wasm" {
		return fmt.Errorf("roundtrip mismatch: got %q", v)
	}

	return nil
}
```

- [ ] **Step 12.2: Verify the wasm submodule now builds end-to-end**

Run: `cd wasm && GOOS=js GOARCH=wasm go build ./... && cd ..`
Expected: exit 0, no output.

If compilation fails with undefined anystore methods, inspect the public API with `grep -n "^func.*DB\b" ../db.go` or `grep -n "^func.*Collection" ../collection.go` and correct the `main.go` calls accordingly. Do not change `main.go` to use private APIs.

- [ ] **Step 12.3: Commit**

```bash
git add wasm/main.go
git -c commit.gpgsign=false commit -m "wasm: add smoke-test main

Opens a file-backed DB at /anystore-smoke/smoke.db, inserts one document
and reads it back. File-backed path (not InMemory) is required to
exercise the VFS OpenFile hook — InMemory bypasses osOpenFile (pager.go
:322). Runtime stays alive via a blocking channel so the console log is
inspectable after load."
```

---

## Task 13: Write `js/loader.js`

**Files:**
- Create: `wasm/js/loader.js`

- [ ] **Step 13.1: Create `wasm/js/loader.js`**

Write this exact content:

```js
// loader.js — minimal wrapper around Go's wasm_exec.js runtime.
//
// Usage from a browser page that has already loaded wasm_exec.js via
// a classic <script> tag:
//
//   import { loadAnyStoreWasm } from './loader.js';
//   await loadAnyStoreWasm();
//
// The default wasm URL is './anystore.wasm' (relative to the loader).
// Pass a different URL to load from elsewhere.
//
// Returns { go, instance } so callers can inspect the runtime or
// trigger a second module load if needed.

export async function loadAnyStoreWasm(wasmUrl = './anystore.wasm') {
    const GoCtor = globalThis.Go;
    if (typeof GoCtor !== 'function') {
        throw new Error(
            'loader.js: global `Go` constructor not found. ' +
            'Load wasm_exec.js from the Go toolchain before calling loadAnyStoreWasm().'
        );
    }
    const go = new GoCtor();

    let instance;
    if (typeof WebAssembly.instantiateStreaming === 'function') {
        const src = await WebAssembly.instantiateStreaming(fetch(wasmUrl), go.importObject);
        instance = src.instance;
    } else {
        // Safari < 15 and older Firefox: fall back to arrayBuffer.
        const resp = await fetch(wasmUrl);
        const bytes = await resp.arrayBuffer();
        const src = await WebAssembly.instantiate(bytes, go.importObject);
        instance = src.instance;
    }

    // go.run is non-blocking from the caller's POV — it registers its
    // goroutines on the JS event loop and returns a Promise that resolves
    // when main exits. Our main.go blocks forever on a channel by design,
    // so the returned promise stays pending. That's intentional: we want
    // the runtime to stay loaded.
    go.run(instance);

    return { go, instance };
}
```

- [ ] **Step 13.2: Commit**

```bash
git add wasm/js/loader.js
git -c commit.gpgsign=false commit -m "wasm: add minimal JS loader (streaming + fallback)"
```

---

## Task 14: Final build verification and .wasm artifact

**Files:** none (verification only)

- [ ] **Step 14.1: Verify full wasm build produces artifacts**

Run: `cd wasm && make clean && make build && cd ..`
Expected:
- Exit 0, no errors.
- `wasm/dist/anystore.wasm` exists and is non-empty.
- `wasm/dist/wasm_exec.js` exists.
- `wasm/dist/loader.js` exists.
- `wasm/dist/anystore.wasm.gz` exists.

Check sizes: `ls -lah wasm/dist/`
Expected: `anystore.wasm` in the range 8–40 MB uncompressed, 2–8 MB gzipped.

- [ ] **Step 14.2: Confirm the root module is still green**

Run: `go build ./...`
Expected: exit 0.

Run: `go build -tags vfs ./...`
Expected: exit 0.

Run: `GOOS=js GOARCH=wasm go build ./...`
Expected: exit 0.

Run: `go test ./... -count=1 -short -timeout=180s`
Expected: `ok` for every package.

- [ ] **Step 14.3: Manual browser smoke test (optional but recommended)**

Follow the manual steps in `wasm/README.md`:

1. `cd wasm/dist`
2. Create `index.html` with the snippet from the README.
3. `python3 -m http.server 8000`
4. Open `http://localhost:8000/` in Chrome; check DevTools console.

Expected console output:
```
[anystore-vfs] using IndexedDB backend (persistent, universal)
anystore wasm smoke ok, backend=indexeddb
```

(Backend may show `memory` on a non-secure origin or `opfs` if loaded inside a Worker.)

If the smoke test shows a runtime panic mentioning `sentinel` or `os.Stat` on the DB path, this is the §8-risk playing out — raise a follow-up task to make sentinel a no-op on js/wasm or to extend `anystore.VFS` to cover `os.Stat` / `os.ReadFile` / `os.WriteFile` / `os.Remove`. Do **not** modify those files as part of this plan — scope was explicitly fenced in the spec.

No commit — verification only.

---

## Post-plan state

- Root module compiles under `GOOS=js GOARCH=wasm` with VFS auto-enabled, no `-tags vfs` required.
- Pre-existing `-tags vfs` compile break (from `ce57450`) is fixed.
- `wasm/` submodule produces `anystore.wasm` + loader.
- No JS bindings for `DB` / `Collection` / `Query` — downstream SDKs build their own `main.go`.
- Follow-ups (not in this plan): sentinel/`os.Stat` VFS plumbing if the smoke test requires it; headless-browser CI; migrating `any-crdt-sdk/wasm` to import this module.
