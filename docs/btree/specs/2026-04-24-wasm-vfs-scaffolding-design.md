# Design: WASM build + VFS scaffolding

**Date:** 2026-04-24
**Branch:** `btree` (or fresh branch)
**Scope:** root module build tags, new `wasm/` submodule, no API changes
**Status:** Draft — awaiting user review

---

## 1. Goal

Make `any-store` compile cleanly under `GOOS=js GOARCH=wasm` and ship a
`wasm/` subfolder (separate Go module) that demonstrates the build and provides
three reusable VFS backends — OPFS, IndexedDB, in-memory. No JavaScript
bindings for `DB` / `Collection` / `Query` are in scope; downstream SDKs
(e.g. `any-crdt-sdk`) import the wasm module and expose their own JS API.

Reference implementation: `../any-crdt-sdk/wasm/`. The scaffolding there is
what we are extracting and adapting.

## 2. Scope

**In scope.**
- Core build-tag changes so `GOOS=js GOARCH=wasm go build ./...` succeeds with
  no `-tags vfs` required. VFS is auto-active on wasm.
- Split `internal/btree/dbfile_lock.go` along the `unix` / `!unix` axis so
  `syscall.Flock` does not leak to wasm. This also fixes a pre-existing
  `-tags vfs` compile break introduced by commit `ce57450` (see §4).
- New `wasm/` folder at repository root containing:
  - A separate Go module (`github.com/anyproto/any-store/wasm`).
  - Three VFS backends (OPFS, IndexedDB, MemFS) ported from `any-crdt-sdk/wasm`.
  - A `DetectVFS()` entry that picks the best available backend and wires it
    via `anystore.SetVFS`.
  - A smoke-test `main.go`.
  - A `Makefile` producing `dist/anystore.wasm` + `wasm_exec.js`.
  - `js/loader.js` — a minimal hand-written JS wrapper.

**Out of scope.**
- JS bindings for `DB` / `Collection` / `Query` / transactions. Downstream
  SDKs build their own `main` and call into any-store directly from Go.
- TypeScript. `loader.js` is plain JavaScript.
- Extending `anystore.VFS` to cover top-level `os.Stat` / `os.ReadFile` /
  `os.WriteFile` / `os.Remove` used by `db.go` and `internal/durability/sentinel`.
  If the wasm smoke test surfaces a failure here, fix it in a follow-up commit
  — not as part of this scaffolding.
- Windows. The `!unix` lock stub will compile on Windows, but we make no
  claim about Windows support.
- Migrating `any-crdt-sdk/wasm` to import the new `wasm/` module. That SDK
  keeps its local copy for now; migration is a separate change.

## 3. Core build-tag changes

Today, VFS injection is gated by `-tags vfs`. Under `GOOS=js` there is no
real OS filesystem, so VFS is mandatory — we fold `js && wasm` into the
existing `vfs` tag expression.

| File | Current tag | New tag |
|---|---|---|
| `internal/btree/osfuncs_vfs.go` | `vfs` | `vfs \|\| (js && wasm)` |
| `internal/btree/osfuncs.go` | `!vfs` | `!vfs && !(js && wasm)` |
| `internal/btree/osfuncs_vfs_sync_other.go` | `vfs && !linux` | `(vfs \|\| (js && wasm)) && !linux` |
| `internal/btree/osfuncs_sync_other.go` | `!vfs && !linux` | `!vfs && !(js && wasm) && !linux` |

The linux-only files (`osfuncs_sync_linux.go`, `osfuncs_vfs_sync_linux.go`)
need no change — `GOOS=js` is never linux.

`anystore.SetVFS` / `anystore.ResetVFS` in `vfs.go` are unconditional
wrappers that delegate to `btree.SetVFS` / `btree.ResetVFS`. Under `!vfs`
today, `btree.SetVFS` panics with
`"btree: SetVFS requires building with -tags vfs"`
(`internal/btree/osfuncs.go:17-24`). Under the new tag expression, the
panic stubs remain in `osfuncs.go` but now fire only when
`!vfs && !(js && wasm)`. The panic message and the doc comments on
`anystore.SetVFS` / `ResetVFS` are updated to mention both triggers:

```go
// SetVFS replaces OS-level operations. Panics unless built with -tags vfs
// or GOOS=js GOARCH=wasm.
```
```go
panic("btree: SetVFS requires building with -tags vfs or GOOS=js GOARCH=wasm")
```

### Wasm-specific panic defaults

Under `-tags vfs` today, `osfuncs_vfs.go` initializes `defaultOpenFile`,
`defaultRemove`, `defaultFdatasync` to real `os` functions. That is the
wrong default on wasm — `os.OpenFile` on `GOOS=js` does not panic, it
returns an ENOSYS error, producing a silent-wrong path if the user
forgets to call `SetVFS`.

To make §6's "panic on missing VFS" contract real, add a small
wasm-only file:

**`internal/btree/osfuncs_vfs_js.go`** (`//go:build js && wasm`)

```go
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

Order of init: Go runs `init()` functions in filename order within a
package. `osfuncs_vfs.go`'s `var (...)` block initializes the defaults
first; `osfuncs_vfs_js.go`'s `init()` overwrites them before any user
code runs. Subsequent `anystore.SetVFS(vfs)` replaces the panic stubs
with the caller's VFS — the contract matches the design.

`ResetVFS` under wasm now restores the panic stubs (not `os.OpenFile`),
which is the correct "unset" state. That is an intended side-effect.

### Existing InProcess auto-forcing — already sufficient

`internal/btree/db.go:201-204` already forces `opts.InProcess = true` when
`hasMmapShm == false`. `hasMmapShm` is defined per platform via build tags;
under `GOOS=js GOARCH=wasm` the `!((linux || darwin) && (amd64 || arm64))`
branch in `shm_other.go` wins and the constant is `false`. Nothing new is
needed to disable multi-process SHM on wasm. If a user passes
`InProcess=false` on wasm, `NewDB` silently overrides — consistent with
Windows and the same behavior the comment at `db.go:56` promises.

## 4. `dbfile_lock.go` split (also fixes pre-existing `-tags vfs` break)

`internal/btree/dbfile_lock.go` was introduced in commit `ce57450` with
signatures hardcoded to `*os.File`. This breaks `-tags vfs` builds today —
`pager.go:350` and `pager.go:2442` pass `fileHandle`, which aliases to
the `File` interface under `vfs`. We fix it as part of this change by
switching the lock signatures to the existing `fileHandle` alias.

`fileHandle` already resolves to the right type under every tag combination:
- `!vfs` (`internal/btree/osfuncs.go:8`): `type fileHandle = *os.File`
- `vfs` (`internal/btree/osfuncs_vfs.go:7`): `type fileHandle = File`

And `File` requires `Fd() uintptr`, so `syscall.Flock(int(fd.Fd()), ...)`
compiles under both. Under wasm, `syscall.Flock` does not exist at all,
so we additionally split the file by `unix` vs `!unix`.

Both call sites (`pager.go:348`, `pager.go:2439`) are guarded by
`!p.inProcess`. On wasm `InProcess` is force-true (§3), so the locks are
never called at runtime — but must still compile.

**`internal/btree/dbfile_lock_unix.go`** (`//go:build unix`) — current
logic, `*os.File` → `fileHandle`:

```go
func acquireSharedDBLock(fd fileHandle) error              { return flockNB(fd, syscall.LOCK_SH) }
func tryUpgradeDBLockExclusive(fd fileHandle) (bool, error) { /* unchanged */ }
func downgradeDBLockToShared(fd fileHandle) error           { return flockNB(fd, syscall.LOCK_SH) }

func flockNB(fd fileHandle, how int) error {
    if fd == nil {
        return fmt.Errorf("btree: dbfile lock: nil fd")
    }
    if err := syscall.Flock(int(fd.Fd()), how|syscall.LOCK_NB); err != nil {
        /* unchanged */
    }
    return nil
}
```

Note on `fd == nil`: under `!vfs` this compares a `*os.File` pointer,
under `vfs` it checks interface-nil. Both match the existing caller
contract — `pager.go` only invokes these after a successful `OpenFile`.

**`internal/btree/dbfile_lock_other.go`** (`//go:build !unix`) — no-op
stubs:

```go
func acquireSharedDBLock(_ fileHandle) error              { return nil }
func tryUpgradeDBLockExclusive(_ fileHandle) (bool, error) { return true, nil }
func downgradeDBLockToShared(_ fileHandle) error           { return nil }
```

No `flockNB` on non-unix — nothing calls it outside `dbfile_lock_unix.go`.

**`internal/btree/dbfile_lock_test.go`** — currently passes real `*os.File`.
Under `-tags vfs`, `fileHandle` is the `File` interface, and `*os.File`
implements all seven `File` methods, so the test compiles and passes as-is.
Add `//go:build unix` so the test file does not appear on wasm / Windows
builds where the underlying flock does not exist.

## 5. `wasm/` folder layout

Separate Go module. Files ported from `../any-crdt-sdk/wasm/` except the
CRDT-specific `main.go`.

```
wasm/
  go.mod                  # module github.com/anyproto/any-store/wasm
  go.sum
  Makefile                # GOOS=js GOARCH=wasm go build
  README.md               # one-page build + manual-test instructions
  main.go                 # smoke test
  vfs_detect.go           # DetectVFS() -> "opfs"|"indexeddb"|"memory"
  vfs_js.go               # awaitPromise, awaitIDBRequest, goSliceToJS, jsToGoSlice
  vfs_opfs.go             # OPFS (Worker + sync access handle)
  vfs_indexeddb.go        # IndexedDB (in-mem buffer, flush on Sync/Close)
  memfs.go                # in-memory fallback
  js/
    loader.js             # minimal wasm loader (hand-written, committed)
  dist/                   # build output, .gitignored
```

All `.go` files under `wasm/` carry `//go:build js && wasm` so
`go build ./...` inside the module on a non-wasm target is a trivial no-op
(empty package).

### `go.mod`

```
module github.com/anyproto/any-store/wasm

go 1.23.0

require github.com/anyproto/any-store v0.0.0

replace github.com/anyproto/any-store => ../
```

The `replace` keeps the submodule tied to the working tree during
development. Tagged releases can drop or invert the replace as needed —
not a concern for this change.

### `DetectVFS()`

Signature and behavior match `any-crdt-sdk/wasm/vfs_detect.go`:

```go
func DetectVFS() string {
    if hasOPFS()      { /* try OPFS, call anystore.SetVFS, return "opfs" */ }
    if hasIndexedDB() { /* try IDB,  call anystore.SetVFS, return "indexeddb" */ }
    /* fall back */   { /* MemFS,    call anystore.SetVFS, return "memory" */ }
}
```

`osfuncs.SetOSFuncs` (from `any-sync`) is NOT called — that is an SDK-layer
concern, not an any-store concern.

### `main.go` (smoke test)

No exported JS functions. Executes once at wasm instantiation:

1. `backend := DetectVFS()` — picks and wires a VFS.
2. Open a **file-backed** `anystore.DB` at `/anystore-smoke/smoke.db`
   (InMemory omitted / false). A file-backed path is required to exercise
   `osOpenFile`; `InMemory: true` bypasses the VFS entirely (`pager.go:322`).
3. Insert one document and read it back.
4. `console.log("anystore wasm smoke ok, backend=" + backend)`.
5. `<-make(chan struct{})` to keep the goroutine alive (standard wasm
   pattern — the module stays loaded for inspection).

Failures `console.error` the message. No panic recovery — wasm panics
surface to the browser console naturally via the wasm runtime's
uncaught-panic handler.

### `Makefile`

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

No `-tags vfs` — VFS is implicit under `GOOS=js GOARCH=wasm` after the
tag changes in §3.

### `js/loader.js`

~30 lines. Instantiates `wasm_exec.js`, fetches and streams
`anystore.wasm`, runs it, resolves once `main.go` has logged its ready
message. Committed as-is; no bundler step.

## 6. Runtime contract

Calling `anystore.NewDB` with a non-InMemory path without first calling
`anystore.SetVFS` under `GOOS=js GOARCH=wasm` panics at the first
`osOpenFile` invocation with:

> `btree: SetVFS not called — anystore on wasm requires a VFS backend
> (path=/whatever)`

The panic is implemented by the wasm-only `init()` in
`osfuncs_vfs_js.go` (see §3) replacing the default OS funcs with panic
stubs. Downstream callers use `DetectVFS()` as the recommended entry
point; it calls `SetVFS` before returning, so the panic never fires in
normal use.

Rationale for panic over error: returning an error would require
plumbing a "no VFS set" check through the pager's `OpenFile` call site
and through every sentinel / durability code path. Panic keeps the
contract at the boundary layer and mirrors how a C program faults on
a null function pointer.

`InMemory: true` under wasm does **not** require `SetVFS` — it bypasses
`osOpenFile` entirely (`pager.go:322`). Useful for tests or transient
workloads with no persistence need.

## 7. Verification

- `go build ./...` at repo root on linux and darwin — unchanged.
- `go build -tags vfs ./...` at repo root — **fixed**, currently broken on
  `main` / `btree` by `ce57450` (see §4).
- `GOOS=js GOARCH=wasm go build ./...` at repo root — **new**, must succeed.
- `go test ./...` at repo root — unchanged.
- `go test -tags vfs ./internal/btree/...` — **fixed**, currently broken by
  the same `ce57450` regression. Once the lock signatures move to
  `fileHandle`, the existing `checkpoint_fault_test.go` suite builds and
  exercises the VFS injection path again.
- `cd wasm && make build` — produces `dist/anystore.wasm`.
- Manual browser load of the smoke test, documented in `wasm/README.md`:
  serve `dist/` with `python3 -m http.server`, open `loader.html` (one-off,
  not committed), verify the `anystore wasm smoke ok, backend=...` log.

## 8. Risks and non-questions

- **`os.Stat` / `os.ReadFile` / `os.WriteFile` / `os.Remove` at
  `any-store/vfs.go:10-13` and `internal/durability/sentinel/sentinel.go`
  are not plumbed through `SetVFS`.** Verified at the time of writing:
  `GOOS=js GOARCH=wasm go build ./internal/durability/sentinel/...`
  succeeds — `syscall.Getpid` and the `os` wrappers exist as stubs on
  the wasm target. Runtime behavior is forgiving in the cold path:
  sentinel's `OnOpen` returns `(false, nil)` on ENOENT. The smoke test
  may still trip on `MarkDirty` writing to a non-existent dir or on
  `db.go:511`'s `osStat(db.btreeDB.Path())`. If that happens, fix in a
  follow-up — either by extending `anystore.VFS` to cover these four
  funcs, or by making sentinel a no-op on js/wasm. Not pre-emptively
  solved here.
- **Reference copy drift.** `any-crdt-sdk/wasm` retains its local copies
  of `memfs.go` / `vfs_*.go`. Future divergence is possible. Accepted;
  the SDK migration is out of scope.
- **No browser CI.** Manual browser verification only. Headless-browser
  CI (playwright / puppeteer) is a separate follow-up.

## 9. Alternatives considered

- **Keep `-tags vfs` as the only way to enable the VFS shim.** Rejected:
  downstream wasm users would have to carry the tag everywhere and the
  default `go build` for wasm would silently produce a broken binary.
- **Same-module `wasm/` package.** Rejected: pulls `syscall/js` deps into
  the main module's transitive graph, annoys non-wasm consumers on Goland
  / LSP tooling, and goes against the pattern set by `any-crdt-sdk/wasm`.
- **Return an error from `NewDB` instead of panicking when no VFS is set.**
  Rejected as noted in §6 — adds a check-on-every-open path for a
  programmer error. Match existing `-tags vfs` contract instead.
- **Add a one-time `console.warn` when wasm silently overrides
  `InProcess=false`.** Rejected — Windows does the same silently, the
  comment at `db.go:56` documents the behavior, wasm isn't special.
