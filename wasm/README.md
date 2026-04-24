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
