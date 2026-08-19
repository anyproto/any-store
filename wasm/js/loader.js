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
