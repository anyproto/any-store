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
	then := js.FuncOf(func(this js.Value, args []js.Value) any {
		result = args[0]
		close(ch)
		return nil
	})
	catch := js.FuncOf(func(this js.Value, args []js.Value) any {
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

	onSuccess := js.FuncOf(func(_ js.Value, args []js.Value) any {
		result = args[0].Get("target").Get("result")
		close(ch)
		return nil
	})
	onError := js.FuncOf(func(_ js.Value, args []js.Value) any {
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
