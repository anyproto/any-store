//go:build js && wasm

package main

import (
	"context"
	"fmt"
	"syscall/js"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
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
