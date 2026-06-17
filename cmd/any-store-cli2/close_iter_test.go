package main

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/require"
)

// Test_CloseWithOpenIter reproduces the "Ctrl+C freezes forever" bug.
//
// A find() that returns more than one page (batchSize=30) stores the still-open
// iterator in conn.lastIter and prints `Type "it" for more`. That iterator
// holds a read transaction, which holds the btree reader lock. db.Close() blocks
// in db.mu.Lock() ("wait for all active readers to finish") until every reader
// drains, so a leaked iterator hangs Close — and thus the whole process — on
// exit. lineClose() must close the iterator before closing the db.
func Test_CloseWithOpenIter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	require.NoError(t, openConn(path))

	coll, err := conn.db.CreateCollection(mainCtx.Ctx(), "coll")
	require.NoError(t, err)

	// Insert more than one page so the iterator is left open.
	docs := make([]*anyenc.Value, 35)
	for i := range docs {
		docs[i] = anyenc.MustParseJson(`{"id":"` + string(rune('a'+i%26)) + intToStr(i) + `"}`)
	}
	require.NoError(t, coll.Insert(mainCtx.Ctx(), docs...))

	cmd := Cmd{Cmd: "find", Collection: "coll", Query: Query{Find: json.RawMessage("{}")}}
	_, err = conn.Find(cmd)
	require.NoError(t, err)

	// Precondition: the paged result left an iterator (and its read tx) open.
	require.NotNil(t, conn.lastIter, "find of >30 docs must leave a paged iterator open")

	// Mimic lineClose()'s teardown order. The fix is that closeLastIter() runs
	// before db.Close(); without it, db.Close() never returns.
	done := make(chan error, 1)
	go func() {
		conn.closeLastIter()
		done <- conn.db.Close()
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("db.Close() hung with a leaked open iterator — Ctrl+C would freeze forever")
	}

	conn = nil
}

// Test_CloseHangsThenUnblocks documents the underlying contract: an open reader
// blocks Close, and releasing the reader unblocks it. This is the library
// behavior the CLI fix relies on (Close intentionally drains readers).
func Test_CloseHangsThenUnblocks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	require.NoError(t, openConn(path))
	defer func() { conn = nil }()

	coll, err := conn.db.CreateCollection(mainCtx.Ctx(), "coll")
	require.NoError(t, err)
	docs := make([]*anyenc.Value, 35)
	for i := range docs {
		docs[i] = anyenc.MustParseJson(`{"id":"` + string(rune('a'+i%26)) + intToStr(i) + `"}`)
	}
	require.NoError(t, coll.Insert(mainCtx.Ctx(), docs...))

	_, err = conn.Find(Cmd{Cmd: "find", Collection: "coll", Query: Query{Find: json.RawMessage("{}")}})
	require.NoError(t, err)
	require.NotNil(t, conn.lastIter)

	done := make(chan error, 1)
	go func() { done <- conn.db.Close() }()

	// With the iterator still open, Close must NOT complete.
	select {
	case <-done:
		t.Fatal("db.Close() returned while a reader was still open — expected it to block")
	case <-time.After(300 * time.Millisecond):
	}

	// Releasing the reader unblocks Close.
	conn.closeLastIter()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("db.Close() did not return after the reader was released")
	}
}

func intToStr(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
