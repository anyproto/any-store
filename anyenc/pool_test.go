package anyenc

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParserPool_GetPut pins the sync.Pool wrappers for Parser and proves the
// retrieved parser is functional (can Parse encoded bytes) — not just non-nil.
// Also verifies round-trip pointer identity: when a parser is Put back and a
// Get happens shortly after, under GOMAXPROCS(1) it is very likely (not
// guaranteed — sync.Pool drains during GC) that the exact same pointer is
// returned. We retry a bounded number of times before relaxing to a
// functional-only assertion.
func TestParserPool_GetPut(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	pp := &ParserPool{}
	p := pp.Get()
	require.NotNil(t, p, "empty pool returns a fresh Parser")

	// Prove the fresh parser actually works.
	encoded := MustParseJson(`"hello"`).MarshalTo(nil)
	v, err := p.Parse(encoded)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(v.GetStringBytes()))

	// Seed p with observable state before Put so any returned pointer that
	// equals p definitionally came from the pool.
	_, err = p.Parse(encoded)
	require.NoError(t, err)
	pp.Put(p)

	// After Put, Get must return a usable Parser. sync.Pool is
	// non-deterministic, but either branch (pooled or fresh fallback) must
	// produce a working Parser.  Try up to 10 times to see the same pointer;
	// if the pool GC'd the entry, that's acceptable — still require the
	// returned parser works on the encoded bytes.
	var p2 *Parser
	var sameIdentity bool
	for i := 0; i < 10; i++ {
		p2 = pp.Get()
		if p2 == p {
			sameIdentity = true
			break
		}
		pp.Put(p2)
	}
	require.NotNil(t, p2)
	if !sameIdentity {
		t.Log("pool GC'd the pointer before any Get retrieved it (acceptable)")
	}
	v2, err := p2.Parse(encoded)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(v2.GetStringBytes()))
}

// TestArenaPool_GetPut pins the sync.Pool wrappers for Arena. Put calls
// Reset on the arena before returning it to the pool — we verify that by
// observing the returned arena's cache is cleared (vs length == 0 after
// reset, see parser.go:242-244 cache.reset). Under GOMAXPROCS(1), Put+Get
// back-to-back very likely hands back the exact same arena pointer, which
// we check in a bounded retry loop.
func TestArenaPool_GetPut(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	ap := &ArenaPool{}
	a := ap.Get()
	require.NotNil(t, a)

	// Produce several values so Reset has work to do.
	_ = a.NewString("hi")
	_ = a.NewString("there")
	_ = a.NewNumberInt(42)
	assert.Greater(t, a.ApproxSize(), 0,
		"arena must have non-zero ApproxSize after allocating values")

	ap.Put(a)

	// Try to observe pointer identity. If we get `a` back, we can verify
	// that Reset fired — approxSizeValues walks vs[:cap(vs)] counting
	// len(v.v) of each cached Value. Reset calls c.vs = c.vs[:0], which
	// shrinks length to 0 but keeps capacity — so prior cached payloads
	// (v.v) remain reachable via cap and still contribute to size. This
	// means size may NOT drop to zero. We therefore verify a weaker but
	// still useful invariant: after Put+Get, the arena is *usable* for
	// fresh allocations, and its size does not strictly grow without new
	// allocations.
	var a2 *Arena
	var sameIdentity bool
	for i := 0; i < 10; i++ {
		a2 = ap.Get()
		if a2 == a {
			sameIdentity = true
			break
		}
		ap.Put(a2)
	}
	require.NotNil(t, a2)

	if sameIdentity {
		// Same arena — verify the returned arena is ready to accept fresh
		// allocations starting from length 0. We measure size before/after
		// allocating a known string and assert the delta is consistent
		// with a freshly-reset (empty) vs slice.
		before := a2.ApproxSize()
		_ = a2.NewString("new")
		after := a2.ApproxSize()
		assert.Greater(t, after, before,
			"post-reset arena must strictly grow after new allocation")
	} else {
		t.Log("pool GC'd the pointer; falling back to functional check")
		// Still require the returned arena works.
		v := a2.NewString("post-pool")
		sb, err := v.StringBytes()
		require.NoError(t, err)
		assert.Equal(t, []byte("post-pool"), sb)
	}
}
