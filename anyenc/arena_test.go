package anyenc

import (
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testObject(a *Arena) *Value {
	arr := a.NewArray()
	arr.SetArrayItem(0, a.NewNumberFloat64(1))
	arr.SetArrayItem(1, a.NewNumberFloat64(2))
	obj := a.NewObject()
	obj.Set("s", a.NewString("string"))
	obj.Set("n", a.NewNumberFloat64(4.4))
	obj.Set("i", a.NewNumberInt(5))
	obj.Set("a", arr)
	obj.Set("b", a.NewBinary([]byte("binary")))
	return obj
}

func TestArena_Reset(t *testing.T) {
	ap := &ArenaPool{}
	var exp = `{"s":"string","n":4.4,"i":5,"a":[1,2],"b":{"$binary":"YmluYXJ5"}}`
	var n = 10
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			for range 100 {
				a := ap.Get()
				a.Reset()
				res := testObject(a)
				assert.Equal(t, exp, res.String())
				ap.Put(a)
			}
		}()
	}
	wg.Wait()
}

func TestArena_NewBool(t *testing.T) {
	a := &Arena{}
	
	t.Run("true", func(t *testing.T) {
		v := a.NewBool(true)
		assert.NotNil(t, v)
		assert.Equal(t, TypeTrue, v.Type())
		assert.True(t, v.GetBool())
	})
	
	t.Run("false", func(t *testing.T) {
		v := a.NewBool(false)
		assert.NotNil(t, v)
		assert.Equal(t, TypeFalse, v.Type())
		assert.False(t, v.GetBool())
	})
	
	t.Run("consistency with NewTrue and NewFalse", func(t *testing.T) {
		assert.Equal(t, a.NewTrue(), a.NewBool(true))
		assert.Equal(t, a.NewFalse(), a.NewBool(false))
	})
}

func BenchmarkArena_Reset(b *testing.B) {
	a := &Arena{}
	b.ReportAllocs()
	for range b.N {
		a.Reset()
		_ = testObject(a)
	}
}

// TestArena_ApproxSize pins Arena.ApproxSize — an empty arena reports a
// baseline size (possibly 0 depending on initial cap), and allocating a
// non-empty value strictly grows the reported size.
//
// The formula (see parser.go:259-264 approxSizeValues) is:
//
//	sum over vs[:cap(vs)] of (len(v.v) + valueSize)
//
// so an 11-byte string contributes at least 11 + valueSize bytes. We assert
// the delta covers the 11 payload bytes (the valueSize offset is added on top),
// which proves the string payload was actually accounted for.
func TestArena_ApproxSize(t *testing.T) {
	a := &Arena{}
	emptySize := a.ApproxSize()
	assert.GreaterOrEqual(t, emptySize, 0)

	// Allocate an 11-byte string. The cache stores a Value with len(v.v)==11,
	// plus valueSize overhead. A broken implementation that returns a constant
	// would fail the strict `>` assertion.
	_ = a.NewStringBytes([]byte("hello world"))
	after := a.ApproxSize()
	assert.Greater(t, after, emptySize,
		"ApproxSize must strictly grow after allocating a non-empty value")
	assert.GreaterOrEqual(t, after-emptySize, 11,
		"delta must cover at least the 11 payload bytes of 'hello world'")
}

// Copy keeps every value as it is in memory, and a change to its source's
// keys and payloads does not reach the copy, including what an encode and
// parse would change or refuse: a key "\x1f" (encoded like the empty key), a
// -0 number (encoded as +0) and nesting past the parser's depth limit.
func TestArena_Copy(t *testing.T) {
	src := &Arena{}
	v := testObject(src)
	v.Set("\x1f", src.NewString("wart"))
	v.Set("negzero", src.NewNumberFloat64(math.Copysign(0, -1)))
	v.Set("vec", src.NewVectorF32([]float32{1, 2.5}))
	v.Set("oid", src.NewObjectID(NewObjectID()))
	v.Set("dt", src.NewDateTimeMillis(1_700_000_000_000))
	v.Set("t", src.NewTrue())
	v.Set("f", src.NewFalse())
	v.Set("null", src.NewNull())
	deep := src.NewArray()
	for cur, i := deep, 0; i < 2*maxParseDepth; i++ {
		next := src.NewArray()
		cur.SetArrayItem(0, next)
		cur = next
	}
	v.Set("deep", deep)
	want := v.String()

	a := &Arena{}
	c := a.Copy(v)
	assert.Equal(t, want, c.String())
	assert.Equal(t, "wart", c.GetString("\x1f"))
	assert.True(t, math.Signbit(c.GetFloat64("negzero")))
	assert.Nil(t, a.Copy(nil))

	// The copy and the source are independent both ways.
	c.Set("s", a.NewString("changed"))
	c.GetArray("a")[0] = a.NewNumberInt(9)
	c.Del("i")
	assert.Equal(t, want, v.String())
	src.Reset()
	testObject(src)
	assert.Equal(t, "changed", c.GetString("s"))
	assert.Equal(t, 9, c.GetInt("a", "0"))
	assert.Equal(t, "wart", c.GetString("\x1f"))

	// The keys of a value ParseOwned returns are in the bytes it was given,
	// and its string and binary payloads in the parser's values; the copy
	// shares neither, so it outlives a change to both.
	buf := testObject(src).MarshalTo(nil)
	var p Parser
	pv, err := p.ParseOwned(buf)
	assert.NoError(t, err)
	parsed := pv.String()
	pc := a.Copy(pv)
	for _, k := range []string{"s", "b"} {
		payload := pv.Get(k).v
		assert.NotEmpty(t, payload, k)
		for i := range payload {
			payload[i] = 'X'
		}
	}
	for i := range buf {
		buf[i] = 'X'
	}
	assert.Equal(t, parsed, pc.String())
}

// Copy takes an object's keys in one allocation, however many keys it has,
// and the keys stay as they are when the arena is reset and reused: the
// arena does not hand their bytes out again.
func TestArena_CopyKeysOneAllocation(t *testing.T) {
	src := &Arena{}
	v := src.NewObject()
	for i := range 100 {
		v.Set(fmt.Sprintf("key%d", i), src.NewNumberInt(i))
	}
	a := &Arena{}
	a.Copy(v) // the arena's values, which a reset keeps for reuse
	assert.Equal(t, 1.0, testing.AllocsPerRun(10, func() {
		a.Reset()
		a.Copy(v)
	}))

	a.Reset()
	keys := a.Copy(v).GoType().(map[string]any)
	a.Reset()
	for range 3 {
		w := a.NewObject()
		for i := range 100 {
			w.Set(fmt.Sprintf("other%d", i), a.NewNumberInt(i))
		}
		a.Copy(w)
	}
	assert.Len(t, keys, 100)
	for k, n := range keys {
		assert.Equal(t, fmt.Sprintf("key%v", n), k)
	}
}

// A true, false or null in storage the caller owns copies to a value of the
// arena's, so a later change to that storage does not reach the copy.
func TestArena_CopyScalarTheCallerOwns(t *testing.T) {
	a := &Arena{}
	for _, tc := range []struct {
		from, to *Value
	}{
		{a.NewTrue(), a.NewFalse()},
		{a.NewFalse(), a.NewNull()},
		{a.NewNull(), a.NewTrue()},
	} {
		v := *tc.from
		c := a.Copy(&v)
		v = *tc.to
		assert.Equal(t, tc.from.Type(), c.Type())
		assert.NotSame(t, &v, c)
	}
}
