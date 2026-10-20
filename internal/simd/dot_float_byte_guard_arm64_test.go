//go:build !noasm && arm64 && (darwin || linux)

package simd

import (
	"math"
	"math/rand"
	"os"
	"syscall"
	"testing"
	"unsafe"
)

// guardedTail returns size bytes of fresh memory that end flush against an
// inaccessible page, so any read past the returned slice faults.
func guardedTail(t *testing.T, size int) []byte {
	t.Helper()
	pg := os.Getpagesize()
	body := (size + pg - 1) / pg * pg
	mem, err := syscall.Mmap(-1, 0, body+pg, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		t.Skipf("mmap: %v", err)
	}
	t.Cleanup(func() { _ = syscall.Munmap(mem) })
	if err := syscall.Mprotect(mem[body:], syscall.PROT_NONE); err != nil {
		t.Skipf("mprotect: %v", err)
	}
	return mem[body-size : body : body]
}

// TestDotFloatByteNEONNoOverread runs every test length with q and u ending
// exactly at a guard page: a kernel that reads one float or byte too many
// crashes the test instead of silently summing neighbouring memory.
func TestDotFloatByteNEONNoOverread(t *testing.T) {
	lengths := floatByteLengths()
	maxN := lengths[len(lengths)-1]
	qmem, umem := guardedTail(t, 4*maxN), guardedTail(t, maxN)
	r := rand.New(rand.NewSource(7593))
	for _, n := range lengths {
		q := unsafe.Slice((*float32)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(qmem)), len(qmem)-4*n)), n)
		u := umem[len(umem)-n:]
		for i := range q {
			q[i] = r.Float32()*2 - 1
			u[i] = byte(r.Intn(256))
		}
		got := float64(dotFloatByteNEON(q, u))
		if want := refDotFloatByte(q, u); !(math.Abs(got-want) <= floatByteTol(q, u)) { // NaN fails
			t.Fatalf("n=%d: got %v want %v", n, got, want)
		}
	}
}
