//go:build !noasm

package simd

import (
	"testing"

	"github.com/anyproto/any-store/v2/internal/simd/asm"
)

// TestEmptyVectorsAMD64Kernels dispatches to each vendored amd64 kernel set
// whatever cpuid reports (under Rosetta it hides AVX2, yet the instructions
// still run) and checks that empty vectors never reach it: these kernels read
// past the end of a zero-length slice and fault. With the guard in place no
// kernel instruction runs, so this is safe on any amd64 CPU.
func TestEmptyVectorsAMD64Kernels(t *testing.T) {
	for _, k := range []struct {
		name    string
		dot, l2 func(a, b []float32) float32
	}{
		{"AVX2", asm.DotAVX256, asm.L2AVX256},
		{"AVX512", asm.DotAVX512, asm.L2AVX512},
	} {
		t.Run(k.name, func(t *testing.T) {
			useKernels(k.dot, k.l2, asm.DotFloatByteAVX256, func() { checkEmptyVectors(t) })
		})
	}
}
