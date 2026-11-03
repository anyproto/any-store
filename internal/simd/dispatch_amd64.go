//go:build !noasm

package simd

import (
	"github.com/anyproto/any-store/v2/internal/simd/asm"
	"golang.org/x/sys/cpu"
)

// init selects the best amd64 kernel. Mirrors weaviate's distancer dispatch:
// AVX512 only on AMX-class CPUs, AVX2 otherwise. On a CPU without AVX2 (e.g.
// pre-Haswell Ivy Bridge) neither branch is taken and the pure-Go fallback in
// fallback.go stays active — exactly the path that beat vek's slow fallback.
func init() {
	switch {
	case cpu.X86.HasAVX512 && cpu.X86.HasAMXBF16:
		dotImpl = dotAVX512
		l2Impl = l2AVX512
		accelerated = true
	case cpu.X86.HasAVX2:
		dotImpl = dotAVX2
		l2Impl = l2AVX2
		accelerated = true
	}
	if cpu.X86.HasAVX2 {
		dotFBImpl = dotFloatByteAVX2
		acceleratedFB = true
	}
}

// The vendored AVX2 and AVX-512 kernels read past the end of an empty first
// vector: their short path computes n-1 as unsigned, so 0 wraps and a 4-wide
// loop runs until a 32-bit counter does. These adapters return 0 for one, as
// every other kernel does. The asm wrappers inline into them, so they add no
// call.

func dotAVX2(a, b []float32) float32 {
	if len(a) == 0 {
		return 0
	}
	return asm.DotAVX256(a, b)
}

func l2AVX2(a, b []float32) float32 {
	if len(a) == 0 {
		return 0
	}
	return asm.L2AVX256(a, b)
}

func dotAVX512(a, b []float32) float32 {
	if len(a) == 0 {
		return 0
	}
	return asm.DotAVX512(a, b)
}

func l2AVX512(a, b []float32) float32 {
	if len(a) == 0 {
		return 0
	}
	return asm.L2AVX512(a, b)
}

func dotFloatByteAVX2(q []float32, u []byte) float32 {
	if len(q) == 0 {
		return 0
	}
	return asm.DotFloatByteAVX256(q, u)
}
