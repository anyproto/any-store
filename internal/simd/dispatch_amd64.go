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
		dotImpl = asm.DotAVX512
		l2Impl = asm.L2AVX512
		accelerated = true
	case cpu.X86.HasAVX2:
		dotImpl = asm.DotAVX256
		l2Impl = asm.L2AVX256
		accelerated = true
	}
	if cpu.X86.HasAVX2 {
		dotFBImpl = asm.DotFloatByteAVX256
		acceleratedFB = true
	}
}
