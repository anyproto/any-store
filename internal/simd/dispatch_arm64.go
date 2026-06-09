package simd

import (
	"github.com/anyproto/any-store/v2/internal/simd/asm"
	"golang.org/x/sys/cpu"
)

// init selects the best arm64 kernel: SVE when present, else NEON (ASIMD).
// Without ASIMD the pure-Go fallback stays active.
func init() {
	if cpu.ARM64.HasASIMD {
		if cpu.ARM64.HasSVE {
			dotImpl = asm.Dot_SVE
			l2Impl = asm.L2_SVE
		} else {
			dotImpl = asm.Dot_Neon
			l2Impl = asm.L2_Neon
		}
		accelerated = true
		// Only a NEON float×byte kernel exists (no SVE variant upstream).
		dotFBImpl = asm.DotFloatByte_Neon
		acceleratedFB = true
	}
}
