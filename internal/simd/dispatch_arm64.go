//go:build !noasm

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
		// float×byte: any-store's own NEON kernel (dot_float_byte_arm64.s), not
		// the vendored asm.DotFloatByte_Neon, which is latency-bound at ~3× the
		// f32 Dot. There is no SVE variant; SVE cores run the NEON one.
		dotFBImpl = dotFloatByteNEON
		acceleratedFB = true
	}
}
