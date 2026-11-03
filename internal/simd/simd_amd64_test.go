//go:build !noasm

package simd

// The AVX2 and AVX-512 kernels, through the adapters init installs, join
// TestKernelLengths whatever cpuid reports, so the length cases reach them
// under Rosetta, which hides AVX2 from cpuid yet runs it, and on CPUs that
// dispatch to neither.
func init() {
	archKernelSets = append(archKernelSets,
		kernelSet{name: "AVX2", dot: dotAVX2, l2: l2AVX2, dotFB: dotFloatByteAVX2, probe: true},
		kernelSet{name: "AVX512", dot: dotAVX512, l2: l2AVX512, dotFB: dotFloatByteAVX2, probe: true},
	)
}
