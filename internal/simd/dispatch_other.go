//go:build !amd64 && !arm64

package simd

// On every other platform (wasm/js, 386, ...) there is no vendored SIMD kernel,
// so the pure-Go fallbacks in fallback.go stay active and accelerated is false.
// This file exists so the package builds (and so we don't import asm, whose
// kernels are arch-specific) on those targets.
