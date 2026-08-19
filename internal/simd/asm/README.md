# Vendored SIMD distance kernels

The `asm` package in this directory is **vendored from Weaviate**:

- Source: https://github.com/weaviate/weaviate
- Path: `adapters/repos/db/vector/hnsw/distancer/asm`
- Commit: `6f5e0bb713661c0e1c8edcd4e13ff01025499ecc`
- License: BSD-3-Clause (see `LICENSE`; copyright © Weaviate B.V.)

Only the **dot-product** and **L2** kernels are vendored (float32, byte/uint8,
and mixed float×byte), for amd64 (AVX2/AVX512) and arm64 (NEON/SVE). The
hamming, prefetch, geo and the higher-level `distancer` Provider abstraction
were intentionally left out — any-store only needs the raw kernels.

Runtime CPU dispatch (which kernel to select) and all higher-level vector ops
(norm, cosine, normalize, residual, the float×int8 quantized path, and the
pure-Go fallback used on wasm / non-AVX2 / non-SIMD CPUs) live in the parent
`internal/simd` package, not here.

The `.s` files are committed assembly (GoAT/avo generated upstream); we do **not**
regenerate them, so no avo/goat build-time toolchain is required.

Do not edit these files by hand. To update, re-vendor from the upstream commit
above and re-run the `internal/simd` tests.
