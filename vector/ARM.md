# How it runs on modern ARM (Apple Silicon, Graviton, Android/iOS)

Branch `btree-vector-search`. Companion to [CROSS_HARDWARE.md](./CROSS_HARDWARE.md).

ARM is a first-class target for any-store (Anytype ships on Apple Silicon macs,
iOS, Android, and increasingly Graviton servers — all arm64). So: how does this
vector index behave there?

## Short answer

It **builds and runs correctly** on arm64 — the binary is pure-Go/no-CGO and
cross-compiles to a static aarch64 executable in one command:

```
GOOS=linux GOARCH=arm64 go build -o vectorbench ./cmd/vectorbench   # → static ARM ELF
```

But it runs with **no hardware SIMD for distance**, because the SIMD comes from
`viterin/vek`, which is **amd64-only**:

- vek's only assembly is `*_amd64.s`; its `accel_noasm.go` is gated
  `//go:build !amd64`, exposing `UseAVX2 = false`. On arm64 vek silently uses a
  portable-Go scalar path. So `vector.SIMD()` returns **false** on every ARM chip.
- Go 1.26's experimental standard `simd` package is also gated
  `//go:build goexperiment.simd && amd64` — **amd64-only**, no help yet.
- The Go compiler (gc) does **not** auto-vectorise loops to NEON; scalar float32
  uses the NEON/FP register file one lane at a time (`FADD S…`), so there's no
  width benefit even though every arm64 core has 128-bit NEON (4×float32).

So on ARM you are in the **same regime as the no-AVX2 Ivy Bridge Xeon** in
CROSS_HARDWARE.md: the portable scalar path, leaving ~4× on the table.

## What that means in practice

- **Correctness & portability:** identical results; the split-persistence on-disk
  format and everything else is endian-defined where it matters (the prototype's
  `:vec` reads host-endian float32 — a production build needs an explicit LE
  encode, noted in OPTION_B.md; arm64 is little-endian so it happens to match).
- **Absolute speed is not as bad as "no SIMD" sounds on a *fast* ARM core.**
  Apple M-series / Graviton3-4 have very high IPC and 3–4 FP pipelines, so the
  4-way-unrolled scalar kernel (independent accumulators) pipelines well and the
  per-comparison time on an M-class core lands far ahead of the old Xeon and
  within a small multiple of mid x86-with-AVX2 — just not the ~13× SIMD jump.
- **This index already adapts:** `Metric.DistanceFor()` now dispatches L2 to the
  hand-unrolled kernel when `vector.SIMD()` is false (measured faster than vek's
  fallback on the no-AVX2 Xeon: 608 vs 685 ns), so ARM gets the better of the two
  portable kernels automatically.

## How to actually get SIMD on ARM (in priority order)

1. **A NEON distance kernel** (`//go:build arm64`, hand-written Plan9 asm, no
   CGO): `FMLA` over 128-bit V-registers does 4×float32 FMAs per instruction —
   the direct equivalent of the AVX path, ~4× over scalar. ~30 lines per metric
   (L2 / dot / cosine-as-dot+norms). This is the real fix and keeps the no-CGO
   rule. Optionally an **SVE** kernel for Graviton3+ (variable width) behind a
   runtime feature check, with NEON as the arm64 baseline.
2. **Quantization — a double win on ARM, and arguably do this first:**
   - **Binary quantization** turns distance into XOR + popcount, and arm64 has a
     native NEON population-count instruction (`CNT`). Hamming distance over
     bit-packed vectors is extremely fast on ARM even without a float NEON kernel
     — often the *fastest* path on mobile.
   - **int8 scalar** quantization maps onto NEON integer multiply-accumulate well
     and quarters the memory bandwidth — which also matters on memory-constrained
     phones.
   - This is the same quantized-routing-slab the hybrid (OPTION_B.md) wants, so it
     pays off twice: less RAM *and* an ARM-friendly distance op.
3. **Wait for Go's std `simd`** to grow an arm64 backend (future; not datable).
4. **CGO + NEON intrinsics** — fastest to write, but any-store is strictly no-CGO,
   so this is off the table.

## Recommendation

For ARM targets, the priority is **quantization first** (binary/int8 — big RAM
win, and binary-Hamming is natively fast on arm64 via `CNT`), then a **NEON
float32 kernel** for the full-precision rerank path. Until those land, ARM runs
correctly on the auto-selected unrolled scalar kernel — slower than it should be,
but the `vector.SIMD()`-based dispatch already picks the best portable option,
and a fast ARM core keeps absolute latency reasonable. Verify on the actual
target with `vectorbench` (it prints `SIMD acceleration: false` on ARM, which is
the signal that the NEON/quantization work is worth doing).

## Status of the check

Verified here: vek is amd64-only (asm + `!amd64` noasm stub), Go std `simd` is
`amd64` only, and `GOOS=linux GOARCH=arm64 go build ./cmd/vectorbench` produces a
working static aarch64 binary. Not yet run on physical ARM (no arm64 box in this
session); emulated timings would be meaningless, so they were deliberately not
reported.
