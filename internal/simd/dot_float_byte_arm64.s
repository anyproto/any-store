//go:build !noasm && arm64

#include "textflag.h"

// any-store-owned NEON float×byte dot product (GO-7590). Replaces the vendored
// dot_float_byte_neon, whose loop chains every fmla through one register and
// so runs at FMA latency (~3× slower than the f32 Dot at dim768).
//
// Layout: 32 elements per iteration into eight independent accumulators
// (V0–V7), so no fmla waits on the previous one and the loop is bounded by
// issue width, not latency. Then at most one 16-element block (V0–V3), 4-wide
// steps (V0), the horizontal reduction, and a 1-element scalar loop. Every
// length, 0 included, is handled here; the Go wrapper only checks len(u).
//
// Bytes are widened straight from 8 to 32 bits with TBL (an index ≥ 16
// selects 0), one instruction per 4 lanes instead of two UXTL levels.
//
// Go's assembler (≤ 1.26) has no vector UCVTF/FADD/FADDP, so those are WORD
// encodings, each with its instruction in the comment. To re-verify them, run
// this in bash in this directory. It needs clang and llvm-objdump (on macOS
// OBJDUMP=objdump is LLVM's; a GNU objdump works only if built for AArch64).
// It exits non-zero, printing MISMATCH, COUNT or the offending line, on a wrong
// encoding, a WORD without a parsable instruction comment, or any WORD it
// failed to extract:
//
//	(
//	set -euo pipefail
//	f=dot_float_byte_arm64.s
//	d=$(mktemp -d); trap 'rm -rf "$d"' EXIT; : > "$d/want"; : > "$d/insn"
//	awk -v d="$d" '/^[ \t]*WORD[ \t]/ {
//		if (!match($0, /\$0[xX][0-9a-fA-F]+/)) { print "BAD OPERAND: " $0; exit 1 }
//		h = tolower(substr($0, RSTART + 3, RLENGTH - 3)); while (length(h) < 8) h = "0" h
//		c = index($0, "//"); s = c ? substr($0, c + 2) : ""; gsub(/^[ \t]+|[ \t]+$/, "", s)
//		if (s == "") { print "NO INSTRUCTION COMMENT: " $0; exit 1 }
//		print "0x" h > (d "/want"); print s > (d "/insn") }' "$f"
//	n=$(grep -cE '^[[:space:]]*WORD([[:space:]]|$)' "$f" || true)
//	clang -c -x assembler --target=aarch64-linux-gnu -o "$d/w.o" "$d/insn"
//	"${OBJDUMP:-llvm-objdump}" -d "$d/w.o" | awk '/^ *[0-9a-f]+:/ {print "0x" $2}' > "$d/got"
//	w=$(awk 'END {print NR}' "$d/want"); g=$(awk 'END {print NR}' "$d/got")
//	[ "$n" -gt 0 ] && [ "$w" -eq "$n" ] && [ "$g" -eq "$n" ] || { echo "COUNT: $n WORD, $w parsed, $g assembled"; exit 1; }
//	paste "$d/want" "$d/got" | awk '$1 != $2 {bad = 1; print "MISMATCH", $0} END {exit bad}'
//	)
//
// and read the kernel back as Go assembled it (WORDs decode by name). This
// needs an arm64 binary; GOARCH=arm64 cross-compiles it on any host:
//
//	(d=$(mktemp -d); trap 'rm -rf "$d"' EXIT
//	GOARCH=arm64 go test -c -o "$d/simd.test" . && go tool objdump -s dotFloatByteNEONAsm "$d/simd.test")
//
// Leaf, no frame, no stack. Uses only R0–R4, V0–V25 and V28–V31 (the Go ABI
// has no callee-saved FP registers); R18 and R26–R30 are untouched.

// fbIdx holds four TBL index vectors: vector k zero-extends bytes 4k..4k+3 of
// a 16-byte block into four uint32 lanes (little-endian; 0xff selects 0).
DATA fbIdx<>+0x00(SB)/8, $0xffffff01ffffff00
DATA fbIdx<>+0x08(SB)/8, $0xffffff03ffffff02
DATA fbIdx<>+0x10(SB)/8, $0xffffff05ffffff04
DATA fbIdx<>+0x18(SB)/8, $0xffffff07ffffff06
DATA fbIdx<>+0x20(SB)/8, $0xffffff09ffffff08
DATA fbIdx<>+0x28(SB)/8, $0xffffff0bffffff0a
DATA fbIdx<>+0x30(SB)/8, $0xffffff0dffffff0c
DATA fbIdx<>+0x38(SB)/8, $0xffffff0fffffff0e
GLOBL fbIdx<>(SB), RODATA|NOPTR, $64

// func dotFloatByteNEONAsm(q *float32, u *byte, n int) float32
TEXT ·dotFloatByteNEONAsm(SB), NOSPLIT, $0-28
	MOVD	q+0(FP), R0
	MOVD	u+8(FP), R1
	MOVD	n+16(FP), R2
	MOVD	$fbIdx<>(SB), R3
	VLD1	(R3), [V28.B16, V29.B16, V30.B16, V31.B16]

	VEOR	V0.B16, V0.B16, V0.B16
	VEOR	V1.B16, V1.B16, V1.B16
	VEOR	V2.B16, V2.B16, V2.B16
	VEOR	V3.B16, V3.B16, V3.B16
	VEOR	V4.B16, V4.B16, V4.B16
	VEOR	V5.B16, V5.B16, V5.B16
	VEOR	V6.B16, V6.B16, V6.B16
	VEOR	V7.B16, V7.B16, V7.B16

	LSR	$5, R2, R3 // 32-element blocks
	CBZ	R3, tail16

loop32:
	VLD1.P	32(R1), [V16.B16, V17.B16]
	VLD1.P	64(R0), [V18.S4, V19.S4, V20.S4, V21.S4]
	VLD1.P	64(R0), [V22.S4, V23.S4, V24.S4, V25.S4]
	VTBL	V28.B16, [V16.B16], V8.B16
	VTBL	V29.B16, [V16.B16], V9.B16
	VTBL	V30.B16, [V16.B16], V10.B16
	VTBL	V31.B16, [V16.B16], V11.B16
	VTBL	V28.B16, [V17.B16], V12.B16
	VTBL	V29.B16, [V17.B16], V13.B16
	VTBL	V30.B16, [V17.B16], V14.B16
	VTBL	V31.B16, [V17.B16], V15.B16
	WORD	$0x6e21d908 // ucvtf v8.4s, v8.4s
	WORD	$0x6e21d929 // ucvtf v9.4s, v9.4s
	WORD	$0x6e21d94a // ucvtf v10.4s, v10.4s
	WORD	$0x6e21d96b // ucvtf v11.4s, v11.4s
	WORD	$0x6e21d98c // ucvtf v12.4s, v12.4s
	WORD	$0x6e21d9ad // ucvtf v13.4s, v13.4s
	WORD	$0x6e21d9ce // ucvtf v14.4s, v14.4s
	WORD	$0x6e21d9ef // ucvtf v15.4s, v15.4s
	VFMLA	V8.S4, V18.S4, V0.S4
	VFMLA	V9.S4, V19.S4, V1.S4
	VFMLA	V10.S4, V20.S4, V2.S4
	VFMLA	V11.S4, V21.S4, V3.S4
	VFMLA	V12.S4, V22.S4, V4.S4
	VFMLA	V13.S4, V23.S4, V5.S4
	VFMLA	V14.S4, V24.S4, V6.S4
	VFMLA	V15.S4, V25.S4, V7.S4
	SUBS	$1, R3, R3
	BNE	loop32

tail16:
	TBZ	$4, R2, tail4 // n&16 == 0: no 16-element block
	VLD1.P	16(R1), [V16.B16]
	VLD1.P	64(R0), [V18.S4, V19.S4, V20.S4, V21.S4]
	VTBL	V28.B16, [V16.B16], V8.B16
	VTBL	V29.B16, [V16.B16], V9.B16
	VTBL	V30.B16, [V16.B16], V10.B16
	VTBL	V31.B16, [V16.B16], V11.B16
	WORD	$0x6e21d908 // ucvtf v8.4s, v8.4s
	WORD	$0x6e21d929 // ucvtf v9.4s, v9.4s
	WORD	$0x6e21d94a // ucvtf v10.4s, v10.4s
	WORD	$0x6e21d96b // ucvtf v11.4s, v11.4s
	VFMLA	V8.S4, V18.S4, V0.S4
	VFMLA	V9.S4, V19.S4, V1.S4
	VFMLA	V10.S4, V20.S4, V2.S4
	VFMLA	V11.S4, V21.S4, V3.S4

tail4:
	AND	$12, R2, R3 // remaining 4-element steps, in elements (0, 4, 8, 12)
	CBZ	R3, reduce

loop4:
	VLD1.P	4(R1), V16.S[0] // bytes land in lane 0; fbIdx[0] reads only those
	VLD1.P	16(R0), [V18.S4]
	VTBL	V28.B16, [V16.B16], V8.B16
	WORD	$0x6e21d908 // ucvtf v8.4s, v8.4s
	VFMLA	V8.S4, V18.S4, V0.S4
	SUBS	$4, R3, R3
	BNE	loop4

reduce:
	WORD	$0x4e21d400 // fadd v0.4s, v0.4s, v1.4s
	WORD	$0x4e23d442 // fadd v2.4s, v2.4s, v3.4s
	WORD	$0x4e25d484 // fadd v4.4s, v4.4s, v5.4s
	WORD	$0x4e27d4c6 // fadd v6.4s, v6.4s, v7.4s
	WORD	$0x4e22d400 // fadd v0.4s, v0.4s, v2.4s
	WORD	$0x4e26d484 // fadd v4.4s, v4.4s, v6.4s
	WORD	$0x4e24d400 // fadd v0.4s, v0.4s, v4.4s
	WORD	$0x6e20d400 // faddp v0.4s, v0.4s, v0.4s
	WORD	$0x7e30d800 // faddp s0, v0.2s

	AND	$3, R2, R3 // last 0..3 elements, scalar
	CBZ	R3, done

loop1:
	MOVBU.P	1(R1), R4
	FMOVS.P	4(R0), F2
	UCVTFWS	R4, F1
	FMADDS	F1, F0, F2, F0 // F0 += F2*F1
	SUBS	$1, R3, R3
	BNE	loop1

done:
	FMOVS	F0, ret+24(FP)
	RET
