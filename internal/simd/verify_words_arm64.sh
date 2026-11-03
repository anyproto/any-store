#!/usr/bin/env bash
# Checks the WORD encodings in dot_float_byte_arm64.s (vector UCVTF/FADD/FADDP,
# which Go's assembler cannot spell) in two independent ways:
#
#  1. clang: each WORD's hex must equal what clang assembles from the
#     instruction in its trailing comment. Needs clang and llvm-objdump (on
#     macOS OBJDUMP=objdump is LLVM's; a GNU objdump works only if built for
#     AArch64).
#  2. Go: the package is built for arm64 (GOARCH=arm64 cross-compiles on any
#     host) and read back with go tool objdump. Each WORD's source line must be
#     in the binary with the same hex and decode to the comment's mnemonic.
#     Those decoded lines are printed.
#
# Exits non-zero, printing MISMATCH, COUNT, MISSING or the offending line, on a
# wrong encoding, a WORD without a parsable instruction comment, or any WORD it
# failed to extract or to find in the binary. Run from any directory.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
f=dot_float_byte_arm64.s
cd "$here"
d=$(mktemp -d)
trap 'rm -rf "$d"' EXIT
: > "$d/want"; : > "$d/insn"; : > "$d/lines"

# want: the WORD hex; insn: the commented instruction, for clang;
# lines: source line, hex and mnemonic, for the Go check.
awk -v d="$d" '/^[ \t]*WORD[ \t]/ {
	if (!match($0, /\$0[xX][0-9a-fA-F]+/)) { print "BAD OPERAND: " $0; exit 1 }
	h = tolower(substr($0, RSTART + 3, RLENGTH - 3)); while (length(h) < 8) h = "0" h
	c = index($0, "//"); s = c ? substr($0, c + 2) : ""; gsub(/^[ \t]+|[ \t]+$/, "", s)
	if (s == "") { print "NO INSTRUCTION COMMENT: " $0; exit 1 }
	split(s, m, /[ \t]+/)
	print "0x" h > (d "/want"); print s > (d "/insn"); print FNR, h, tolower(m[1]) > (d "/lines") }' "$f"
n=$(grep -cE '^[[:space:]]*WORD([[:space:]]|$)' "$f" || true)

# 1. clang
clang -c -x assembler --target=aarch64-linux-gnu -o "$d/w.o" "$d/insn"
"${OBJDUMP:-llvm-objdump}" -d "$d/w.o" | awk '/^ *[0-9a-f]+:/ {print "0x" $2}' > "$d/got"
w=$(awk 'END {print NR}' "$d/want"); g=$(awk 'END {print NR}' "$d/got")
[ "$n" -gt 0 ] && [ "$w" -eq "$n" ] && [ "$g" -eq "$n" ] || { echo "COUNT: $n WORD, $w parsed, $g assembled"; exit 1; }
paste "$d/want" "$d/got" | awk '$1 != $2 {bad = 1; print "MISMATCH", $0} END {exit bad}'
echo "clang: $n WORD encodings match their comments"

# 2. Go
GOARCH=arm64 go test -c -o "$d/simd.test" .
go tool objdump -s 'dotFloatByteNEONAsm' "$d/simd.test" > "$d/go"
awk -v f="$f" '
	FNR == NR { hex[$1] = $2; mn[$1] = $3; next }
	{ split($1, p, ":"); l = p[2] }
	p[1] == f && (l in hex) {
		seen[l] = 1; print
		m = tolower($4); if (m == "v" mn[l]) m = mn[l] # Go spells some vector ops with a V prefix
		if ($3 != hex[l] || m != mn[l]) { bad = 1; print "MISMATCH line " l ": want " hex[l] " " mn[l] }
	}
	END {
		for (l in hex) if (!(l in seen)) { bad = 1; print "MISSING: line " l " not in the binary" }
		exit bad
	}' "$d/lines" "$d/go"
echo "go: $n WORD encodings decode to their comments' mnemonics"
