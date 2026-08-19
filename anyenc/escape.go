package anyenc

import "bytes"

// String and object-key escaping.
//
// Strings and keys are EOS(0x00)-terminated so that encoded values stay
// memcmp-orderable. A raw 0x00 inside the data would be read as the
// terminator, so it is escaped as the pair (0x00, 0xFF). The scan is
// unambiguous because the escape tail 0xFF never legally follows a true
// terminator: after a string or key terminator comes a type tag
// (0x01..0x0C normal, 0xF3..0xFE inverted), a container EOS, an
// emptyKey-prefixed or non-0xFF-leading key byte (see below), or the end
// of input. Order is preserved: "a" < "a\x00" < "a\x01b" ⇔
// (61 00) < (61 00 FF 00) < (61 01 62 00).
//
// BACK-COMPATIBILITY: the historical encoder silently STRIPPED EOS bytes
// from strings and keys, so existing stored data contains no escape pairs
// and parses bit-identically under these rules; NUL-free values also
// encode bit-identically, so old and new encodings interoperate inside
// the same btree. Two legacy shapes are documented as UNSUPPORTED — both
// require an object key that is invalid UTF-8 at byte 0, which the JSON
// ingestion path cannot produce, written by the OLD encoder:
//   - a key whose first byte is 0xFF following a string value (the value's
//     terminator plus that byte read as an escape pair); new writes
//     emptyKey-prefix such keys;
//   - a key starting with the two bytes (0x1F, 0xFF), which the decode
//     prefix-strip rule below reads as a new-format escaped key "\xff...".
// Numbers: -0.0 now encodes as +0.0 (see AppendFloat64); indexes built
// before this change that contain -0.0 entries should be rebuilt.
//
// CAVEAT (prefix overlap): the encoding of "a" is a byte-prefix of the
// encoding of "a\x00b" — unavoidable while keeping the old single-byte
// terminator. Whole-key comparisons stay correct (the escape pair sorts
// exactly where the raw string would), but PREFIX-based index bounds must
// account for escape continuations (0xFF after a complete ascending
// encoding, 0x00 after an inverted one) — see qplanner bound padding.
//
// Object keys reserve their FIRST byte:
//   - emptyKey (0x1F) alone encodes the empty key "";
//   - a key whose first byte is 0x00 or 0xFF is prefixed with emptyKey.
//     Decode strips the prefix only when the byte after it is 0x00 (which
//     the old encoder could never produce there) or 0xFF (possible in old
//     data only for an invalid-UTF-8 key, unsupported — see above), so
//     old-format keys that genuinely start with 0x1F otherwise decode
//     verbatim. A key therefore never starts with EOS (keeps the
//     end-of-object check unambiguous; a leading 0x00 would also invert
//     to the inverted end-of-object 0xFF) nor with 0xFF (would collide
//     with the escape tail after a preceding string's terminator).
//   - known wart kept for compatibility: the single-byte key "\x1f"
//     still collides with the empty key and decodes as "".
//
// Inverted (descending index-key) fields invert every byte: terminator
// 0xFF, escape pair (0xFF, 0x00). The scan takes the eos byte and derives
// the escape tail by negation, so the same code serves both directions.

// appendEscaped appends s to dst, escaping every EOS byte as (EOS, 0xFF).
// The marshal hot loops (Value.MarshalTo, marshalObject) do not call this —
// they open-code the short-input scan to avoid call overhead per field (the
// historical appendIgnoreEOS was small enough to inline; this isn't) and
// fall back to appendEscapedSlow.
func appendEscaped(dst []byte, s []byte) []byte {
	if len(s) > 32 {
		return appendEscapedSlow(dst, s)
	}
	for i := 0; i < len(s); i++ {
		if s[i] == EOS {
			return appendEscapedSlow(dst, s)
		}
	}
	return append(dst, s...)
}

// appendEscapedSlow scans with SIMD IndexByte chunks: optimal for long
// inputs and for inputs that actually contain EOS bytes.
func appendEscapedSlow(dst []byte, s []byte) []byte {
	for {
		i := bytes.IndexByte(s, EOS)
		if i < 0 {
			return append(dst, s...)
		}
		dst = append(dst, s[:i+1]...)
		dst = append(dst, ^EOS)
		s = s[i+1:]
	}
}

// appendUnescaped appends the escaped segment s (terminator excluded) to dst,
// collapsing every (EOS, 0xFF) pair back to a single EOS. Call only when the
// scan reported escapes; the caller keeps the plain copy on the common path.
func appendUnescaped(dst []byte, s []byte) []byte {
	for {
		i := bytes.IndexByte(s, EOS)
		if i < 0 || i+1 >= len(s) {
			return append(dst, s...)
		}
		dst = append(dst, s[:i+1]...) // include the EOS, drop the 0xFF tail
		s = s[i+2:]
	}
}

// scanTerm returns the index of the terminator byte of an escaped string or
// key segment and whether any escape pairs were seen. eos is the terminator
// byte: EOS for normal data, ^EOS for inverted index-key data; the escape
// tail is its bitwise negation in both cases.
func scanTerm(b []byte, eos byte) (term int, escaped bool, ok bool) {
	esc := ^eos
	var pos int
	for {
		i := bytes.IndexByte(b[pos:], eos)
		if i < 0 {
			return 0, false, false
		}
		i += pos
		if i+1 < len(b) && b[i+1] == esc {
			escaped = true
			pos = i + 2
			continue
		}
		return i, escaped, true
	}
}

// appendEscapedKey appends an object key with the rules described above,
// followed by the EOS terminator. marshalObject open-codes the common case
// (non-empty key, no reserved leading byte, no NUL) and only calls this for
// the rest.
func appendEscapedKey(dst []byte, key string) []byte {
	if len(key) == 0 {
		return append(dst, emptyKey, EOS)
	}
	if key[0] == EOS || key[0] == ^EOS {
		dst = append(dst, emptyKey)
	}
	dst = appendEscapedSlow(dst, s2b(key))
	return append(dst, EOS)
}

// decodeKey converts a raw escaped key segment (terminator excluded) into the
// key string. Zero-copy (unsafe view into raw) on the common path; allocates
// only when the key was escaped.
func decodeKey(raw []byte, escaped bool) string {
	if len(raw) > 0 && raw[0] == emptyKey {
		if len(raw) == 1 {
			return ""
		}
		if raw[1] == EOS || raw[1] == ^EOS {
			raw = raw[1:]
		}
	}
	if escaped {
		return string(appendUnescaped(nil, raw))
	}
	return b2s(raw)
}
