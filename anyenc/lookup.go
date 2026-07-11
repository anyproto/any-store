package anyenc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/klauspost/compress/s2"
)

// lookup.go implements lazy field extraction: reading one field's raw encoded
// bytes out of an encoded document without building the *Value tree. The
// brute-force vector scan reads one vector field from every document on every
// query; a full Parse there costs ~70% of query time (tree build + per-element
// Value churn). RawByPath walks objects key-by-key and skips non-matching
// values with the parser's existing length-only mode (parseValue with c=nil).

// RawByPath returns the raw encoded bytes (leading tag included) of the value
// at the given path inside an encoded document, without building a value tree.
// A compressed document is decompressed into the parser's reusable buffer
// first — the returned slice may alias it and is only valid until the next
// call on this parser. Returns nil when the path is absent. Only valid for
// stored documents (no inverted index-key forms).
func (p *Parser) RawByPath(doc []byte, path ...string) ([]byte, error) {
	// Blocked-by-array maps to nil in both variants, so exact can be dropped.
	raw, _, err := p.RawByPathChecked(doc, path...)
	return raw, err
}

// ValidateRaw checks that b is one structurally well-formed encoded value with
// nothing trailing, using the parser's length-only mode — the same walk a full
// parse performs, minus the Value tree. Callers that skip the full parse for
// speed (the raw-filter reject path) use it to keep the parse error surface:
// corruption anywhere in the document still fails, it is not silently skipped.
func ValidateRaw(b []byte) error {
	_, tail, err := parseValue(b, nil, 0)
	if err != nil {
		return err
	}
	if len(tail) != 0 {
		return fmt.Errorf("unexpected tail")
	}
	return nil
}

// RawByPathChecked is RawByPath with an exactness signal for callers that must
// mirror Value.Get semantics precisely. exact=false means the raw walk hit a
// TypeArray container at a path segment — Value.Get would descend into it when
// the segment is a valid numeric index, so the caller must fall back to a full
// parse to decide. When exact=true, the result (including nil for an absent
// path) is byte-for-byte what MarshalTo of Get's result would produce.
func (p *Parser) RawByPathChecked(doc []byte, path ...string) (raw []byte, exact bool, err error) {
	b, blocked, err := p.rawSeek(doc, path)
	if err != nil || blocked {
		return nil, false, err
	}
	if b == nil {
		return nil, true, nil
	}
	tail, err := skipLen(b, len(path))
	if err != nil {
		return nil, false, err
	}
	return b[:len(b)-len(tail)], true, nil
}

// Float32sByPath decodes the vector field at path directly into buf — the
// fused RawByPath + AppendFloat32s: the decode self-terminates, so the value
// is walked once instead of twice (skip-parse for length, then decode).
// ok=false when the path is absent or the value is not a vector.
func (p *Parser) Float32sByPath(doc []byte, buf []float32, path ...string) ([]float32, bool, error) {
	b, _, err := p.rawSeek(doc, path)
	if err != nil || b == nil {
		return buf, false, err
	}
	out, ok := AppendFloat32s(b, buf)
	return out, ok, nil
}

// DecodedDoc returns the document with a compressed root decoded. For an
// uncompressed root it returns doc unchanged; for a TypeCompressedObjectS2
// root it decompresses into the parser's reusable buffer and returns that —
// the result is only valid until the next decompressing call on this parser.
// Lets multi-field readers (e.g. a filter referencing several keys) pay the
// decompression once and run RawByPath walks against the decoded bytes.
func (p *Parser) DecodedDoc(doc []byte) ([]byte, error) {
	if len(doc) == 0 {
		return nil, fmt.Errorf("expected value, but got 0 bytes")
	}
	if Type(doc[0]) != TypeCompressedObjectS2 {
		return doc, nil
	}
	b := doc
	if len(b) < 5 {
		return nil, fmt.Errorf("compressed object: expected at least 5 bytes, got %d", len(b))
	}
	compLen := binary.BigEndian.Uint32(b[1:5])
	// Exactly one compressed root, nothing trailing: a full parse of the same
	// document rejects a tail after the envelope, and callers use DecodedDoc to
	// stand in for that parse (raw-filter reject path) — keep the surfaces equal.
	if uint32(len(b)-5) != compLen {
		return nil, fmt.Errorf("compressed object: expected %d compressed bytes, got %d", compLen, len(b)-5)
	}
	compressed := b[5 : 5+compLen]
	dLen, err := s2.DecodedLen(compressed)
	if err != nil {
		return nil, fmt.Errorf("compressed object: s2 header: %w", err)
	}
	if dLen > maxDecompressedSize {
		return nil, fmt.Errorf("compressed object: decoded size %d exceeds limit %d", dLen, maxDecompressedSize)
	}
	p.c.decompBuf, err = s2.Decode(p.c.decompBuf[:cap(p.c.decompBuf)], compressed)
	if err != nil {
		return nil, fmt.Errorf("compressed object: s2 decode: %w", err)
	}
	return p.c.decompBuf, nil
}

// rawSeek positions at the value for path inside the encoded document and
// returns the remainder of the buffer starting at that value's tag byte
// (unbounded — the caller knows how to consume it), or nil when absent.
// blocked reports that the walk hit a TypeArray container at a path segment:
// Value.Get would traverse it when the segment is a valid numeric index, but
// the raw walker does not — callers needing exact Get parity must fall back
// to a full parse in that case (see RawByPathChecked).
func (p *Parser) rawSeek(doc []byte, path []string) (val []byte, blocked bool, err error) {
	b, err := p.DecodedDoc(doc)
	if err != nil {
		return nil, false, err
	}

	for depth, seg := range path {
		if len(b) == 0 || Type(b[0]) != TypeObject {
			if len(b) > 0 && Type(b[0]) == TypeArray {
				return nil, true, nil // array container: Get-index semantics need a full parse
			}
			return nil, false, nil // path runs through a non-object — absent
		}
		b = b[1:]
		var match []byte
		for {
			if len(b) == 0 {
				return nil, false, fmt.Errorf("parse object: unexpected end")
			}
			if b[0] == byte(EOS) {
				break // end of object, key not found
			}
			// Key scan mirrors parseObject: IndexByte plus escape-pair check.
			eosI := bytes.IndexByte(b, byte(EOS))
			if eosI < 0 {
				return nil, false, fmt.Errorf("parse object key: end of string not found")
			}
			escaped := false
			if eosI+1 < len(b) && b[eosI+1] == ^byte(EOS) {
				var ok bool
				eosI, escaped, ok = scanTerm(b, byte(EOS))
				if !ok {
					return nil, false, fmt.Errorf("parse object key: end of string not found")
				}
			}
			raw := b[:eosI]
			var equal bool
			if escaped || (len(raw) > 0 && raw[0] == emptyKey) {
				equal = decodeKey(raw, escaped) == seg
			} else {
				equal = string(raw) == seg
			}
			if equal {
				match = b[eosI+1:]
				break
			}
			// Skip this value with the parser's length-only mode.
			var err error
			if _, b, err = parseValue(b[eosI+1:], nil, depth+1); err != nil {
				return nil, false, err
			}
		}
		if match == nil {
			return nil, false, nil
		}
		b = match
	}
	return b, false, nil
}

// skipLen returns the tail after the encoded value at the start of b, using
// the parser's length-only mode.
func skipLen(b []byte, depth int) ([]byte, error) {
	_, tail, err := parseValue(b, nil, depth)
	return tail, err
}

// AppendFloat32s decodes the raw encoded value (as returned by RawByPath)
// into buf as a float32 vector. Both vector representations are supported:
// the packed TypeVectorF32 blob and a TypeArray of numbers. Returns ok=false
// when the value is neither (callers treat that as "field is not a vector").
func AppendFloat32s(raw []byte, buf []float32) ([]float32, bool) {
	if len(raw) == 0 {
		return buf, false
	}
	switch Type(raw[0]) {
	case TypeVectorF32:
		b := raw[1:]
		if len(b) < 4 {
			return buf, false
		}
		l := binary.BigEndian.Uint32(b)
		if l%4 != 0 || len(b[4:]) < int(l) {
			return buf, false
		}
		b = b[4 : 4+l]
		for i := 0; i+4 <= len(b); i += 4 {
			buf = append(buf, math.Float32frombits(binary.LittleEndian.Uint32(b[i:])))
		}
		return buf, true
	case TypeArray:
		b := raw[1:]
		for {
			if len(b) == 0 {
				return buf, false
			}
			if b[0] == byte(EOS) {
				return buf, true
			}
			if Type(b[0]) != TypeNumber || len(b) < 9 {
				return buf, false
			}
			buf = append(buf, float32(BytesToFloat64(b[1:])))
			b = b[9:]
		}
	default:
		return buf, false
	}
}
