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
	b, err := p.rawSeek(doc, path)
	if err != nil || b == nil {
		return nil, err
	}
	// Bound the value: its length is the distance to the skip-parse tail.
	tail, err := skipLen(b, len(path))
	if err != nil {
		return nil, err
	}
	return b[:len(b)-len(tail)], nil
}

// Float32sByPath decodes the vector field at path directly into buf — the
// fused RawByPath + AppendFloat32s: the decode self-terminates, so the value
// is walked once instead of twice (skip-parse for length, then decode).
// ok=false when the path is absent or the value is not a vector.
func (p *Parser) Float32sByPath(doc []byte, buf []float32, path ...string) ([]float32, bool, error) {
	b, err := p.rawSeek(doc, path)
	if err != nil || b == nil {
		return buf, false, err
	}
	out, ok := AppendFloat32s(b, buf)
	return out, ok, nil
}

// rawSeek positions at the value for path inside the encoded document and
// returns the remainder of the buffer starting at that value's tag byte
// (unbounded — the caller knows how to consume it), or nil when absent.
func (p *Parser) rawSeek(doc []byte, path []string) ([]byte, error) {
	if len(doc) == 0 {
		return nil, fmt.Errorf("expected value, but got 0 bytes")
	}
	b := doc
	if Type(b[0]) == TypeCompressedObjectS2 {
		if len(b) < 5 {
			return nil, fmt.Errorf("compressed object: expected at least 5 bytes, got %d", len(b))
		}
		compLen := binary.BigEndian.Uint32(b[1:5])
		if uint32(len(b)-5) < compLen {
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
		b = p.c.decompBuf
	}

	for depth, seg := range path {
		if len(b) == 0 || Type(b[0]) != TypeObject {
			return nil, nil // path runs through a non-object — absent
		}
		b = b[1:]
		var match []byte
		for {
			if len(b) == 0 {
				return nil, fmt.Errorf("parse object: unexpected end")
			}
			if b[0] == byte(EOS) {
				break // end of object, key not found
			}
			// Key scan mirrors parseObject: IndexByte plus escape-pair check.
			eosI := bytes.IndexByte(b, byte(EOS))
			if eosI < 0 {
				return nil, fmt.Errorf("parse object key: end of string not found")
			}
			escaped := false
			if eosI+1 < len(b) && b[eosI+1] == ^byte(EOS) {
				var ok bool
				eosI, escaped, ok = scanTerm(b, byte(EOS))
				if !ok {
					return nil, fmt.Errorf("parse object key: end of string not found")
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
				return nil, err
			}
		}
		if match == nil {
			return nil, nil
		}
		b = match
	}
	return b, nil
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
