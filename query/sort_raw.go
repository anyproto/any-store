package query

import (
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/syncpool"
)

// RawSort is an optional fast path implemented by sorts that can build their
// key from the raw encoded document (root already decoded — see
// anyenc.Parser.DecodedDoc) without a full parse. The unindexed sort path
// fetches and parses every candidate document just to read the sort fields;
// seeking the fields' raw fragments instead skips the tree build and the
// scanning of everything past them (see RawFilter for the same idea on the
// filter side).
//
// handled=false means the key could not be built from the raw bytes (an array
// container on a path, malformed bytes) — k is returned unchanged (any partial
// append truncated) and the caller must fall back to AppendKey on the parsed
// document. When handled=true, the appended bytes are identical to what
// AppendKey would produce (TestSortAppendKeyRawParity).
type RawSort interface {
	AppendKeyRaw(k anyenc.Tuple, doc []byte, buf *syncpool.DocBuffer) (anyenc.Tuple, bool)
}

func (s *SortField) AppendKeyRaw(k anyenc.Tuple, doc []byte, buf *syncpool.DocBuffer) (anyenc.Tuple, bool) {
	if buf == nil {
		return k, false
	}
	raw, exact, err := buf.Parser.RawByPathChecked(doc, s.Path...)
	if err != nil || !exact {
		return k, false
	}
	// An absent path appends TypeNull via the nil Value, exactly like
	// v.Get(...) returning nil on the parsed-document path.
	var fv *anyenc.Value
	if raw != nil {
		if fv, err = buf.Parser.ParseOwned(raw); err != nil {
			return k, false
		}
	}
	if !s.Reverse {
		return k.Append(fv), true
	}
	return k.AppendInverted(fv), true
}

func (ss Sorts) AppendKeyRaw(k anyenc.Tuple, doc []byte, buf *syncpool.DocBuffer) (anyenc.Tuple, bool) {
	// All fields must build lazily or none: a partial key must not survive, so
	// on any unhandled field the appended prefix is truncated back.
	start := len(k)
	for _, s := range ss {
		rs, isRaw := s.(RawSort)
		if !isRaw {
			return k[:start], false
		}
		var handled bool
		if k, handled = rs.AppendKeyRaw(k, doc, buf); !handled {
			return k[:start], false
		}
	}
	return k, true
}

// AppendKeyRaw is inert like AppendKey: relevance order comes from the
// full-text scan itself, no document field is read.
func (TextScoreSort) AppendKeyRaw(k anyenc.Tuple, _ []byte, _ *syncpool.DocBuffer) (anyenc.Tuple, bool) {
	return k, true
}
