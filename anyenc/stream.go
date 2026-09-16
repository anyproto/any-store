package anyenc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
)

// A stream is a sequence of encoded values written back to back with nothing
// between them, so one MarshalTo output, or several concatenated, is a valid
// stream. Values delimit themselves, except that a top-level string needs the
// byte after its terminator to tell an escape pair from the end of the value
// (see escape.go), which a Reader gets from the next value or from EOF.

const (
	// streamBufSize is the Writer flush threshold and the Reader's minimum
	// buffer growth.
	streamBufSize = 64 << 10
	// streamBufKeep is the buffer size a Writer or Reader keeps without question.
	// Above it, a buffer grown by one huge value is given back once the stream
	// has moved on, so the value does not pin it for the rest of a long export.
	// The bound is generous because re-growing costs a copy per doubling:
	// ordinary large values never reach it, only outliers do.
	streamBufKeep = 4 << 20
	// streamShrinkAfter is how many consecutive small values it takes to call a
	// large value past. A large value resets the count, so a stream that keeps
	// producing them never gives its buffer back and never re-grows it.
	streamShrinkAfter = 64
	// maxStreamValueSize bounds one value: what a Reader buffers for it and
	// what a Writer emits for it, so corrupt input (a lost terminator, a bogus
	// length header) fails instead of pulling the rest of the stream into
	// memory, and no Writer builds a dump its Reader would reject. Same bound
	// the parser puts on a decompressed object.
	maxStreamValueSize = maxDecompressedSize
	// maxEmptyReads is how many consecutive (0, nil) reads Reader tolerates
	// before failing with io.ErrNoProgress.
	maxEmptyReads = 100
)

var (
	// ErrValueTooLarge reports a value over the stream limit: written, or
	// declared by a length header in the input.
	ErrValueTooLarge = errors.New("anyenc: value exceeds stream limit")
	// ErrUnknownValueType reports a value MarshalTo cannot encode, which would
	// leave nothing in the stream in its place.
	ErrUnknownValueType = errors.New("anyenc: value of unknown type")
)

// Writer writes a stream of encoded values to an io.Writer.
//
// Values are buffered: call Flush after the last Write. After an error, every
// call returns that error.
type Writer struct {
	w        io.Writer
	buf      []byte
	err      error
	limit    int // max bytes for one value
	sinceBig int // consecutive small values written
}

// NewWriter returns a Writer that writes to w.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w, buf: make([]byte, 0, streamBufSize), limit: maxStreamValueSize}
}

// Write appends the encoding of v to the stream. Values a Reader could not
// read back are rejected and left out of the stream, without ending it: one
// over the size limit, and one of an unknown type, which MarshalTo encodes as
// nothing at all. Only v itself is checked for that - a value of unknown type
// nested inside v encodes as nothing wherever it sits, stream or document.
func (w *Writer) Write(v *Value) error {
	if w.err != nil {
		return w.err
	}
	// The size is measured after marshaling, which for an oversized value
	// allocates it first. Pre-checking with IsSizeBigger costs 40% of Write on
	// every ordinary value to bound a case the caller already holds in memory:
	// here the limit keeps dumps readable, it does not fend off hostile input
	// the way the Reader's does.
	mark := len(w.buf)
	w.buf = v.MarshalTo(w.buf)
	n := len(w.buf) - mark
	switch {
	case n == 0:
		w.buf = w.buf[:mark]
		return fmt.Errorf("%w: type %d", ErrUnknownValueType, v.Type())
	case n > w.limit:
		w.buf = w.buf[:mark]
		return fmt.Errorf("%w: %d bytes, limit %d", ErrValueTooLarge, n, w.limit)
	}
	if n > streamBufSize {
		w.sinceBig = 0
	} else {
		w.sinceBig++
	}
	if len(w.buf) >= streamBufSize {
		return w.Flush()
	}
	return nil
}

// Flush writes buffered values to the underlying io.Writer.
func (w *Writer) Flush() error {
	if w.err != nil || len(w.buf) == 0 {
		return w.err
	}
	n, err := w.w.Write(w.buf)
	if err == nil && n < len(w.buf) {
		err = io.ErrShortWrite
	}
	if cap(w.buf) > streamBufKeep && w.sinceBig >= streamShrinkAfter {
		w.buf = make([]byte, 0, streamBufSize)
	} else {
		w.buf = w.buf[:0]
	}
	w.err = err
	return err
}

// Reader reads a stream of encoded values from an io.Reader.
//
// Read returns io.EOF when the stream ends between values, and
// io.ErrUnexpectedEOF when it ends inside one. Every error is final: later
// calls return it again.
type Reader struct {
	r        io.Reader
	buf      []byte
	off      int // start of the pending value in buf
	scan     valueScanner
	scanning bool  // the pending value did not parse from what is buffered
	srcErr   error // error from r (io.EOF included), reported once buf is drained
	err      error
	limit    int // max bytes buffered for one value
	sinceBig int // consecutive small values read
}

// NewReader returns a Reader that reads from r.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: r, limit: maxStreamValueSize}
}

// Read decodes the next value of the stream with p. The value is parsed in
// place and references the Reader's buffer: it is valid until the next Read,
// or the next Parse* call on p.
func (r *Reader) Read(p *Parser) (*Value, error) {
	for r.err == nil {
		b := r.buf[r.off:]
		final := r.srcErr == io.EOF
		if !r.scanning && len(b) > 0 {
			// Usually the whole value is buffered: one parse finds its end.
			p.c.reset()
			v, tail, err := parseValue(b, &p.c, 0)
			// A top-level string's terminator is settled only by the next byte
			// (see escape.go), so one ending the buffer goes to the scanner.
			if err == nil && (len(tail) > 0 || final || v.t != TypeString) {
				r.took(len(b) - len(tail))
				return v, nil
			}
			r.scanning = true
		}
		// The parse ran out of input or failed: the scanner tells which,
		// resuming across reads until the value is complete.
		n, err := r.scan.next(b, r.limit, final)
		if err != nil {
			r.err = err
			break
		}
		if n == 0 {
			r.err = r.fill(b)
			continue
		}
		p.c.reset()
		v, tail, err := parseValue(b[:n], &p.c, 0)
		if err == nil && len(tail) != 0 {
			// The scanner's length rules disagree with the parser's; skipping
			// the extra bytes would silently drop part of the stream.
			err = fmt.Errorf("anyenc: scanned %d bytes, parsed %d", n, n-len(tail))
		}
		if err != nil {
			r.err = err
			break
		}
		r.took(n)
		r.scanning = false
		return v, nil
	}
	return nil, r.err
}

// took consumes the n bytes of the value just returned. A large value resets
// the small-value run that fill waits for before giving its buffer back.
func (r *Reader) took(n int) {
	r.off += n
	if n > streamBufSize {
		r.sinceBig = 0
	} else {
		r.sinceBig++
	}
}

// fill reads more input for the pending value b. It returns the terminal
// error when no input can complete b.
func (r *Reader) fill(b []byte) error {
	switch {
	case r.srcErr == io.EOF && len(b) == 0:
		return io.EOF
	case r.srcErr == io.EOF:
		return io.ErrUnexpectedEOF
	case r.srcErr != nil:
		return r.srcErr
	case len(b) > r.limit:
		return fmt.Errorf("%w: buffered %d bytes, limit %d", ErrValueTooLarge, len(b), r.limit)
	}
	switch {
	case cap(r.buf) > streamBufKeep && len(b) <= streamBufSize && r.sinceBig >= streamShrinkAfter:
		// The big value that grew this buffer is long past; give it back.
		r.buf = append(make([]byte, 0, streamBufSize), b...)
		r.off = 0
	case r.off > 0:
		r.buf = r.buf[:copy(r.buf, b)]
		r.off = 0
	}
	// A value of exactly limit bytes is legal, and a top-level string needs one
	// byte past its terminator to end (see escape.go), so the window is one
	// byte wider than the limit.
	window := r.limit + 1
	if len(r.buf) == cap(r.buf) {
		grow := max(len(r.buf), streamBufSize)
		// A length-prefixed value says up front how much it needs: one growth
		// instead of a dozen doublings, each copying the partial value.
		if need := r.scan.need - len(r.buf); need > grow {
			grow = need
		}
		r.buf = slices.Grow(r.buf, min(grow, window-len(r.buf)))
	}
	// Grow rounds capacity up, so the read window, not the capacity, is what
	// holds buffering to the limit.
	end := min(cap(r.buf), window)
	for range maxEmptyReads {
		n, err := r.r.Read(r.buf[len(r.buf):end])
		r.buf = r.buf[:len(r.buf)+n]
		if err != nil {
			r.srcErr = err
			return nil
		}
		if n > 0 {
			return nil
		}
	}
	return io.ErrNoProgress
}

// valueScanner finds where the first value of a growing buffer ends. It walks
// containers iteratively and keeps its place between calls, so a value that
// arrives over many reads is scanned once in total, not once per read. It
// checks only what locating the end needs; Parse validates the whole value.
type valueScanner struct {
	pos    int    // length of the pending value scanned so far
	open   []bool // open containers, innermost last; true for an object
	atKey  bool   // pos is at an object key or the object's terminator
	resume int    // where the pending string or key terminator search continues
	need   int    // total length of the pending value when a header declares it
}

// next returns the length of the value starting at b[0], or 0 when b ends
// before the value does. Until a value is returned, each call's b must extend
// the previous one. final reports that no bytes follow b. A value whose length
// header declares more than limit bytes is rejected on the spot, before the
// caller buffers any of it; limit <= 0 does not limit.
func (s *valueScanner) next(b []byte, limit int, final bool) (int, error) {
	s.need = 0
	for s.pos == 0 || len(s.open) > 0 {
		if s.pos >= len(b) {
			return 0, nil
		}
		inArray := len(s.open) > 0 && !s.open[len(s.open)-1]
		if (s.atKey || inArray) && b[s.pos] == EOS {
			s.open = s.open[:len(s.open)-1]
			s.pos++
			s.atKey = len(s.open) > 0 && s.open[len(s.open)-1]
			continue
		}
		if s.atKey {
			end, ok := s.term(b, s.pos, false)
			if !ok {
				return 0, nil
			}
			s.pos = end + 1
			s.atKey = false
			continue
		}
		switch t := Type(b[s.pos]); t {
		case TypeObject, TypeArray:
			if len(s.open) >= maxParseDepth {
				return 0, fmt.Errorf("max parse depth (%d) exceeded", maxParseDepth)
			}
			s.open = append(s.open, t == TypeObject)
			s.pos++
			s.atKey = t == TypeObject
			continue
		case TypeString:
			end, ok := s.term(b, s.pos+1, final)
			if !ok {
				return 0, nil
			}
			s.pos = end + 1
		default:
			n, err := scalarLen(b[s.pos:])
			if err != nil {
				return 0, err
			}
			if n == 0 {
				return 0, nil // length header still incomplete
			}
			end := int64(s.pos) + n
			if end > math.MaxInt || (limit > 0 && end > int64(limit)) {
				return 0, fmt.Errorf("%w: header declares %d bytes, limit %d", ErrValueTooLarge, end, limit)
			}
			if end > int64(len(b)) {
				s.need = int(end)
				return 0, nil
			}
			s.pos = int(end)
		}
		s.atKey = len(s.open) > 0 && s.open[len(s.open)-1]
	}
	n := s.pos
	*s = valueScanner{open: s.open}
	return n, nil
}

// term returns the index of the terminator of the string or key starting at
// start: the first EOS not followed by the escape tail (see escape.go). An EOS
// at the end of b is the terminator only when final; otherwise the next byte
// decides. ok is false when b ends first.
func (s *valueScanner) term(b []byte, start int, final bool) (int, bool) {
	i := max(start, s.resume)
	for {
		j := bytes.IndexByte(b[i:], EOS)
		if j < 0 {
			s.resume = len(b)
			return 0, false
		}
		i += j
		if i+1 == len(b) && !final {
			s.resume = i
			return 0, false
		}
		if i+1 == len(b) || b[i+1] != ^EOS {
			s.resume = 0
			return i, true
		}
		i += 2
	}
}

// scalarLen returns the encoded length of the non-string scalar at the start
// of b, or 0 when b is too short to tell. It is int64 because a length header
// is an untrusted uint32, which overflows int where int is 32 bits.
func scalarLen(b []byte) (int64, error) {
	switch Type(b[0]) {
	case TypeNull, TypeTrue, TypeFalse:
		return 1, nil
	case TypeNumber:
		return 9, nil
	case TypeObjectID:
		return 1 + objectIDLen, nil
	case TypeDateTime:
		return 1 + dateTimeLen, nil
	case TypeBinary, TypeVectorF32, TypeCompressedObjectS2:
		if len(b) < 5 {
			return 0, nil
		}
		return 5 + int64(binary.BigEndian.Uint32(b[1:5])), nil
	default:
		return 0, fmt.Errorf("unknown type %d", b[0])
	}
}
