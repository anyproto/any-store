package anyenc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
)

// A stream is a sequence of encoded values written back to back with nothing
// between them. Every value is self-delimiting, so one MarshalTo output, or
// several concatenated, is a valid stream.

const (
	// streamBufSize is the Writer flush threshold and the Reader's minimum
	// buffer growth.
	streamBufSize = 64 << 10
	// maxStreamValueSize bounds what a Reader buffers for one value, so corrupt
	// input (a lost terminator, a bogus length header) fails instead of pulling
	// the rest of the stream into memory. Same bound the parser puts on a
	// decompressed object.
	maxStreamValueSize = maxDecompressedSize
	// maxEmptyReads is how many consecutive (0, nil) reads Reader tolerates
	// before failing with io.ErrNoProgress.
	maxEmptyReads = 100
)

// Writer writes a stream of encoded values to an io.Writer.
//
// Values are buffered: call Flush after the last Write. After an error, every
// call returns that error.
type Writer struct {
	w   io.Writer
	buf []byte
	err error
}

// NewWriter returns a Writer that writes to w.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w, buf: make([]byte, 0, streamBufSize)}
}

// Write appends the encoding of v to the stream.
func (w *Writer) Write(v *Value) error {
	if w.err != nil {
		return w.err
	}
	w.buf = v.MarshalTo(w.buf)
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
	w.buf = w.buf[:0]
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
				r.off += len(b) - len(tail)
				return v, nil
			}
			r.scanning = true
		}
		// The parse ran out of input or failed: the scanner tells which,
		// resuming across reads until the value is complete.
		n, err := r.scan.next(b, final)
		if err != nil {
			r.err = err
			break
		}
		if n == 0 {
			r.err = r.fill(b)
			continue
		}
		p.c.reset()
		v, _, err := parseValue(b[:n], &p.c, 0)
		if err != nil {
			r.err = err
			break
		}
		r.off += n
		r.scanning = false
		return v, nil
	}
	return nil, r.err
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
	case len(b) >= r.limit:
		return fmt.Errorf("stream value exceeds %d bytes", r.limit)
	}
	if r.off > 0 {
		r.buf = r.buf[:copy(r.buf, b)]
		r.off = 0
	}
	if len(r.buf) == cap(r.buf) {
		r.buf = slices.Grow(r.buf, max(len(r.buf), streamBufSize))
	}
	for range maxEmptyReads {
		n, err := r.r.Read(r.buf[len(r.buf):cap(r.buf)])
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
}

// next returns the length of the value starting at b[0], or 0 when b ends
// before the value does. Until a value is returned, each call's b must extend
// the previous one. final reports that no bytes follow b.
func (s *valueScanner) next(b []byte, final bool) (int, error) {
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
			// Only a top-level string can end at the last byte of the stream;
			// inside a container more of the value follows every terminator.
			end, ok := s.term(b, s.pos+1, final && len(s.open) == 0)
			if !ok {
				return 0, nil
			}
			s.pos = end + 1
		default:
			n, err := scalarLen(b[s.pos:])
			if err != nil {
				return 0, err
			}
			if n == 0 || s.pos+n > len(b) {
				return 0, nil
			}
			s.pos += n
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
// of b, or 0 when b is too short to tell.
func scalarLen(b []byte) (int, error) {
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
		return 5 + int(binary.BigEndian.Uint32(b[1:5])), nil
	default:
		return 0, fmt.Errorf("unknown type %d", b[0])
	}
}
