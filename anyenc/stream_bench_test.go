package anyenc

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

func benchStreamDocs() (small, large *Value) {
	small = MustParseJson(`{"id":"6650a1b2c3d4e5f607182930","title":"Quarterly planning notes","count":42,"score":0.87,` +
		`"done":false,"tags":["planning","q3","team"],"meta":{"author":"someone","rev":7,"archived":null}}`)
	a := &Arena{}
	large = a.NewObject()
	for i := range 10_000 {
		large.Set(fmt.Sprintf("field%05d", i), a.NewString(fmt.Sprintf("value %d of the large benchmark document", i)))
	}
	return small, large
}

func BenchmarkWriter(b *testing.B) {
	small, large := benchStreamDocs()
	for _, bc := range []struct {
		name string
		doc  *Value
	}{
		{"small", small},
		{"large", large},
	} {
		b.Run(bc.name, func(b *testing.B) {
			w := NewWriter(io.Discard)
			b.SetBytes(int64(len(bc.doc.MarshalTo(nil))))
			b.ReportAllocs()
			for b.Loop() {
				if err := w.Write(bc.doc); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkReader reads an endless stream of one repeated document. The
// parse_* cases are the baseline, Parser.ParseOwned on the document alone, so
// the gap to the matching reader case is the Reader's own cost. Reads land
// mid-value as a file's would, except in the *_aligned_reads cases, where every
// read stops at a value boundary and each value is parsed once, in place.
func BenchmarkReader(b *testing.B) {
	small, large := benchStreamDocs()
	for _, bc := range []struct {
		name    string
		doc     *Value
		chunk   int
		aligned bool
	}{
		{"small", small, 0, false},
		{"small_aligned_reads", small, 0, true},
		{"large", large, 0, false},
		{"large_aligned_reads", large, 0, true},
		{"large_4KiB_reads", large, 4 << 10, false},
	} {
		enc := bc.doc.MarshalTo(nil)
		b.Run(bc.name, func(b *testing.B) {
			var src io.Reader = &repeatReader{data: enc, aligned: bc.aligned}
			if bc.chunk > 0 {
				src = &chunkReader{r: src, n: bc.chunk}
			}
			r := NewReader(src)
			p := &Parser{}
			b.SetBytes(int64(len(enc)))
			b.ReportAllocs()
			for b.Loop() {
				if _, err := r.Read(p); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	for _, bc := range []struct {
		name string
		doc  *Value
	}{
		{"parse_small", small},
		{"parse_large", large},
	} {
		enc := bc.doc.MarshalTo(nil)
		b.Run(bc.name, func(b *testing.B) {
			p := &Parser{}
			b.SetBytes(int64(len(enc)))
			b.ReportAllocs()
			for b.Loop() {
				if _, err := p.ParseOwned(enc); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkWriterReaderRoundTrip(b *testing.B) {
	small, _ := benchStreamDocs()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	p := &Parser{}
	b.ReportAllocs()
	for b.Loop() {
		buf.Reset()
		for range 1000 {
			_ = w.Write(small)
		}
		_ = w.Flush()
		r := NewReader(&buf)
		for {
			if _, err := r.Read(p); err == io.EOF {
				break
			} else if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// repeatReader serves data over and over without end. It fills the whole
// buffer, like a file, unless aligned is set, where each read stops at the end
// of one copy of data.
type repeatReader struct {
	data    []byte
	off     int
	aligned bool
}

func (r *repeatReader) Read(b []byte) (int, error) {
	if r.aligned {
		n := copy(b, r.data[r.off:])
		r.off = (r.off + n) % len(r.data)
		return n, nil
	}
	var n int
	for n < len(b) {
		c := copy(b[n:], r.data[r.off:])
		n += c
		r.off = (r.off + c) % len(r.data)
	}
	return n, nil
}
