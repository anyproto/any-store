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
// the gap to the matching reader case is the Reader's own cost.
// large_4KiB_reads feeds each document in ~140 reads; the scanner resumes
// across them, so throughput should hold.
func BenchmarkReader(b *testing.B) {
	small, large := benchStreamDocs()
	for _, bc := range []struct {
		name  string
		doc   *Value
		chunk int
	}{
		{"small", small, 0},
		{"large", large, 0},
		{"large_4KiB_reads", large, 4 << 10},
	} {
		enc := bc.doc.MarshalTo(nil)
		b.Run(bc.name, func(b *testing.B) {
			var src io.Reader = &repeatReader{data: enc}
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

// repeatReader serves data over and over without end.
type repeatReader struct {
	data []byte
	off  int
}

func (r *repeatReader) Read(b []byte) (int, error) {
	n := copy(b, r.data[r.off:])
	r.off = (r.off + n) % len(r.data)
	return n, nil
}
