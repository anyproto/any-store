package anyenc

import (
	"bytes"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errBoom = errors.New("boom")

// sampleEncodings returns top-level encodings covering every type, escaped
// strings and keys, and a compressed object.
func sampleEncodings(t testing.TB) [][]byte {
	a := &Arena{}
	var encs [][]byte
	for _, js := range encDecData {
		encs = append(encs, MustParseJson(js).MarshalTo(nil))
	}
	doc := a.NewObject()
	doc.Set("", a.NewString("empty key"))
	doc.Set("nul\x00key", a.NewString("nul\x00value"))
	doc.Set("\x00lead", a.NewNull())
	doc.Set("\xfflead", a.NewTrue())
	doc.Set("bin", a.NewBinary([]byte{0, 1, 2, 0xff}))
	doc.Set("vec", a.NewVectorF32([]float32{1, 2, 3}))
	doc.Set("oid", a.NewObjectID(NewObjectID()))
	doc.Set("dt", a.NewDateTimeMillis(-1))
	doc.Set("text", a.NewString(strings.Repeat("compressible ", 40)))
	encs = append(encs, doc.MarshalTo(nil))
	compressed, _ := doc.MarshalCompressed(nil, nil)
	require.Equal(t, byte(TypeCompressedObjectS2), compressed[0])
	encs = append(encs, compressed)
	for _, v := range []*Value{
		a.NewString(""), a.NewString("\x00"), a.NewString("a\x00b"),
		a.NewBinary(nil), a.NewVectorF32([]float32{1}), a.NewObjectID(NewObjectID()), a.NewDateTimeMillis(0),
	} {
		encs = append(encs, v.MarshalTo(nil))
	}
	return encs
}

// readAll reads r to io.EOF and returns each value re-marshaled.
func readAll(t *testing.T, r *Reader) [][]byte {
	t.Helper()
	p := &Parser{}
	var got [][]byte
	for {
		v, err := r.Read(p)
		if err == io.EOF {
			return got
		}
		require.NoError(t, err)
		got = append(got, v.MarshalTo(nil))
	}
}

func decoded(encs [][]byte) [][]byte {
	out := make([][]byte, len(encs))
	for i, enc := range encs {
		out[i] = MustParse(enc).MarshalTo(nil)
	}
	return out
}

func TestReader_RoundTrip(t *testing.T) {
	a := &Arena{}
	big := a.NewObject()
	for i := range 2000 {
		big.Set(strings.Repeat("k", i%50)+string(rune('a'+i%26)), a.NewString(strings.Repeat("v\x00", i%40)))
	}
	encs := sampleEncodings(t)
	encs = append(encs,
		a.NewString(strings.Repeat("s\x00", 50_000)).MarshalTo(nil),
		a.NewBinary(bytes.Repeat([]byte{0, 0xff}, 100_000)).MarshalTo(nil),
		big.MarshalTo(nil),
	)
	encs = append(encs, encs...)
	stream := bytes.Join(encs, nil)
	want := decoded(encs)

	sources := []struct {
		name string
		wrap func(io.Reader) io.Reader
	}{
		{"whole", func(r io.Reader) io.Reader { return r }},
		{"half", iotest.HalfReader},
		{"data_err", iotest.DataErrReader},
		{"chunks_7", func(r io.Reader) io.Reader { return &chunkReader{r: r, n: 7} }},
	}
	for _, src := range sources {
		t.Run(src.name, func(t *testing.T) {
			got := readAll(t, NewReader(src.wrap(bytes.NewReader(stream))))
			assert.Equal(t, want, got)
		})
	}
}

func TestReader_ReadSizes(t *testing.T) {
	// Sweeping the read size puts a read boundary at every offset, on both the
	// in-place parse and the resumed scan.
	encs := sampleEncodings(t)
	stream := bytes.Join(encs, nil)
	want := decoded(encs)
	escaped := []byte{byte(TypeString), 'a', 0, 0xff, 'b', 0, byte(TypeString), 'c', 0}
	for n := 1; n <= 64; n++ {
		got := readAll(t, NewReader(&chunkReader{r: bytes.NewReader(stream), n: n}))
		require.Equal(t, want, got, "read size %d", n)

		r := NewReader(&chunkReader{r: bytes.NewReader(escaped), n: n})
		p := &Parser{}
		v, err := r.Read(p)
		require.NoError(t, err)
		require.Equal(t, "a\x00b", v.GetString(), "read size %d", n)
		v, err = r.Read(p)
		require.NoError(t, err)
		require.Equal(t, "c", v.GetString(), "read size %d", n)
		_, err = r.Read(p)
		require.Equal(t, io.EOF, err, "read size %d", n)
	}
}

func TestReader_Empty(t *testing.T) {
	r := NewReader(bytes.NewReader(nil))
	p := &Parser{}
	for range 2 {
		_, err := r.Read(p)
		assert.Equal(t, io.EOF, err)
	}
}

func TestReader_Truncated(t *testing.T) {
	encs := sampleEncodings(t)
	stream := append(bytes.Join(encs, nil), MustParseJson(`{"a":"b"}`).MarshalTo(nil)[:5]...)
	r := NewReader(bytes.NewReader(stream))
	p := &Parser{}
	for range encs {
		_, err := r.Read(p)
		require.NoError(t, err)
	}
	for range 2 {
		_, err := r.Read(p)
		assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	}
}

func TestReader_Corrupt(t *testing.T) {
	good := MustParseJson(`{"a":1}`).MarshalTo(nil)
	cases := []struct {
		name string
		bad  []byte
		msg  string
	}{
		// caught by the scanner
		{"unknown_type", []byte{0x42, byte(TypeNull)}, "unknown type"},
		{"inverted_field", []byte{byte(TypeObject), 'k', 0, byte(iTypeString), 'x', 0xff, 0}, "unknown type"},
		{"max_depth", bytes.Repeat([]byte{byte(TypeArray)}, maxParseDepth+1), "max parse depth"},
		// located by the scanner, rejected by the parse
		{"vector_len", []byte{byte(TypeVectorF32), 0, 0, 0, 3, 1, 2, 3}, "multiple of 4"},
		{"nested_compressed", []byte{byte(TypeObject), 'k', 0, byte(TypeCompressedObjectS2), 0, 0, 0, 0, 0}, "unknown type"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// errBoom proves the error comes without reading past the bad value.
			stream := append(slices.Clip(good), tc.bad...)
			r := NewReader(io.MultiReader(bytes.NewReader(stream), iotest.ErrReader(errBoom)))
			p := &Parser{}
			_, err := r.Read(p)
			require.NoError(t, err)
			for range 2 {
				_, err = r.Read(p)
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.msg)
			}
		})
	}
}

// TestValueScanner checks the scanner against every sample encoding: no
// proper prefix yields a value, whether scanned fresh or resumed byte by byte,
// and the complete encoding yields exactly its length.
func TestValueScanner(t *testing.T) {
	for _, enc := range sampleEncodings(t) {
		_, tail, err := parseValue(enc, nil, 0)
		require.NoError(t, err)
		require.Empty(t, tail)

		var resumed valueScanner
		for i := range len(enc) {
			var fresh valueScanner
			n, err := fresh.next(enc[:i], false)
			require.NoError(t, err)
			require.Zero(t, n, "fresh prefix %x of %x", enc[:i], enc)
			n, err = resumed.next(enc[:i], false)
			require.NoError(t, err)
			require.Zero(t, n, "resumed prefix %x of %x", enc[:i], enc)
		}
		// A following value settles a top-level string's terminator.
		n, err := resumed.next(append(slices.Clip(enc), byte(TypeNull)), false)
		require.NoError(t, err)
		require.Equal(t, len(enc), n, "%x", enc)

		var fresh valueScanner
		n, err = fresh.next(enc, true)
		require.NoError(t, err)
		require.Equal(t, len(enc), n, "%x", enc)
	}
}

func TestReader_SourceError(t *testing.T) {
	encs := sampleEncodings(t)
	for name, tail := range map[string][]byte{
		"between_values": nil,
		"inside_value":   encs[len(encs)-1][:3],
		"after_string":   {byte(TypeString), 'a', 0},
	} {
		t.Run(name, func(t *testing.T) {
			stream := append(bytes.Join(encs, nil), tail...)
			r := NewReader(io.MultiReader(bytes.NewReader(stream), iotest.ErrReader(errBoom)))
			p := &Parser{}
			for range encs {
				_, err := r.Read(p)
				require.NoError(t, err)
			}
			for range 2 {
				_, err := r.Read(p)
				assert.ErrorIs(t, err, errBoom)
			}
		})
	}
}

func TestReader_ValueLimit(t *testing.T) {
	// A string that never terminates must not be buffered past the limit.
	src := io.MultiReader(bytes.NewReader([]byte{byte(TypeString)}), infiniteReader{}, iotest.ErrReader(errBoom))
	r := NewReader(src)
	r.limit = 1 << 20
	_, err := r.Read(&Parser{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds")
	assert.LessOrEqual(t, cap(r.buf), 4<<20)
}

func TestReader_NoProgress(t *testing.T) {
	_, err := NewReader(emptyReader{}).Read(&Parser{})
	assert.ErrorIs(t, err, io.ErrNoProgress)
}

func TestWriter(t *testing.T) {
	var out bytes.Buffer
	w := NewWriter(&out)
	var want []byte
	for _, js := range encDecData {
		v := MustParseJson(js)
		require.NoError(t, w.Write(v))
		want = v.MarshalTo(want)
	}
	require.NoError(t, w.Write(nil))
	want = append(want, byte(TypeNull))
	assert.Zero(t, out.Len(), "small values stay buffered until Flush")
	require.NoError(t, w.Flush())
	assert.Equal(t, want, out.Bytes())

	a := &Arena{}
	big := a.NewBinary(make([]byte, streamBufSize))
	require.NoError(t, w.Write(big))
	assert.Equal(t, len(want)+len(big.MarshalTo(nil)), out.Len(), "a full buffer flushes on Write")
}

func TestWriter_ReaderRoundTrip(t *testing.T) {
	var out bytes.Buffer
	w := NewWriter(&out)
	encs := sampleEncodings(t)
	var want [][]byte
	for i := range 5000 {
		for _, enc := range encs[:i%5+1] {
			require.NoError(t, w.Write(MustParse(enc)))
			want = append(want, MustParse(enc).MarshalTo(nil))
		}
	}
	require.NoError(t, w.Flush())
	got := readAll(t, NewReader(&chunkReader{r: &out, n: 1000}))
	assert.Equal(t, want, got)
}

func TestWriter_Errors(t *testing.T) {
	v := MustParseJson(`{"a":1}`)
	for name, tc := range map[string]struct {
		dst  io.Writer
		want error
	}{
		"error": {errWriter{}, errBoom},
		"short": {shortWriter{}, io.ErrShortWrite},
	} {
		t.Run(name, func(t *testing.T) {
			w := NewWriter(tc.dst)
			require.NoError(t, w.Write(v))
			assert.ErrorIs(t, w.Flush(), tc.want)
			assert.ErrorIs(t, w.Write(v), tc.want)
			assert.ErrorIs(t, w.Flush(), tc.want)
		})
	}
}

type chunkReader struct {
	r io.Reader
	n int
}

func (c *chunkReader) Read(b []byte) (int, error) {
	return c.r.Read(b[:min(len(b), c.n)])
}

type infiniteReader struct{}

func (infiniteReader) Read(b []byte) (int, error) {
	for i := range b {
		b[i] = 'x'
	}
	return len(b), nil
}

type emptyReader struct{}

func (emptyReader) Read([]byte) (int, error) { return 0, nil }

type errWriter struct{}

func (errWriter) Write([]byte) (int, error) { return 0, errBoom }

type shortWriter struct{}

func (shortWriter) Write(b []byte) (int, error) { return len(b) - 1, nil }
