package anyenc

import (
	"bytes"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDateTime_ValueRoundTrip(t *testing.T) {
	a := &Arena{}
	ts := time.Date(2026, 8, 5, 17, 0, 0, 123e6, time.UTC)

	enc := a.NewDateTime(ts).MarshalTo(nil)
	// Fixed 9 bytes: tag + 8 offset-binary millis bytes, no length prefix.
	require.Len(t, enc, 1+dateTimeLen)
	assert.Equal(t, byte(TypeDateTime), enc[0])

	v, err := Parse(enc)
	require.NoError(t, err)
	assert.Equal(t, TypeDateTime, v.Type())

	got, err := v.DateTime()
	require.NoError(t, err)
	assert.Equal(t, ts, got)

	ms, err := v.DateTimeMillis()
	require.NoError(t, err)
	assert.Equal(t, ts.UnixMilli(), ms)
}

func TestDateTime_MillisTruncation(t *testing.T) {
	a := &Arena{}
	ts := time.Date(2026, 8, 5, 17, 0, 0, 123456789, time.UTC)
	got, err := a.NewDateTime(ts).DateTime()
	require.NoError(t, err)
	assert.Equal(t, ts.Truncate(time.Millisecond), got)
}

func TestDateTime_EncodedOrder(t *testing.T) {
	// Chronological order must equal byte order of the encoding, across the
	// signed range: pre-1970 negatives, zero, and extremes.
	millis := []int64{
		-1 << 62,
		time.Date(1799, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(),
		-1,
		0,
		1,
		time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC).UnixMilli(),
		1 << 62,
	}
	require.True(t, slices.IsSorted(millis))
	a := &Arena{}
	var prev []byte
	for _, ms := range millis {
		enc := a.NewDateTimeMillis(ms).MarshalTo(nil)
		if prev != nil {
			assert.Negative(t, bytes.Compare(prev, enc), "millis %d must sort after its predecessor", ms)
		}
		prev = enc
	}
}

func TestDateTime_InObject(t *testing.T) {
	a := &Arena{}
	ts := time.Date(2026, 8, 5, 17, 0, 0, 0, time.UTC)
	doc := a.NewObject()
	doc.Set("createdAt", a.NewDateTime(ts))
	doc.Set("n", a.NewNumberInt(7))

	v, err := Parse(doc.MarshalTo(nil))
	require.NoError(t, err)

	assert.Equal(t, ts, v.GetDateTime("createdAt"))
	assert.True(t, v.GetDateTime("n").IsZero(), "wrong type must return zero time")
	assert.True(t, v.GetDateTime("missing").IsZero())
}

func TestDateTime_Accessors_WrongType(t *testing.T) {
	a := &Arena{}
	v := a.NewString("not a date")
	_, err := v.DateTime()
	assert.Error(t, err)
	_, err = v.DateTimeMillis()
	assert.Error(t, err)
}

func TestDateTime_ExtJsonRoundTrip(t *testing.T) {
	a := &Arena{}
	ts := time.Date(2026, 8, 5, 17, 0, 0, 123e6, time.UTC)
	v := a.NewDateTime(ts)

	s := v.String()
	assert.Equal(t, `{"$date":"2026-08-05T17:00:00.123Z"}`, s)

	back, err := ParseJson(s)
	require.NoError(t, err)
	require.Equal(t, TypeDateTime, back.Type())
	got, err := back.DateTime()
	require.NoError(t, err)
	assert.Equal(t, ts, got)
}

func TestDateTime_ExtJsonRoundTripOutOfRFC3339Range(t *testing.T) {
	// Years outside 0..9999 render as millis numbers, not RFC3339 strings.
	a := &Arena{}
	for _, ms := range []int64{253402300800000, -100000000000000} {
		s := a.NewDateTimeMillis(ms).String()
		back, err := ParseJson(s)
		require.NoError(t, err, s)
		require.Equal(t, TypeDateTime, back.Type(), s)
		got, err := back.DateTimeMillis()
		require.NoError(t, err)
		assert.Equal(t, ms, got, s)
	}
}

func TestDateTime_ExtJsonDecodeForms(t *testing.T) {
	t.Run("millis number", func(t *testing.T) {
		v, err := ParseJson(`{"$date": 1754413200123}`)
		require.NoError(t, err)
		require.Equal(t, TypeDateTime, v.Type())
		ms, _ := v.DateTimeMillis()
		assert.Equal(t, int64(1754413200123), ms)
	})
	t.Run("rfc3339 with offset", func(t *testing.T) {
		v, err := ParseJson(`{"$date": "2026-08-05T19:00:00+02:00"}`)
		require.NoError(t, err)
		require.Equal(t, TypeDateTime, v.Type())
		got, _ := v.DateTime()
		assert.Equal(t, time.Date(2026, 8, 5, 17, 0, 0, 0, time.UTC), got)
	})
	t.Run("malformed stays object", func(t *testing.T) {
		v, err := ParseJson(`{"$date": "not a date"}`)
		require.NoError(t, err)
		assert.Equal(t, TypeObject, v.Type())
	})
	t.Run("bool payload stays object", func(t *testing.T) {
		v, err := ParseJson(`{"$date": true}`)
		require.NoError(t, err)
		assert.Equal(t, TypeObject, v.Type())
	})
}

func TestDateTime_GoType(t *testing.T) {
	a := &Arena{}
	ts := time.Date(2026, 8, 5, 17, 0, 0, 0, time.UTC)
	got, ok := a.NewDateTime(ts).GoType().(time.Time)
	require.True(t, ok)
	assert.Equal(t, ts, got)
}

func TestDateTime_AppendAnyValue(t *testing.T) {
	a := &Arena{}
	ts := time.Date(2026, 8, 5, 17, 0, 0, 123e6, time.UTC)
	assert.Equal(t,
		a.NewDateTime(ts).MarshalTo(nil),
		AppendAnyValue(nil, ts),
		"AppendAnyValue must match MarshalTo byte-for-byte")
}

func BenchmarkDateTime_Parse(b *testing.B) {
	a := &Arena{}
	enc := a.NewDateTimeMillis(1754413200123).MarshalTo(nil)
	p := &Parser{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		v, err := p.Parse(enc)
		if err != nil {
			b.Fatal(err)
		}
		if _, err = v.DateTimeMillis(); err != nil {
			b.Fatal(err)
		}
	}
}
