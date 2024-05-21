package objectid

import (
	"encoding/binary"
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestString(t *testing.T) {
	id := NewObjectID()
	require.Contains(t, id.String(), id.Hex())
}

func BenchmarkHex(b *testing.B) {
	id := NewObjectID()
	for i := 0; i < b.N; i++ {
		id.Hex()
	}
}

func BenchmarkObjectIDFromHex(b *testing.B) {
	id := NewObjectID().Hex()
	for i := 0; i < b.N; i++ {
		_, _ = ObjectIDFromHex(id)
	}
}

func TestFromHex_RoundTrip(t *testing.T) {
	before := NewObjectID()
	after, err := ObjectIDFromHex(before.Hex())
	require.NoError(t, err)

	require.Equal(t, before, after)
}

func TestFromHex_InvalidHex(t *testing.T) {
	_, err := ObjectIDFromHex("this is not a valid hex string!")
	require.Error(t, err)
}

func TestFromHex_WrongLength(t *testing.T) {
	_, err := ObjectIDFromHex("deadbeef")
	require.Equal(t, ErrInvalidHex, err)
}

func TestIsValidObjectID(t *testing.T) {
	testCases := []struct {
		givenID  string
		expected bool
	}{
		{
			givenID:  "5ef7fdd91c19e3222b41b839",
			expected: true,
		},
		{
			givenID:  "5ef7fdd91c19e3222b41b83",
			expected: false,
		},
	}

	for _, testcase := range testCases {
		got := IsValidObjectID(testcase.givenID)
		assert.Equal(t, testcase.expected, got, "expected hex string to be valid ObjectID: %v, got %v", testcase.expected, got)
	}
}

func TestTimeStamp(t *testing.T) {
	testCases := []struct {
		Hex      string
		Expected string
	}{
		{
			"000000001111111111111111",
			"1970-01-01 00:00:00 +0000 UTC",
		},
		{
			"7FFFFFFF1111111111111111",
			"2038-01-19 03:14:07 +0000 UTC",
		},
		{
			"800000001111111111111111",
			"2038-01-19 03:14:08 +0000 UTC",
		},
		{
			"FFFFFFFF1111111111111111",
			"2106-02-07 06:28:15 +0000 UTC",
		},
	}

	for _, testcase := range testCases {
		id, err := ObjectIDFromHex(testcase.Hex)
		require.NoError(t, err)
		secs := int64(binary.BigEndian.Uint32(id[0:4]))
		timestamp := time.Unix(secs, 0).UTC()
		require.Equal(t, testcase.Expected, timestamp.String())
	}
}

func TestCreateFromTime(t *testing.T) {
	testCases := []struct {
		time     string
		Expected string
	}{
		{
			"1970-01-01T00:00:00.000Z",
			"00000000",
		},
		{
			"2038-01-19T03:14:07.000Z",
			"7fffffff",
		},
		{
			"2038-01-19T03:14:08.000Z",
			"80000000",
		},
		{
			"2106-02-07T06:28:15.000Z",
			"ffffffff",
		},
	}

	layout := "2006-01-02T15:04:05.000Z"
	for _, testcase := range testCases {
		time, err := time.Parse(layout, testcase.time)
		require.NoError(t, err)

		id := NewObjectIDFromTimestamp(time)
		timeStr := hex.EncodeToString(id[0:4])

		require.Equal(t, testcase.Expected, timeStr)
	}
}

func TestGenerationTime(t *testing.T) {
	testCases := []struct {
		hex      string
		Expected string
	}{
		{
			"000000001111111111111111",
			"1970-01-01 00:00:00 +0000 UTC",
		},
		{
			"7FFFFFFF1111111111111111",
			"2038-01-19 03:14:07 +0000 UTC",
		},
		{
			"800000001111111111111111",
			"2038-01-19 03:14:08 +0000 UTC",
		},
		{
			"FFFFFFFF1111111111111111",
			"2106-02-07 06:28:15 +0000 UTC",
		},
	}

	for _, testcase := range testCases {
		id, err := ObjectIDFromHex(testcase.hex)
		require.NoError(t, err)

		genTime := id.Timestamp()
		require.Equal(t, testcase.Expected, genTime.String())
	}
}

func TestCounterOverflow(t *testing.T) {
	objectIDCounter = 0xFFFFFFFF
	NewObjectID()
	require.Equal(t, uint32(0), objectIDCounter)
}
