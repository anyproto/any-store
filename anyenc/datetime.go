package anyenc

import (
	"encoding/binary"
	"fmt"
	"slices"
	"time"
)

// dateTimeLen is the fixed on-wire payload width of a dateTime (no length
// header): the encoding is [TypeDateTime][8 bytes] = 9 bytes total.
//
// The payload is the signed Unix-millisecond timestamp as a big-endian int64
// with the sign bit flipped (offset-binary, same trick AppendFloat64 uses for
// positive floats), so the byte order equals chronological order for the full
// signed range — pre-1970 values included — and the encoding stays
// bytes.Compare-orderable in index keys.
const dateTimeLen = 8

func appendDateTimeMillis(dst []byte, ms int64) []byte {
	return binary.BigEndian.AppendUint64(dst, uint64(ms)^(1<<63))
}

func dateTimeMillisFromBytes(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b) ^ (1 << 63))
}

// DateTime returns the value as a UTC time.Time (millisecond precision), or an
// error if v does not contain a dateTime.
func (v *Value) DateTime() (time.Time, error) {
	ms, err := v.DateTimeMillis()
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(ms).UTC(), nil
}

// DateTimeMillis returns the Unix-millisecond timestamp of a TypeDateTime
// value, or an error if v does not contain a dateTime.
func (v *Value) DateTimeMillis() (int64, error) {
	if v.Type() != TypeDateTime {
		return 0, fmt.Errorf("value doesn't contain dateTime; it contains %s", v.Type())
	}
	// The parser and Arena.NewDateTimeMillis both guarantee len(v.v) == dateTimeLen.
	return dateTimeMillisFromBytes(v.v), nil
}

// GetDateTime returns the dateTime at the given path as a UTC time.Time, or the
// zero time.Time if the value is absent or not a TypeDateTime. The zero time is
// indistinguishable from a stored zero — use DateTime() (error) or check Type()
// to disambiguate.
func (v *Value) GetDateTime(keys ...string) time.Time {
	vv := v.Get(keys...)
	if vv.Type() != TypeDateTime {
		return time.Time{}
	}
	return time.UnixMilli(dateTimeMillisFromBytes(vv.v)).UTC()
}

// parseDateTime parses a dateTime body (after the leading tag): dateTimeLen
// fixed raw bytes with no length header. Like parseObjectID, the inverted
// (reverse index-key, c==nil) skip is the same constant advance as the normal
// path — the payload is never decoded on the length-only path.
func parseDateTime(b []byte, c *cache) (*Value, []byte, error) {
	if len(b) < dateTimeLen {
		return nil, nil, fmt.Errorf("expected %d bytes to read dateTime, but got %d", dateTimeLen, len(b))
	}
	if c != nil {
		v := c.getValue()
		v.t = TypeDateTime
		v.v = slices.Grow(v.v[:0], dateTimeLen)[:dateTimeLen]
		copy(v.v, b[:dateTimeLen])
		return v, b[dateTimeLen:], nil
	}
	return nil, b[dateTimeLen:], nil
}
