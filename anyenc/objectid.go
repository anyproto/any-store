// ObjectID support. The type and generator are adapted from
// gopkg.in/mgo.v2/bson by Gustavo Niemeyer (BSON ObjectId).

package anyenc

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync/atomic"
	"time"
)

// objectIDLen is the fixed on-wire payload width of an ObjectID (no length
// header): the encoding is [TypeObjectID][12 raw bytes] = 13 bytes total.
const objectIDLen = 12

// ErrInvalidHex indicates that a hex string cannot be converted to an ObjectID.
var ErrInvalidHex = errors.New("the provided hex string is not a valid ObjectID")

// ObjectID is a 12-byte BSON-style identifier: a 4-byte big-endian Unix
// timestamp (seconds), 5 process-unique random bytes, and a 3-byte big-endian
// counter. It is a first-class anyenc value type (TypeObjectID) and encodes as a
// fixed 13 bytes (tag + 12 raw bytes). Because the layout is big-endian, the
// encoding is byte-for-byte (bytes.Compare) orderable and therefore time-sortable.
type ObjectID [objectIDLen]byte

// NilObjectID is the zero value for ObjectID.
var NilObjectID ObjectID

var objectIDCounter = readRandomUint32()
var processUnique = processUniqueBytes()

// NewObjectID generates a new ObjectID stamped with the current time.
func NewObjectID() ObjectID {
	return NewObjectIDFromTimestamp(time.Now())
}

// NewObjectIDFromTimestamp generates a new ObjectID whose timestamp part is the
// given time.
func NewObjectIDFromTimestamp(timestamp time.Time) ObjectID {
	var b [objectIDLen]byte
	binary.BigEndian.PutUint32(b[0:4], uint32(timestamp.Unix()))
	copy(b[4:9], processUnique[:])
	putObjectIDUint24(b[9:12], atomic.AddUint32(&objectIDCounter, 1))
	return b
}

// ObjectIDFromHex creates a new ObjectID from a 24-character hex string. It
// returns ErrInvalidHex if the string is not a valid ObjectID.
func ObjectIDFromHex(s string) (ObjectID, error) {
	if len(s) != 2*objectIDLen {
		return NilObjectID, ErrInvalidHex
	}
	var oid [objectIDLen]byte
	if _, err := hex.Decode(oid[:], []byte(s)); err != nil {
		return NilObjectID, err
	}
	return oid, nil
}

// Timestamp extracts the time part of the ObjectID (UTC, second precision).
func (id ObjectID) Timestamp() time.Time {
	unixSecs := binary.BigEndian.Uint32(id[0:4])
	return time.Unix(int64(unixSecs), 0).UTC()
}

// Hex returns the 24-character lowercase hex encoding of the ObjectID.
func (id ObjectID) Hex() string {
	var buf [2 * objectIDLen]byte
	hex.Encode(buf[:], id[:])
	return string(buf[:])
}

// String returns a debug representation of the ObjectID.
func (id ObjectID) String() string {
	return fmt.Sprintf("ObjectID(%q)", id.Hex())
}

// IsZero returns true if id is the zero ObjectID.
func (id ObjectID) IsZero() bool {
	return id == NilObjectID
}

func processUniqueBytes() [5]byte {
	var b [5]byte
	if _, err := io.ReadFull(rand.Reader, b[:]); err != nil {
		panic(fmt.Errorf("cannot initialize objectid with crypto/rand.Reader: %v", err))
	}
	return b
}

func readRandomUint32() uint32 {
	var b [4]byte
	if _, err := io.ReadFull(rand.Reader, b[:]); err != nil {
		panic(fmt.Errorf("cannot initialize objectid with crypto/rand.Reader: %v", err))
	}
	return (uint32(b[0]) << 0) | (uint32(b[1]) << 8) | (uint32(b[2]) << 16) | (uint32(b[3]) << 24)
}

func putObjectIDUint24(b []byte, v uint32) {
	b[0] = byte(v >> 16)
	b[1] = byte(v >> 8)
	b[2] = byte(v)
}

// ObjectID returns the id for a TypeObjectID value, or an error if v does not
// contain an objectID. The returned value is a copy (12-byte stack copy) and
// does not alias the parser buffer.
func (v *Value) ObjectID() (ObjectID, error) {
	if v.Type() != TypeObjectID {
		return NilObjectID, fmt.Errorf("value doesn't contain objectID; it contains %s", v.Type())
	}
	// The parser and Arena.NewObjectID both guarantee len(v.v) == objectIDLen,
	// so the slice-to-array conversion never panics on a TypeObjectID value.
	return ObjectID(v.v), nil
}

// GetObjectID returns the ObjectID at the given path, or NilObjectID if the
// value is absent or not a TypeObjectID. NilObjectID is indistinguishable from a
// stored zero id — use ObjectID() (error) or check Type() to disambiguate.
func (v *Value) GetObjectID(keys ...string) ObjectID {
	vv := v.Get(keys...)
	if vv.Type() != TypeObjectID {
		return NilObjectID
	}
	return ObjectID(vv.v)
}

// parseObjectID parses an objectID body (after the leading tag): objectIDLen
// fixed raw bytes with no length header. objectID is fixed-width, so the
// inverted (reverse index-key, c==nil) skip is the same constant advance as the
// normal path — the payload bytes are bitwise-inverted on disk for a descending
// field but are never decoded on the length-only path, so nothing needs
// un-inverting (unlike parseBinary's inverted length header).
func parseObjectID(b []byte, c *cache) (*Value, []byte, error) {
	if len(b) < objectIDLen {
		return nil, nil, fmt.Errorf("expected %d bytes to read objectID, but got %d", objectIDLen, len(b))
	}
	if c != nil {
		v := c.getValue()
		v.t = TypeObjectID
		v.v = slices.Grow(v.v[:0], objectIDLen)[:objectIDLen]
		copy(v.v, b[:objectIDLen])
		return v, b[objectIDLen:], nil
	}
	return nil, b[objectIDLen:], nil
}
