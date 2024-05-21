// Based on gopkg.in/mgo.v2/bson by Gustavo Niemeyer

package objectid

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

// ErrInvalidHex indicates that a hex string cannot be converted to an ObjectID.
var ErrInvalidHex = errors.New("the provided hex string is not a valid ObjectID")

// ObjectID is the BSON ObjectID type.
type ObjectID [12]byte

// NilObjectID is the zero value for ObjectID.
var NilObjectID ObjectID

var objectIDCounter = readRandomUint32()
var processUnique = processUniqueBytes()

// NewObjectID generates a new ObjectID.
func NewObjectID() ObjectID {
	return NewObjectIDFromTimestamp(time.Now())
}

// IsValidObjectID returns true if the provided hex string represents a valid ObjectID and false if not.
//
// Deprecated: Use ObjectIDFromHex and check the error instead.
func IsValidObjectID(s string) bool {
	_, err := ObjectIDFromHex(s)
	return err == nil
}

// NewObjectIDFromTimestamp generates a new ObjectID based on the given time.
func NewObjectIDFromTimestamp(timestamp time.Time) ObjectID {
	var b [12]byte

	binary.BigEndian.PutUint32(b[0:4], uint32(timestamp.Unix()))
	copy(b[4:9], processUnique[:])
	putUint24(b[9:12], atomic.AddUint32(&objectIDCounter, 1))

	return b
}

// Timestamp extracts the time part of the ObjectId.
func (id ObjectID) Timestamp() time.Time {
	unixSecs := binary.BigEndian.Uint32(id[0:4])
	return time.Unix(int64(unixSecs), 0).UTC()
}

// Hex returns the hex encoding of the ObjectID as a string.
func (id ObjectID) Hex() string {
	var buf [24]byte
	hex.Encode(buf[:], id[:])
	return string(buf[:])
}

func (id ObjectID) String() string {
	return fmt.Sprintf("ObjectID(%q)", id.Hex())
}

// IsZero returns true if id is the empty ObjectID.
func (id ObjectID) IsZero() bool {
	return id == NilObjectID
}

// ObjectIDFromHex creates a new ObjectID from a hex string. It returns an error if the hex string is not a
// valid ObjectID.
func ObjectIDFromHex(s string) (ObjectID, error) {
	if len(s) != 24 {
		return NilObjectID, ErrInvalidHex
	}

	var oid [12]byte
	_, err := hex.Decode(oid[:], []byte(s))
	if err != nil {
		return NilObjectID, err
	}

	return oid, nil
}

func processUniqueBytes() [5]byte {
	var b [5]byte
	_, err := io.ReadFull(rand.Reader, b[:])
	if err != nil {
		panic(fmt.Errorf("cannot initialize objectid package with crypto.rand.Reader: %v", err))
	}

	return b
}

func readRandomUint32() uint32 {
	var b [4]byte
	_, err := io.ReadFull(rand.Reader, b[:])
	if err != nil {
		panic(fmt.Errorf("cannot initialize objectid package with crypto.rand.Reader: %v", err))
	}

	return (uint32(b[0]) << 0) | (uint32(b[1]) << 8) | (uint32(b[2]) << 16) | (uint32(b[3]) << 24)
}

func putUint24(b []byte, v uint32) {
	b[0] = byte(v >> 16)
	b[1] = byte(v >> 8)
	b[2] = byte(v)
}
