package key

import (
	"bytes"
	"strings"
)

const Separator = byte(0)

func KeyFromString(s string) (k Key) {
	s = strings.Trim(s, "/")
	parts := strings.Split(s, "/")
	for _, part := range parts {
		k = k.AppendPart([]byte(part))
	}
	return
}

type Key []byte

func (k Key) AppendPart(part []byte) Key {
	res := append(k, Separator)
	return append(res, part...)
}

func (k Key) AppendString(part string) Key {
	res := append(k, Separator)
	return append(res, part...)
}

func (k Key) LastPart() Key {
	pos := bytes.LastIndexByte(k, Separator)
	if pos == -1 {
		return nil
	}
	return k[pos+1:]
}

func (k Key) Equal(k2 Key) bool {
	return bytes.Equal(k, k2)
}

func (k Key) String() string {
	return string(bytes.Replace(k, []byte{Separator}, []byte("/"), -1))
}

func (k Key) Copy() Key {
	return bytes.Clone(k)
}

func (k Key) CopyTo(k2 Key) []byte {
	return append(k2, k...)
}

func NewNS(prefix Key) NS {
	return NS{
		prefix:    prefix,
		prefixLen: len(prefix),
	}
}

type NS struct {
	prefix    Key
	prefixLen int
}

func (ns NS) Peek() Key {
	return ns.prefix[:ns.prefixLen]
}

func (ns NS) Copy() NS {
	return NS{
		prefixLen: ns.prefixLen,
		prefix:    bytes.Clone(ns.prefix[:ns.prefixLen]),
	}
}

func (ns NS) CopyTo(k Key) []byte {
	return ns.prefix.CopyTo(k)
}

func (ns NS) String() string {
	return ns.prefix[:ns.prefixLen].String()
}
