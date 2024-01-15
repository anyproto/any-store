package key

import (
	"bytes"
	"strings"
)

func NewNS(ns string) *NS {
	if !strings.HasPrefix(ns, "/") {
		ns = "/" + ns
	}
	return &NS{
		prefix:    append([]byte(ns), eos),
		prefixLen: len(ns) + 1,
	}
}

type NS struct {
	prefix    Key
	prefixLen int
}

func (ns *NS) ReuseKey(m func(k Key) Key) {
	ns.prefix = m(ns.prefix[:ns.prefixLen])
}

func (ns *NS) GetKey() Key {
	return bytes.Clone(ns.prefix[:ns.prefixLen])
}

func (ns *NS) Bytes() []byte {
	return ns.prefix[:ns.prefixLen]
}

func (ns *NS) CopyTo(k Key) Key {
	return append(k, ns.prefix[:ns.prefixLen]...)
}

func (ns *NS) String() string {
	return string(ns.prefix[:ns.prefixLen-1])
}

func (ns *NS) Copy() *NS {
	return &NS{
		prefix:    bytes.Clone(ns.prefix),
		prefixLen: ns.prefixLen,
	}
}

func (ns *NS) Len() int {
	return ns.prefixLen
}
