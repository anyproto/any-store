//go:build !debugtrace

package btree

const debugTrace = false

func trace(format string, args ...any) {}
